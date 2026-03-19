#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CDH 6.3.3 discovery. Python 3.6+ compatible.
Bulk-export Cloudera Navigator audit events + metadata from MySQL databases.

Audit DB -- daily-partitioned tables:
  HIVE_AUDIT_EVENTS_YYYY_MM_DD   -- Hive/Impala queries (database, table, SQL)
  HDFS_AUDIT_EVENTS_YYYY_MM_DD   -- HDFS file operations (~20M/day, aggregated)
  HUE_AUDIT_EVENTS_YYYY_MM_DD    -- Hue web UI actions

Metadata DB (optional, --meta-db-name):
  NAV_HOURLYMETRICS              -- hourly activity counts per cluster
  NAV_SOURCEINFO                 -- cluster service inventory

Output:
  navigator_access.csv   -- audit events in dependency_access schema
  navigator_objects.csv  -- service inventory from NAV_SOURCEINFO (if --meta-db-name)
  navigator_metrics.csv  -- hourly activity metrics (if --meta-db-name)

Requires: PyMySQL (pip install PyMySQL)
"""

from __future__ import print_function

import argparse
import csv
import datetime as dt
import getpass
import os
import re
import sys
from collections import defaultdict

try:
    import pymysql
    HAS_PYMYSQL = True
except ImportError:
    HAS_PYMYSQL = False

OBJECT_FIELDS = ["service", "object_type", "object_id", "owner", "group", "extra"]
ACCESS_FIELDS = [
    "window_start", "service", "user", "do_as", "client_ip",
    "app_name", "op", "object_type", "object_id", "cnt",
]
METRICS_FIELDS = [
    "cluster_id", "date", "hour",
    "files_created", "files_modified", "files_accessed",
    "databases_created", "tables_created", "tables_loaded",
    "tables_deleted", "schema_changes", "partitions_created",
    "queries_executed", "denied_access",
]

LOG = "[nav_audit]"


def get_connection(host, port, db_name, user, password):
    if not HAS_PYMYSQL:
        print("ERROR: PyMySQL not installed. pip install PyMySQL", file=sys.stderr)
        sys.exit(1)
    return pymysql.connect(
        host=host, port=port,
        user=user, password=password or None,
        database=db_name,
        cursorclass=pymysql.cursors.SSDictCursor,
        connect_timeout=30, read_timeout=1800,
    )


def table_exists(conn, table):
    with conn.cursor() as cur:
        cur.execute("SHOW TABLES LIKE %s", (table,))
        return cur.fetchone() is not None


def date_range(since, until):
    """Yield dates from since to until (inclusive)."""
    d = since
    while d <= until:
        yield d
        d += dt.timedelta(days=1)


def format_ts(val):
    if val is None:
        return ""
    if hasattr(val, "strftime"):
        return val.strftime("%Y-%m-%dT%H:%M:%SZ")
    return str(val)


def s(val):
    if val is None:
        return ""
    return str(val).strip()


def truncate_hdfs_path(path, depth=4):
    """Truncate an HDFS path to N directory levels for aggregation."""
    if not path:
        return ""
    parts = path.strip().split("/")
    if len(parts) <= depth + 1:
        return path.strip()
    return "/".join(parts[:depth + 1])


# ------------------------------------------------------------------ #
#  HIVE audit extraction (full detail -- most valuable)              #
# ------------------------------------------------------------------ #

def extract_hive_day(conn, day_str, writer):
    """Extract one day of HIVE_AUDIT_EVENTS. Returns row count."""
    table = "HIVE_AUDIT_EVENTS_{}".format(day_str)
    if not table_exists(conn, table):
        return 0

    sql = """
        SELECT EVENT_TIME, SERVICE_NAME, USERNAME, IMPERSONATOR,
               IP_ADDR, OPERATION, DATABASE_NAME, TABLE_NAME,
               OBJECT_TYPE, RESOURCE_PATH, OPERATION_TEXT
        FROM `{}`
    """.format(table)

    count = 0
    with conn.cursor() as cur:
        cur.execute(sql)
        for r in cur:
            db_name = s(r.get("DATABASE_NAME"))
            tbl_name = s(r.get("TABLE_NAME"))
            op_text = s(r.get("OPERATION_TEXT"))

            if db_name and tbl_name:
                obj_id = "{}.{}".format(db_name, tbl_name)
            elif db_name:
                obj_id = db_name
            else:
                obj_id = s(r.get("RESOURCE_PATH")) or ""

            obj_type = s(r.get("OBJECT_TYPE")) or "TABLE"

            writer.writerow({
                "window_start": format_ts(r.get("EVENT_TIME")),
                "service": "hive",
                "user": s(r.get("USERNAME")),
                "do_as": s(r.get("IMPERSONATOR")),
                "client_ip": s(r.get("IP_ADDR")),
                "app_name": op_text[:200] if op_text else "",
                "op": s(r.get("OPERATION")).upper() or "QUERY",
                "object_type": obj_type.lower(),
                "object_id": obj_id,
                "cnt": 1,
            })
            count += 1
    return count


# ------------------------------------------------------------------ #
#  HDFS audit extraction (aggregated by user+op+path)                #
# ------------------------------------------------------------------ #

def extract_hdfs_day(conn, day_str, hdfs_agg, path_depth):
    """Extract one day of HDFS_AUDIT_EVENTS, aggregate into hdfs_agg dict."""
    table = "HDFS_AUDIT_EVENTS_{}".format(day_str)
    if not table_exists(conn, table):
        return 0

    sql = """
        SELECT USERNAME, OPERATION, SRC
        FROM `{}`
    """.format(table)

    count = 0
    with conn.cursor() as cur:
        cur.execute(sql)
        for r in cur:
            user = s(r.get("USERNAME"))
            op = s(r.get("OPERATION")).upper()
            src = truncate_hdfs_path(s(r.get("SRC")), path_depth)
            if not src:
                continue
            key = (user, op, src)
            hdfs_agg[key] += 1
            count += 1
    return count


def write_hdfs_aggregated(writer, hdfs_agg, since_str, until_str):
    """Write aggregated HDFS rows to CSV."""
    window = "{}_to_{}".format(since_str, until_str)
    count = 0
    for (user, op, src), cnt in sorted(hdfs_agg.items(), key=lambda x: -x[1]):
        writer.writerow({
            "window_start": window,
            "service": "hdfs",
            "user": user,
            "do_as": "",
            "client_ip": "",
            "app_name": "",
            "op": op,
            "object_type": "hdfs_path",
            "object_id": src,
            "cnt": cnt,
        })
        count += 1
    return count


# ------------------------------------------------------------------ #
#  HUE audit extraction                                              #
# ------------------------------------------------------------------ #

def extract_hue_day(conn, day_str, writer):
    """Extract one day of HUE_AUDIT_EVENTS."""
    table = "HUE_AUDIT_EVENTS_{}".format(day_str)
    if not table_exists(conn, table):
        return 0

    sql = """
        SELECT EVENT_TIME, SERVICE_NAME, USERNAME, IMPERSONATOR,
               IP_ADDR, OPERATION, SERVICE, OPERATION_TEXT, URL
        FROM `{}`
    """.format(table)

    count = 0
    with conn.cursor() as cur:
        cur.execute(sql)
        for r in cur:
            writer.writerow({
                "window_start": format_ts(r.get("EVENT_TIME")),
                "service": "hue",
                "user": s(r.get("USERNAME")),
                "do_as": s(r.get("IMPERSONATOR")),
                "client_ip": s(r.get("IP_ADDR")),
                "app_name": s(r.get("OPERATION_TEXT"))[:200],
                "op": s(r.get("OPERATION")).upper() or "ACCESS",
                "object_type": s(r.get("SERVICE")).lower() or "hue",
                "object_id": s(r.get("URL")) or "",
                "cnt": 1,
            })
            count += 1
    return count


# ------------------------------------------------------------------ #
#  Metadata DB: NAV_SOURCEINFO (cluster service inventory)           #
# ------------------------------------------------------------------ #

def extract_sourceinfo(conn):
    """Extract NAV_SOURCEINFO -> object rows."""
    sql = "SELECT * FROM `NAV_SOURCEINFO`"
    rows_out = []
    with conn.cursor() as cur:
        cur.execute(sql)
        for r in cur:
            source_type = s(r.get("SOURCETYPE"))
            source_name = s(r.get("NAME")) or s(r.get("ORIGINALNAME"))
            cluster = s(r.get("CLUSTERDISPLAYNAME")) or s(r.get("CLUSTERID"))

            rows_out.append({
                "service": "navigator",
                "object_type": "cluster_service",
                "object_id": source_name,
                "owner": "",
                "group": "",
                "extra": "type={}|cluster={}|identity={}".format(
                    source_type, cluster, s(r.get("SOURCEIDENTITY"))),
            })
    return rows_out


# ------------------------------------------------------------------ #
#  Metadata DB: NAV_HOURLYMETRICS (activity metrics)                 #
# ------------------------------------------------------------------ #

def extract_hourlymetrics(conn, since, until):
    """Extract NAV_HOURLYMETRICS for the given date range."""
    sql = """
        SELECT CLUSTERID, METRICSDATE, METRICSHOUR,
               FILECREATIONCOUNT, FILESMODIFIEDCOUNT, FILESACCESSEDCOUNT,
               DATABASECREATIONCOUNT, TABLECREATIONCOUNT, TABLESLOADEDCOUNT,
               TABLESDELETEDCOUNT, TBLSCHEMACHANGEDCOUNT, PARTITIONCREATIONCOUNT,
               QUERIESEXECUTEDCOUNT, DENIEDACCESSCOUNT
        FROM `NAV_HOURLYMETRICS`
        WHERE METRICSDATE >= %s AND METRICSDATE <= %s
        ORDER BY METRICSDATE, METRICSHOUR
    """
    rows_out = []
    with conn.cursor() as cur:
        cur.execute(sql, (since.strftime("%Y-%m-%d"), until.strftime("%Y-%m-%d")))
        for r in cur:
            rows_out.append({
                "cluster_id": s(r.get("CLUSTERID")),
                "date": s(r.get("METRICSDATE")),
                "hour": s(r.get("METRICSHOUR")),
                "files_created": r.get("FILECREATIONCOUNT") or 0,
                "files_modified": r.get("FILESMODIFIEDCOUNT") or 0,
                "files_accessed": r.get("FILESACCESSEDCOUNT") or 0,
                "databases_created": r.get("DATABASECREATIONCOUNT") or 0,
                "tables_created": r.get("TABLECREATIONCOUNT") or 0,
                "tables_loaded": r.get("TABLESLOADEDCOUNT") or 0,
                "tables_deleted": r.get("TABLESDELETEDCOUNT") or 0,
                "schema_changes": r.get("TBLSCHEMACHANGEDCOUNT") or 0,
                "partitions_created": r.get("PARTITIONCREATIONCOUNT") or 0,
                "queries_executed": r.get("QUERIESEXECUTEDCOUNT") or 0,
                "denied_access": r.get("DENIEDACCESSCOUNT") or 0,
            })
    return rows_out


# ------------------------------------------------------------------ #
#  Main                                                               #
# ------------------------------------------------------------------ #

def main():
    ap = argparse.ArgumentParser(
        description="Bulk-export Navigator audit events + metadata from MySQL databases."
    )
    ap.add_argument("--db-host", default="localhost")
    ap.add_argument("--db-port", type=int, default=3306)
    ap.add_argument("--db-name", required=True, help="Navigator audit database name")
    ap.add_argument("--meta-db-name", default="",
                    help="Navigator metadata database name (optional, for metrics + services)")
    ap.add_argument("--db-user", required=True)
    ap.add_argument("--db-password", default="",
                    help="Leave empty to be prompted securely")
    ap.add_argument("--since", required=True,
                    help="Start date YYYY-MM-DD (inclusive)")
    ap.add_argument("--until", default="",
                    help="End date YYYY-MM-DD (inclusive, default: today)")
    ap.add_argument("--services", default="hive,hdfs,hue",
                    help="Comma-separated audit services to extract (default: hive,hdfs,hue)")
    ap.add_argument("--hdfs-path-depth", type=int, default=4,
                    help="Truncate HDFS paths to N levels for aggregation (default: 4)")
    ap.add_argument("--out-access", default="navigator_access.csv")
    ap.add_argument("--out-objects", default="navigator_objects.csv")
    ap.add_argument("--out-metrics", default="navigator_metrics.csv")
    args = ap.parse_args()

    if not args.db_password:
        args.db_password = getpass.getpass("Password for {}@{}: ".format(
            args.db_user, args.db_host))

    since = dt.datetime.strptime(args.since, "%Y-%m-%d").date()
    if args.until:
        until = dt.datetime.strptime(args.until, "%Y-%m-%d").date()
    else:
        until = dt.date.today()

    audit_services = set(svc.strip().lower() for svc in args.services.split(","))
    print("{} Date range: {} to {}".format(LOG, since, until), file=sys.stderr)
    print("{} Audit services: {}".format(LOG, audit_services), file=sys.stderr)

    out_dir = os.path.dirname(os.path.abspath(args.out_access))
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir)

    # ---------------------------------------------------------- #
    #  1. Audit DB extraction                                     #
    # ---------------------------------------------------------- #
    conn = get_connection(args.db_host, args.db_port, args.db_name,
                          args.db_user, args.db_password)

    total_hive = 0
    total_hdfs_raw = 0
    total_hue = 0
    hdfs_agg = defaultdict(int)

    with open(args.out_access, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=ACCESS_FIELDS)
        writer.writeheader()

        for day in date_range(since, until):
            day_str = day.strftime("%Y_%m_%d")
            day_display = day.strftime("%Y-%m-%d")

            if "hive" in audit_services:
                n = extract_hive_day(conn, day_str, writer)
                total_hive += n
                if n > 0:
                    print("{} {} HIVE: {:,} events (total: {:,})".format(
                        LOG, day_display, n, total_hive), file=sys.stderr)

            if "hdfs" in audit_services:
                n = extract_hdfs_day(conn, day_str, hdfs_agg, args.hdfs_path_depth)
                total_hdfs_raw += n
                if n > 0:
                    print("{} {} HDFS: {:,} raw events (agg keys: {:,})".format(
                        LOG, day_display, n, len(hdfs_agg)), file=sys.stderr)

            if "hue" in audit_services:
                n = extract_hue_day(conn, day_str, writer)
                total_hue += n

        if hdfs_agg:
            hdfs_written = write_hdfs_aggregated(
                writer, hdfs_agg,
                since.strftime("%Y-%m-%d"), until.strftime("%Y-%m-%d"))
            print("{} HDFS aggregated: {:,} raw -> {:,} unique user+op+path rows".format(
                LOG, total_hdfs_raw, hdfs_written), file=sys.stderr)

    conn.close()

    total_access = total_hive + len(hdfs_agg) + total_hue
    print("{} Wrote {:,} access rows to {}".format(LOG, total_access, args.out_access), file=sys.stderr)
    print("{}   Hive:  {:,} detail rows".format(LOG, total_hive), file=sys.stderr)
    print("{}   HDFS:  {:,} aggregated rows (from {:,} raw)".format(
        LOG, len(hdfs_agg), total_hdfs_raw), file=sys.stderr)
    print("{}   Hue:   {:,} detail rows".format(LOG, total_hue), file=sys.stderr)

    # ---------------------------------------------------------- #
    #  2. Metadata DB extraction (optional)                       #
    # ---------------------------------------------------------- #
    if args.meta_db_name:
        print("{} Connecting to metadata DB: {}".format(LOG, args.meta_db_name), file=sys.stderr)
        meta_conn = get_connection(args.db_host, args.db_port, args.meta_db_name,
                                   args.db_user, args.db_password)
        try:
            # NAV_SOURCEINFO -> objects CSV
            source_rows = extract_sourceinfo(meta_conn)
            with open(args.out_objects, "w", newline="") as f:
                w = csv.DictWriter(f, fieldnames=OBJECT_FIELDS)
                w.writeheader()
                w.writerows(source_rows)
            print("{} Wrote {} service inventory rows to {}".format(
                LOG, len(source_rows), args.out_objects), file=sys.stderr)

            # NAV_HOURLYMETRICS -> metrics CSV
            metrics_rows = extract_hourlymetrics(meta_conn, since, until)
            with open(args.out_metrics, "w", newline="") as f:
                w = csv.DictWriter(f, fieldnames=METRICS_FIELDS)
                w.writeheader()
                w.writerows(metrics_rows)
            print("{} Wrote {} hourly metrics rows to {}".format(
                LOG, len(metrics_rows), args.out_metrics), file=sys.stderr)

        finally:
            meta_conn.close()

    print("{} Done.".format(LOG), file=sys.stderr)


if __name__ == "__main__":
    main()
