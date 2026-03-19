#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CDH 6.3.3 discovery. Python 3.6+ compatible.
Bulk-export Oozie workflows, coordinators, bundles, AND their actions
directly from the Oozie database (MySQL or Postgres). A handful of SQL
queries replaces thousands of paginated REST API calls, finishing in
seconds/minutes instead of hours.

Output (same schema as oozie_2_csv.py / oozie_2_csv.sh):
  oozie_objects.csv  - dependency_objects  (jobs + actions + HDFS app paths)
  oozie_access.csv   - dependency_access   (run history, optional)
  oozie_lineage.csv  - dependency_lineage  (coord->wf, bundle->coord, wf->action, optional)

Oozie DB credentials are in oozie-site.xml  (oozie.service.JPAService.jdbc.*).
Find them with:
  grep -A1 'oozie.service.JPAService.jdbc' /etc/oozie/conf/oozie-site.xml

Requires: PyMySQL (--db-type mysql) or psycopg2-binary (--db-type postgres).
"""

from __future__ import print_function

import argparse
import csv
import getpass
import os
import re
import sys

try:
    import pymysql
    HAS_PYMYSQL = True
except ImportError:
    HAS_PYMYSQL = False

try:
    import psycopg2
    import psycopg2.extras
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False


OBJECT_FIELDS = ["service", "object_type", "object_id", "owner", "group", "extra"]
ACCESS_FIELDS = [
    "window_start", "service", "user", "do_as", "client_ip",
    "app_name", "op", "object_type", "object_id", "cnt",
]
LINEAGE_FIELDS = [
    "from_object_type", "from_object_id",
    "to_object_type", "to_object_id", "relation",
]

# ------------------------------------------------------------------ #
# Oozie DB tables queried:
#   WF_JOBS       - workflow definitions / runs
#   COORD_JOBS    - coordinator definitions / runs
#   BUNDLE_JOBS   - bundle definitions / runs
#   WF_ACTIONS    - individual nodes inside a workflow (hive, sqoop, shell, ...)
#   COORD_ACTIONS - coordinator materialised instances -> child workflow id
# ------------------------------------------------------------------ #

JOB_TABLES = [
    ("WF_JOBS", "oozie_workflow"),
    ("COORD_JOBS", "oozie_coordinator"),
    ("BUNDLE_JOBS", "oozie_bundle"),
]


def normalize_app_path(path):
    """Strip hdfs://nameservice:port prefix, trailing slashes."""
    if not path:
        return ""
    path = path.strip().rstrip("/")
    if path.startswith("hdfs://"):
        m = re.match(r"hdfs://[^/]+(?::\d+)?(/.+)", path)
        if m:
            return m.group(1).rstrip("/") or "/"
    return path or "/"


def get_connection(args):
    if args.db_type == "mysql":
        if not HAS_PYMYSQL:
            print("ERROR: PyMySQL not installed. pip install PyMySQL", file=sys.stderr)
            sys.exit(1)
        return pymysql.connect(
            host=args.db_host,
            port=args.db_port,
            user=args.db_user,
            password=args.db_password or None,
            database=args.db_name,
            cursorclass=pymysql.cursors.DictCursor,
        )
    elif args.db_type == "postgres":
        if not HAS_PSYCOPG2:
            print("ERROR: psycopg2 not installed. pip install psycopg2-binary", file=sys.stderr)
            sys.exit(1)
        return psycopg2.connect(
            host=args.db_host,
            port=args.db_port,
            dbname=args.db_name,
            user=args.db_user,
            password=args.db_password or None,
        )
    else:
        print("ERROR: --db-type must be mysql or postgres", file=sys.stderr)
        sys.exit(1)


# ------------------------------------------------------------------ #
#  Generic fetch helpers                                              #
# ------------------------------------------------------------------ #

def _fetch_mysql(conn, sql, batch_size):
    with conn.cursor() as cur:
        cur.execute(sql)
        while True:
            rows = cur.fetchmany(batch_size)
            if not rows:
                break
            for r in rows:
                yield r


def _fetch_postgres(conn, sql, batch_size):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(sql)
    while True:
        rows = cur.fetchmany(batch_size)
        if not rows:
            break
        for r in rows:
            yield dict(r)
    cur.close()


def fetch_rows(conn, db_type, sql_mysql, sql_postgres, batch_size):
    if db_type == "mysql":
        return _fetch_mysql(conn, sql_mysql, batch_size)
    return _fetch_postgres(conn, sql_postgres, batch_size)


# ------------------------------------------------------------------ #
#  Job-level queries                                                  #
# ------------------------------------------------------------------ #

JOB_COLS = "id, app_name, app_path, user_name, group_name, status_str, created_time, start_time, end_time"

def job_sql_mysql(table):
    return "SELECT {cols} FROM {t} ORDER BY created_time DESC".format(cols=JOB_COLS, t=table)

def job_sql_postgres(table):
    return 'SELECT {cols} FROM "{t}" ORDER BY created_time DESC'.format(cols=JOB_COLS, t=table)


# ------------------------------------------------------------------ #
#  WF_ACTIONS query (action nodes inside workflows)                   #
# ------------------------------------------------------------------ #

WF_ACTIONS_MYSQL = """
    SELECT a.id            AS action_id,
           a.wf_id         AS wf_id,
           a.name          AS action_name,
           a.type          AS action_type,
           a.status_str    AS status,
           a.external_id   AS external_id,
           a.tracker_uri   AS tracker_uri,
           a.created_time  AS created_time,
           j.user_name     AS user_name
      FROM WF_ACTIONS a
      JOIN WF_JOBS    j ON j.id = a.wf_id
     WHERE a.name NOT IN (':start:', ':end:', ':kill:', ':join:', ':fork:')
     ORDER BY a.wf_id, a.created_time
"""

WF_ACTIONS_POSTGRES = """
    SELECT a.id            AS action_id,
           a.wf_id         AS wf_id,
           a.name          AS action_name,
           a.type          AS action_type,
           a.status_str    AS status,
           a.external_id   AS external_id,
           a.tracker_uri   AS tracker_uri,
           a.created_time  AS created_time,
           j.user_name     AS user_name
      FROM "WF_ACTIONS" a
      JOIN "WF_JOBS"    j ON j.id = a.wf_id
     WHERE a.name NOT IN (':start:', ':end:', ':kill:', ':join:', ':fork:')
     ORDER BY a.wf_id, a.created_time
"""

# ------------------------------------------------------------------ #
#  COORD_ACTIONS query (coordinator -> child workflow mapping)        #
# ------------------------------------------------------------------ #

COORD_ACTIONS_MYSQL = """
    SELECT id            AS action_id,
           job_id        AS coord_id,
           action_number AS action_number,
           external_id   AS child_wf_id,
           status        AS status,
           created_time  AS created_time
      FROM COORD_ACTIONS
     ORDER BY job_id, action_number
"""

COORD_ACTIONS_POSTGRES = """
    SELECT id            AS action_id,
           job_id        AS coord_id,
           action_number AS action_number,
           external_id   AS child_wf_id,
           status        AS status,
           created_time  AS created_time
      FROM "COORD_ACTIONS"
     ORDER BY job_id, action_number
"""


def format_ts(val):
    if val is None:
        return ""
    if hasattr(val, "strftime"):
        return val.strftime("%Y-%m-%dT%H:%M:%SZ")
    return str(val)


def s(val):
    """Safe strip: handles None and non-str."""
    if val is None:
        return ""
    return str(val).strip()


# ------------------------------------------------------------------ #
#  Main                                                               #
# ------------------------------------------------------------------ #

def main():
    ap = argparse.ArgumentParser(
        description="Bulk-export Oozie jobs from Oozie DB (1000x faster than REST API)."
    )
    ap.add_argument("--db-type", choices=["mysql", "postgres"], default="mysql")
    ap.add_argument("--db-host", default="localhost")
    ap.add_argument("--db-port", type=int, default=None,
                    help="Default: 3306 (mysql), 5432 (postgres)")
    ap.add_argument("--db-name", default="oozie",
                    help="Oozie database name (default: oozie)")
    ap.add_argument("--db-user", default="oozie")
    ap.add_argument("--db-password", default="",
                    help="Leave empty to be prompted securely")
    ap.add_argument("--batch-size", type=int, default=10000)
    ap.add_argument("--include-actions", action="store_true",
                    help="Also export WF_ACTIONS rows (hive, sqoop, shell, ...)")
    ap.add_argument("--out-objects", default="oozie_objects.csv")
    ap.add_argument("--out-access", default="",
                    help="Write job run history to this CSV (optional)")
    ap.add_argument("--out-lineage", default="",
                    help="Write coord->wf and wf->action lineage (optional)")
    args = ap.parse_args()

    if args.db_port is None:
        args.db_port = 3306 if args.db_type == "mysql" else 5432

    if args.db_user and not args.db_password:
        args.db_password = getpass.getpass("Password for {}@{}: ".format(args.db_user, args.db_host))

    conn = get_connection(args)
    try:
        out_dir = os.path.dirname(os.path.abspath(args.out_objects))
        if out_dir and not os.path.exists(out_dir):
            os.makedirs(out_dir)

        object_rows = []
        access_rows = []
        lineage_rows = []
        seen_app_paths = set()
        total_jobs = 0

        # ---------------------------------------------------------- #
        #  1. Job tables: WF_JOBS, COORD_JOBS, BUNDLE_JOBS           #
        # ---------------------------------------------------------- #
        for table, object_type in JOB_TABLES:
            count = 0
            try:
                rows = fetch_rows(
                    conn, args.db_type,
                    job_sql_mysql(table), job_sql_postgres(table),
                    args.batch_size,
                )
                for r in rows:
                    job_id   = s(r.get("id"))
                    app_name = s(r.get("app_name"))
                    app_path = s(r.get("app_path"))
                    user     = s(r.get("user_name"))
                    group    = s(r.get("group_name"))
                    status   = s(r.get("status_str"))
                    created  = format_ts(r.get("created_time"))

                    obj_id = app_path.rstrip("/") if app_path else job_id

                    extra_parts = []
                    if job_id:
                        extra_parts.append("job_id={}".format(job_id))
                    if app_name:
                        extra_parts.append("app_name={}".format(app_name))
                    if status:
                        extra_parts.append("status={}".format(status))
                    if created:
                        extra_parts.append("created={}".format(created))

                    object_rows.append({
                        "service": "oozie",
                        "object_type": object_type,
                        "object_id": obj_id,
                        "owner": user,
                        "group": group,
                        "extra": "|".join(extra_parts),
                    })

                    if app_path:
                        hdfs_path = normalize_app_path(app_path)
                        if hdfs_path and hdfs_path != "/" and hdfs_path not in seen_app_paths:
                            seen_app_paths.add(hdfs_path)
                            object_rows.append({
                                "service": "hdfs",
                                "object_type": "hdfs_path",
                                "object_id": hdfs_path,
                                "owner": user,
                                "group": "",
                                "extra": "referenced_by_oozie|{}".format(job_id),
                            })

                    if args.out_access and created:
                        access_rows.append({
                            "window_start": created,
                            "service": "oozie",
                            "user": user,
                            "do_as": "",
                            "client_ip": "",
                            "app_name": app_name or obj_id,
                            "op": "SUBMIT",
                            "object_type": object_type,
                            "object_id": obj_id,
                            "cnt": 1,
                        })

                    count += 1
                    if count % 50000 == 0:
                        print("[oozie_db] {}: {} rows...".format(table, count), file=sys.stderr)

            except Exception as e:
                print("[oozie_db] WARN: could not read {}: {}".format(table, e), file=sys.stderr)
                continue

            total_jobs += count
            print("[oozie_db] {}: {} jobs".format(table, count), file=sys.stderr)

        # ---------------------------------------------------------- #
        #  2. COORD_ACTIONS  (coordinator -> child workflow lineage)  #
        # ---------------------------------------------------------- #
        coord_action_count = 0
        try:
            rows = fetch_rows(
                conn, args.db_type,
                COORD_ACTIONS_MYSQL, COORD_ACTIONS_POSTGRES,
                args.batch_size,
            )
            for r in rows:
                coord_id    = s(r.get("coord_id"))
                child_wf_id = s(r.get("child_wf_id"))
                if args.out_lineage and coord_id and child_wf_id:
                    lineage_rows.append({
                        "from_object_type": "oozie_coordinator",
                        "from_object_id": coord_id,
                        "to_object_type": "oozie_workflow",
                        "to_object_id": child_wf_id,
                        "relation": "spawns",
                    })
                coord_action_count += 1
        except Exception as e:
            print("[oozie_db] WARN: could not read COORD_ACTIONS: {}".format(e), file=sys.stderr)

        if coord_action_count:
            print("[oozie_db] COORD_ACTIONS: {} rows".format(coord_action_count), file=sys.stderr)

        # ---------------------------------------------------------- #
        #  3. WF_ACTIONS  (workflow node details: hive, sqoop, ...)   #
        # ---------------------------------------------------------- #
        wf_action_count = 0
        if args.include_actions:
            try:
                rows = fetch_rows(
                    conn, args.db_type,
                    WF_ACTIONS_MYSQL, WF_ACTIONS_POSTGRES,
                    args.batch_size,
                )
                for r in rows:
                    action_id   = s(r.get("action_id"))
                    wf_id       = s(r.get("wf_id"))
                    action_name = s(r.get("action_name"))
                    action_type = s(r.get("action_type"))
                    status      = s(r.get("status"))
                    external_id = s(r.get("external_id"))
                    tracker_uri = s(r.get("tracker_uri"))
                    created     = format_ts(r.get("created_time"))
                    user        = s(r.get("user_name"))

                    extra_parts = ["action_id={}".format(action_id)]
                    if action_type:
                        extra_parts.append("action_type={}".format(action_type))
                    if status:
                        extra_parts.append("status={}".format(status))
                    if external_id:
                        extra_parts.append("external_id={}".format(external_id))
                    if tracker_uri:
                        extra_parts.append("tracker={}".format(tracker_uri))
                    if created:
                        extra_parts.append("created={}".format(created))

                    object_rows.append({
                        "service": "oozie",
                        "object_type": "oozie_wf_action",
                        "object_id": "{}@{}".format(wf_id, action_name) if wf_id else action_id,
                        "owner": user,
                        "group": "",
                        "extra": "|".join(extra_parts),
                    })

                    if args.out_lineage and wf_id:
                        lineage_rows.append({
                            "from_object_type": "oozie_workflow",
                            "from_object_id": wf_id,
                            "to_object_type": "oozie_wf_action",
                            "to_object_id": "{}@{}".format(wf_id, action_name),
                            "relation": "contains",
                        })

                    if args.out_access and created:
                        access_rows.append({
                            "window_start": created,
                            "service": "oozie",
                            "user": user,
                            "do_as": "",
                            "client_ip": "",
                            "app_name": action_name,
                            "op": action_type.upper() if action_type else "ACTION",
                            "object_type": "oozie_wf_action",
                            "object_id": "{}@{}".format(wf_id, action_name),
                            "cnt": 1,
                        })

                    wf_action_count += 1
                    if wf_action_count % 100000 == 0:
                        print("[oozie_db] WF_ACTIONS: {} rows...".format(wf_action_count), file=sys.stderr)

            except Exception as e:
                print("[oozie_db] WARN: could not read WF_ACTIONS: {}".format(e), file=sys.stderr)

            if wf_action_count:
                print("[oozie_db] WF_ACTIONS: {} action nodes".format(wf_action_count), file=sys.stderr)

        # ---------------------------------------------------------- #
        #  Write CSVs                                                 #
        # ---------------------------------------------------------- #
        with open(args.out_objects, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=OBJECT_FIELDS)
            w.writeheader()
            w.writerows(object_rows)

        print("[oozie_db] Wrote {} object rows ({} jobs, {} actions, {} app paths) to {}".format(
            len(object_rows), total_jobs, wf_action_count, len(seen_app_paths),
            args.out_objects), file=sys.stderr)

        if args.out_access and access_rows:
            with open(args.out_access, "w", newline="") as f:
                w = csv.DictWriter(f, fieldnames=ACCESS_FIELDS)
                w.writeheader()
                w.writerows(access_rows)
            print("[oozie_db] Wrote {} access rows to {}".format(
                len(access_rows), args.out_access), file=sys.stderr)

        if args.out_lineage and lineage_rows:
            with open(args.out_lineage, "w", newline="") as f:
                w = csv.DictWriter(f, fieldnames=LINEAGE_FIELDS)
                w.writeheader()
                w.writerows(lineage_rows)
            print("[oozie_db] Wrote {} lineage rows to {}".format(
                len(lineage_rows), args.out_lineage), file=sys.stderr)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
