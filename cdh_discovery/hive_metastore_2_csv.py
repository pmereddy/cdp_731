#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CDH 6.3.3 discovery. Python 3.6+ compatible.
Bulk-export Hive/Impala table list from the Hive Metastore database (MySQL or Postgres).
Use this when you have 100k+ tables: one DB query replaces hundreds of thousands of
DESCRIBE FORMATTED calls, so the job finishes in minutes instead of days.

Output schema matches hive_tables_2_csv.py (dependency_objects): service, object_type,
object_id, owner, group, extra (with storage format in extra).

Requires: PyMySQL (--db-type mysql) or psycopg2-binary (--db-type postgres).
HMS DB credentials are usually in Hive/Impala config (e.g. hive-site.xml).
"""

from __future__ import print_function

import argparse
import csv
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

# Map HMS INPUT_FORMAT class name (lowercase) to storage label
INPUT_FORMAT_MAP = {
    "parquet": "parquet",
    "orc": "orc",
    "kudu": "kudu",
    "text": "textfile",
    "sequencefile": "sequencefile",
    "avro": "avro",
    "rcfile": "rcfile",
    "json": "json",
}


def infer_storage(input_format, serde_lib):
    """Infer storage from HMS INPUT_FORMAT and SERDE (Java class names)."""
    s = (input_format or "") + " " + (serde_lib or "")
    s = s.lower()
    if "kudu" in s:
        return "kudu"
    for key, val in INPUT_FORMAT_MAP.items():
        if key in s:
            return val
    return "unknown"


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


def run_mysql(conn, exclude_set, batch_size):
    """Query HMS MySQL: DBS + TBLS + SDS. Yield (db_name, tbl_name, owner, tbl_type, input_format, location, serde_lib)."""
    # Column names match Hive 2/3 metastore schema
    sql = """
        SELECT
            d.NAME AS db_name,
            t.TBL_NAME AS tbl_name,
            t.OWNER AS owner,
            t.TBL_TYPE AS tbl_type,
            s.INPUT_FORMAT AS input_format,
            s.LOCATION AS location,
            s.SERDE_ID AS serde_id
        FROM DBS d
        JOIN TBLS t ON t.DB_ID = d.DB_ID
        LEFT JOIN SDS s ON t.SD_ID = s.SD_ID
        ORDER BY d.NAME, t.TBL_NAME
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        while True:
            rows = cur.fetchmany(batch_size)
            if not rows:
                break
            for r in rows:
                db = (r.get("db_name") or "").strip()
                if db.lower() in exclude_set:
                    continue
                yield (
                    db,
                    (r.get("tbl_name") or "").strip(),
                    (r.get("owner") or "").strip(),
                    (r.get("tbl_type") or "").strip(),
                    r.get("input_format") or "",
                    r.get("location") or "",
                    r.get("serde_id"),
                )


def run_postgres(conn, exclude_set, batch_size):
    """Query HMS Postgres: same shape as MySQL."""
    sql = """
        SELECT
            d."NAME" AS db_name,
            t."TBL_NAME" AS tbl_name,
            t."OWNER" AS owner,
            t."TBL_TYPE" AS tbl_type,
            s."INPUT_FORMAT" AS input_format,
            s."LOCATION" AS location,
            s."SERDE_ID" AS serde_id
        FROM "DBS" d
        JOIN "TBLS" t ON t."DB_ID" = d."DB_ID"
        LEFT JOIN "SDS" s ON t."SD_ID" = s."SD_ID"
        ORDER BY d."NAME", t."TBL_NAME"
    """
    cur = conn.cursor()
    cur.execute(sql)
    while True:
        rows = cur.fetchmany(batch_size)
        if not rows:
            break
        for r in rows:
            db = (r.get("db_name") or "").strip()
            if db.lower() in exclude_set:
                continue
            yield (
                db,
                (r.get("tbl_name") or "").strip(),
                (r.get("owner") or "").strip(),
                (r.get("tbl_type") or "").strip(),
                r.get("input_format") or "",
                r.get("location") or "",
                r.get("serde_id"),
            )
    cur.close()


def main():
    ap = argparse.ArgumentParser(
        description="Bulk-export Hive table list from Metastore DB (fast for 100k+ tables)."
    )
    ap.add_argument("--db-type", choices=["mysql", "postgres"], default="mysql")
    ap.add_argument("--db-host", default="localhost")
    ap.add_argument("--db-port", type=int, default=None,
                    help="Default: 3306 (mysql), 5432 (postgres)")
    ap.add_argument("--db-name", default="metastore")
    ap.add_argument("--db-user", default="hive")
    ap.add_argument("--db-password", default="")
    ap.add_argument("--exclude-databases", default="sys,information_schema,_impala_builtins",
                    help="Comma-separated DB names to skip")
    ap.add_argument("--batch-size", type=int, default=10000)
    ap.add_argument("--out-objects", default="hive_tables.csv")
    args = ap.parse_args()

    if args.db_port is None:
        args.db_port = 3306 if args.db_type == "mysql" else 5432

    exclude_set = set(d.strip().lower() for d in args.exclude_databases.split(",") if d.strip())

    conn = get_connection(args)
    try:
        if args.db_type == "mysql":
            rows = run_mysql(conn, exclude_set, args.batch_size)
        else:
            rows = run_postgres(conn, exclude_set, args.batch_size)

        out_dir = os.path.dirname(os.path.abspath(args.out_objects))
        if out_dir and not os.path.exists(out_dir):
            os.makedirs(out_dir)

        count = 0
        with open(args.out_objects, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=OBJECT_FIELDS)
            w.writeheader()
            for db_name, tbl_name, owner, tbl_type, input_format, location, serde_id in rows:
                if not db_name or not tbl_name:
                    continue
                storage = infer_storage(input_format, None)
                extra_parts = []
                if tbl_type:
                    extra_parts.append("type={}".format(tbl_type))
                if storage:
                    extra_parts.append("storage={}".format(storage))
                if location:
                    extra_parts.append("location={}".format(location))
                w.writerow({
                    "service": "impala",
                    "object_type": "hive_table",
                    "object_id": "{}.{}".format(db_name, tbl_name),
                    "owner": owner,
                    "group": "",
                    "extra": "|".join(extra_parts),
                })
                count += 1
                if count % 50000 == 0:
                    print("[hive_metastore] Wrote {} rows...".format(count), file=sys.stderr)

        print("[hive_metastore] Wrote {} tables to {}".format(count, args.out_objects), file=sys.stderr)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
