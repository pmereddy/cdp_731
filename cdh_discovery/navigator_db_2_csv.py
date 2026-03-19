#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CDH 6.3.3 discovery. Python 3.6+ compatible.
Bulk-export Cloudera Navigator data directly from its MySQL/Postgres databases.

Navigator uses TWO databases (may be on the same or different hosts):
  1. Audit DB   -- every Hive/Impala/HDFS/Spark access event (who did what, when)
  2. Metadata DB -- entities, lineage relations, policies, tags

This script auto-discovers tables in both databases and extracts:
  - navigator_objects.csv   -- entities (tables, files, databases, queries)
  - navigator_access.csv    -- audit events (access patterns)
  - navigator_lineage.csv   -- lineage relations (data flow edges)

Find credentials in Cloudera Manager:
  CM -> Cloudera Management Service -> Configuration -> search "Navigator"
  Or on the Navigator host:
    grep -r 'nav.*db\|database' /var/run/cloudera-scm-agent/process/*/navigator.properties

Requires: PyMySQL (--db-type mysql) or psycopg2-binary (--db-type postgres).

Usage:
  # Single DB (audit + metadata in same database)
  python3 navigator_db_2_csv.py --db-host nav-db-host --db-name nav \\
    --db-user nav --out-objects navigator_objects.csv

  # Separate DBs for audit and metadata
  python3 navigator_db_2_csv.py --db-host nav-db-host \\
    --audit-db-name nav_audit --meta-db-name nav_metadata \\
    --db-user nav --out-objects navigator_objects.csv
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

LOG_PREFIX = "[nav_db]"


def get_connection(db_type, host, port, db_name, user, password):
    if db_type == "mysql":
        if not HAS_PYMYSQL:
            print("ERROR: PyMySQL not installed. pip install PyMySQL", file=sys.stderr)
            sys.exit(1)
        return pymysql.connect(
            host=host, port=port, user=user,
            password=password or None, database=db_name,
            cursorclass=pymysql.cursors.DictCursor,
            connect_timeout=30, read_timeout=600,
        )
    elif db_type == "postgres":
        if not HAS_PSYCOPG2:
            print("ERROR: psycopg2 not installed. pip install psycopg2-binary", file=sys.stderr)
            sys.exit(1)
        return psycopg2.connect(
            host=host, port=port, dbname=db_name,
            user=user, password=password or None,
        )
    else:
        print("ERROR: --db-type must be mysql or postgres", file=sys.stderr)
        sys.exit(1)


def list_tables(conn, db_type):
    """Return list of table names in the current database."""
    if db_type == "mysql":
        with conn.cursor() as cur:
            cur.execute("SHOW TABLES")
            return [list(r.values())[0] for r in cur.fetchall()]
    else:
        cur = conn.cursor()
        cur.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public' ORDER BY table_name
        """)
        tables = [r[0] for r in cur.fetchall()]
        cur.close()
        return tables


def describe_table(conn, db_type, table):
    """Return list of column names for a table."""
    if db_type == "mysql":
        with conn.cursor() as cur:
            cur.execute("DESCRIBE `{}`".format(table))
            return [r.get("Field") or list(r.values())[0] for r in cur.fetchall()]
    else:
        cur = conn.cursor()
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            ORDER BY ordinal_position
        """, (table,))
        cols = [r[0] for r in cur.fetchall()]
        cur.close()
        return cols


def count_rows(conn, db_type, table):
    """Return approximate row count."""
    q = 'SELECT COUNT(*) AS cnt FROM "{}"'.format(table) if db_type == "postgres" \
        else "SELECT COUNT(*) AS cnt FROM `{}`".format(table)
    try:
        if db_type == "mysql":
            with conn.cursor() as cur:
                cur.execute(q)
                return cur.fetchone().get("cnt", 0)
        else:
            cur = conn.cursor()
            cur.execute(q)
            cnt = cur.fetchone()[0]
            cur.close()
            return cnt
    except Exception:
        return -1


def fetch_rows(conn, db_type, sql, batch_size):
    """Generic row fetcher. Yields dicts."""
    if db_type == "mysql":
        with conn.cursor() as cur:
            cur.execute(sql)
            while True:
                rows = cur.fetchmany(batch_size)
                if not rows:
                    break
                for r in rows:
                    yield r
    else:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql)
        while True:
            rows = cur.fetchmany(batch_size)
            if not rows:
                break
            for r in rows:
                yield dict(r)
        cur.close()


def s(val):
    if val is None:
        return ""
    return str(val).strip()


def format_ts(val):
    if val is None:
        return ""
    if hasattr(val, "strftime"):
        return val.strftime("%Y-%m-%dT%H:%M:%SZ")
    v = str(val).strip()
    if v.isdigit() and len(v) >= 13:
        from datetime import datetime
        return datetime.utcfromtimestamp(int(v) / 1000.0).strftime("%Y-%m-%dT%H:%M:%SZ")
    return v


# ------------------------------------------------------------------ #
#  Column matching helpers                                            #
# ------------------------------------------------------------------ #

def find_col(columns, *candidates):
    """Find the first matching column name (case-insensitive)."""
    lower_cols = {c.lower(): c for c in columns}
    for cand in candidates:
        if cand.lower() in lower_cols:
            return lower_cols[cand.lower()]
    return None


def find_audit_table(tables):
    """Identify the audit events table from table list."""
    candidates = [
        "AUDIT_COMMAND_LOG", "audit_command_log",
        "NAVIGATOR_AUDIT_EVENTS", "navigator_audit_events",
        "AUDIT_EVENTS", "audit_events",
        "NAV_AUDIT_EVENTS", "nav_audit_events",
        "HDFS_AUDIT", "hdfs_audit",
    ]
    for c in candidates:
        if c in tables:
            return c
    for t in tables:
        tl = t.lower()
        if "audit" in tl and ("event" in tl or "command" in tl or "log" in tl):
            return t
    for t in tables:
        if "audit" in t.lower():
            return t
    return None


def find_entity_tables(tables):
    """Identify entity/metadata tables."""
    result = {}
    for t in tables:
        tl = t.lower()
        if "entit" in tl or "element" in tl or tl.startswith("nav_element"):
            result["entities"] = t
        if "relation" in tl and "lineage" not in tl:
            result["relations"] = t
        if "lineage" in tl:
            result["lineage"] = t
        if "policy" in tl or "policies" in tl:
            result["policies"] = t
        if "tag" in tl and "stag" not in tl:
            result["tags"] = t
    return result


# ------------------------------------------------------------------ #
#  Extraction functions                                               #
# ------------------------------------------------------------------ #

def extract_audit(conn, db_type, table, columns, batch_size, limit):
    """Extract audit events. Returns list of access row dicts."""
    cols = columns
    c_ts = find_col(cols, "EVENT_TIME", "event_time", "TIMESTAMP", "timestamp",
                    "TIME", "time", "AUDIT_TIME", "audit_time", "CREATED_AT")
    c_user = find_col(cols, "USERNAME", "username", "USER_NAME", "user_name",
                      "USER", "user", "PRINCIPAL")
    c_ip = find_col(cols, "IP_ADDR", "ip_addr", "IP_ADDRESS", "ip_address",
                    "IPADDRESS", "ipAddress", "CLIENT_IP", "client_ip")
    c_op = find_col(cols, "OPERATION", "operation", "COMMAND", "command",
                    "ACTION", "action", "OP", "op", "OPERATION_TEXT")
    c_service = find_col(cols, "SERVICE_NAME", "service_name", "SERVICE", "service",
                         "COMPONENT", "component")
    c_resource = find_col(cols, "RESOURCE_PATH", "resource_path", "RESOURCE", "resource",
                          "TABLE_NAME", "table_name", "OBJECT_ID", "URL", "url",
                          "SRC", "src", "DST", "dst")
    c_db = find_col(cols, "DATABASE_NAME", "database_name", "DB_NAME", "db_name",
                    "DATABASE", "database")
    c_tbl = find_col(cols, "TABLE_NAME", "table_name")
    c_allowed = find_col(cols, "ALLOWED", "allowed", "RESULT", "result")
    c_obj_type = find_col(cols, "OBJECT_TYPE", "object_type", "RESOURCE_TYPE",
                          "resource_type", "ENTITY_TYPE")
    c_app = find_col(cols, "APPLICATION", "application", "APP_NAME",
                     "OPERATION_TEXT", "operation_text")

    select_cols = []
    for c in [c_ts, c_user, c_ip, c_op, c_service, c_resource, c_db, c_tbl,
              c_allowed, c_obj_type, c_app]:
        if c and c not in select_cols:
            select_cols.append(c)

    if not select_cols:
        print("{} WARN: no recognizable columns in audit table {}".format(LOG_PREFIX, table), file=sys.stderr)
        return []

    order_col = c_ts or select_cols[0]
    if db_type == "postgres":
        quoted = ', '.join('"{}"'.format(c) for c in select_cols)
        sql = 'SELECT {cols} FROM "{tbl}" ORDER BY "{order}" DESC'.format(
            cols=quoted, tbl=table, order=order_col)
    else:
        quoted = ", ".join("`{}`".format(c) for c in select_cols)
        sql = "SELECT {cols} FROM `{tbl}` ORDER BY `{order}` DESC".format(
            cols=quoted, tbl=table, order=order_col)

    if limit:
        sql += " LIMIT {}".format(limit)

    rows_out = []
    count = 0
    for r in fetch_rows(conn, db_type, sql, batch_size):
        ts = format_ts(r.get(c_ts)) if c_ts else ""
        user = s(r.get(c_user)) if c_user else ""
        ip = s(r.get(c_ip)) if c_ip else ""
        op = s(r.get(c_op)) if c_op else ""
        svc = s(r.get(c_service)) if c_service else "navigator"
        resource = s(r.get(c_resource)) if c_resource else ""
        db_name = s(r.get(c_db)) if c_db else ""
        tbl_name = s(r.get(c_tbl)) if c_tbl and c_tbl != c_resource else ""
        obj_type = s(r.get(c_obj_type)) if c_obj_type else ""
        app = s(r.get(c_app)) if c_app else ""

        if db_name and tbl_name and not resource:
            resource = "{}.{}".format(db_name, tbl_name)
        elif db_name and not resource:
            resource = db_name

        rows_out.append({
            "window_start": ts,
            "service": svc.lower() if svc else "navigator",
            "user": user,
            "do_as": "",
            "client_ip": ip,
            "app_name": app,
            "op": op.upper() if op else "ACCESS",
            "object_type": obj_type.lower() if obj_type else "entity",
            "object_id": resource,
            "cnt": 1,
        })
        count += 1
        if count % 500000 == 0:
            print("{} audit: {} rows...".format(LOG_PREFIX, count), file=sys.stderr)

    return rows_out


def extract_entities(conn, db_type, table, columns, batch_size, limit):
    """Extract entity/metadata rows. Returns list of object row dicts."""
    cols = columns
    c_id = find_col(cols, "IDENTITY", "identity", "ID", "id", "ENTITY_ID",
                    "entity_id", "SOURCE_ID", "source_id")
    c_type = find_col(cols, "ENTITY_TYPE", "entity_type", "TYPE", "type",
                      "SOURCE_TYPE", "source_type")
    c_name = find_col(cols, "ORIGINAL_NAME", "original_name", "DISPLAY_NAME",
                      "display_name", "NAME", "name")
    c_path = find_col(cols, "FILE_SYSTEM_PATH", "file_system_path", "PATH", "path",
                      "SOURCE_URL", "source_url")
    c_owner = find_col(cols, "OWNER_NAME", "owner_name", "OWNER", "owner")
    c_desc = find_col(cols, "DESCRIPTION", "description", "ORIGINAL_DESCRIPTION")
    c_deleted = find_col(cols, "DELETED", "deleted")
    c_db = find_col(cols, "PARENT_NAME", "parent_name", "DATABASE_NAME",
                    "database_name", "DB_NAME")
    c_svc = find_col(cols, "SOURCE_NAME", "source_name", "SERVICE_NAME",
                     "service_name", "CLUSTER_NAME")

    select_cols = []
    for c in [c_id, c_type, c_name, c_path, c_owner, c_desc, c_deleted, c_db, c_svc]:
        if c and c not in select_cols:
            select_cols.append(c)

    if not select_cols:
        print("{} WARN: no recognizable columns in entity table {}".format(LOG_PREFIX, table), file=sys.stderr)
        return []

    if db_type == "postgres":
        quoted = ', '.join('"{}"'.format(c) for c in select_cols)
        sql = 'SELECT {cols} FROM "{tbl}"'.format(cols=quoted, tbl=table)
    else:
        quoted = ", ".join("`{}`".format(c) for c in select_cols)
        sql = "SELECT {cols} FROM `{tbl}`".format(cols=quoted, tbl=table)

    if c_deleted:
        if db_type == "postgres":
            sql += ' WHERE "{}" = false OR "{}" IS NULL'.format(c_deleted, c_deleted)
        else:
            sql += " WHERE `{}` = 0 OR `{}` = 'false' OR `{}` IS NULL".format(
                c_deleted, c_deleted, c_deleted)

    if limit:
        sql += " LIMIT {}".format(limit)

    type_map = {
        "TABLE": ("hive", "hive_table"),
        "VIEW": ("hive", "hive_view"),
        "DATABASE": ("hive", "hive_database"),
        "FILE": ("hdfs", "hdfs_path"),
        "DIRECTORY": ("hdfs", "hdfs_path"),
        "FSDIRECTORY": ("hdfs", "hdfs_path"),
        "FSFILE": ("hdfs", "hdfs_path"),
        "HDFS_FILE": ("hdfs", "hdfs_path"),
        "HDFS_DIRECTORY": ("hdfs", "hdfs_path"),
        "COLUMN": ("hive", "hive_column"),
        "FIELD": ("hive", "hive_column"),
        "HIVE_TABLE": ("hive", "hive_table"),
        "HIVE_DATABASE": ("hive", "hive_database"),
        "HIVE_COLUMN": ("hive", "hive_column"),
        "IMPALA_QUERY": ("impala", "impala_query"),
        "HIVE_QUERY": ("hive", "hive_query"),
        "MAPREDUCE": ("yarn", "mr_job"),
        "MR_JOB": ("yarn", "mr_job"),
        "SPARK": ("spark", "spark_app"),
        "PIG": ("pig", "pig_script"),
        "OOZIE": ("oozie", "oozie_workflow"),
        "SQOOP": ("sqoop", "sqoop_job"),
        "KUDU_TABLE": ("kudu", "kudu_table"),
    }

    rows_out = []
    count = 0
    for r in fetch_rows(conn, db_type, sql, batch_size):
        etype = s(r.get(c_type)).upper() if c_type else "UNKNOWN"
        name = s(r.get(c_name)) if c_name else ""
        path = s(r.get(c_path)) if c_path else ""
        eid = s(r.get(c_id)) if c_id else ""
        owner = s(r.get(c_owner)) if c_owner else ""
        db_name = s(r.get(c_db)) if c_db else ""
        svc = s(r.get(c_svc)) if c_svc else ""

        obj_id = path or name or eid
        if not obj_id:
            continue

        if db_name and name and etype in ("TABLE", "VIEW", "HIVE_TABLE"):
            obj_id = "{}.{}".format(db_name, name)

        service, obj_type = type_map.get(etype, ("navigator", etype.lower()))

        extra_parts = []
        if eid:
            extra_parts.append("nav_id={}".format(eid))
        if etype:
            extra_parts.append("nav_type={}".format(etype))
        if svc:
            extra_parts.append("source={}".format(svc))

        rows_out.append({
            "service": service,
            "object_type": obj_type,
            "object_id": obj_id,
            "owner": owner,
            "group": "",
            "extra": "|".join(extra_parts),
        })
        count += 1
        if count % 100000 == 0:
            print("{} entities: {} rows...".format(LOG_PREFIX, count), file=sys.stderr)

    return rows_out


def extract_relations(conn, db_type, table, columns, batch_size, limit):
    """Extract lineage/relation rows. Returns list of lineage row dicts."""
    cols = columns
    c_from = find_col(cols, "SOURCE_ID", "source_id", "FROM_ENTITY_ID",
                      "from_entity_id", "ENDPOINT1", "endpoint1",
                      "FROM_ID", "from_id", "LEFT_ENTITY_ID")
    c_to = find_col(cols, "TARGET_ID", "target_id", "TO_ENTITY_ID",
                    "to_entity_id", "ENDPOINT2", "endpoint2",
                    "TO_ID", "to_id", "RIGHT_ENTITY_ID")
    c_type = find_col(cols, "TYPE", "type", "RELATION_TYPE", "relation_type",
                      "REL_TYPE", "rel_type")
    c_from_type = find_col(cols, "SOURCE_TYPE", "source_type",
                           "FROM_ENTITY_TYPE", "from_entity_type")
    c_to_type = find_col(cols, "TARGET_TYPE", "target_type",
                         "TO_ENTITY_TYPE", "to_entity_type")

    select_cols = []
    for c in [c_from, c_to, c_type, c_from_type, c_to_type]:
        if c and c not in select_cols:
            select_cols.append(c)

    if not c_from or not c_to:
        print("{} WARN: cannot identify from/to columns in relation table {}".format(
            LOG_PREFIX, table), file=sys.stderr)
        return []

    if db_type == "postgres":
        quoted = ', '.join('"{}"'.format(c) for c in select_cols)
        sql = 'SELECT {cols} FROM "{tbl}"'.format(cols=quoted, tbl=table)
    else:
        quoted = ", ".join("`{}`".format(c) for c in select_cols)
        sql = "SELECT {cols} FROM `{tbl}`".format(cols=quoted, tbl=table)

    if limit:
        sql += " LIMIT {}".format(limit)

    rows_out = []
    count = 0
    for r in fetch_rows(conn, db_type, sql, batch_size):
        from_id = s(r.get(c_from))
        to_id = s(r.get(c_to))
        if not from_id or not to_id:
            continue
        rel_type = s(r.get(c_type)) if c_type else "DATA_FLOW"
        from_type = s(r.get(c_from_type)).lower() if c_from_type else "entity"
        to_type = s(r.get(c_to_type)).lower() if c_to_type else "entity"

        rows_out.append({
            "from_object_type": from_type,
            "from_object_id": from_id,
            "to_object_type": to_type,
            "to_object_id": to_id,
            "relation": rel_type,
        })
        count += 1
        if count % 500000 == 0:
            print("{} relations: {} rows...".format(LOG_PREFIX, count), file=sys.stderr)

    return rows_out


# ------------------------------------------------------------------ #
#  Main                                                               #
# ------------------------------------------------------------------ #

def process_database(conn, db_type, db_name, args, object_rows, access_rows, lineage_rows):
    """Process a single Navigator database -- discover tables and extract data."""
    print("{} --- Database: {} ---".format(LOG_PREFIX, db_name), file=sys.stderr)

    tables = list_tables(conn, db_type)
    print("{} Tables found: {}".format(LOG_PREFIX, tables), file=sys.stderr)

    # Discover and describe each table
    table_info = {}
    for t in tables:
        try:
            cols = describe_table(conn, db_type, t)
            cnt = count_rows(conn, db_type, t)
            table_info[t] = {"columns": cols, "count": cnt}
            print("{}   {:40s} {:>10,} rows  cols={}".format(
                LOG_PREFIX, t, cnt, cols[:8]), file=sys.stderr)
        except Exception as e:
            print("{}   {:40s} ERROR: {}".format(LOG_PREFIX, t, e), file=sys.stderr)

    # Find audit table
    audit_table = find_audit_table(tables)
    if audit_table and audit_table in table_info:
        info = table_info[audit_table]
        print("{} Extracting audit from: {} ({:,} rows)".format(
            LOG_PREFIX, audit_table, info["count"]), file=sys.stderr)
        rows = extract_audit(conn, db_type, audit_table, info["columns"],
                             args.batch_size, args.audit_limit)
        access_rows.extend(rows)
        print("{} Extracted {} audit rows".format(LOG_PREFIX, len(rows)), file=sys.stderr)

    # Find entity tables
    entity_map = find_entity_tables(tables)
    if "entities" in entity_map:
        t = entity_map["entities"]
        info = table_info.get(t, {})
        if info:
            print("{} Extracting entities from: {} ({:,} rows)".format(
                LOG_PREFIX, t, info["count"]), file=sys.stderr)
            rows = extract_entities(conn, db_type, t, info["columns"],
                                    args.batch_size, args.entity_limit)
            object_rows.extend(rows)
            print("{} Extracted {} entity rows".format(LOG_PREFIX, len(rows)), file=sys.stderr)

    # Find relations/lineage tables
    for key in ("relations", "lineage"):
        if key in entity_map:
            t = entity_map[key]
            info = table_info.get(t, {})
            if info:
                print("{} Extracting {} from: {} ({:,} rows)".format(
                    LOG_PREFIX, key, t, info["count"]), file=sys.stderr)
                rows = extract_relations(conn, db_type, t, info["columns"],
                                         args.batch_size, args.lineage_limit)
                lineage_rows.extend(rows)
                print("{} Extracted {} {} rows".format(LOG_PREFIX, len(rows), key), file=sys.stderr)


def main():
    ap = argparse.ArgumentParser(
        description="Bulk-export Cloudera Navigator data from its MySQL/Postgres databases."
    )
    ap.add_argument("--db-type", choices=["mysql", "postgres"], default="mysql")
    ap.add_argument("--db-host", default="localhost")
    ap.add_argument("--db-port", type=int, default=None,
                    help="Default: 3306 (mysql), 5432 (postgres)")
    ap.add_argument("--db-user", default="nav")
    ap.add_argument("--db-password", default="",
                    help="Leave empty to be prompted securely")
    ap.add_argument("--db-name", default="",
                    help="Single database name (if audit+metadata are in one DB)")
    ap.add_argument("--audit-db-name", default="",
                    help="Audit database name (if separate from metadata)")
    ap.add_argument("--meta-db-name", default="",
                    help="Metadata database name (if separate from audit)")
    ap.add_argument("--batch-size", type=int, default=10000)
    ap.add_argument("--audit-limit", type=int, default=0,
                    help="Max audit rows to extract (0 = all)")
    ap.add_argument("--entity-limit", type=int, default=0,
                    help="Max entity rows to extract (0 = all)")
    ap.add_argument("--lineage-limit", type=int, default=0,
                    help="Max lineage rows to extract (0 = all)")
    ap.add_argument("--discover-only", action="store_true",
                    help="Only discover tables and columns, don't extract data")
    ap.add_argument("--out-objects", default="navigator_objects.csv")
    ap.add_argument("--out-access", default="navigator_access.csv")
    ap.add_argument("--out-lineage", default="navigator_lineage.csv")
    args = ap.parse_args()

    if args.db_port is None:
        args.db_port = 3306 if args.db_type == "mysql" else 5432

    if args.db_user and not args.db_password:
        args.db_password = getpass.getpass("Password for {}@{}: ".format(args.db_user, args.db_host))

    # Determine which database(s) to scan
    db_names = []
    if args.db_name:
        db_names.append(args.db_name)
    if args.audit_db_name and args.audit_db_name not in db_names:
        db_names.append(args.audit_db_name)
    if args.meta_db_name and args.meta_db_name not in db_names:
        db_names.append(args.meta_db_name)

    if not db_names:
        # Try common Navigator DB names
        db_names = ["nav", "navigator", "nav_audit", "nav_metadata",
                    "cloudera_nav", "scm"]
        print("{} No --db-name specified. Will try: {}".format(LOG_PREFIX, db_names), file=sys.stderr)

    object_rows = []
    access_rows = []
    lineage_rows = []

    for db_name in db_names:
        try:
            conn = get_connection(args.db_type, args.db_host, args.db_port,
                                  db_name, args.db_user, args.db_password)
        except Exception as e:
            print("{} Could not connect to '{}': {}".format(LOG_PREFIX, db_name, e), file=sys.stderr)
            continue

        try:
            if args.discover_only:
                tables = list_tables(conn, args.db_type)
                print("{} === {} === ({} tables)".format(LOG_PREFIX, db_name, len(tables)), file=sys.stderr)
                for t in tables:
                    try:
                        cols = describe_table(conn, args.db_type, t)
                        cnt = count_rows(conn, args.db_type, t)
                        print("{}   {:40s} {:>10,} rows  {}".format(
                            LOG_PREFIX, t, cnt, cols), file=sys.stderr)
                    except Exception as e:
                        print("{}   {:40s} ERROR: {}".format(LOG_PREFIX, t, e), file=sys.stderr)
            else:
                process_database(conn, args.db_type, db_name, args,
                                 object_rows, access_rows, lineage_rows)
        finally:
            conn.close()

    if args.discover_only:
        print("{} Discovery complete. Re-run without --discover-only to extract data.".format(LOG_PREFIX), file=sys.stderr)
        return

    # Write CSVs
    out_dir = os.path.dirname(os.path.abspath(args.out_objects))
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir)

    with open(args.out_objects, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=OBJECT_FIELDS)
        w.writeheader()
        w.writerows(object_rows)
    print("{} Wrote {} entity rows to {}".format(LOG_PREFIX, len(object_rows), args.out_objects), file=sys.stderr)

    with open(args.out_access, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=ACCESS_FIELDS)
        w.writeheader()
        w.writerows(access_rows)
    print("{} Wrote {} audit/access rows to {}".format(LOG_PREFIX, len(access_rows), args.out_access), file=sys.stderr)

    with open(args.out_lineage, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=LINEAGE_FIELDS)
        w.writeheader()
        w.writerows(lineage_rows)
    print("{} Wrote {} lineage rows to {}".format(LOG_PREFIX, len(lineage_rows), args.out_lineage), file=sys.stderr)


if __name__ == "__main__":
    main()
