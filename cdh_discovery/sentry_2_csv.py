#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CDH 6.3.3 discovery. Python 3.6+ compatible.
Exports Sentry roles and privileges from the Sentry backend database to CSV.

CDH uses Sentry (not Ranger) for authorization. Sentry stores roles and
privileges in a database (MySQL or PostgreSQL). This script reads that DB
and produces:
  - sentry_objects.csv: roles and privilege targets (server, db, table, URI)
  - sentry_access.csv: role–principal (user/group) and role–privilege links
      in the same schema as dependency_access for consistency.

Requires: PyMySQL (--db-type mysql) or psycopg2-binary (--db-type postgres).
Sentry table names may vary by CDH/Sentry version; use --role-table etc. if needed.
"""

from __future__ import print_function

import argparse
import csv
import os
import sys

# Optional DB drivers
try:
    import psycopg2
    import psycopg2.extras
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False

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


def get_connection(args):
    """Return a DB connection (cursor-style) for the configured driver."""
    if args.db_type == "postgres":
        if not HAS_PSYCOPG2:
            print("ERROR: psycopg2 not installed. pip install psycopg2-binary", file=sys.stderr)
            sys.exit(1)
        conn = psycopg2.connect(
            host=args.db_host,
            port=args.db_port,
            dbname=args.db_name,
            user=args.db_user,
            password=args.db_password or None,
        )
        return conn
    elif args.db_type == "mysql":
        if not HAS_PYMYSQL:
            print("ERROR: PyMySQL not installed. pip install PyMySQL", file=sys.stderr)
            sys.exit(1)
        conn = pymysql.connect(
            host=args.db_host,
            port=args.db_port,
            user=args.db_user,
            password=args.db_password or None,
            database=args.db_name,
            cursorclass=pymysql.cursors.DictCursor,
        )
        return conn
    else:
        print("ERROR: unsupported --db-type", file=sys.stderr)
        sys.exit(1)


def fetch_roles(conn, args):
    """Fetch all roles. Returns list of dicts with at least role_id, role_name."""
    cur = conn.cursor()
    if args.db_type == "postgres":
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute(
            'SELECT * FROM "{role_table}"'.format(role_table=args.role_table)
            if args.db_type == "postgres"
            else "SELECT * FROM `{role_table}`".format(role_table=args.role_table)
        )
        rows = cur.fetchall()
    except Exception as e:
        print("WARN: could not read role table '{}': {}".format(args.role_table, e), file=sys.stderr)
        return []
    finally:
        cur.close()

    # Normalize to role_id, role_name (column names vary: ROLE_ID/ROLE_NAME or role_id/role_name)
    out = []
    for r in (dict(row) for row in rows):
        rid = r.get("ROLE_ID") or r.get("role_id") or r.get("SENTRY_ROLE_ID") or ""
        rname = r.get("ROLE_NAME") or r.get("role_name") or r.get("SENTRY_ROLE_NAME") or ""
        if not rname and rid:
            rname = str(rid)
        out.append({"role_id": rid, "role_name": rname, "raw": r})
    return out


def fetch_privileges(conn, args):
    """Fetch grant privileges if table exists. Returns list of dicts."""
    cur = conn.cursor()
    if args.db_type == "postgres":
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute(
            'SELECT * FROM "{priv_table}"'.format(priv_table=args.privilege_table)
            if args.db_type == "postgres"
            else "SELECT * FROM `{priv_table}`".format(priv_table=args.privilege_table)
        )
        rows = cur.fetchall()
    except Exception as e:
        print("WARN: could not read privilege table '{}': {}".format(args.privilege_table, e), file=sys.stderr)
        return []
    finally:
        cur.close()

    return [dict(row) for row in rows]


def fetch_role_user_group(conn, args):
    """Fetch role-user and role-group mappings if tables exist."""
    result = []  # list of (role_id, principal_type, principal_name)
    # Default Sentry table names (CDH / Apache Sentry)
    group_table = args.role_group_table or "SENTRY_ROLE_GROUP_MAP"
    user_table = args.role_user_table or "SENTRY_ROLE_USER_MAP"
    for table, principal in [(user_table, "user"), (group_table, "group")]:
        cur = conn.cursor()
        if args.db_type == "postgres":
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            q = 'SELECT * FROM "{}"'.format(table) if args.db_type == "postgres" else "SELECT * FROM `{}`".format(table)
            cur.execute(q)
            rows = cur.fetchall()
        except Exception as e:
            print("WARN: could not read {} table '{}': {}".format(principal, table, e), file=sys.stderr)
            continue
        finally:
            cur.close()

        for r in rows:
            d = dict(r) if not isinstance(r, dict) else r
            role_id = d.get("ROLE_ID") or d.get("role_id")
            name = d.get("USER_NAME") or d.get("user_name") or d.get("GROUP_NAME") or d.get("group_name") or ""
            if role_id is not None and name:
                result.append((str(role_id), principal, name))
    return result


def main():
    ap = argparse.ArgumentParser(
        description="CDH discovery: export Sentry roles and privileges from Sentry DB to CSV."
    )
    ap.add_argument("--db-type", choices=["mysql", "postgres"], default="mysql",
                    help="Sentry database type (default: mysql)")
    ap.add_argument("--db-host", default="localhost", help="Database host")
    ap.add_argument("--db-port", type=int, default=None,
                    help="Port (default: 3306 for MySQL, 5432 for Postgres)")
    ap.add_argument("--db-name", default="sentry", help="Database name (default: sentry)")
    ap.add_argument("--db-user", default="sentry", help="Database user")
    ap.add_argument("--db-password", default="", help="Database password")
    ap.add_argument("--role-table", default="SENTRY_ROLE",
                    help="Sentry role table name (default: SENTRY_ROLE)")
    ap.add_argument("--privilege-table", default="SENTRY_DB_PRIVILEGE",
                    help="Sentry privilege table (default: SENTRY_DB_PRIVILEGE; try SENTRY_GRANT_PRIVILEGE if missing)")
    ap.add_argument("--role-group-table", default="",
                    help="Role-group map table (default: auto from role table)")
    ap.add_argument("--role-user-table", default="",
                    help="Role-user map table (default: auto)")
    ap.add_argument("--out-objects", default="sentry_objects.csv",
                    help="Output CSV for roles/objects (default: sentry_objects.csv)")
    ap.add_argument("--out-access", default="sentry_access.csv",
                    help="Output CSV for role-principal/privilege (default: sentry_access.csv)")
    args = ap.parse_args()

    if args.db_port is None:
        args.db_port = 3306 if args.db_type == "mysql" else 5432

    conn = get_connection(args)
    try:
        roles = fetch_roles(conn, args)
        print("[cdh_sentry] Found {} roles".format(len(roles)), file=sys.stderr)

        role_by_id = {str(r["role_id"]): r["role_name"] for r in roles}

        # Objects: one row per role (and optionally per privilege target)
        out_dir = os.path.dirname(os.path.abspath(args.out_objects))
        if out_dir and not os.path.exists(out_dir):
            os.makedirs(out_dir)

        with open(args.out_objects, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=OBJECT_FIELDS)
            w.writeheader()
            for r in roles:
                w.writerow({
                    "service": "sentry",
                    "object_type": "role",
                    "object_id": r["role_name"] or str(r["role_id"]),
                    "owner": "",
                    "group": "",
                    "extra": "role_id={}".format(r["role_id"]),
                })

        # Privileges: optional second table for DB/table/URI objects
        privs = fetch_privileges(conn, args)
        if privs:
            with open(args.out_objects, "a", newline="") as f:
                w = csv.DictWriter(f, fieldnames=OBJECT_FIELDS)
                for p in privs:
                    obj_id = (p.get("DB_NAME") or p.get("db_name") or "") + "." + (p.get("TABLE_NAME") or p.get("table_name") or "")
                    if not obj_id.strip("."):
                        obj_id = p.get("URI") or p.get("uri") or p.get("SERVER_NAME") or p.get("server_name") or "privilege"
                    w.writerow({
                        "service": "sentry",
                        "object_type": "privilege_target",
                        "object_id": obj_id.strip(".") or "privilege",
                        "owner": "",
                        "group": "",
                        "extra": "scope={}".format(p.get("PRIVILEGE_SCOPE") or p.get("privilege_scope") or ""),
                    })

        # Access: role–principal (user/group) and optionally role–privilege
        role_principals = fetch_role_user_group(conn, args)
        with open(args.out_access, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=ACCESS_FIELDS)
            w.writeheader()
            for role_id, principal_type, principal_name in role_principals:
                role_name = role_by_id.get(role_id, role_id)
                w.writerow({
                    "window_start": "",
                    "service": "sentry",
                    "user": principal_name if principal_type == "user" else "",
                    "do_as": "",
                    "client_ip": "",
                    "app_name": "",
                    "op": "GRANT_ROLE",
                    "object_type": "role",
                    "object_id": role_name,
                    "cnt": 1,
                })
        print("[cdh_sentry] Wrote {} roles to {}, {} access rows to {}".format(
            len(roles), args.out_objects, len(role_principals), args.out_access), file=sys.stderr)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
