#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CDH 6.3.3 discovery. Python 3.6+ compatible.
Authentication modes (--auth-mode):
  cookie   - Pass a browser session cookie string (default, most reliable)
  login    - Authenticate via form login (j_spring_security_check) to get a cookie
  basic    - HTTP Basic auth (may not work if Navigator uses form-based auth)

Produces:
  - navigator_objects.csv  -- entities (HDFS paths, Hive tables, etc.)
  - navigator_access.csv   -- audit events
  - navigator_lineage.csv  -- relations/lineage edges

Endpoints used:
  GET /api/v3/entities?query=...&limit=&offset=
  GET /api/v3/audits?startTime=&endTime=&limit=&offset=
  GET /api/v3/relations (or lineage) for relation edges
"""

from __future__ import print_function

import argparse
import csv
import getpass
import os
import sys
import time
from urllib.parse import urlencode, urljoin

try:
    import requests
except ImportError:
    requests = None


OBJECT_FIELDS = ["service", "object_type", "object_id", "owner", "group", "extra"]
ACCESS_FIELDS = [
    "window_start", "service", "user", "do_as", "client_ip",
    "app_name", "op", "object_type", "object_id", "cnt",
]
LINEAGE_FIELDS = [
    "from_object_type", "from_object_id",
    "to_object_type", "to_object_id",
    "relation",
]

# Navigator entity type -> (service, object_type) for dependency model
NAV_TYPE_TO_SERVICE = {
    "FILE": ("hdfs", "hdfs_path"),
    "DIRECTORY": ("hdfs", "hdfs_path"),
    "TABLE": ("impala", "hive_table"),
    "VIEW": ("impala", "hive_table"),
    "DATABASE": ("impala", "hive_database"),
    "COLUMN": ("impala", "hive_column"),
    "PARTITION": ("impala", "hive_partition"),
    "KUDU_TABLE": ("kudu", "kudu_table"),
    "TOPIC": ("kafka", "topic"),
}


def get_entity_id(entity):
    """Prefer identity, then path, then name for object_id."""
    identity = entity.get("identity") or entity.get("id")
    if identity:
        return str(identity).strip()
    path = entity.get("path") or (entity.get("attributes") or {}).get("path")
    if path:
        return path.strip() if isinstance(path, str) else str(path)
    name = entity.get("name") or (entity.get("attributes") or {}).get("name")
    if name:
        return name.strip() if isinstance(name, str) else str(name)
    return ""


def get_entity_type(entity):
    """Return Navigator type string (e.g. FILE, TABLE)."""
    t = entity.get("type") or (entity.get("attributes") or {}).get("type")
    if t:
        return (t.strip() if isinstance(t, str) else str(t)).upper()
    return "UNKNOWN"


def get_owner(entity):
    """Extract owner from entity attributes if present."""
    attrs = entity.get("attributes") or {}
    return attrs.get("owner") or attrs.get("ownerName") or entity.get("ownerName") or ""


def entity_to_object_row(entity):
    """Map Navigator entity to dependency_objects row."""
    obj_id = get_entity_id(entity)
    if not obj_id:
        return None
    nav_type = get_entity_type(entity)
    service, object_type = NAV_TYPE_TO_SERVICE.get(
        nav_type, ("navigator", nav_type.lower() if nav_type else "entity")
    )
    return {
        "service": service,
        "object_type": object_type,
        "object_id": obj_id,
        "owner": get_owner(entity),
        "group": "",
        "extra": "navigator_type={}".format(nav_type),
    }


def fetch_entities(session, base_url, entity_types, limit, offset_step, sleep_ms):
    """Paginate through /api/v3/entities for given types. Yields entity dicts."""
    seen = set()
    for nav_type in entity_types:
        query = "type:{}".format(nav_type)
        offset = 0
        while True:
            params = {"query": query, "limit": limit, "offset": offset}
            url = "{}/entities?{}".format(base_url.rstrip("/"), urlencode(params))
            resp = session.get(url, timeout=120)
            if not resp.ok:
                print("[navigator] WARN: entities {} offset {} -> HTTP {}".format(
                    nav_type, offset, resp.status_code), file=sys.stderr)
                break
            data = resp.json()
            items = data if isinstance(data, list) else (data.get("entities") or data.get("items") or [])
            if not items:
                break
            for ent in items:
                eid = ent.get("identity") or ent.get("id") or get_entity_id(ent)
                if eid and eid not in seen:
                    seen.add(eid)
                    yield ent
            if len(items) < limit:
                break
            offset += len(items)
            print("[navigator] entities type={} offset={} fetched={} total_seen={}".format(
                nav_type, offset, len(items), len(seen)), file=sys.stderr)
            time.sleep(sleep_ms / 1000.0)


def fetch_audits(session, base_url, start_time, end_time, limit, offset_step, sleep_ms):
    """Paginate through /api/v3/audits. Yields event dicts."""
    offset = 0
    while True:
        params = {"limit": limit, "offset": offset}
        if start_time:
            params["startTime"] = start_time
        if end_time:
            params["endTime"] = end_time
        url = "{}/audits?{}".format(base_url.rstrip("/"), urlencode(params))
        resp = session.get(url, timeout=120)
        if not resp.ok:
            print("[navigator] WARN: audits offset {} -> HTTP {}".format(offset, resp.status_code), file=sys.stderr)
            break
        data = resp.json()
        items = data if isinstance(data, list) else (data.get("audits") or data.get("events") or data.get("items") or [])
        if not items:
            break
        for evt in items:
            yield evt
        if len(items) < limit:
            break
        offset += len(items)
        print("[navigator] audits offset={} fetched={}".format(offset, len(items)), file=sys.stderr)
        time.sleep(sleep_ms / 1000.0)


def audit_to_access_row(evt):
    """Map Navigator audit event to dependency_access row. Field names vary by Navigator version."""
    ts = evt.get("timestamp") or evt.get("time") or evt.get("eventTime") or ""
    if isinstance(ts, (int, float)):
        from datetime import datetime
        ts = datetime.utcfromtimestamp(ts / 1000.0).strftime("%Y-%m-%dT%H:%M:%SZ") if ts else ""
    user = evt.get("user") or evt.get("username") or evt.get("actor") or ""
    op = evt.get("operation") or evt.get("action") or evt.get("accessType") or "ACCESS"
    resource = evt.get("resource") or evt.get("resourcePath") or evt.get("path") or evt.get("objectId") or ""
    service = evt.get("service") or evt.get("serviceName") or "navigator"
    obj_type = evt.get("resourceType") or evt.get("objectType") or "entity"
    return {
        "window_start": ts if isinstance(ts, str) else str(ts),
        "service": service,
        "user": user,
        "do_as": evt.get("doAs") or "",
        "client_ip": evt.get("ip") or evt.get("clientIp") or "",
        "app_name": evt.get("application") or evt.get("appName") or "",
        "op": op,
        "object_type": obj_type,
        "object_id": resource if isinstance(resource, str) else str(resource),
        "cnt": 1,
    }


def fetch_relations(session, base_url, entity_ids_batch, relation_types, limit, sleep_ms):
    """Fetch relations for a batch of entity IDs. Returns list of relation dicts."""
    if not entity_ids_batch:
        return []
    params = {"limit": limit}
    if entity_ids_batch:
        params["entityIds"] = ",".join(str(i) for i in entity_ids_batch[:50])
    if relation_types:
        params["types"] = ",".join(relation_types)
    url = "{}/relations?{}".format(base_url.rstrip("/"), urlencode(params))
    resp = session.get(url, timeout=120)
    if not resp.ok:
        return []
    data = resp.json()
    items = data if isinstance(data, list) else (data.get("relations") or data.get("items") or [])
    time.sleep(sleep_ms / 1000.0)
    return items


def relation_to_lineage_row(rel, id_to_entity):
    """Map Navigator relation to lineage CSV row. id_to_entity: identity -> entity dict."""
    from_id = rel.get("fromEntityId") or rel.get("fromId") or rel.get("sourceId")
    to_id = rel.get("toEntityId") or rel.get("toId") or rel.get("targetId")
    if not from_id or not to_id:
        return None
    from_ent = id_to_entity.get(str(from_id)) or id_to_entity.get(from_id)
    to_ent = id_to_entity.get(str(to_id)) or id_to_entity.get(to_id)
    rel_type = rel.get("type") or rel.get("relationType") or "RELATES_TO"
    return {
        "from_object_type": get_entity_type(from_ent) if from_ent else "entity",
        "from_object_id": get_entity_id(from_ent) if from_ent else str(from_id),
        "to_object_type": get_entity_type(to_ent) if to_ent else "entity",
        "to_object_id": get_entity_id(to_ent) if to_ent else str(to_id),
        "relation": rel_type,
    }


def main():
    ap = argparse.ArgumentParser(
        description="CDH discovery: export Cloudera Navigator entities, audits, and lineage to CSV."
    )
    ap.add_argument("--base-url", required=True,
                    help="Navigator API base URL (e.g. https://navigator-host:7187/api/v3)")
    ap.add_argument("--auth-mode", choices=["cookie", "login", "basic"], default="cookie",
                    help="Authentication mode (default: cookie)")
    ap.add_argument("--cookie", default="",
                    help="Browser cookie string for cookie auth (e.g. 'JSESSIONID=abc123'). "
                         "Copy from browser DevTools -> Network -> Request Headers -> Cookie")
    ap.add_argument("--user", default="", help="Username for login/basic auth")
    ap.add_argument("--password", default="",
                    help="Password for login/basic auth (leave empty to be prompted)")
    ap.add_argument("--verify-tls", action="store_true", default=False,
                    help="Verify TLS certificates")
    ap.add_argument("--entity-types", default="FILE,DIRECTORY,TABLE,VIEW,DATABASE,KUDU_TABLE",
                    help="Comma-separated entity types to fetch (default: FILE,DIRECTORY,TABLE,VIEW,DATABASE,KUDU_TABLE)")
    ap.add_argument("--limit", type=int, default=100,
                    help="Page size for entities and audits (default: 100)")
    ap.add_argument("--sleep-ms", type=int, default=200,
                    help="Sleep between API pages in ms (default: 200)")
    ap.add_argument("--start-time", default="",
                    help="Audit start time (Navigator format, e.g. 2024-01-01T00:00:00Z)")
    ap.add_argument("--end-time", default="",
                    help="Audit end time (Navigator format)")
    ap.add_argument("--skip-entities", action="store_true", default=False,
                    help="Do not fetch entities")
    ap.add_argument("--skip-audits", action="store_true", default=False,
                    help="Do not fetch audits")
    ap.add_argument("--skip-lineage", action="store_true", default=False,
                    help="Do not fetch relations/lineage (default: fetch lineage if entities were fetched)")
    ap.add_argument("--fetch-lineage", action="store_true", default=False,
                    help="Explicitly fetch relations/lineage (same as not using --skip-lineage)")
    ap.add_argument("--out-objects", default="navigator_objects.csv",
                    help="Output CSV for entities (default: navigator_objects.csv)")
    ap.add_argument("--out-access", default="navigator_access.csv",
                    help="Output CSV for audits (default: navigator_access.csv)")
    ap.add_argument("--out-lineage", default="navigator_lineage.csv",
                    help="Output CSV for lineage edges (default: navigator_lineage.csv)")
    args = ap.parse_args()

    if args.fetch_lineage:
        args.skip_lineage = False

    if requests is None:
        print("ERROR: requests not installed. Run: pip install requests", file=sys.stderr)
        sys.exit(1)

    session = requests.Session()
    session.verify = args.verify_tls
    base_url = args.base_url.rstrip("/")

    # Derive the Navigator root URL from base_url (strip /api/vN)
    nav_root = base_url
    for suffix in ("/api/v3", "/api/v9", "/api/v13", "/api"):
        if nav_root.endswith(suffix):
            nav_root = nav_root[:-len(suffix)]
            break

    if args.auth_mode == "cookie":
        if not args.cookie:
            args.cookie = getpass.getpass(
                "Paste browser cookie string (DevTools -> Network -> Cookie header): ")
        session.headers["Cookie"] = args.cookie
        print("[navigator] Using cookie auth", file=sys.stderr)

    elif args.auth_mode == "login":
        if not args.user:
            args.user = input("Navigator username: ")
        if not args.password:
            args.password = getpass.getpass("Password for {}: ".format(args.user))
        login_url = "{}/j_spring_security_check".format(nav_root)
        resp = session.post(login_url, data={
            "j_username": args.user,
            "j_password": args.password,
        }, allow_redirects=False)
        if resp.status_code in (200, 302):
            print("[navigator] Login successful (HTTP {})".format(resp.status_code), file=sys.stderr)
        else:
            print("[navigator] Login may have failed (HTTP {}). Trying anyway...".format(
                resp.status_code), file=sys.stderr)

    elif args.auth_mode == "basic":
        if not args.user:
            args.user = input("Navigator username: ")
        if not args.password:
            args.password = getpass.getpass("Password for {}: ".format(args.user))
        session.auth = (args.user, args.password)
        print("[navigator] Using basic auth", file=sys.stderr)

    session.headers.setdefault("Content-Type", "application/json")

    entity_types = [t.strip().upper() for t in args.entity_types.split(",") if t.strip()]
    out_dir = os.path.dirname(os.path.abspath(args.out_objects))
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir)

    id_to_entity = {}

    if not args.skip_entities:
        print("[navigator] Fetching entities (types={})...".format(entity_types), file=sys.stderr)
        object_rows = []
        for ent in fetch_entities(session, base_url, entity_types, args.limit, args.limit, args.sleep_ms):
            row = entity_to_object_row(ent)
            if row:
                object_rows.append(row)
                eid = get_entity_id(ent)
                if eid:
                    id_to_entity[eid] = ent
                    id_to_entity[str(ent.get("identity") or ent.get("id") or "")] = ent
        with open(args.out_objects, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=OBJECT_FIELDS)
            w.writeheader()
            w.writerows(object_rows)
        print("[navigator] Wrote {} entities to {}".format(len(object_rows), args.out_objects), file=sys.stderr)
    else:
        print("[navigator] Skipping entities (--skip-entities)", file=sys.stderr)

    if not args.skip_audits:
        print("[navigator] Fetching audits...", file=sys.stderr)
        access_rows = []
        for evt in fetch_audits(
            session, base_url,
            args.start_time or None, args.end_time or None,
            args.limit, args.limit, args.sleep_ms
        ):
            access_rows.append(audit_to_access_row(evt))
        with open(args.out_access, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=ACCESS_FIELDS)
            w.writeheader()
            w.writerows(access_rows)
        print("[navigator] Wrote {} audit rows to {}".format(len(access_rows), args.out_access), file=sys.stderr)
    else:
        print("[navigator] Skipping audits (--skip-audits)", file=sys.stderr)

    if not args.skip_lineage and id_to_entity:
        print("[navigator] Fetching relations (lineage)...", file=sys.stderr)
        lineage_rows = []
        ids_batch = list(id_to_entity.keys())[:1000]
        rels = fetch_relations(session, base_url, ids_batch, [], args.limit, args.sleep_ms)
        for rel in rels:
            row = relation_to_lineage_row(rel, id_to_entity)
            if row:
                lineage_rows.append(row)
        with open(args.out_lineage, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=LINEAGE_FIELDS)
            w.writeheader()
            w.writerows(lineage_rows)
        print("[navigator] Wrote {} lineage edges to {}".format(len(lineage_rows), args.out_lineage), file=sys.stderr)
    elif not args.skip_lineage:
        print("[navigator] No entities loaded; skipping lineage. Run without --skip-entities first.", file=sys.stderr)
    else:
        print("[navigator] Skipping lineage. Use --fetch-lineage to include relations.", file=sys.stderr)


if __name__ == "__main__":
    main()
