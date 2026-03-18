#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CDH 6.3.3 discovery. Python 3.6+ compatible.
Exports Oozie workflows, coordinators, and bundles from the Oozie REST API
into dependency_objects CSV (and optionally dependency_access for run history).

Captures:
  - Oozie objects: workflow, coordinator, bundle (app path, owner, status, job id)
  - HDFS app-path dependency: each job's application path as hdfs_path so you can
    link "workflow W uses HDFS path P"
  - Optional: run history as access-like rows (user, job, time) for dependency_access

Oozie REST API (v1): list jobs with jobtype=wf|coordinator|bundle, then get job
detail (appPath, user, appName, status, createdTime, etc.). CDH Oozie typically
runs on port 11000; use HTTP or HTTPS and optional Kerberos.
"""

from __future__ import print_function

import argparse
import csv
import getpass
import os
import re
import sys
import time
from urllib.parse import urlencode

try:
    import requests
except ImportError:
    requests = None

try:
    from requests_kerberos import HTTPKerberosAuth, OPTIONAL as KRB_OPTIONAL
    HAS_KRB = True
except ImportError:
    HAS_KRB = False


OBJECT_FIELDS = ["service", "object_type", "object_id", "owner", "group", "extra"]
ACCESS_FIELDS = [
    "window_start", "service", "user", "do_as", "client_ip",
    "app_name", "op", "object_type", "object_id", "cnt",
]


def normalize_app_path(path):
    """Normalize Oozie app path to a canonical HDFS path (strip trailing slash, default to path as-is)."""
    if not path:
        return ""
    path = (path or "").strip().rstrip("/")
    # If it's a full URI (hdfs://host:8020/path), keep it or normalize to /path for consistency
    if path.startswith("hdfs://"):
        # Keep full URI or strip to path; for dependency_objects /path is often used
        m = re.match(r"hdfs://[^/]+(?:\d+)?(/.+)", path)
        if m:
            return m.group(1).rstrip("/") or "/"
    return path or "/"


def get_session(args):
    s = requests.Session()
    s.verify = args.verify_tls
    if args.auth_mode == "kerberos" and HAS_KRB:
        s.auth = HTTPKerberosAuth(mutual_authentication=KRB_OPTIONAL)
    elif args.auth_mode == "basic" and args.user:
        s.auth = (args.user, args.password or "")
    s.headers.setdefault("Content-Type", "application/json")
    return s


def list_jobs(session, base_url, jobtype, length, offset):
    """GET /v1/jobs?jobtype=wf|coordinator|bundle&len=&offset= Returns list of job info or ids."""
    url = "{}/v1/jobs?{}".format(
        base_url.rstrip("/"),
        urlencode({"jobtype": jobtype, "len": length, "offset": offset}),
    )
    resp = session.get(url, timeout=60)
    if not resp.ok:
        return None, resp.status_code, resp.text[:500]
    data = resp.json()
    # Response can be {"workflows": [{"id": "0000001-...", ...}, ...]} or similar
    jobs = []
    if "workflows" in data:
        jobs = data["workflows"]
    elif "coordinatorjobs" in data:
        jobs = data["coordinatorjobs"]
    elif "bundlejobs" in data:
        jobs = data["bundlejobs"]
    elif "jobs" in data:
        jobs = data["jobs"]
    elif isinstance(data, list):
        jobs = data
    return jobs, resp.status_code, None


def job_detail(session, base_url, job_id):
    """GET /v1/job/<id>?show=info Returns job detail (appPath, user, appName, status, etc.)."""
    url = "{}/v1/job/{}?show=info".format(base_url.rstrip("/"), job_id)
    resp = session.get(url, timeout=30)
    if not resp.ok:
        return None
    return resp.json()


def main():
    ap = argparse.ArgumentParser(
        description="CDH discovery: export Oozie workflows, coordinators, bundles to dependency_objects CSV."
    )
    ap.add_argument("--oozie-url", required=True,
                    help="Oozie server URL (e.g. http://oozie-host:11000/oozie)")
    ap.add_argument("--auth-mode", choices=["none", "basic", "kerberos"], default="none")
    ap.add_argument("--user", default="")
    ap.add_argument("--password", default="",
                    help="Password for basic auth (leave empty to be prompted securely)")
    ap.add_argument("--verify-tls", action="store_true", default=False)
    ap.add_argument("--len", type=int, default=100,
                    help="Page size for job list (default: 100)")
    ap.add_argument("--sleep-ms", type=int, default=100,
                    help="Sleep between API calls in ms (default: 100)")
    ap.add_argument("--include-app-path-rows", action="store_true", default=True,
                    help="Emit HDFS app-path as dependency_objects rows (default: True)")
    ap.add_argument("--out-objects", default="oozie_objects.csv",
                    help="Output dependency_objects CSV (default: oozie_objects.csv)")
    ap.add_argument("--out-access", default="",
                    help="If set, write job run history (user, time) to this dependency_access CSV")
    args = ap.parse_args()

    if requests is None:
        print("ERROR: requests not installed. pip install requests", file=sys.stderr)
        sys.exit(1)
    if args.auth_mode == "kerberos" and not HAS_KRB:
        print("ERROR: Kerberos auth requires requests-kerberos. pip install requests-kerberos", file=sys.stderr)
        sys.exit(1)

    if args.auth_mode == "basic" and args.user and not args.password:
        args.password = getpass.getpass("Password for {}: ".format(args.user))

    session = get_session(args)
    base = args.oozie_url

    # Job types and their object_type names
    job_types = [
        ("wf", "oozie_workflow"),
        ("coordinator", "oozie_coordinator"),
        ("bundle", "oozie_bundle"),
    ]

    object_rows = []
    access_rows = []
    seen_job_ids = set()
    seen_app_paths = set()

    for jobtype, object_type in job_types:
        offset = 0
        while True:
            jobs, status, err = list_jobs(session, base, jobtype, args.len, offset)
            if jobs is None:
                print("WARN: list {} offset {} -> {} {}".format(jobtype, offset, status, err), file=sys.stderr)
                break
            if not jobs:
                break
            for j in jobs:
                if isinstance(j, str):
                    job_id = j
                    app_path = user = app_name = status = created = ""
                else:
                    job_id = j.get("id") or j.get("jobId") or ""
                    app_path = j.get("appPath") or j.get("appName") or ""
                    user = j.get("user") or j.get("userName") or ""
                    app_name = j.get("appName") or j.get("name") or app_path or job_id
                    status = j.get("status") or ""
                    created = j.get("createdTime") or ""
                if not job_id:
                    continue
                if job_id in seen_job_ids:
                    continue
                seen_job_ids.add(job_id)
                if not app_path and job_id:
                    detail = job_detail(session, base, job_id)
                    if detail:
                        app_path = detail.get("appPath") or detail.get("appName") or ""
                        user = user or detail.get("user") or detail.get("userName") or ""
                        app_name = app_name or detail.get("appName") or detail.get("name") or app_path or job_id
                        status = status or detail.get("status") or ""
                        created = created or detail.get("createdTime") or ""
                    time.sleep(args.sleep_ms / 1000.0)

                object_id = app_path.strip().rstrip("/") if app_path else job_id
                extra_parts = []
                if job_id:
                    extra_parts.append("job_id={}".format(job_id))
                if status:
                    extra_parts.append("status={}".format(status))
                if created:
                    extra_parts.append("created={}".format(created))

                object_rows.append({
                    "service": "oozie",
                    "object_type": object_type,
                    "object_id": object_id,
                    "owner": user,
                    "group": "",
                    "extra": "|".join(extra_parts),
                })

                if args.out_access and created:
                    try:
                        ts = int(created) / 1000.0
                        from datetime import datetime
                        window_start = datetime.utcfromtimestamp(ts).strftime("%Y-%m-%dT%H:%M:%SZ")
                    except Exception:
                        window_start = str(created)
                    access_rows.append({
                        "window_start": window_start,
                        "service": "oozie",
                        "user": user,
                        "do_as": "",
                        "client_ip": "",
                        "app_name": app_name or object_id,
                        "op": "SUBMIT",
                        "object_type": object_type,
                        "object_id": object_id,
                        "cnt": 1,
                    })

                if args.include_app_path_rows and app_path:
                    hdfs_path = normalize_app_path(app_path)
                    if hdfs_path and hdfs_path not in seen_app_paths:
                        seen_app_paths.add(hdfs_path)
                        object_rows.append({
                            "service": "hdfs",
                            "object_type": "hdfs_path",
                            "object_id": hdfs_path if hdfs_path.startswith("/") else "/" + hdfs_path.lstrip("/"),
                            "owner": user,
                            "group": "",
                            "extra": "referenced_by_oozie|{}".format(job_id),
                        })

            if len(jobs) < args.len:
                break
            offset += len(jobs)
            print("[oozie] {} offset {} fetched {} total objects so far".format(
                jobtype, offset, len(object_rows)), file=sys.stderr)
            time.sleep(args.sleep_ms / 1000.0)

    out_dir = os.path.dirname(os.path.abspath(args.out_objects))
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir)

    with open(args.out_objects, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=OBJECT_FIELDS)
        w.writeheader()
        w.writerows(object_rows)

    print("[oozie] Wrote {} rows to {}".format(len(object_rows), args.out_objects), file=sys.stderr)

    if args.out_access and access_rows:
        with open(args.out_access, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=ACCESS_FIELDS)
            w.writeheader()
            w.writerows(access_rows)
        print("[oozie] Wrote {} access rows to {}".format(len(access_rows), args.out_access), file=sys.stderr)


if __name__ == "__main__":
    main()
