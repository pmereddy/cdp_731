#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CDH 6.3.3 discovery. Python 3.6+ compatible.
Lists Kudu tables via the `kudu` CLI and produces a dependency_objects CSV.

Use this only if your CDH cluster has Kudu deployed. Default (list-only) is fast;
--describe adds owner per table but is slow on large clusters.
"""

from __future__ import print_function

import argparse
import csv
import json
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed


OBJECT_FIELDS = [
    "service", "object_type", "object_id", "owner", "group", "extra",
]


def run_cmd(cmd, timeout=120):
    try:
        p = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True
        )
        out, err = p.communicate(timeout=timeout)
        return p.returncode, out, err
    except subprocess.TimeoutExpired:
        p.kill()
        p.communicate()
        return -1, "", "Command timed out after {} seconds".format(timeout)


def list_tables(kudu_bin, master):
    rc, out, err = run_cmd([kudu_bin, "table", "list", master])
    if rc != 0:
        print("[cdh_kudu] ERROR: kudu table list failed: {}".format(err.strip()), file=sys.stderr)
        sys.exit(1)
    tables = [line.strip() for line in out.splitlines() if line.strip()]
    return tables


def describe_one(kudu_bin, master, table_name, timeout, sleep_ms):
    """Describe a single table and return (table_name, owner)."""
    rc, out, err = run_cmd(
        [kudu_bin, "table", "describe", master, table_name, "--format", "json"],
        timeout=timeout,
    )
    if sleep_ms > 0:
        time.sleep(sleep_ms / 1000.0)
    if rc != 0:
        return table_name, ""
    try:
        desc = json.loads(out)
        return table_name, desc.get("owner") or ""
    except (json.JSONDecodeError, ValueError):
        return table_name, ""


def main():
    ap = argparse.ArgumentParser(
        description=(
            "CDH discovery: export Kudu table metadata to dependency_objects CSV. "
            "Skip if your CDH cluster does not use Kudu."
        )
    )
    ap.add_argument("--kudu-master", required=True,
                    help="Kudu master RPC address (e.g. kudu-master:7051)")
    ap.add_argument("--kudu-bin", default="kudu",
                    help="Path to kudu CLI binary (default: kudu)")
    ap.add_argument("--describe", action="store_true", default=False,
                    help="Run 'kudu table describe' per table for owner (SLOW on large clusters)")
    ap.add_argument("--workers", type=int, default=1,
                    help="Parallel workers for describe calls (default: 1)")
    ap.add_argument("--sleep-ms", type=int, default=0,
                    help="Sleep between describe calls per worker in ms (default: 0)")
    ap.add_argument("--timeout", type=int, default=60,
                    help="Timeout per CLI call in seconds (default: 60)")
    ap.add_argument("--max-tables", type=int, default=None,
                    help="Cap on number of tables to describe (for testing)")
    ap.add_argument("--out-objects", default="kudu_objects.csv",
                    help="Output CSV path (default: kudu_objects.csv)")
    args = ap.parse_args()

    tables = list_tables(args.kudu_bin, args.kudu_master)
    print("[cdh_kudu] Found {} tables".format(len(tables)), file=sys.stderr)

    if args.max_tables is not None:
        tables = tables[:args.max_tables]
        print("[cdh_kudu] Capped to {} tables (--max-tables)".format(len(tables)), file=sys.stderr)

    owner_map = {}

    if args.describe:
        print("[cdh_kudu] Describing {} tables with {} workers...".format(
            len(tables), args.workers), file=sys.stderr)

        completed = 0
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            futures = {
                executor.submit(
                    describe_one,
                    args.kudu_bin, args.kudu_master, t, args.timeout, args.sleep_ms
                ): t
                for t in tables
            }
            for fut in as_completed(futures):
                table_name, owner = fut.result()
                owner_map[table_name] = owner
                completed += 1
                if completed % 500 == 0:
                    print("[cdh_kudu] Described {}/{} tables".format(completed, len(tables)),
                          file=sys.stderr)

    with open(args.out_objects, "w", newline="") as fout:
        writer = csv.DictWriter(fout, fieldnames=OBJECT_FIELDS)
        writer.writeheader()
        for table_name in tables:
            writer.writerow({
                "service": "kudu",
                "object_type": "kudu_table",
                "object_id": table_name,
                "owner": owner_map.get(table_name, ""),
                "group": "",
                "extra": "",
            })

    print("[cdh_kudu] Wrote {} tables to {}".format(len(tables), args.out_objects), file=sys.stderr)


if __name__ == "__main__":
    main()
