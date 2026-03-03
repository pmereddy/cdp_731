#!/usr/bin/env python3
"""
kudu_migrate.py -- Migrate Kudu table data between CDP clusters.

Wraps the 'kudu table copy' CLI tool to copy Kudu table data directly
between clusters. Use alongside impala_migrate.py which handles DDL,
grants, and HDFS-backed table data.

Workflow:
  1. impala_migrate.py export/import  -- creates the Kudu table DDL on target
  2. kudu_migrate.py copy-table       -- copies the data via Kudu native API

Requires:
  - Python 3.6+
  - 'kudu' CLI on $PATH (ships with CDP)
  - Network connectivity between source and target Kudu masters
  - Valid Kerberos TGT (kinit before running)

No external Python packages required.
"""

import argparse
import json
import logging
import os
import re
import subprocess
import sys
import textwrap
from typing import Any, Dict, List, Optional

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_FMT = "%(asctime)s  %(levelname)-7s  %(message)s"
log = logging.getLogger("kudu_migrate")


def _setup_logging(level="INFO"):
    logging.basicConfig(
        format=LOG_FMT,
        level=getattr(logging, level.upper(), logging.INFO),
    )


# ---------------------------------------------------------------------------
# Colour helpers
# ---------------------------------------------------------------------------
if sys.stdout.isatty():
    _G = "\033[0;32m"; _R = "\033[0;31m"; _Y = "\033[0;33m"
    _C = "\033[0;36m"; _B = "\033[1m"; _RST = "\033[0m"
else:
    _G = _R = _Y = _C = _B = _RST = ""


# ═══════════════════════════════════════════════════════════════════════════
# Config loader (reuses the same impala_migrate.env file)
# ═══════════════════════════════════════════════════════════════════════════
class Config:
    DEFAULTS = {
        "SOURCE_KUDU_MASTERS": "",
        "TARGET_KUDU_MASTERS": "",
        "KUDU_BIN": "kudu",
        "KRB_PRINCIPAL": "",
        "KRB_KEYTAB": "",
    }

    def __init__(self, env_path, cli_overrides=None):
        self._d = dict(self.DEFAULTS)
        self._load_file(env_path)
        if cli_overrides:
            for k, v in cli_overrides.items():
                if v is not None:
                    self._d[k] = v

    def _load_file(self, path):
        if not os.path.isfile(path):
            log.warning("Config file %s not found; using defaults + CLI args", path)
            return
        with open(path) as fh:
            for line in fh:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                m = re.match(r'^([A-Z_][A-Z0-9_]*)=["\']?(.*?)["\']?\s*$', line)
                if m:
                    self._d[m.group(1)] = m.group(2)

    def __getattr__(self, name):
        try:
            return self._d[name]
        except KeyError:
            raise AttributeError("Config has no key '{}'".format(name))

    def get(self, name, default=""):
        return self._d.get(name, default)


# ═══════════════════════════════════════════════════════════════════════════
# Kudu CLI wrapper
# ═══════════════════════════════════════════════════════════════════════════
class KuduClient:

    def __init__(self, cfg, dry_run=False):
        self.cfg = cfg
        self.dry_run = dry_run
        self.kudu_bin = cfg.KUDU_BIN

    def _run(self, cmd, label=""):
        log.info("[KuduClient] %s: %s", label, " ".join(cmd))
        if self.dry_run:
            log.info("[KuduClient] DRY-RUN -- skipped")
            return 0, "", ""
        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            timeout=7200,
        )
        return result.returncode, result.stdout.strip(), result.stderr.strip()

    def list_tables(self, masters):
        """List all Kudu tables on the given masters."""
        cmd = [self.kudu_bin, "table", "list", masters]
        rc, stdout, stderr = self._run(cmd, "list tables")
        if rc != 0:
            raise RuntimeError(
                "kudu table list failed:\n  stderr: {}".format(stderr)
            )
        return [t.strip() for t in stdout.splitlines() if t.strip()]

    def copy_table(self, src_masters, src_table, dst_masters,
                   dst_table=None, predicates=None):
        """Copy a Kudu table between clusters."""
        dst_table = dst_table or src_table
        cmd = [
            self.kudu_bin, "table", "copy",
            src_masters, src_table,
            dst_masters, dst_table,
        ]
        if predicates:
            cmd.extend(["--predicates={}".format(predicates)])

        rc, stdout, stderr = self._run(cmd, "copy {}".format(src_table))
        if rc != 0:
            raise RuntimeError(
                "kudu table copy failed for {}:\n  stderr: {}".format(
                    src_table, stderr
                )
            )
        return stdout

    def row_count(self, masters, table):
        """Get row count for a Kudu table."""
        cmd = [self.kudu_bin, "table", "scan", masters, table,
               "--show_values=false", "--num_threads=4"]
        rc, stdout, stderr = self._run(cmd, "count {}".format(table))
        if rc != 0:
            raise RuntimeError(
                "kudu table scan failed for {}:\n  stderr: {}".format(
                    table, stderr
                )
            )
        for line in stderr.splitlines() + stdout.splitlines():
            m = re.search(r'Total count\s+(\d+)', line, re.IGNORECASE)
            if m:
                return int(m.group(1))
        return -1


# ═══════════════════════════════════════════════════════════════════════════
# Pre-flight
# ═══════════════════════════════════════════════════════════════════════════
def preflight_checks(cfg):
    errors = []

    result = subprocess.run(
        ["klist", "-s"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if result.returncode != 0:
        if cfg.KRB_PRINCIPAL and cfg.KRB_KEYTAB:
            log.info("No Kerberos ticket; performing kinit with keytab ...")
            kr = subprocess.run(
                ["kinit", "-kt", cfg.KRB_KEYTAB, cfg.KRB_PRINCIPAL],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
            )
            if kr.returncode != 0:
                errors.append("kinit failed: {}".format(kr.stderr.strip()))
        else:
            errors.append(
                "No valid Kerberos ticket. Run 'kinit' first "
                "or set KRB_PRINCIPAL/KRB_KEYTAB in .env"
            )

    rc = subprocess.run(
        [cfg.KUDU_BIN, "--help"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).returncode
    if rc != 0:
        errors.append("kudu CLI not found at: {}".format(cfg.KUDU_BIN))

    if not cfg.SOURCE_KUDU_MASTERS:
        errors.append("SOURCE_KUDU_MASTERS is not set")
    if not cfg.TARGET_KUDU_MASTERS:
        errors.append("TARGET_KUDU_MASTERS is not set")

    if errors:
        for e in errors:
            log.error("Pre-flight FAILED: %s", e)
        sys.exit(1)

    log.info("Pre-flight checks passed.")


# ═══════════════════════════════════════════════════════════════════════════
# Commands
# ═══════════════════════════════════════════════════════════════════════════
def cmd_copy_table(cfg, args, client):
    src_table = args.table
    dst_table = args.dst_table or src_table
    src_masters = cfg.SOURCE_KUDU_MASTERS
    dst_masters = cfg.TARGET_KUDU_MASTERS

    log.info("%s=== Copying Kudu table: %s ===%s", _C, src_table, _RST)
    log.info("  Source:  %s @ %s", src_table, src_masters)
    log.info("  Target:  %s @ %s", dst_table, dst_masters)

    if args.verify_after and not args.dry_run:
        log.info("Pre-copy: counting source rows ...")
        src_count = client.row_count(src_masters, src_table)
        log.info("  Source rows: %d", src_count)

    output = client.copy_table(
        src_masters, src_table, dst_masters, dst_table,
        predicates=args.predicates,
    )
    if output:
        log.info("  kudu output: %s", output)

    log.info("%s=> Copy of %s complete%s", _G, src_table, _RST)

    if args.verify_after and not args.dry_run:
        log.info("Post-copy: counting target rows ...")
        tgt_count = client.row_count(dst_masters, dst_table)
        log.info("  Target rows: %d", tgt_count)
        if src_count == tgt_count:
            log.info("  %sRow count MATCH (%d)%s", _G, src_count, _RST)
        else:
            log.warning("  %sRow count MISMATCH: source=%d target=%d%s",
                        _R, src_count, tgt_count, _RST)


def cmd_copy_db(cfg, args, client):
    """Copy all Kudu tables that belong to a database (impala::db.table naming)."""
    src_masters = cfg.SOURCE_KUDU_MASTERS
    dst_masters = cfg.TARGET_KUDU_MASTERS
    database = args.database
    prefix = "impala::{}.".format(database)

    log.info("%s=== Copying all Kudu tables for database: %s ===%s",
             _B, database, _RST)

    all_tables = client.list_tables(src_masters)
    matched = [t for t in all_tables if t.startswith(prefix)]

    if not matched:
        log.warning("No Kudu tables found matching prefix '%s'", prefix)
        return

    log.info("Found %d Kudu tables for %s", len(matched), database)
    for i, tbl in enumerate(matched, 1):
        log.info("\n[%d/%d] %s", i, len(matched), tbl)
        try:
            output = client.copy_table(src_masters, tbl, dst_masters, tbl)
            if output:
                log.info("  %s", output)
            log.info("  %sOK%s", _G, _RST)
        except Exception as e:
            log.error("  %sFAILED: %s%s", _R, e, _RST)

    log.info("\n%s=> Database copy complete (%d tables)%s",
             _G, len(matched), _RST)


def cmd_verify(cfg, args, client):
    src_masters = cfg.SOURCE_KUDU_MASTERS
    dst_masters = cfg.TARGET_KUDU_MASTERS

    if args.table:
        tables = [args.table]
    else:
        prefix = "impala::{}.".format(args.database)
        all_src = client.list_tables(src_masters)
        tables = [t for t in all_src if t.startswith(prefix)]

    if not tables:
        log.warning("No tables to verify.")
        return

    print("\n{}{}{}".format(_B, "=" * 72, _RST))
    print("{}  Kudu Verification{}".format(_B, _RST))
    print("{}{}{}".format(_B, "=" * 72, _RST))
    header = "  {:<45} {:>12} {:>12} {:>8}".format(
        "Table", "Src Rows", "Tgt Rows", "Match"
    )
    print(header)
    print("  {} {} {} {}".format("-" * 45, "-" * 12, "-" * 12, "-" * 8))

    for tbl in tables:
        try:
            src_count = client.row_count(src_masters, tbl)
            tgt_count = client.row_count(dst_masters, tbl)
            match = src_count == tgt_count
            status = "{}OK{}".format(_G, _RST) if match else "{}FAIL{}".format(_R, _RST)
            print("  {:<45} {:>12} {:>12} {:>18}".format(
                tbl, src_count, tgt_count, status
            ))
        except Exception as e:
            print("  {:<45} {}ERROR: {}{}".format(tbl, _R, str(e)[:30], _RST))
    print()


def cmd_list_tables(cfg, args, client):
    masters = cfg.SOURCE_KUDU_MASTERS
    if args.database:
        prefix = "impala::{}.".format(args.database)
    else:
        prefix = ""

    all_tables = client.list_tables(masters)
    matched = [t for t in all_tables if t.startswith(prefix)] if prefix else all_tables

    print("\n{}{}{}".format(_B, "=" * 72, _RST))
    print("{}  Kudu Tables on {}{}".format(_B, masters, _RST))
    print("{}{}{}".format(_B, "=" * 72, _RST))
    for t in matched:
        print("  {}".format(t))
    print("\n  Total: {}".format(len(matched)))
    print()


# ═══════════════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════════════
def build_parser():
    p = argparse.ArgumentParser(
        prog="kudu_migrate",
        description="Migrate Kudu table data between CDP clusters.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent("""\
        Examples:
          # Copy a single Kudu table
          kudu_migrate.py copy-table -t impala::sales.orders

          # Copy with verification
          kudu_migrate.py copy-table -t impala::sales.orders --verify-after

          # Copy all Kudu tables in a database
          kudu_migrate.py copy-db -d sales

          # Verify row counts
          kudu_migrate.py verify -d sales

          # List Kudu tables
          kudu_migrate.py list-tables -d sales
        """),
    )

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--env", default=None,
                        help="Path to impala_migrate.env")
    common.add_argument("--source-masters", default=None,
                        help="Source Kudu masters (overrides env)")
    common.add_argument("--target-masters", default=None,
                        help="Target Kudu masters (overrides env)")
    common.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARN", "WARNING", "ERROR"])
    common.add_argument("--dry-run", action="store_true",
                        help="Show what would be done without executing")

    sub = p.add_subparsers(dest="command", help="Command to run")

    ct = sub.add_parser("copy-table", parents=[common],
                        help="Copy a single Kudu table")
    ct.add_argument("--table", "-t", required=True,
                    help="Kudu table name (e.g. impala::db.table)")
    ct.add_argument("--dst-table", default=None,
                    help="Target table name (default: same as source)")
    ct.add_argument("--predicates", default=None,
                    help="JSON predicates for partial copy")
    ct.add_argument("--verify-after", action="store_true",
                    help="Verify row counts after copy")

    cd = sub.add_parser("copy-db", parents=[common],
                        help="Copy all Kudu tables for a database")
    cd.add_argument("--database", "-d", required=True,
                    help="Database name (matches impala::db.* tables)")

    v = sub.add_parser("verify", parents=[common],
                       help="Verify Kudu table row counts")
    v.add_argument("--database", "-d", default=None,
                   help="Database name (verify all tables in db)")
    v.add_argument("--table", "-t", default=None,
                   help="Single table to verify")

    lt = sub.add_parser("list-tables", parents=[common],
                        help="List Kudu tables on source cluster")
    lt.add_argument("--database", "-d", default=None,
                    help="Filter by database name")

    return p


def _resolve_env_path(args):
    if args.env:
        return args.env
    script_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(script_dir, "impala_migrate.env")


def _build_config(args):
    overrides = {}
    if getattr(args, "source_masters", None):
        overrides["SOURCE_KUDU_MASTERS"] = args.source_masters
    if getattr(args, "target_masters", None):
        overrides["TARGET_KUDU_MASTERS"] = args.target_masters
    return Config(_resolve_env_path(args), overrides)


def main():
    parser = build_parser()
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    _setup_logging(args.log_level)

    cfg = _build_config(args)
    dry_run = getattr(args, "dry_run", False)

    if dry_run:
        log.info("%s*** DRY-RUN MODE ***%s", _Y, _RST)

    preflight_checks(cfg)

    client = KuduClient(cfg, dry_run=dry_run)

    if args.command == "copy-table":
        cmd_copy_table(cfg, args, client)
    elif args.command == "copy-db":
        cmd_copy_db(cfg, args, client)
    elif args.command == "verify":
        cmd_verify(cfg, args, client)
    elif args.command == "list-tables":
        cmd_list_tables(cfg, args, client)


if __name__ == "__main__":
    main()
