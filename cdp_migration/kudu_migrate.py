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
        "SOURCE_IMPALA_HOST": "",
        "SOURCE_IMPALA_PORT": "21050",
        "TARGET_IMPALA_HOST": "",
        "TARGET_IMPALA_PORT": "21050",
        "IMPALA_SHELL_BIN": "impala-shell",
        "IMPALA_SHELL_EXTRA_OPTS": "",
        "EFS_STAGING_DIR": "/mnt/efs/shared/impala_migrate_staging",
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

    @staticmethod
    def _strip_impala_prefix(table_name):
        """Strip 'impala::' prefix if present."""
        if table_name.startswith("impala::"):
            return table_name[len("impala::"):]
        return table_name

    def copy_table(self, src_masters, src_table, dst_masters,
                   dst_table=None, predicates=None, check_dst_exists=True):
        """Copy a Kudu table between clusters.
        Default dst_table strips 'impala::' from src_table.
        If check_dst_exists is True, verifies the destination table exists on target
        before copying (avoids opaque 'partitioning must be specified' errors).
        """
        if dst_table is None:
            dst_table = self._strip_impala_prefix(src_table)
        if check_dst_exists and not self.dry_run:
            existing = set(self.list_tables(dst_masters))
            alt = dst_table if dst_table.startswith("impala::") else "impala::" + dst_table
            if dst_table in existing:
                pass  # use dst_table as-is
            elif alt in existing:
                dst_table = alt  # target uses impala:: prefix
            elif src_table in existing:
                dst_table = src_table  # target has same name as source; omit -dst_table
            else:
                raise RuntimeError(
                    "Destination table '{}' (or '{}') not found on target Kudu cluster.\n"
                    "Create the table on the target first with:\n"
                    "  impala_migrate.py import-table -d <database> -t <table>\n"
                    "Then run this copy again. (The 'Table partitioning must be specified' "
                    "error from the Kudu CLI usually means the destination table did not exist.)"
                    .format(dst_table, alt)
                )
            log.info("Using destination table name on target: %s", dst_table)
        cmd = [
            self.kudu_bin, "table", "copy",
            src_masters, src_table,
            dst_masters,
        ]
        if dst_table != src_table:
            cmd.append("-dst_table={}".format(dst_table))
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
# Impala-based copy (fallback when kudu table copy fails with partition error)
# ═══════════════════════════════════════════════════════════════════════════
def _parse_impala_table(name):
    """Return (database, table) from 'impala::db.t' or 'db.t'."""
    n = name[len("impala::"):] if name.startswith("impala::") else name
    if "." not in n:
        raise ValueError("Table name must be database.table or impala::database.table")
    db, tbl = n.split(".", 1)
    return db.strip(), tbl.strip()


def _impala_run(cfg, host, port, database, query, label="Impala"):
    """Run a single query via impala-shell; return stdout. Raises on failure."""
    cmd = [cfg.get("IMPALA_SHELL_BIN", "impala-shell"), "-k", "-i", "{}:{}".format(host, port),
           "-d", database, "-q", query, "--quiet"]
    extra = (cfg.get("IMPALA_SHELL_EXTRA_OPTS") or "").strip()
    if extra:
        cmd.extend(extra.split())
    log.info("[%s] %s", label, query[:120] + "..." if len(query) > 120 else query)
    result = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
        timeout=3600,
    )
    if result.returncode != 0:
        raise RuntimeError(
            "impala-shell failed ({}): {}".format(label, result.stderr.strip())
        )
    return result.stdout.strip()


def _impala_run_delimited(cfg, host, port, database, query, label="Impala"):
    """Run query with -B tab output; return list of rows (list of columns)."""
    cmd = [cfg.get("IMPALA_SHELL_BIN", "impala-shell"), "-k", "-i", "{}:{}".format(host, port),
           "-d", database, "-q", query, "--quiet", "-B", "--output_delimiter=\t"]
    extra = (cfg.get("IMPALA_SHELL_EXTRA_OPTS") or "").strip()
    if extra:
        cmd.extend(extra.split())
    result = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
        timeout=300,
    )
    if result.returncode != 0:
        raise RuntimeError(
            "impala-shell failed ({}): {}".format(label, result.stderr.strip())
        )
    rows = []
    for line in result.stdout.strip().splitlines():
        if line.strip():
            rows.append([c.strip() for c in line.split("\t")])
    return rows


def _get_kudu_columns(cfg, database, table):
    """Return list of (name, type) for the table (from DESCRIBE)."""
    rows = _impala_run_delimited(
        cfg,
        cfg.SOURCE_IMPALA_HOST,
        cfg.SOURCE_IMPALA_PORT,
        database,
        "DESCRIBE `{}`".format(table),
        "SOURCE",
    )
    # DESCRIBE: name, type, comment; skip partition row if present
    cols = []
    for row in rows:
        if len(row) >= 2 and row[0] and not row[0].startswith("#"):
            name, typ = row[0], row[1]
            if name.upper() in ("PARTITION", "PARTITIONED BY"):
                break
            cols.append((name, typ))
    return cols


def copy_table_via_impala(cfg, database, table, dry_run=False):
    """
    Copy Kudu table data from source to target using Parquet staging on EFS.
    Use when 'kudu table copy' fails with 'Table partitioning must be specified'.
    """
    efs_base = (cfg.get("EFS_STAGING_DIR") or "").rstrip("/")
    if not efs_base:
        raise RuntimeError("EFS_STAGING_DIR is not set; required for copy-table-impala")
    if not cfg.get("SOURCE_IMPALA_HOST") or not cfg.get("TARGET_IMPALA_HOST"):
        raise RuntimeError("SOURCE_IMPALA_HOST and TARGET_IMPALA_HOST must be set for copy-table-impala")

    staging_table = "_kudu_staging_{}".format(table)
    # Use file:// so Impala writes to the EFS mount (local path on each node)
    staging_location = "file://{}/kudu_copy/{}/{}".format(efs_base, database, table)

    if dry_run:
        log.info("[DRY-RUN] Would copy %s.%s via Parquet staging at %s", database, table, staging_location)
        return

    cols = _get_kudu_columns(cfg, database, table)
    col_defs = ", ".join("`{}` {}".format(c[0], c[1]) for c in cols)
    create_parquet = (
        "CREATE EXTERNAL TABLE `{}`.`{}` ({}) "
        "STORED AS PARQUET LOCATION '{}'"
    ).format(database, staging_table, col_defs, staging_location)

    log.info("Step 1/5: Create Parquet staging table on source and export from Kudu ...")
    _impala_run(cfg, cfg.SOURCE_IMPALA_HOST, cfg.SOURCE_IMPALA_PORT, database,
                "DROP TABLE IF EXISTS `{}`.`{}`".format(database, staging_table), "SOURCE")
    _impala_run(cfg, cfg.SOURCE_IMPALA_HOST, cfg.SOURCE_IMPALA_PORT, database,
                create_parquet, "SOURCE")
    _impala_run(cfg, cfg.SOURCE_IMPALA_HOST, cfg.SOURCE_IMPALA_PORT, database,
                "INSERT INTO `{}`.`{}` SELECT * FROM `{}`.`{}`".format(
                    database, staging_table, database, table),
                "SOURCE")

    log.info("Step 2/5: Create external table on target pointing to same EFS path ...")
    _impala_run(cfg, cfg.TARGET_IMPALA_HOST, cfg.TARGET_IMPALA_PORT, database,
                "DROP TABLE IF EXISTS `{}`.`{}`".format(database, staging_table), "TARGET")
    _impala_run(cfg, cfg.TARGET_IMPALA_HOST, cfg.TARGET_IMPALA_PORT, database,
                create_parquet, "TARGET")

    log.info("Step 3/5: INSERT INTO Kudu table on target from Parquet ...")
    _impala_run(cfg, cfg.TARGET_IMPALA_HOST, cfg.TARGET_IMPALA_PORT, database,
                "INSERT INTO `{}`.`{}` SELECT * FROM `{}`.`{}`".format(
                    database, table, database, staging_table),
                "TARGET")

    log.info("Step 4/5: Drop staging tables ...")
    _impala_run(cfg, cfg.SOURCE_IMPALA_HOST, cfg.SOURCE_IMPALA_PORT, database,
                "DROP TABLE IF EXISTS `{}`.`{}`".format(database, staging_table), "SOURCE")
    _impala_run(cfg, cfg.TARGET_IMPALA_HOST, cfg.TARGET_IMPALA_PORT, database,
                "DROP TABLE IF EXISTS `{}`.`{}`".format(database, staging_table), "TARGET")

    log.info("Step 5/5: Done.")


# ═══════════════════════════════════════════════════════════════════════════
# Pre-flight
# ═══════════════════════════════════════════════════════════════════════════
def preflight_checks(cfg, require_kudu=True):
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

    if require_kudu:
        rc = subprocess.run(
            [cfg.KUDU_BIN, "--version"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        ).returncode
        if rc != 0:
            errors.append("kudu CLI not found at: {}".format(cfg.KUDU_BIN))

    if require_kudu:
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
    dst_table = args.dst_table or KuduClient._strip_impala_prefix(src_table)
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
        check_dst_exists=not getattr(args, "no_check_dst", False),
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
        dst_tbl = KuduClient._strip_impala_prefix(tbl)
        log.info("\n[%d/%d] %s -> %s", i, len(matched), tbl, dst_tbl)
        try:
            output = client.copy_table(src_masters, tbl, dst_masters, dst_table=dst_tbl)
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
            dst_tbl = KuduClient._strip_impala_prefix(tbl)
            src_count = client.row_count(src_masters, tbl)
            tgt_count = client.row_count(dst_masters, dst_tbl)
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
    p.add_argument("--env", default=None, help="Path to impala_migrate.env")
    p.add_argument("--source-masters", default=None, help="Override SOURCE_KUDU_MASTERS")
    p.add_argument("--target-masters", default=None, help="Override TARGET_KUDU_MASTERS")
    p.add_argument("--dry-run", action="store_true", help="Do not run copy commands")
    p.add_argument("--verbose", "-v", action="store_true", help="Verbose logging")
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--env", default=None)
    common.add_argument("--source-masters", default=None)
    common.add_argument("--target-masters", default=None)
    common.add_argument("--dry-run", action="store_true")
    common.add_argument("--verbose", "-v", action="store_true")

    sub = p.add_subparsers(dest="command", required=True)

    ct = sub.add_parser("copy-table", parents=[common],
                        help="Copy a single Kudu table")
    ct.add_argument("--table", "-t", required=True,
                    help="Kudu table name (e.g. impala::db.table)")
    ct.add_argument("--dst-table", default=None,
                    help="Target table name (default: source without impala:: prefix)")
    ct.add_argument("--no-check-dst", action="store_true",
                    help="Skip checking that destination table exists (use with --dst-table if name differs)")
    ct.add_argument("--predicates", default=None,
                    help="JSON predicates for partial copy")
    ct.add_argument("--verify-after", action="store_true",
                    help="Verify row counts after copy")

    cti = sub.add_parser("copy-table-impala", parents=[common],
                        help="Copy Kudu table via Parquet staging (use when kudu table copy fails)")
    cti.add_argument("--table", "-t", required=True,
                     help="Kudu table (e.g. impala::db.table)")

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
    default = os.path.join(os.path.dirname(__file__), "impala_migrate.env")
    return default


def main():
    parser = build_parser()
    args = parser.parse_args()
    _setup_logging("DEBUG" if getattr(args, "verbose", False) else "INFO")

    env_path = _resolve_env_path(args)
    cli_overrides = {}
    if getattr(args, "source_masters", None):
        cli_overrides["SOURCE_KUDU_MASTERS"] = args.source_masters
    if getattr(args, "target_masters", None):
        cli_overrides["TARGET_KUDU_MASTERS"] = args.target_masters
    cfg = Config(env_path, cli_overrides)
    preflight_checks(cfg, require_kudu=(args.command != "copy-table-impala"))
    client = KuduClient(cfg, dry_run=getattr(args, "dry_run", False))

    if args.command == "copy-table":
        cmd_copy_table(cfg, args, client)
    elif args.command == "copy-table-impala":
        database, table = _parse_impala_table(args.table)
        log.info("%s=== Copying Kudu table via Impala (Parquet staging): %s.%s ===%s",
                 _C, database, table, _RST)
        copy_table_via_impala(cfg, database, table, dry_run=getattr(args, "dry_run", False))
        log.info("%s=> Copy complete%s", _G, _RST)
    elif args.command == "copy-db":
        cmd_copy_db(cfg, args, client)
    elif args.command == "verify":
        cmd_verify(cfg, args, client)
    elif args.command == "list-tables":
        cmd_list_tables(cfg, args, client)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
