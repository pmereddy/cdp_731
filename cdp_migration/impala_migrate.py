#!/usr/bin/env python3
"""
impala_migrate.py -- Migrate Impala tables/databases between CDP clusters.

Supports decoupled export/import workflow or combined migrate in one shot.

Requires:
  - Python 3.6+
  - impala-shell on $PATH (or configured in impala_migrate.env)
  - hadoop / hdfs CLI on $PATH
  - Valid Kerberos TGT (kinit before running)
  - Shared EFS mount accessible from both clusters

No external Python packages required.
"""

import argparse
import datetime
import json
import logging
import os
import re
import subprocess
import sys
import textwrap
from typing import Any, Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_FMT = "%(asctime)s  %(levelname)-7s  %(message)s"
log = logging.getLogger("impala_migrate")


def _setup_logging(level: str = "INFO"):
    logging.basicConfig(format=LOG_FMT, level=getattr(logging, level.upper(), logging.INFO))


# ---------------------------------------------------------------------------
# Colour helpers (disabled when not a TTY)
# ---------------------------------------------------------------------------
if sys.stdout.isatty():
    _G = "\033[0;32m"; _R = "\033[0;31m"; _Y = "\033[0;33m"
    _C = "\033[0;36m"; _B = "\033[1m"; _RST = "\033[0m"
else:
    _G = _R = _Y = _C = _B = _RST = ""


# ═══════════════════════════════════════════════════════════════════════════
# Config loader
# ═══════════════════════════════════════════════════════════════════════════
class Config:
    """Load key=value pairs from the .env file and CLI overrides."""

    DEFAULTS = {
        "SOURCE_IMPALA_PORT": "21050",
        "TARGET_IMPALA_PORT": "21050",
        "IMPALA_SHELL_BIN": "impala-shell",
        "HADOOP_BIN": "hadoop",
        "HDFS_BIN": "hdfs",
        "IMPALA_SHELL_EXTRA_OPTS": "",
        "DISTCP_BANDWIDTH_MB": "0",
        "DISTCP_MAX_MAPS": "20",
        "VERIFY_SAMPLE_ROWS": "10000",
        "KRB_PRINCIPAL": "",
        "KRB_KEYTAB": "",
    }

    def __init__(self, env_path: str, cli_overrides: Optional[Dict[str, str]] = None):
        self._d: Dict[str, str] = dict(self.DEFAULTS)
        self._load_file(env_path)
        if cli_overrides:
            for k, v in cli_overrides.items():
                if v is not None:
                    self._d[k] = v

    def _load_file(self, path: str):
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

    def __getattr__(self, name: str) -> str:
        try:
            return self._d[name]
        except KeyError:
            raise AttributeError(f"Config has no key '{name}'")

    def get(self, name: str, default: str = "") -> str:
        return self._d.get(name, default)

    def get_int(self, name: str, default: int = 0) -> int:
        try:
            return int(self._d.get(name, str(default)))
        except ValueError:
            return default


# ═══════════════════════════════════════════════════════════════════════════
# ImpalaClient -- wraps impala-shell subprocess
# ═══════════════════════════════════════════════════════════════════════════
class ImpalaClient:
    """Execute queries via impala-shell and parse output."""

    def __init__(self, cfg: Config, host: str, port: str, label: str = ""):
        self.cfg = cfg
        self.host = host
        self.port = port
        self.label = label or host
        self._conn_str = f"{host}:{port}"

    def _base_cmd(self) -> List[str]:
        cmd = [self.cfg.IMPALA_SHELL_BIN, "-k", "-i", self._conn_str]
        extra = self.cfg.IMPALA_SHELL_EXTRA_OPTS.strip()
        if extra:
            cmd.extend(extra.split())
        return cmd

    def execute(self, query: str, database: Optional[str] = None) -> str:
        cmd = self._base_cmd()
        if database:
            cmd.extend(["-d", database])
        cmd.extend(["-q", query, "--quiet"])
        log.debug("[%s] Running: %s", self.label, " ".join(cmd))
        log.debug("[%s] Query: %s", self.label, query[:200])
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, timeout=600)
        if result.returncode != 0:
            stderr = result.stderr.strip()
            raise RuntimeError(
                f"impala-shell failed on {self.label}:\n"
                f"  query : {query[:300]}\n"
                f"  stderr: {stderr}"
            )
        return result.stdout.strip()

    def execute_delimited(self, query: str, database: Optional[str] = None,
                          delimiter: str = "\t") -> List[List[str]]:
        cmd = self._base_cmd()
        if database:
            cmd.extend(["-d", database])
        cmd.extend(["-q", query, "--quiet", "-B", f"--output_delimiter={delimiter}"])
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, timeout=600)
        if result.returncode != 0:
            raise RuntimeError(
                f"impala-shell failed on {self.label}:\n"
                f"  query : {query[:300]}\n"
                f"  stderr: {result.stderr.strip()}"
            )
        rows: List[List[str]] = []
        for line in result.stdout.strip().splitlines():
            if line:
                rows.append(line.split(delimiter))
        return rows

    def execute_scalar(self, query: str, database: Optional[str] = None) -> str:
        rows = self.execute_delimited(query, database)
        if rows:
            return rows[0][0]
        return ""


# ═══════════════════════════════════════════════════════════════════════════
# MetadataExtractor
# ═══════════════════════════════════════════════════════════════════════════
class TableMeta:
    """Holds parsed table metadata."""
    def __init__(self):
        self.database: str = ""
        self.table: str = ""
        self.create_ddl: str = ""
        self.is_external: bool = False
        self.is_partitioned: bool = False
        self.location: str = ""
        self.file_format: str = ""
        self.partition_columns: List[str] = []
        self.partitions: List[str] = []
        self.columns: List[Tuple[str, str]] = []
        self.grants: List[str] = []
        self.row_count: int = -1
        self.is_kudu: bool = False
        self.is_view: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "database": self.database,
            "table": self.table,
            "create_ddl": self.create_ddl,
            "is_external": self.is_external,
            "is_partitioned": self.is_partitioned,
            "is_kudu": self.is_kudu,
            "is_view": self.is_view,
            "location": self.location,
            "file_format": self.file_format,
            "partition_columns": self.partition_columns,
            "partitions": self.partitions,
            "columns": self.columns,
            "grants": self.grants,
            "row_count": self.row_count,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "TableMeta":
        m = cls()
        m.database = d["database"]
        m.table = d["table"]
        m.create_ddl = d["create_ddl"]
        m.is_external = d["is_external"]
        m.is_partitioned = d["is_partitioned"]
        m.location = d["location"]
        m.file_format = d["file_format"]
        m.partition_columns = d.get("partition_columns", [])
        m.partitions = d.get("partitions", [])
        m.columns = [tuple(c) for c in d.get("columns", [])]
        m.grants = d.get("grants", [])
        m.row_count = d.get("row_count", -1)
        m.is_kudu = d.get("is_kudu", False)
        m.is_view = d.get("is_view", False)
        return m


class MetadataExtractor:
    """Extract table and database metadata from an Impala cluster."""

    def __init__(self, client: ImpalaClient):
        self.client = client

    def list_databases(self) -> List[str]:
        rows = self.client.execute_delimited("SHOW DATABASES")
        return [r[0] for r in rows if r[0]]

    def list_tables(self, database: str) -> List[str]:
        rows = self.client.execute_delimited("SHOW TABLES", database=database)
        return [r[0] for r in rows if r[0]]

    def get_create_ddl(self, database: str, table: str) -> str:
        out = self.client.execute(f"SHOW CREATE TABLE `{database}`.`{table}`")
        lines = []
        for line in out.splitlines():
            stripped = line.strip()
            if not stripped:
                continue
            if stripped.startswith("+"):
                continue
            if stripped.startswith("|"):
                cleaned = stripped.strip("|").strip()
                if cleaned and cleaned.lower() != "result":
                    lines.append(cleaned)
            elif stripped.lower() == "result":
                continue
            else:
                lines.append(stripped)
        return "\n".join(lines)

    def describe_formatted(self, database: str, table: str) -> Dict[str, str]:
        rows = self.client.execute_delimited(
            f"DESCRIBE FORMATTED `{database}`.`{table}`"
        )
        info: Dict[str, str] = {}
        for row in rows:
            if len(row) >= 2:
                key = row[0].strip().rstrip(":").strip()
                val = row[1].strip()
                if key:
                    info[key] = val
        return info

    def get_columns(self, database: str, table: str) -> List[Tuple[str, str]]:
        rows = self.client.execute_delimited(
            f"DESCRIBE `{database}`.`{table}`"
        )
        cols = []
        for row in rows:
            if len(row) >= 2:
                name = row[0].strip()
                ctype = row[1].strip()
                if name and not name.startswith("#") and name != "name":
                    cols.append((name, ctype))
        return cols

    def get_partitions(self, database: str, table: str,
                       partition_columns: Optional[List[str]] = None) -> List[str]:
        try:
            rows = self.client.execute_delimited(
                f"SHOW PARTITIONS `{database}`.`{table}`"
            )
            if not partition_columns:
                partition_columns = []
            num_pcols = len(partition_columns)
            seen = set()
            parts = []
            for row in rows:
                if num_pcols > 0 and len(row) >= num_pcols:
                    segments = []
                    for idx in range(num_pcols):
                        val = row[idx].strip()
                        segments.append("{}={}".format(partition_columns[idx], val))
                    key = "/".join(segments)
                else:
                    key = "/".join(c.strip() for c in row if c.strip())
                if not key or key.startswith("Total"):
                    continue
                if key not in seen:
                    seen.add(key)
                    parts.append(key)
            return parts
        except RuntimeError:
            return []

    def get_grants(self, database: str, table: str) -> List[str]:
        try:
            out = self.client.execute(
                f"SHOW GRANT ON TABLE `{database}`.`{table}`"
            )
            grants = []
            for line in out.splitlines():
                stripped = line.strip()
                if stripped and not stripped.startswith("+") and not stripped.startswith("|"):
                    continue
                cleaned = stripped.strip("|").strip()
                if cleaned and "---" not in cleaned and cleaned.lower() != "privilege":
                    grants.append(cleaned)
            return grants
        except RuntimeError:
            log.debug("Could not retrieve grants for %s.%s (Ranger may not be configured)", database, table)
            return []

    def get_row_count(self, database: str, table: str) -> int:
        val = self.client.execute_scalar(
            f"SELECT COUNT(*) FROM `{database}`.`{table}`"
        )
        try:
            return int(val)
        except (ValueError, TypeError):
            return -1

    def get_table_meta(self, database: str, table: str) -> TableMeta:
        meta = TableMeta()
        meta.database = database
        meta.table = table
        meta.create_ddl = self.get_create_ddl(database, table)
        desc = self.describe_formatted(database, table)
        table_type_str = desc.get("Table Type", "").upper()
        meta.is_external = table_type_str.startswith("EXTERNAL")
        meta.is_view = "VIEW" in table_type_str or not desc.get("Location", "")
        meta.location = desc.get("Location", "")
        meta.file_format = desc.get("InputFormat", "")
        meta.columns = self.get_columns(database, table)

        meta.is_kudu = "stored as kudu" in meta.create_ddl.lower()

        part_cols = []
        ddl_lower = meta.create_ddl.lower()
        if "partitioned by" in ddl_lower:
            meta.is_partitioned = True
            m = re.search(r'PARTITIONED BY\s*\((.*?)\)', meta.create_ddl,
                          re.IGNORECASE | re.DOTALL)
            if m:
                for col_def in m.group(1).split(","):
                    col_name = col_def.strip().split()[0].strip("`")
                    part_cols.append(col_name)
            meta.partition_columns = part_cols
            meta.partitions = self.get_partitions(database, table, part_cols)
        else:
            meta.partition_columns = part_cols
        meta.grants = self.get_grants(database, table)
        return meta


# ═══════════════════════════════════════════════════════════════════════════
# ExportManifest -- metadata bundle written to EFS during export
# ═══════════════════════════════════════════════════════════════════════════
class ExportManifest:
    """Read/write the per-table export manifest stored on EFS."""

    @staticmethod
    def manifest_path(staging_dir: str, database: str, table: str) -> str:
        return os.path.join(staging_dir, database, table, "_manifest.json")

    @staticmethod
    def write(staging_dir: str, meta: TableMeta, row_count: int):
        path = ExportManifest.manifest_path(staging_dir, meta.database, meta.table)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        payload = {
            "exported_at": datetime.datetime.utcnow().isoformat(),
            "source_row_count": row_count,
            "metadata": meta.to_dict(),
        }
        with open(path, "w") as fh:
            json.dump(payload, fh, indent=2, default=str)
        log.info("Export manifest written: %s", path)

    @staticmethod
    def read(staging_dir: str, database: str, table: str) -> Dict[str, Any]:
        path = ExportManifest.manifest_path(staging_dir, database, table)
        if not os.path.isfile(path):
            raise FileNotFoundError(
                f"No export manifest at {path}. Run 'export-table' first."
            )
        with open(path) as fh:
            return json.load(fh)

    @staticmethod
    def exists(staging_dir: str, database: str, table: str) -> bool:
        return os.path.isfile(
            ExportManifest.manifest_path(staging_dir, database, table)
        )

    @staticmethod
    def list_exported(staging_dir: str, database: str) -> List[str]:
        db_dir = os.path.join(staging_dir, database)
        if not os.path.isdir(db_dir):
            return []
        tables = []
        for entry in sorted(os.listdir(db_dir)):
            manifest = os.path.join(db_dir, entry, "_manifest.json")
            if os.path.isfile(manifest):
                tables.append(entry)
        return tables


# ═══════════════════════════════════════════════════════════════════════════
# DataMover
# ═══════════════════════════════════════════════════════════════════════════
class DataMover:
    """Move data between HDFS locations using EFS as staging."""

    def __init__(self, cfg: Config, dry_run: bool = False):
        self.cfg = cfg
        self.dry_run = dry_run
        self.efs_staging = cfg.EFS_STAGING_DIR

    @staticmethod
    def _shell_quote(path: str) -> str:
        """Shell-quote a path for safe use in shell=True commands."""
        import shlex
        return shlex.quote(path)

    def _run(self, cmd: List[str], label: str = "") -> subprocess.CompletedProcess:
        log.info("[DataMover] %s: %s", label, " ".join(cmd))
        if self.dry_run:
            log.info("[DataMover] DRY-RUN -- skipped")
            return subprocess.CompletedProcess(cmd, 0, "", "")
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, timeout=7200)
        if result.returncode != 0:
            raise RuntimeError(
                f"DataMover command failed ({label}):\n"
                f"  cmd   : {' '.join(cmd)}\n"
                f"  stderr: {result.stderr.strip()}"
            )
        return result

    def _staging_path(self, database: str, table: str,
                      partition: Optional[str] = None) -> str:
        base = os.path.join(self.efs_staging, database, table, "data")
        if partition:
            base = os.path.join(base, partition)
        return base

    def _ensure_staging_dir(self, path: str):
        if self.dry_run:
            log.info("[DataMover] DRY-RUN -- would create %s", path)
            return
        os.makedirs(path, exist_ok=True)

    # -- export (HDFS -> EFS) ----------------------------------------------
    def export_to_efs(self, strategy: str, hdfs_src: str,
                      database: str, table: str,
                      partition: Optional[str] = None):
        dest = self._staging_path(database, table, partition)
        parent = os.path.dirname(dest.rstrip("/"))
        self._ensure_staging_dir(parent)
        if strategy == "distcp":
            cmd = [
                self.cfg.HADOOP_BIN, "distcp",
                "-m", str(self.cfg.get_int("DISTCP_MAX_MAPS", 20)),
                "-overwrite",
            ]
            bw = self.cfg.get_int("DISTCP_BANDWIDTH_MB", 0)
            if bw > 0:
                cmd.extend(["-bandwidth", str(bw)])
            cmd.extend([hdfs_src.rstrip("/") + "/", f"file://{dest}"])
            self._run(cmd, f"distcp HDFS->EFS {database}.{table}")
        elif strategy in ("hdfs-cp", "efs-direct"):
            if os.path.isdir(dest):
                import shutil
                shutil.rmtree(dest)
            cmd = [self.cfg.HDFS_BIN, "dfs", "-get",
                   hdfs_src.rstrip("/"), dest]
            self._run(cmd, f"hdfs-get HDFS->EFS {database}.{table}")
        else:
            raise ValueError(f"Unknown strategy: {strategy}")

    # -- import (EFS -> HDFS) ----------------------------------------------
    def import_from_efs(self, strategy: str, hdfs_dest: str,
                        database: str, table: str,
                        partition: Optional[str] = None):
        if strategy == "efs-direct":
            log.info("efs-direct: skipping EFS->HDFS copy (target reads from EFS)")
            return
        src = self._staging_path(database, table, partition)
        if strategy == "distcp":
            cmd = [
                self.cfg.HADOOP_BIN, "distcp",
                "-m", str(self.cfg.get_int("DISTCP_MAX_MAPS", 20)),
                "-overwrite",
                f"file://{src}/", hdfs_dest.rstrip("/") + "/",
            ]
            self._run(cmd, f"distcp EFS->HDFS {database}.{table}")
        elif strategy == "hdfs-cp":
            if not os.path.isdir(src):
                log.warning("No staging data dir found: %s", src)
                return
            entries = sorted(os.listdir(src))
            if not entries:
                log.warning("No files found in staging dir: %s", src)
                return
            dest = hdfs_dest.rstrip("/")
            total = len(entries)
            log.info("  Uploading %d entries to %s", total, dest)
            for idx, entry in enumerate(entries, 1):
                entry_path = os.path.join(src, entry)
                cmd = '{} dfs -put -f {} {}'.format(
                    self.cfg.HDFS_BIN,
                    self._shell_quote(entry_path),
                    self._shell_quote(dest),
                )
                if idx == 1 or idx == total or idx % 100 == 0:
                    log.info("  [%d/%d] %s", idx, total, entry)
                result = subprocess.run(
                    cmd, shell=True,
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                    universal_newlines=True, timeout=7200,
                )
                if result.returncode != 0:
                    log.error("  FAILED [%d/%d] %s: %s",
                              idx, total, entry, result.stderr.strip())
                    raise RuntimeError(
                        f"DataMover command failed (hdfs-put {database}.{table}):\n"
                        f"  entry : {entry}\n"
                        f"  stderr: {result.stderr.strip()}"
                    )
        else:
            raise ValueError(f"Unknown strategy: {strategy}")

    def staging_data_path(self, database: str, table: str) -> str:
        return self._staging_path(database, table)


# ═══════════════════════════════════════════════════════════════════════════
# Verifier
# ═══════════════════════════════════════════════════════════════════════════
class Verifier:
    """Compare tables between source and target clusters."""

    def __init__(self, src_client: ImpalaClient, tgt_client: ImpalaClient, cfg: Config):
        self.src = MetadataExtractor(src_client)
        self.tgt = MetadataExtractor(tgt_client)
        self.sample_rows = cfg.get_int("VERIFY_SAMPLE_ROWS", 10000)

    def _checksum_query(self, database: str, table: str,
                        columns: List[Tuple[str, str]]) -> str:
        col_list = ", CAST(`{}` AS STRING)".format(
            "` AS STRING), CAST(`".join(c[0] for c in columns)
        )
        concat_expr = f"CONCAT_WS('|'{col_list})"
        agg = f"SUM(fnv_hash({concat_expr}))"
        q = f"SELECT {agg} FROM `{database}`.`{table}`"
        if self.sample_rows > 0:
            q += f" LIMIT {self.sample_rows}"
        return q

    def verify_row_count(self, database: str, table: str) -> Tuple[int, int, bool]:
        src_count = self.src.get_row_count(database, table)
        tgt_count = self.tgt.get_row_count(database, table)
        return src_count, tgt_count, src_count == tgt_count

    def verify_checksum(self, database: str, table: str,
                        columns: List[Tuple[str, str]]) -> Tuple[str, str, bool]:
        q = self._checksum_query(database, table, columns)
        src_hash = self.src.client.execute_scalar(q)
        tgt_hash = self.tgt.client.execute_scalar(q)
        return src_hash, tgt_hash, src_hash == tgt_hash

    def verify_partitions(self, database: str, table: str) -> Tuple[int, int, List[str]]:
        src_parts = set(self.src.get_partitions(database, table))
        tgt_parts = set(self.tgt.get_partitions(database, table))
        missing = sorted(src_parts - tgt_parts)
        return len(src_parts), len(tgt_parts), missing

    def full_verify(self, database: str, table: str) -> Dict[str, Any]:
        result: Dict[str, Any] = {"database": database, "table": table}

        log.info("Verifying %s.%s ...", database, table)

        src_count, tgt_count, count_ok = self.verify_row_count(database, table)
        result["source_rows"] = src_count
        result["target_rows"] = tgt_count
        result["row_count_match"] = count_ok

        meta = self.src.get_table_meta(database, table)
        if meta.columns:
            src_hash, tgt_hash, hash_ok = self.verify_checksum(database, table, meta.columns)
            result["source_checksum"] = src_hash
            result["target_checksum"] = tgt_hash
            result["checksum_match"] = hash_ok

        if meta.is_partitioned:
            sp, tp, missing = self.verify_partitions(database, table)
            result["source_partitions"] = sp
            result["target_partitions"] = tp
            result["missing_partitions"] = missing

        return result


# ═══════════════════════════════════════════════════════════════════════════
# MigrationEngine -- orchestrates export, import, migrate, verify
# ═══════════════════════════════════════════════════════════════════════════
class MigrationEngine:
    """Orchestrate table/database migration between clusters."""

    def __init__(self, cfg: Config, dry_run: bool = False):
        self.cfg = cfg
        self.dry_run = dry_run
        self.data_mover = DataMover(cfg, dry_run)

    def _source_client(self) -> ImpalaClient:
        return ImpalaClient(
            self.cfg, self.cfg.SOURCE_IMPALA_HOST,
            self.cfg.SOURCE_IMPALA_PORT, "SOURCE",
        )

    def _target_client(self) -> ImpalaClient:
        return ImpalaClient(
            self.cfg, self.cfg.TARGET_IMPALA_HOST,
            self.cfg.TARGET_IMPALA_PORT, "TARGET",
        )

    @staticmethod
    def _clean_impala_output(text: str) -> str:
        """Strip impala-shell box-drawing borders and column headers."""
        lines = []
        for line in text.splitlines():
            stripped = line.strip()
            if not stripped:
                continue
            if stripped.startswith("+") and stripped.endswith("+"):
                continue
            if stripped.startswith("|"):
                cleaned = stripped.strip("|").strip()
                if cleaned and cleaned.lower() != "result":
                    lines.append(cleaned)
            elif stripped.lower() == "result":
                continue
            else:
                lines.append(stripped)
        return "\n".join(lines)

    def _exec_on_target(self, tgt: ImpalaClient, query: str,
                        database: Optional[str] = None):
        query = self._clean_impala_output(query)
        query = self._remove_long_properties(query)
        if self.dry_run:
            log.info("[DRY-RUN] Would execute on TARGET: %s", query[:300])
            return ""
        return tgt.execute(query, database)

    @staticmethod
    def _rewrite_location(ddl: str, new_location: str) -> str:
        return re.sub(
            r"(LOCATION\s+')[^']*(')",
            rf"\g<1>{new_location}\g<2>",
            ddl,
            flags=re.IGNORECASE,
        )

    def _remap_hdfs_path(self, path: str) -> str:
        """Replace SOURCE_HDFS_BASE prefix with TARGET_HDFS_BASE in a path."""
        src_base = self.cfg.get("SOURCE_HDFS_BASE", "").rstrip("/")
        tgt_base = self.cfg.get("TARGET_HDFS_BASE", "").rstrip("/")
        if src_base and tgt_base and path.startswith(src_base):
            return tgt_base + path[len(src_base):]
        return path

    @staticmethod
    def _rewrite_kudu_masters(ddl: str, target_masters: str) -> str:
        return re.sub(
            r"('kudu\.master_addresses'\s*=\s*')[^']*(')",
            rf"\g<1>{target_masters}\g<2>",
            ddl,
            flags=re.IGNORECASE,
        )

    @staticmethod
    def _make_if_not_exists(ddl: str) -> str:
        ddl = re.sub(
            r'CREATE\s+EXTERNAL\s+TABLE\b',
            'CREATE EXTERNAL TABLE IF NOT EXISTS',
            ddl, count=1, flags=re.IGNORECASE,
        )
        ddl = re.sub(
            r'CREATE\s+TABLE\b(?!\s+IF)',
            'CREATE TABLE IF NOT EXISTS',
            ddl, count=1, flags=re.IGNORECASE,
        )
        ddl = re.sub(
            r'CREATE\s+VIEW\b(?!\s+IF)',
            'CREATE VIEW IF NOT EXISTS',
            ddl, count=1, flags=re.IGNORECASE,
        )
        return ddl

    @staticmethod
    def _strip_location(ddl: str) -> str:
        """Remove the LOCATION clause from a DDL statement."""
        return re.sub(
            r"\n?\s*LOCATION\s+'[^']*'",
            "",
            ddl,
            flags=re.IGNORECASE,
        )

    @staticmethod
    def _remove_long_properties(ddl: str, max_len: int = 4000) -> str:
        """Remove property key-value pairs whose value exceeds Impala's limit."""
        def _check_match(m):
            key = m.group(1)
            val = m.group(2)
            if len(val) <= max_len:
                return m.group(0)
            log.warning("Removing oversized property '%s' (%d chars > %d limit)",
                        key, len(val), max_len)
            return ""
        ddl = re.sub(
            r",?\s*'([^']+)'\s*=\s*'((?:[^'\\]|\\.)*)'",
            _check_match,
            ddl,
        )
        ddl = re.sub(r',\s*\)', ')', ddl)
        ddl = re.sub(r'\(\s*,', '(', ddl)
        return ddl

    # ── EXPORT ────────────────────────────────────────────────────────────
    def export_table(self, database: str, table: str,
                     strategy: str = "distcp") -> Dict[str, Any]:
        fqn = f"{database}.{table}"
        result: Dict[str, Any] = {"table": fqn, "status": "unknown", "actions": []}

        log.info("%s═══ Exporting %s (strategy=%s) ═══%s", _C, fqn, strategy, _RST)

        src = self._source_client()
        extractor = MetadataExtractor(src)

        # 1. Extract metadata
        log.info("Step 1/4: Extracting metadata from source ...")
        meta = extractor.get_table_meta(database, table)
        if meta.is_view:
            table_type = "VIEW"
        elif meta.is_kudu:
            table_type = "KUDU"
        elif meta.is_external:
            table_type = "EXTERNAL"
        else:
            table_type = "MANAGED"
        log.info("  Type: %s | Partitioned: %s | Format: %s",
                 table_type, meta.is_partitioned, meta.file_format)
        log.info("  Location: %s", meta.location)
        if meta.is_partitioned:
            log.info("  Partitions: %d", len(meta.partitions))
        result["is_kudu"] = meta.is_kudu
        result["is_view"] = meta.is_view
        result["actions"].append("extract_metadata")

        # 2. Get row count (skipped for views)
        if meta.is_view:
            log.info("Step 2/4: %sView -- skipping row count%s", _Y, _RST)
            meta.row_count = -1
        else:
            log.info("Step 2/4: Getting source row count ...")
            meta.row_count = extractor.get_row_count(database, table)
            log.info("  Row count: %d", meta.row_count)
        result["source_row_count"] = meta.row_count

        # 3. Copy data to EFS (skipped for views and Kudu tables)
        if meta.is_view:
            log.info("Step 3/4: %sView -- DDL only, skipping data export%s",
                     _Y, _RST)
            result["actions"].append("skip_data(view)")
        elif meta.is_kudu:
            log.info("Step 3/4: %sKudu table -- skipping data export "
                     "(use kudu_migrate.py for data)%s", _Y, _RST)
            result["actions"].append("skip_data(kudu)")
        else:
            log.info("Step 3/4: Copying data to EFS staging ...")
            log.info("  Source: %s", meta.location)
            if meta.is_partitioned:
                log.info("  Partitions: %d (copying entire directory tree)",
                         len(meta.partitions))
            self.data_mover.export_to_efs(strategy, meta.location, database, table)
            result["actions"].append("export_data")

        # 4. Write manifest
        log.info("Step 4/4: Writing export manifest ...")
        if not self.dry_run:
            ExportManifest.write(self.cfg.EFS_STAGING_DIR, meta, meta.row_count)
        result["actions"].append("write_manifest")

        staging = self.data_mover.staging_data_path(database, table)
        result["staging_path"] = staging
        result["status"] = "exported"
        log.info("%s=> Export of %s complete. Data staged at: %s%s", _G, fqn, staging, _RST)
        return result

    def export_database(self, database: str,
                        strategy: str = "distcp") -> List[Dict[str, Any]]:
        log.info("%s═══ Exporting database: %s ═══%s", _B, database, _RST)
        src = self._source_client()
        extractor = MetadataExtractor(src)
        tables = extractor.list_tables(database)
        log.info("Found %d tables in %s", len(tables), database)

        results = []
        for i, table in enumerate(tables, 1):
            log.info("\n[%d/%d] Exporting %s.%s ...", i, len(tables), database, table)
            try:
                r = self.export_table(database, table, strategy)
                results.append(r)
            except Exception as e:
                log.error("Failed to export %s.%s: %s", database, table, e)
                results.append({
                    "table": f"{database}.{table}",
                    "status": "error",
                    "error": str(e),
                    "actions": [],
                })
        return results

    # ── IMPORT ────────────────────────────────────────────────────────────
    def import_table(self, database: str, table: str,
                     strategy: str = "distcp",
                     force: bool = False) -> Dict[str, Any]:
        fqn = f"{database}.{table}"
        result: Dict[str, Any] = {"table": fqn, "status": "unknown", "actions": []}

        log.info("%s═══ Importing %s (strategy=%s, force=%s) ═══%s",
                 _C, fqn, strategy, force, _RST)

        # 1. Read export manifest from EFS
        log.info("Step 1/6: Reading export manifest ...")
        manifest = ExportManifest.read(self.cfg.EFS_STAGING_DIR, database, table)
        meta = TableMeta.from_dict(manifest["metadata"])
        log.info("  Exported at: %s | Source rows: %s",
                 manifest.get("exported_at", "?"),
                 manifest.get("source_row_count", "?"))
        result["actions"].append("read_manifest")

        tgt = self._target_client()

        # 2. Create database
        log.info("Step 2/6: Creating database on target ...")
        try:
            self._exec_on_target(tgt, f"CREATE DATABASE IF NOT EXISTS `{database}`")
            result["actions"].append("create_database")
        except RuntimeError as e:
            # Database creation can fail when HMS tries to create HDFS dirs
            # for a custom LOCATION.  Check if the database already exists.
            try:
                existing = tgt.execute(f"SHOW DATABASES LIKE '{database}'")
                if database.lower() in existing.lower():
                    log.warning("CREATE DATABASE failed but database already "
                                "exists -- continuing: %s", e)
                    result["actions"].append("create_database(exists)")
                else:
                    raise
            except RuntimeError:
                raise RuntimeError(
                    f"Cannot create database '{database}' on target. "
                    f"You may need to pre-create it manually or ensure "
                    f"the HDFS directory exists. Original error: {e}"
                )

        # 3. Handle force / create table or view
        if force:
            log.info("Step 3/6: --force: dropping target object if exists ...")
            if meta.is_view:
                self._exec_on_target(tgt, f"DROP VIEW IF EXISTS `{database}`.`{table}`")
            else:
                self._exec_on_target(tgt, f"DROP TABLE IF EXISTS `{database}`.`{table}`")
            result["actions"].append("drop_existing")

        # Remap HDFS location if SOURCE/TARGET base paths are configured
        target_location = self._remap_hdfs_path(meta.location) if meta.location else ""
        if target_location and target_location != meta.location:
            log.info("  HDFS location remapped: %s -> %s", meta.location, target_location)

        # For managed (non-external) tables, strip LOCATION so the Hive
        # Metastore assigns the default managed warehouse path.  The actual
        # assigned path is discovered after CREATE TABLE for the data copy.
        strip_managed_location = (
            not meta.is_view
            and not meta.is_kudu
            and not meta.is_external
            and meta.location
        )

        log.info("Step 3/6: Creating %s on target ...",
                 "view" if meta.is_view else "table")
        ddl = meta.create_ddl
        if not meta.is_view:
            if meta.is_kudu:
                target_kudu = self.cfg.get("TARGET_KUDU_MASTERS", "")
                if target_kudu:
                    ddl = self._rewrite_kudu_masters(ddl, target_kudu)
                    result["actions"].append("rewrite_kudu_masters")
            elif strip_managed_location:
                log.info("  Managed table: stripping LOCATION (HMS will assign default)")
                ddl = self._strip_location(ddl)
                result["actions"].append("strip_managed_location")
            elif strategy == "efs-direct":
                efs_data = f"file://{self.data_mover.staging_data_path(database, table)}"
                ddl = self._rewrite_location(ddl, efs_data)
            else:
                if target_location and target_location != meta.location:
                    ddl = self._rewrite_location(ddl, target_location)
                    result["actions"].append("remap_location")
        if not force:
            ddl = self._make_if_not_exists(ddl)

        has_hdfs_data = not meta.is_view and not meta.is_kudu and strategy != "efs-direct"
        if has_hdfs_data and not strip_managed_location and target_location:
            log.info("  Pre-creating HDFS directory: %s", target_location)
            try:
                self.data_mover._run(
                    [self.cfg.HDFS_BIN, "dfs", "-mkdir", "-p", target_location],
                    "mkdir target HDFS dir",
                )
            except RuntimeError as e:
                log.warning("Could not pre-create HDFS dir (may need admin): %s", e)

        self._exec_on_target(tgt, ddl)
        result["actions"].append("create_view" if meta.is_view else "create_table")

        # For managed tables whose LOCATION was stripped, discover the
        # actual path assigned by the Hive Metastore on the target.
        if strip_managed_location:
            tgt_extractor = MetadataExtractor(tgt)
            tgt_desc = tgt_extractor.describe_formatted(database, table)
            target_location = tgt_desc.get("Location", "")
            log.info("  Managed table assigned location: %s", target_location)

        # 4. Copy data from EFS to target HDFS (skipped for views and Kudu)
        if meta.is_view:
            log.info("Step 4/6: %sView -- no data to import%s", _Y, _RST)
            result["actions"].append("skip_data(view)")
        elif meta.is_kudu:
            log.info("Step 4/6: %sKudu table -- skipping data import "
                     "(use kudu_migrate.py for data)%s", _Y, _RST)
            result["actions"].append("skip_data(kudu)")
        else:
            log.info("Step 4/6: Loading data into target ...")
            log.info("  Target: %s", target_location)
            self.data_mover.import_from_efs(strategy, target_location, database, table)
            result["actions"].append("import_data")

        # 5. Recover partitions + compute stats (skipped for views and Kudu)
        if meta.is_view:
            log.info("Step 5/6: View -- skipping partitions and stats")
        elif meta.is_kudu:
            log.info("Step 5/6: Kudu manages its own storage -- skipping partition recovery and stats")
        else:
            if meta.is_partitioned:
                log.info("Step 5/6: Recovering partitions on target ...")
                self._exec_on_target(
                    tgt, f"ALTER TABLE `{database}`.`{table}` RECOVER PARTITIONS"
                )
                result["actions"].append("recover_partitions")

            log.info("Step 5/6: Computing stats on target ...")
            if meta.is_partitioned:
                self._exec_on_target(
                    tgt, f"COMPUTE INCREMENTAL STATS `{database}`.`{table}`"
                )
            else:
                self._exec_on_target(tgt, f"COMPUTE STATS `{database}`.`{table}`")
            result["actions"].append("compute_stats")

        # 6. Apply grants
        if meta.grants:
            log.info("Step 6/6: Applying grants on target ...")
            for grant in meta.grants:
                try:
                    self._exec_on_target(tgt, grant)
                except RuntimeError as e:
                    log.warning("Grant failed (may already exist): %s", e)
            result["actions"].append(f"grants({len(meta.grants)})")

        result["status"] = "imported"
        log.info("%s=> Import of %s complete%s", _G, fqn, _RST)
        return result

    def import_database(self, database: str, strategy: str = "distcp",
                        force: bool = False) -> List[Dict[str, Any]]:
        log.info("%s═══ Importing database: %s ═══%s", _B, database, _RST)
        tables = ExportManifest.list_exported(self.cfg.EFS_STAGING_DIR, database)
        if not tables:
            log.error("No exported tables found for database '%s' in %s",
                      database, self.cfg.EFS_STAGING_DIR)
            return []
        log.info("Found %d exported tables for %s", len(tables), database)

        results = []
        for i, table in enumerate(tables, 1):
            log.info("\n[%d/%d] Importing %s.%s ...", i, len(tables), database, table)
            try:
                r = self.import_table(database, table, strategy, force)
                results.append(r)
            except Exception as e:
                log.error("Failed to import %s.%s: %s", database, table, e)
                results.append({
                    "table": f"{database}.{table}",
                    "status": "error",
                    "error": str(e),
                    "actions": [],
                })
        return results

    # ── MIGRATE (export + import in one shot) ─────────────────────────────
    def migrate_table(self, database: str, table: str,
                      strategy: str = "distcp", force: bool = False,
                      verify_after: bool = False) -> Dict[str, Any]:
        fqn = f"{database}.{table}"
        result: Dict[str, Any] = {"table": fqn, "status": "unknown", "actions": []}

        log.info("%s═══ Migrating %s (strategy=%s, force=%s) ═══%s",
                 _C, fqn, strategy, force, _RST)

        # Phase 1: export
        exp = self.export_table(database, table, strategy)
        if exp["status"] != "exported":
            result["status"] = "error"
            result["error"] = f"Export failed: {exp.get('error', 'unknown')}"
            return result
        result["actions"].extend(exp.get("actions", []))

        # Phase 2: import
        imp = self.import_table(database, table, strategy, force)
        if imp["status"] != "imported":
            result["status"] = "error"
            result["error"] = f"Import failed: {imp.get('error', 'unknown')}"
            return result
        result["actions"].extend(imp.get("actions", []))

        # Phase 3: optional verify
        if verify_after:
            log.info("Verifying migration ...")
            src = self._source_client()
            tgt = self._target_client()
            verifier = Verifier(src, tgt, self.cfg)
            vr = verifier.full_verify(database, table)
            result["verification"] = vr

        result["status"] = "success"
        log.info("%s=> Migration of %s complete%s", _G, fqn, _RST)
        return result

    def migrate_database(self, database: str, strategy: str = "distcp",
                         force: bool = False,
                         verify_after: bool = False) -> List[Dict[str, Any]]:
        log.info("%s═══ Migrating database: %s ═══%s", _B, database, _RST)
        src = self._source_client()
        extractor = MetadataExtractor(src)
        tables = extractor.list_tables(database)
        log.info("Found %d tables in %s", len(tables), database)

        results = []
        for i, table in enumerate(tables, 1):
            log.info("\n[%d/%d] Processing %s.%s ...", i, len(tables), database, table)
            try:
                r = self.migrate_table(database, table, strategy, force,
                                       verify_after)
                results.append(r)
            except Exception as e:
                log.error("Failed to migrate %s.%s: %s", database, table, e)
                results.append({
                    "table": f"{database}.{table}",
                    "status": "error",
                    "error": str(e),
                    "actions": [],
                })
        return results

    # ── VERIFY ────────────────────────────────────────────────────────────
    def verify(self, database: str,
               table: Optional[str] = None) -> List[Dict[str, Any]]:
        src = self._source_client()
        tgt = self._target_client()
        verifier = Verifier(src, tgt, self.cfg)
        tables = [table] if table else MetadataExtractor(src).list_tables(database)
        results = []
        for t in tables:
            try:
                vr = verifier.full_verify(database, t)
                results.append(vr)
            except Exception as e:
                log.error("Verification failed for %s.%s: %s", database, t, e)
                results.append({
                    "database": database, "table": t,
                    "error": str(e),
                })
        return results

    # ── LIST TABLES ───────────────────────────────────────────────────────
    def list_tables(self, database: str) -> List[Dict[str, str]]:
        src = self._source_client()
        extractor = MetadataExtractor(src)
        tables = extractor.list_tables(database)
        info_list = []
        for t in tables:
            meta = extractor.get_table_meta(database, t)
            if meta.is_view:
                ttype = "VIEW"
            elif meta.is_kudu:
                ttype = "KUDU"
            elif meta.is_external:
                ttype = "EXTERNAL"
            else:
                ttype = "MANAGED"
            info_list.append({
                "table": t,
                "type": ttype,
                "location": meta.location,
                "format": meta.file_format,
            })
        return info_list


# ═══════════════════════════════════════════════════════════════════════════
# Pretty-print helpers
# ═══════════════════════════════════════════════════════════════════════════
def _print_results_table(results: List[Dict[str, Any]], title: str):
    print(f"\n{_B}{'=' * 72}{_RST}")
    print(f"{_B}  {title}{_RST}")
    print(f"{_B}{'=' * 72}{_RST}")

    if not results:
        print("  (no results)")
        return

    if "source_rows" in results[0]:
        _print_verify_table(results)
    elif "status" in results[0]:
        _print_migration_table(results)
    elif "type" in results[0]:
        _print_list_table(results)


def _print_verify_table(results: List[Dict[str, Any]]):
    header = f"  {'Table':<35} {'Src Rows':>12} {'Tgt Rows':>12} {'Count':>7} {'Chksum':>7}"
    print(header)
    print(f"  {'-' * 35} {'-' * 12} {'-' * 12} {'-' * 7} {'-' * 7}")
    for r in results:
        if "error" in r:
            fqn = f"{r.get('database', '?')}.{r.get('table', '?')}"
            print(f"  {fqn:<35} {_R}ERROR: {r['error'][:30]}{_RST}")
            continue
        fqn = f"{r['database']}.{r['table']}"
        count_ok = f"{_G}OK{_RST}" if r.get("row_count_match") else f"{_R}FAIL{_RST}"
        chk_ok = f"{_G}OK{_RST}" if r.get("checksum_match") else f"{_R}FAIL{_RST}"
        print(f"  {fqn:<35} {r.get('source_rows', '?'):>12} "
              f"{r.get('target_rows', '?'):>12} {count_ok:>17} {chk_ok:>17}")
        if r.get("missing_partitions"):
            print(f"    {_Y}Missing partitions: {len(r['missing_partitions'])}{_RST}")
    print()


def _print_migration_table(results: List[Dict[str, Any]]):
    header = f"  {'Table':<40} {'Status':<12} {'Actions'}"
    print(header)
    print(f"  {'-' * 40} {'-' * 12} {'-' * 30}")
    for r in results:
        status = r.get("status", "?")
        if status in ("success", "exported", "imported"):
            st = f"{_G}{status}{_RST}"
        elif status == "skipped":
            st = f"{_Y}{status}{_RST}"
        else:
            st = f"{_R}{status}{_RST}"
        actions = ", ".join(r.get("actions", []))
        print(f"  {r.get('table', '?'):<40} {st:<22} {actions}")
        if r.get("error"):
            print(f"    {_R}{r['error'][:70]}{_RST}")
    print()


def _print_list_table(results: List[Dict[str, Any]]):
    header = f"  {'Table':<30} {'Type':<10} {'Location'}"
    print(header)
    print(f"  {'-' * 30} {'-' * 10} {'-' * 50}")
    for r in results:
        print(f"  {r['table']:<30} {r['type']:<10} {r['location']}")
    print()


# ═══════════════════════════════════════════════════════════════════════════
# Pre-flight checks
# ═══════════════════════════════════════════════════════════════════════════
def preflight_checks(cfg: Config, need_source: bool = True,
                     need_target: bool = True):
    """Validate environment before running."""
    errors = []

    # Kerberos ticket
    result = subprocess.run(["klist", "-s"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if result.returncode != 0:
        if cfg.KRB_PRINCIPAL and cfg.KRB_KEYTAB:
            log.info("No active Kerberos ticket; performing kinit with keytab ...")
            kr = subprocess.run(
                ["kinit", "-kt", cfg.KRB_KEYTAB, cfg.KRB_PRINCIPAL],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True,
            )
            if kr.returncode != 0:
                errors.append(f"kinit failed: {kr.stderr.strip()}")
        else:
            errors.append("No valid Kerberos ticket. Run 'kinit' first "
                          "or set KRB_PRINCIPAL/KRB_KEYTAB in .env")

    # EFS staging dir
    efs_stage = cfg.EFS_STAGING_DIR
    efs_mount = cfg.get("EFS_MOUNT", "")
    if efs_mount and not os.path.isdir(efs_mount):
        errors.append(f"EFS mount point does not exist: {efs_mount}")
    elif efs_stage and not os.path.isdir(os.path.dirname(efs_stage)):
        errors.append(f"EFS staging parent dir does not exist: "
                      f"{os.path.dirname(efs_stage)}")

    # impala-shell
    result = subprocess.run([cfg.IMPALA_SHELL_BIN, "--version"],
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    if result.returncode != 0:
        errors.append(f"impala-shell not found at: {cfg.IMPALA_SHELL_BIN}")

    # Check required host configs
    if need_source and not cfg.get("SOURCE_IMPALA_HOST"):
        errors.append("SOURCE_IMPALA_HOST is not set")
    if need_target and not cfg.get("TARGET_IMPALA_HOST"):
        errors.append("TARGET_IMPALA_HOST is not set")

    if errors:
        for e in errors:
            log.error("Pre-flight FAILED: %s", e)
        sys.exit(1)

    log.info("Pre-flight checks passed.")


# ═══════════════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════════════
def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="impala_migrate",
        description="Migrate Impala tables/databases between CDP clusters via shared EFS.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent("""\
        Examples:
          # Export a table to EFS staging
          python3 impala_migrate.py export-table -d sales -t orders

          # Import a previously exported table into target
          python3 impala_migrate.py import-table -d sales -t orders

          # Migrate (export + import) in one shot with verification
          python3 impala_migrate.py migrate-table -d sales -t orders --verify-after

          # Export an entire database
          python3 impala_migrate.py export-db -d sales

          # Import an entire database from EFS staging
          python3 impala_migrate.py import-db -d sales

          # Verify a migrated table
          python3 impala_migrate.py verify -d sales -t orders

          # Dry-run a full database migration
          python3 impala_migrate.py migrate-db -d sales --dry-run

          # List tables in a database
          python3 impala_migrate.py list-tables -d sales
        """),
    )

    sub = p.add_subparsers(dest="command", help="Command to run")

    # -- common args -------------------------------------------------------
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--env", default=None,
                        help="Path to impala_migrate.env (default: ./impala_migrate.env)")
    common.add_argument("--source-host", default=None,
                        help="Source Impala host (overrides env)")
    common.add_argument("--source-port", default=None,
                        help="Source Impala port (overrides env)")
    common.add_argument("--target-host", default=None,
                        help="Target Impala host (overrides env)")
    common.add_argument("--target-port", default=None,
                        help="Target Impala port (overrides env)")
    common.add_argument("--efs-stage", default=None,
                        help="EFS staging directory (overrides env)")
    common.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARN", "WARNING", "ERROR"])
    common.add_argument("--dry-run", action="store_true",
                        help="Show what would be done without making changes")

    strategy_arg = {"default": "distcp",
                    "choices": ["distcp", "efs-direct", "hdfs-cp"],
                    "help": "Data movement strategy (default: distcp)"}

    # -- export-table ------------------------------------------------------
    et = sub.add_parser("export-table", parents=[common],
                        help="Export a single table to EFS staging")
    et.add_argument("--database", "-d", required=True, help="Database name")
    et.add_argument("--table", "-t", required=True, help="Table name")
    et.add_argument("--strategy", **strategy_arg)

    # -- export-db ---------------------------------------------------------
    ed = sub.add_parser("export-db", parents=[common],
                        help="Export all tables in a database to EFS staging")
    ed.add_argument("--database", "-d", required=True, help="Database name")
    ed.add_argument("--strategy", **strategy_arg)

    # -- import-table ------------------------------------------------------
    it = sub.add_parser("import-table", parents=[common],
                        help="Import a previously exported table into target")
    it.add_argument("--database", "-d", required=True, help="Database name")
    it.add_argument("--table", "-t", required=True, help="Table name")
    it.add_argument("--strategy", **strategy_arg)
    it.add_argument("--force", action="store_true",
                    help="Drop and recreate existing target table")

    # -- import-db ---------------------------------------------------------
    id_ = sub.add_parser("import-db", parents=[common],
                         help="Import all exported tables for a database into target")
    id_.add_argument("--database", "-d", required=True, help="Database name")
    id_.add_argument("--strategy", **strategy_arg)
    id_.add_argument("--force", action="store_true",
                     help="Drop and recreate existing target tables")

    # -- migrate-table -----------------------------------------------------
    mt = sub.add_parser("migrate-table", parents=[common],
                        help="Migrate a single table (export + import)")
    mt.add_argument("--database", "-d", required=True, help="Database name")
    mt.add_argument("--table", "-t", required=True, help="Table name")
    mt.add_argument("--strategy", **strategy_arg)
    mt.add_argument("--force", action="store_true",
                    help="Drop and recreate existing target table")
    mt.add_argument("--verify-after", action="store_true",
                    help="Run verification after migration")

    # -- migrate-db --------------------------------------------------------
    md = sub.add_parser("migrate-db", parents=[common],
                        help="Migrate all tables in a database (export + import)")
    md.add_argument("--database", "-d", required=True, help="Database name")
    md.add_argument("--strategy", **strategy_arg)
    md.add_argument("--force", action="store_true",
                    help="Drop and recreate existing target tables")
    md.add_argument("--verify-after", action="store_true",
                    help="Run verification after migration")

    # -- verify ------------------------------------------------------------
    v = sub.add_parser("verify", parents=[common],
                       help="Verify migrated tables (row count + checksum)")
    v.add_argument("--database", "-d", required=True, help="Database name")
    v.add_argument("--table", "-t", default=None,
                   help="Table name (omit to verify all in database)")

    # -- list-tables -------------------------------------------------------
    lt = sub.add_parser("list-tables", parents=[common],
                        help="List tables in a source database")
    lt.add_argument("--database", "-d", required=True, help="Database name")

    return p


def _resolve_env_path(args) -> str:
    if args.env:
        return args.env
    script_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(script_dir, "impala_migrate.env")


def _build_config(args) -> Config:
    overrides: Dict[str, str] = {}
    if getattr(args, "source_host", None):
        overrides["SOURCE_IMPALA_HOST"] = args.source_host
    if getattr(args, "source_port", None):
        overrides["SOURCE_IMPALA_PORT"] = args.source_port
    if getattr(args, "target_host", None):
        overrides["TARGET_IMPALA_HOST"] = args.target_host
    if getattr(args, "target_port", None):
        overrides["TARGET_IMPALA_PORT"] = args.target_port
    if getattr(args, "efs_stage", None):
        overrides["EFS_STAGING_DIR"] = args.efs_stage
    return Config(_resolve_env_path(args), overrides)


# ═══════════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════════
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
        log.info("%s*** DRY-RUN MODE -- no changes will be made ***%s", _Y, _RST)

    # Determine which connections are needed for preflight
    need_source = args.command in (
        "export-table", "export-db", "migrate-table", "migrate-db",
        "verify", "list-tables",
    )
    need_target = args.command in (
        "import-table", "import-db", "migrate-table", "migrate-db", "verify",
    )
    preflight_checks(cfg, need_source=need_source, need_target=need_target)

    engine = MigrationEngine(cfg, dry_run=dry_run)

    # -- dispatch ----------------------------------------------------------
    if args.command == "export-table":
        result = engine.export_table(args.database, args.table, args.strategy)
        _print_results_table([result], f"Export: {args.database}.{args.table}")

    elif args.command == "export-db":
        results = engine.export_database(args.database, args.strategy)
        _print_results_table(results, f"Export: {args.database}")

    elif args.command == "import-table":
        result = engine.import_table(args.database, args.table,
                                     args.strategy, args.force)
        _print_results_table([result], f"Import: {args.database}.{args.table}")

    elif args.command == "import-db":
        results = engine.import_database(args.database, args.strategy, args.force)
        _print_results_table(results, f"Import: {args.database}")

    elif args.command == "migrate-table":
        result = engine.migrate_table(args.database, args.table,
                                      args.strategy, args.force,
                                      args.verify_after)
        _print_results_table([result], f"Migration: {args.database}.{args.table}")

    elif args.command == "migrate-db":
        results = engine.migrate_database(args.database, args.strategy,
                                          args.force, args.verify_after)
        _print_results_table(results, f"Migration: {args.database}")

    elif args.command == "verify":
        results = engine.verify(args.database, getattr(args, "table", None))
        _print_results_table(results, f"Verification: {args.database}")

    elif args.command == "list-tables":
        results = engine.list_tables(args.database)
        _print_results_table(results, f"Tables in {args.database} (source)")


if __name__ == "__main__":
    main()
