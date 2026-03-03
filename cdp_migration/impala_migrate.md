# Impala Cross-Cluster Migration Guide

Migrate Impala tables and databases between two CDP clusters using a shared
EFS mount as the data staging area. Supports decoupled export/import
workflows or combined migration in a single command.

---

## Prerequisites

| Requirement | Details |
|---|---|
| Python | 3.6+ (ships with RHEL 9) |
| `impala-shell` | On `$PATH` or configured in `impala_migrate.env` |
| `hadoop` / `hdfs` CLI | On `$PATH` or configured in `impala_migrate.env` |
| Kerberos ticket | Run `kinit` before the script, or set `KRB_PRINCIPAL` / `KRB_KEYTAB` in `.env` |
| EFS mount | Same EFS volume mounted on nodes of **both** clusters |
| Network | Script host can reach Impala daemons on both clusters (port 21050) |

## Quick Start

```bash
# 1. Edit the config file
cp impala_migrate.env impala_migrate.env.bak
vi impala_migrate.env

# 2. Get a Kerberos ticket
kinit your_principal

# 3. Dry-run a single table migration
python3 impala_migrate.py migrate-table -d sales -t orders --dry-run

# 4. Run the real migration with post-verification
python3 impala_migrate.py migrate-table -d sales -t orders --verify-after
```

---

## Commands

### `export-table` -- Export a single table to EFS staging

```bash
python3 impala_migrate.py export-table \
  -d <database> -t <table> \
  [--strategy distcp|efs-direct|hdfs-cp] [--dry-run]
```

Connects to the **source** cluster only. Steps performed:

1. Extracts DDL and metadata (`SHOW CREATE TABLE`, `DESCRIBE FORMATTED`)
2. Extracts grants (`SHOW GRANT ON TABLE`)
3. Gets source row count
4. Copies data files from source HDFS to EFS staging
5. Writes a `_manifest.json` to EFS with the full table metadata

The export manifest and data land at: `<EFS_STAGING_DIR>/<database>/<table>/`

### `export-db` -- Export all tables in a database

```bash
python3 impala_migrate.py export-db -d <database> \
  [--strategy distcp|efs-direct|hdfs-cp] [--dry-run]
```

Iterates over every table in the database and runs `export-table` for each.

### `import-table` -- Import a previously exported table

```bash
python3 impala_migrate.py import-table \
  -d <database> -t <table> \
  [--strategy distcp|efs-direct|hdfs-cp] [--force] [--dry-run]
```

Connects to the **target** cluster only. Reads from EFS staging. Steps:

1. Reads the `_manifest.json` from EFS (written by `export-table`)
2. Creates database on target (`CREATE DATABASE IF NOT EXISTS`)
3. Creates table on target (using exported DDL, with `IF NOT EXISTS` or `--force` drop)
4. Copies data from EFS staging to target HDFS
5. Recovers partitions (`ALTER TABLE ... RECOVER PARTITIONS`)
6. Computes stats (`COMPUTE STATS` / `COMPUTE INCREMENTAL STATS`)
7. Replays grant statements

### `import-db` -- Import all exported tables for a database

```bash
python3 impala_migrate.py import-db -d <database> \
  [--strategy distcp|efs-direct|hdfs-cp] [--force] [--dry-run]
```

Discovers all tables with a `_manifest.json` under `<EFS_STAGING_DIR>/<database>/`
and imports each one.

### `migrate-table` -- Export + import in one shot

```bash
python3 impala_migrate.py migrate-table \
  -d <database> -t <table> \
  [--strategy distcp|efs-direct|hdfs-cp] \
  [--force] [--verify-after] [--dry-run]
```

Runs `export-table` followed by `import-table` as a single operation.
Requires connectivity to **both** clusters.

### `migrate-db` -- Export + import all tables in a database

```bash
python3 impala_migrate.py migrate-db -d <database> \
  [--strategy distcp|efs-direct|hdfs-cp] \
  [--force] [--verify-after] [--dry-run]
```

### `verify` -- Verify migrated tables

```bash
# Single table
python3 impala_migrate.py verify -d sales -t orders

# All tables in a database
python3 impala_migrate.py verify -d sales
```

Checks performed:

| Check | Method |
|---|---|
| Row count | `SELECT COUNT(*) FROM db.table` on both clusters |
| Partition count | `SHOW PARTITIONS` on both clusters (partitioned tables only) |
| Checksum | `SUM(fnv_hash(CONCAT_WS(...)))` on a sample of rows |

### `list-tables` -- List tables in a source database

```bash
python3 impala_migrate.py list-tables -d sales
```

Prints table name, type (MANAGED/EXTERNAL), HDFS location, and file format.

---

## Decoupled Export/Import Workflow

The key advantage of separate `export` and `import` commands is that they
can be run independently:

- **Different times** -- export tonight, import tomorrow
- **Different machines** -- export from a source-cluster node, import from a target-cluster node
- **Different Kerberos realms** -- `kinit` into the source realm for export, then `kinit` into the target realm for import
- **Inspect before loading** -- review the exported data and manifest on EFS before committing to the target

### Example: cross-realm migration

```bash
# On a source-cluster node (REALM_A)
kinit user@REALM_A
python3 impala_migrate.py export-db -d analytics

# On a target-cluster node (REALM_B)
kinit user@REALM_B
python3 impala_migrate.py import-db -d analytics
```

### EFS staging layout

After export, the EFS staging directory looks like:

```
<EFS_STAGING_DIR>/
  analytics/
    page_views/
      _manifest.json      # metadata, DDL, grants, source row count
      data/                # data files copied from HDFS
        part-00000.parquet
        ...
    sessions/
      _manifest.json
      data/
        year=2025/
          month=01/
            ...
          month=02/
            ...
```

---

## Data Strategies

| Strategy | How it works | Best for |
|---|---|---|
| `distcp` (default) | `hadoop distcp` for both export and import | Large tables, production workloads |
| `efs-direct` | Export copies data to EFS; import rewrites table LOCATION to EFS path | Quick migrations, shared reads |
| `hdfs-cp` | `hdfs dfs -get` / `hdfs dfs -put` through EFS | Smaller tables, simple setups |

### Data flow

```
Export:   Source HDFS ──distcp/get──> EFS Staging
Import:   EFS Staging ──distcp/put──> Target HDFS
                             |
              (efs-direct: target reads directly from EFS)
```

---

## Idempotent Runs

The script is safe to re-run:

- `CREATE DATABASE IF NOT EXISTS` prevents database errors
- `CREATE TABLE IF NOT EXISTS` prevents table errors
- Use `--force` to drop and recreate the target table for a clean re-import

---

## Configuration

### `impala_migrate.env`

```bash
# Source and target Impala daemons
SOURCE_IMPALA_HOST="source-impala-daemon.example.com"
SOURCE_IMPALA_PORT="21050"
TARGET_IMPALA_HOST="target-impala-daemon.example.com"
TARGET_IMPALA_PORT="21050"

# Shared EFS
EFS_MOUNT="/mnt/efs/shared"
EFS_STAGING_DIR="/mnt/efs/shared/impala_migrate_staging"

# Kerberos (optional -- alternative to manual kinit)
KRB_PRINCIPAL=""
KRB_KEYTAB=""

# impala-shell extras (e.g. --ssl --ca_cert=...)
IMPALA_SHELL_EXTRA_OPTS=""

# Tool paths
IMPALA_SHELL_BIN="impala-shell"
HADOOP_BIN="hadoop"
HDFS_BIN="hdfs"

# distcp tuning
DISTCP_BANDWIDTH_MB="0"    # 0 = unlimited
DISTCP_MAX_MAPS="20"

# Verification
VERIFY_SAMPLE_ROWS="10000" # 0 = checksum all rows
```

### CLI overrides

Any env-file value can be overridden on the command line:

```bash
python3 impala_migrate.py export-table \
  --source-host other-impala.example.com \
  --efs-stage /mnt/efs/alt_staging \
  -d sales -t orders
```

---

## Common Options

| Flag | Description |
|---|---|
| `--env FILE` | Path to the env file (default: `./impala_migrate.env`) |
| `--source-host HOST` | Override source Impala host |
| `--source-port PORT` | Override source Impala port |
| `--target-host HOST` | Override target Impala host |
| `--target-port PORT` | Override target Impala port |
| `--efs-stage DIR` | Override EFS staging directory |
| `--strategy STR` | `distcp` (default), `efs-direct`, `hdfs-cp` |
| `--force` | Drop and recreate target table (import/migrate only) |
| `--verify-after` | Verify row counts + checksums after migration (migrate only) |
| `--dry-run` | Print what would happen without making changes |
| `--log-level LVL` | `DEBUG`, `INFO` (default), `WARN`, `ERROR` |

---

## Examples

### Export a table, review, then import

```bash
# Export
python3 impala_migrate.py export-table -d sales -t orders

# Check what was exported
cat /mnt/efs/shared/impala_migrate_staging/sales/orders/_manifest.json
ls /mnt/efs/shared/impala_migrate_staging/sales/orders/data/

# Import
python3 impala_migrate.py import-table -d sales -t orders
```

### Migrate a single table in one shot

```bash
python3 impala_migrate.py migrate-table \
  -d analytics -t page_views --verify-after
```

### Migrate an entire database

```bash
python3 impala_migrate.py migrate-db -d analytics --verify-after
```

### Force re-import (drop + recreate)

```bash
python3 impala_migrate.py import-table \
  -d sales -t orders --force
```

### Verify without migrating

```bash
# Single table
python3 impala_migrate.py verify -d sales -t orders

# All tables in a database
python3 impala_migrate.py verify -d sales
```

### Dry-run

```bash
python3 impala_migrate.py migrate-db -d production --dry-run
```

### Use efs-direct for external tables

```bash
python3 impala_migrate.py migrate-table \
  -d raw -t clickstream --strategy efs-direct
```

---

## Troubleshooting

### `No valid Kerberos ticket`

```bash
kinit your_principal@YOUR.REALM
# or set KRB_PRINCIPAL and KRB_KEYTAB in impala_migrate.env
```

### `impala-shell not found`

Set the full path in the env file:

```bash
IMPALA_SHELL_BIN="/opt/cloudera/parcels/CDH/bin/impala-shell"
```

### `EFS mount point does not exist`

Ensure EFS is mounted on the host running the script:

```bash
sudo mount -t nfs4 -o nfsvers=4.1 \
  fs-XXXXXXXX.efs.us-east-1.amazonaws.com:/ /mnt/efs/shared
```

### `No export manifest` during import

You must run `export-table` (or `export-db`) before `import-table`.
The import reads the `_manifest.json` created during export.

### `distcp` fails with permission errors

Ensure the Kerberos principal has read access on source HDFS and write
access on target HDFS. For cross-realm scenarios, run export and import
as separate steps with the appropriate `kinit` for each realm.

### Checksum mismatch after migration

- Verify data types match (column type changes between clusters affect hashes)
- Check for concurrent writes on the source during migration
- Re-run with `--force` for a clean copy and verify again

### Grants fail to apply

Grant replay depends on Ranger being configured on both clusters. If
only one cluster uses Ranger, grants must be applied manually through
the Ranger Admin UI.

---

## How It Handles Table Types

| Table Type | Export | Import |
|---|---|---|
| **External, non-partitioned** | DDL + data -> EFS | DDL on target, data from EFS -> target HDFS |
| **External, partitioned** | DDL + per-partition data -> EFS | DDL on target, data from EFS, `RECOVER PARTITIONS` |
| **Managed, non-partitioned** | DDL + data -> EFS | DDL on target, data from EFS -> target HDFS |
| **Managed, partitioned** | DDL + per-partition data -> EFS | DDL on target, data from EFS, `RECOVER PARTITIONS` + `COMPUTE INCREMENTAL STATS` |

For `efs-direct` strategy, the target table LOCATION is rewritten to
point directly to the EFS staging path, eliminating the second data copy.
