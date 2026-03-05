#!/usr/bin/env bash
###############################################################################
# hive_migrate.sh -- Export/Import Hive managed tables between CDP clusters
#
# Usage:
#   ./hive_migrate.sh export <database> <table>
#   ./hive_migrate.sh import <database> <table>
#   ./hive_migrate.sh export-db <database>
#   ./hive_migrate.sh import-db <database>
#
# Workflow:
#   export:  Hive EXPORT TABLE -> HDFS -> EFS
#   import:  EFS -> HDFS -> Hive IMPORT TABLE
###############################################################################
set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration -- edit these for your environment
# ---------------------------------------------------------------------------
SOURCE_BEELINE_URL="jdbc:hive2://drona-hive.cargill.com:10000/default;principal=hive/drona-hive.cargill.com@NA.CORP.CARGILL.COM;ssl=true"
TARGET_BEELINE_URL="jdbc:hive2://target-hive.cargill.com:10000/default;principal=hive/target-hive.cargill.com@NA.CORP.CARGILL.COM;ssl=true"

HDFS_EXPORT_BASE="/user/p426976/hive_export"
EFS_BASE="/efs/home/p426976/drona_migration/hive"

HDFS_BIN="hdfs"
# ---------------------------------------------------------------------------

log() { echo "$(date '+%Y-%m-%d %H:%M:%S')  $*"; }

usage() {
    echo "Usage:"
    echo "  $0 export    <database> <table>"
    echo "  $0 import    <database> <table>"
    echo "  $0 export-db <database>"
    echo "  $0 import-db <database>"
    exit 1
}

export_table() {
    local db="$1"
    local tbl="$2"
    local hdfs_path="${HDFS_EXPORT_BASE}/${db}/${tbl}"
    local efs_path="${EFS_BASE}/${db}/${tbl}"

    log "=== Exporting ${db}.${tbl} ==="

    # Clean previous export if exists
    log "Cleaning previous HDFS export path: ${hdfs_path}"
    ${HDFS_BIN} dfs -rm -r -f "${hdfs_path}" 2>/dev/null || true

    # Step 1: Hive EXPORT TABLE to HDFS
    log "Step 1/2: EXPORT TABLE ${db}.${tbl} -> ${hdfs_path}"
    beeline -u "${SOURCE_BEELINE_URL}" --silent=true -e \
        "EXPORT TABLE \`${db}\`.\`${tbl}\` TO '${hdfs_path}'" 2>&1
    if [ $? -ne 0 ]; then
        log "ERROR: EXPORT TABLE failed for ${db}.${tbl}"
        return 1
    fi

    # Step 2: Copy from HDFS to EFS
    log "Step 2/2: HDFS -> EFS: ${efs_path}"
    mkdir -p "$(dirname "${efs_path}")"
    rm -rf "${efs_path}"
    ${HDFS_BIN} dfs -get "${hdfs_path}" "${efs_path}"

    log "OK: ${db}.${tbl} exported to ${efs_path}"
}

import_table() {
    local db="$1"
    local tbl="$2"
    local efs_path="${EFS_BASE}/${db}/${tbl}"
    local hdfs_path="${HDFS_EXPORT_BASE}/${db}/${tbl}"

    if [ ! -d "${efs_path}" ]; then
        log "ERROR: No export found at ${efs_path}. Run 'export' first."
        return 1
    fi

    log "=== Importing ${db}.${tbl} ==="

    # Step 1: Copy from EFS to target HDFS
    log "Step 1/3: EFS -> HDFS: ${hdfs_path}"
    ${HDFS_BIN} dfs -rm -r -f "${hdfs_path}" 2>/dev/null || true
    ${HDFS_BIN} dfs -mkdir -p "$(dirname "${hdfs_path}")"
    ${HDFS_BIN} dfs -put "${efs_path}" "${hdfs_path}"

    # Step 2: Create database on target
    log "Step 2/3: Creating database ${db} on target"
    beeline -u "${TARGET_BEELINE_URL}" --silent=true -e \
        "CREATE DATABASE IF NOT EXISTS \`${db}\`" 2>&1 || true

    # Step 3: Hive IMPORT TABLE from HDFS
    log "Step 3/3: IMPORT TABLE ${db}.${tbl} <- ${hdfs_path}"
    beeline -u "${TARGET_BEELINE_URL}" --silent=true -e \
        "IMPORT TABLE \`${db}\`.\`${tbl}\` FROM '${hdfs_path}'" 2>&1
    if [ $? -ne 0 ]; then
        log "ERROR: IMPORT TABLE failed for ${db}.${tbl}"
        return 1
    fi

    log "OK: ${db}.${tbl} imported"
}

export_database() {
    local db="$1"
    log "=== Exporting all tables in ${db} ==="

    local tables
    tables=$(beeline -u "${SOURCE_BEELINE_URL}" \
        --outputformat=csv2 --showHeader=false --silent=true \
        -e "SHOW TABLES IN \`${db}\`" 2>/dev/null | grep -v '^\s*$')

    local total
    total=$(echo "${tables}" | wc -l | tr -d ' ')
    log "Found ${total} tables in ${db}"

    local idx=0
    local ok=0
    local fail=0
    echo "${tables}" | while read -r tbl; do
        [ -z "${tbl}" ] && continue
        idx=$((idx + 1))
        log "[${idx}/${total}] ${db}.${tbl}"
        if export_table "${db}" "${tbl}"; then
            ok=$((ok + 1))
        else
            fail=$((fail + 1))
            echo "${db}.${tbl}" >> "${EFS_BASE}/_export_failures.txt"
        fi
    done

    log "=== Export complete for ${db} ==="
}

import_database() {
    local db="$1"
    local db_dir="${EFS_BASE}/${db}"

    if [ ! -d "${db_dir}" ]; then
        log "ERROR: No exports found at ${db_dir}"
        return 1
    fi

    log "=== Importing all tables in ${db} ==="

    local tables
    tables=$(ls -1 "${db_dir}" | grep -v '^_')

    local total
    total=$(echo "${tables}" | wc -l | tr -d ' ')
    log "Found ${total} exported tables for ${db}"

    local idx=0
    echo "${tables}" | while read -r tbl; do
        [ -z "${tbl}" ] && continue
        idx=$((idx + 1))
        log "[${idx}/${total}] ${db}.${tbl}"
        if ! import_table "${db}" "${tbl}"; then
            echo "${db}.${tbl}" >> "${EFS_BASE}/_import_failures.txt"
        fi
    done

    log "=== Import complete for ${db} ==="
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
[ $# -lt 2 ] && usage

CMD="$1"
DB="$2"
TBL="${3:-}"

case "${CMD}" in
    export)
        [ -z "${TBL}" ] && usage
        export_table "${DB}" "${TBL}"
        ;;
    import)
        [ -z "${TBL}" ] && usage
        import_table "${DB}" "${TBL}"
        ;;
    export-db)
        export_database "${DB}"
        ;;
    import-db)
        import_database "${DB}"
        ;;
    *)
        usage
        ;;
esac
