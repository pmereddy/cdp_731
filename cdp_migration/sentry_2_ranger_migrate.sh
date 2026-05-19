#!/usr/bin/env bash
###############################################################################
# sentry_to_ranger_migrate.sh
#
# Automates the Sentry (CDH 6.3.3) → Ranger (CDP 7.1.9 SP1) policy migration.
#
# Phases:
#   inventory    — capture Sentry roles/privileges and Ranger baseline
#   export       — run authzmigrator to produce permissions.json
#   consolidate  — consolidate policies via consolidate_policies.py
#   import       — upload to HDFS and import into Ranger
#   validate     — compare policy counts and generate reports
#
# Usage:
#   ./sentry_to_ranger_migrate.sh [--phase <phase>] [--dry-run]
#
# Requires: bash 4+, ssh, python3, curl, hdfs CLI, beeline
###############################################################################
set -euo pipefail

###############################################################################
# CONFIGURATION — edit these before running
###############################################################################

# --- Source cluster (CDH 6.3.3) ---
SOURCE_SENTRY_HOST=""             # Sentry server hostname/IP (SSH target)
SOURCE_HS2_HOST=""                # HiveServer2 hostname
SOURCE_HS2_PORT="10000"
SOURCE_SENTRY_DB_HOST=""          # Sentry metastore DB host
SOURCE_SENTRY_DB_USER="sentry"
SOURCE_SENTRY_DB_PASS=""          # Sentry DB password
SOURCE_SENTRY_DB_NAME="sentry_db"
SOURCE_HMS_DB_NAME="metastore"   # Hive Metastore DB (for ADHOC policy cross-ref)
SOURCE_REALM=""                   # e.g. PROD.EXAMPLE.COM
SOURCE_SSH_USER="ec2-user"
SOURCE_SSH_KEY="~/.ssh/id_rsa"
SOURCE_SSH_OPTS="-o BatchMode=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"

# --- Target cluster (CDP 7.1.9 SP1) ---
TARGET_RANGER_HOST=""             # Ranger Admin host
TARGET_RANGER_PORT="6182"
TARGET_RANGER_PROTO="https"
RANGER_ADMIN_USER="admin"
RANGER_ADMIN_PASS=""
TARGET_HIVE_SERVICE="cm_hive"    # Ranger service name for Hive
TARGET_HDFS_SERVICE="cm_hdfs"    # Ranger service name for HDFS
TARGET_KAFKA_SERVICE="cm_kafka"  # Ranger service name for Kafka
TARGET_SOLR_SERVICE="cm_solr"    # Ranger service name for Solr
TARGET_REALM=""                   # e.g. TEST.CARGILL.LOCAL
TARGET_RANGER_DB_HOST=""          # Ranger MySQL host (for isolation check)
TARGET_RANGER_DB_USER=""
TARGET_RANGER_DB_PASS=""
TARGET_SSH_USER="ec2-user"
TARGET_SSH_KEY="~/.ssh/id_rsa"
TARGET_SSH_OPTS="${SOURCE_SSH_OPTS}"

# --- authzmigrator ---
AUTHZ_TARBALL=""                  # path to authz_export.tar.gz
EXPORT_SERVICES="HIVE,KAFKA"     # HIVE,KAFKA,KUDU
SKIP_OWNER_POLICY="true"
EXPORT_ROLE_PERMISSIONS="true"   # include role-group mappings in export

# --- Local paths ---
BACKUP_DIR="/opt/backup/sentry_ranger_migration_$(date +%Y%m%d_%H%M%S)"
PERMISSIONS_FILE="${BACKUP_DIR}/permissions.json"

###############################################################################
# GLOBALS
###############################################################################
DRY_RUN=false
PHASE="all"
_PASS=0; _WARN=0; _FAIL=0
_TSTAMP="$(date +%Y%m%d_%H%M%S)"

###############################################################################
# REPORT HELPERS (inline — no dependency on cdp_install_719/common.sh)
###############################################################################
MD_REPORT=""
CSV_REPORT=""

init_md_report() {
  local file="$1"; shift; local title="$1"; shift
  {
    echo "# ${title}"
    echo ""
    echo "Generated: $(date '+%Y-%m-%d %H:%M:%S')"
    echo ""
    for kv in "$@"; do
      echo "- **${kv%%=*}**: ${kv#*=}"
    done
    echo ""
    echo "---"
    echo ""
  } > "${file}"
}

md_section() { printf '\n## %s\n\n' "$2" >> "$1"; }

md_table_header() {
  local file="$1"; shift
  local header="| $(printf '%s | ' "$@")"
  local sep="|"
  for _ in "$@"; do sep+="---|"; done
  { echo "${header}"; echo "${sep}"; } >> "${file}"
}

md_table_row() {
  local file="$1"; shift
  echo "| $(printf '%s | ' "$@")" >> "${file}"
}

csv_header() {
  local file="$1"; shift
  local IFS=','
  echo "$*" > "${file}"
}

csv_row() {
  local file="$1"; shift
  local line="" first=true
  for val in "$@"; do
    ${first} || line+=","
    first=false
    if [[ "${val}" == *,* || "${val}" == *\"* ]]; then
      line+="\"${val//\"/\"\"}\""
    else
      line+="${val}"
    fi
  done
  echo "${line}" >> "${file}"
}

pass()  { echo "  [PASS] $*"; ((_PASS++)) || true; }
warn_c(){ echo "  [WARN] $*"; ((_WARN++)) || true; }
fail_c(){ echo "  [FAIL] $*"; ((_FAIL++)) || true; }

banner() {
  echo ""
  echo "================================================================"
  echo "  $*"
  echo "================================================================"
}

ssh_src() {
  ssh ${SOURCE_SSH_OPTS} -i "${SOURCE_SSH_KEY}" "${SOURCE_SSH_USER}@${SOURCE_SENTRY_HOST}" "$@"
}

ssh_tgt() {
  local host="$1"; shift
  ssh ${TARGET_SSH_OPTS} -i "${TARGET_SSH_KEY}" "${TARGET_SSH_USER}@${host}" "$@"
}

ranger_api() {
  local method="$1" endpoint="$2"; shift 2
  curl -sk -u "${RANGER_ADMIN_USER}:${RANGER_ADMIN_PASS}" \
    -X "${method}" \
    "${TARGET_RANGER_PROTO}://${TARGET_RANGER_HOST}:${TARGET_RANGER_PORT}${endpoint}" \
    "$@"
}

###############################################################################
# PHASE 1: INVENTORY & BACKUP
###############################################################################
phase_inventory() {
  banner "Phase 1: Inventory & Backup"
  mkdir -p "${BACKUP_DIR}"

  MD_REPORT="${BACKUP_DIR}/migration_inventory.md"
  CSV_REPORT="${BACKUP_DIR}/migration_inventory.csv"

  init_md_report "${MD_REPORT}" "Sentry-to-Ranger Migration Inventory" \
    "Source Sentry=${SOURCE_SENTRY_HOST}" \
    "Target Ranger=${TARGET_RANGER_HOST}" \
    "Timestamp=${_TSTAMP}"
  csv_header "${CSV_REPORT}" "Category" "Item" "Count" "Details"

  # --- 1a. Sentry roles via SQL ---
  echo ">>> Capturing Sentry roles from database..."
  if [[ -n "${SOURCE_SENTRY_DB_HOST}" ]]; then
    if ${DRY_RUN}; then
      echo "  [DRY-RUN] Would query Sentry DB at ${SOURCE_SENTRY_DB_HOST}"
    else
      ssh_src "mysql -h '${SOURCE_SENTRY_DB_HOST}' \
        -u '${SOURCE_SENTRY_DB_USER}' -p'${SOURCE_SENTRY_DB_PASS}' \
        '${SOURCE_SENTRY_DB_NAME}' -e 'SELECT role_name FROM SENTRY_ROLE ORDER BY role_name;'" \
        > "${BACKUP_DIR}/sentry_roles.txt" 2>/dev/null || true

      ssh_src "mysql -h '${SOURCE_SENTRY_DB_HOST}' \
        -u '${SOURCE_SENTRY_DB_USER}' -p'${SOURCE_SENTRY_DB_PASS}' \
        '${SOURCE_SENTRY_DB_NAME}' -e \"
          SELECT r.role_name, g.group_name
          FROM SENTRY_ROLE r
          JOIN SENTRY_ROLE_GROUP_MAP rgm ON r.role_id = rgm.role_id
          JOIN SENTRY_GROUP g ON rgm.group_id = g.group_id
          ORDER BY r.role_name;\"" \
        > "${BACKUP_DIR}/sentry_role_group_map.txt" 2>/dev/null || true

      ssh_src "mysql -h '${SOURCE_SENTRY_DB_HOST}' \
        -u '${SOURCE_SENTRY_DB_USER}' -p'${SOURCE_SENTRY_DB_PASS}' \
        '${SOURCE_SENTRY_DB_NAME}' -e \"
          SELECT r.role_name, p.server_name, p.db_name, p.table_name,
                 p.column_name, p.URI, p.action, p.with_grant_option
          FROM SENTRY_ROLE r
          JOIN SENTRY_ROLE_DB_PRIVILEGE_MAP rp ON r.role_id = rp.role_id
          JOIN SENTRY_DB_PRIVILEGE p ON rp.db_privilege_id = p.db_privilege_id
          ORDER BY r.role_name, p.db_name, p.table_name;\"" \
        > "${BACKUP_DIR}/sentry_all_privileges.txt" 2>/dev/null || true

      local role_count
      role_count=$(wc -l < "${BACKUP_DIR}/sentry_roles.txt" 2>/dev/null | tr -d ' ')
      role_count=$((role_count - 1))  # subtract header
      local priv_count
      priv_count=$(wc -l < "${BACKUP_DIR}/sentry_all_privileges.txt" 2>/dev/null | tr -d ' ')
      priv_count=$((priv_count - 1))

      pass "Captured ${role_count} Sentry roles"
      pass "Captured ${priv_count} Sentry privileges"

      # Cross-reference with Hive Metastore for ADHOC policy detection
      ssh_src "mysql -h '${SOURCE_SENTRY_DB_HOST}' \
        -u '${SOURCE_SENTRY_DB_USER}' -p'${SOURCE_SENTRY_DB_PASS}' -e \"
          SELECT sr.role_name, sdp.privilege_scope, sdp.db_name,
                 sdp.table_name, sdp.column_name, dbs.DB_LOCATION_URI AS uri, sdp.action
          FROM ${SOURCE_SENTRY_DB_NAME}.SENTRY_ROLE sr
          INNER JOIN ${SOURCE_SENTRY_DB_NAME}.SENTRY_ROLE_DB_PRIVILEGE_MAP srdpm
              ON sr.role_id = srdpm.role_id
          INNER JOIN ${SOURCE_SENTRY_DB_NAME}.SENTRY_DB_PRIVILEGE sdp
              ON sdp.db_privilege_id = srdpm.db_privilege_id
          INNER JOIN ${SOURCE_HMS_DB_NAME}.DBS dbs
              ON sdp.db_name = dbs.NAME
          WHERE sdp.privilege_scope IN ('DATABASE','COLUMN','TABLE')
          ORDER BY sr.role_name, sdp.db_name;\"" \
        > "${BACKUP_DIR}/sentry_hms_cross_ref.txt" 2>/dev/null || true

      local xref_count
      xref_count=$(wc -l < "${BACKUP_DIR}/sentry_hms_cross_ref.txt" 2>/dev/null | tr -d ' ')
      xref_count=$((xref_count - 1))
      pass "Captured ${xref_count} Sentry-HMS cross-references (ADHOC policy detection)"

      local rgm_count
      rgm_count=$(wc -l < "${BACKUP_DIR}/sentry_role_group_map.txt" 2>/dev/null | tr -d ' ')
      rgm_count=$((rgm_count - 1))

      md_section "${MD_REPORT}" "Sentry Inventory (Source)"
      md_table_header "${MD_REPORT}" "Metric" "Value"
      md_table_row "${MD_REPORT}" "Roles" "${role_count}"
      md_table_row "${MD_REPORT}" "Privileges" "${priv_count}"
      md_table_row "${MD_REPORT}" "Role-Group Mappings" "${rgm_count}"
      md_table_row "${MD_REPORT}" "HMS Cross-References" "${xref_count}"
      csv_row "${CSV_REPORT}" "Sentry" "Roles" "${role_count}" ""
      csv_row "${CSV_REPORT}" "Sentry" "Privileges" "${priv_count}" ""
      csv_row "${CSV_REPORT}" "Sentry" "Role-Group Mappings" "${rgm_count}" ""
      csv_row "${CSV_REPORT}" "Sentry" "HMS Cross-References" "${xref_count}" ""
    fi
  else
    warn_c "SOURCE_SENTRY_DB_HOST not set — skipping direct DB inventory"
  fi

  # --- 1b. Check Ranger MySQL isolation ---
  echo ""
  echo ">>> Checking Ranger MySQL isolation level..."
  if [[ -n "${TARGET_RANGER_DB_HOST}" ]] && ! ${DRY_RUN}; then
    local isolation
    isolation=$(ssh_tgt "${TARGET_RANGER_HOST}" "mysql -N -h '${TARGET_RANGER_DB_HOST}' \
      -u '${TARGET_RANGER_DB_USER}' -p'${TARGET_RANGER_DB_PASS}' \
      -e \"SELECT @@GLOBAL.tx_isolation;\"" 2>/dev/null || echo "UNKNOWN")
    isolation=$(echo "${isolation}" | tr -d '[:space:]')
    if [[ "${isolation}" == "READ-COMMITTED" ]]; then
      pass "Ranger MySQL isolation: ${isolation}"
    elif [[ "${isolation}" == "REPEATABLE-READ" ]]; then
      fail_c "Ranger MySQL isolation is REPEATABLE-READ — must be READ-COMMITTED before import"
      echo "  Fix: SET GLOBAL tx_isolation = 'READ-COMMITTED';"
    else
      warn_c "Could not determine Ranger MySQL isolation: ${isolation}"
    fi
    md_section "${MD_REPORT}" "Ranger MySQL Isolation"
    md_table_header "${MD_REPORT}" "Check" "Value"
    md_table_row "${MD_REPORT}" "tx_isolation" "${isolation}"
    csv_row "${CSV_REPORT}" "Ranger DB" "tx_isolation" "" "${isolation}"
  elif [[ -z "${TARGET_RANGER_DB_HOST}" ]]; then
    warn_c "TARGET_RANGER_DB_HOST not set — skipping MySQL isolation check"
  fi

  # --- 1c. Ranger baseline export ---
  echo ""
  echo ">>> Capturing Ranger baseline policies..."
  if [[ -n "${TARGET_RANGER_HOST}" ]]; then
    if ${DRY_RUN}; then
      echo "  [DRY-RUN] Would export Ranger baseline from ${TARGET_RANGER_HOST}"
    else
      local svc_list=("${TARGET_HIVE_SERVICE}" "${TARGET_HDFS_SERVICE}" "${TARGET_KAFKA_SERVICE}" "${TARGET_SOLR_SERVICE}")
      md_section "${MD_REPORT}" "Ranger Baseline (Target)"
      md_table_header "${MD_REPORT}" "Service" "Policy Count"

      for svc in "${svc_list[@]}"; do
        local out="${BACKUP_DIR}/ranger_${svc}_baseline.json"
        ranger_api GET "/service/plugins/policies/exportJson?serviceName=${svc}" -o "${out}" 2>/dev/null || true

        if [[ -s "${out}" ]]; then
          local cnt
          cnt=$(python3 -c "import json; print(len(json.load(open('${out}')).get('policies',[])))" 2>/dev/null || echo 0)
          pass "Ranger ${svc}: ${cnt} baseline policies"
          md_table_row "${MD_REPORT}" "${svc}" "${cnt}"
          csv_row "${CSV_REPORT}" "Ranger Baseline" "${svc}" "${cnt}" ""
        else
          warn_c "Ranger ${svc}: no policies exported (service may not exist)"
          md_table_row "${MD_REPORT}" "${svc}" "N/A"
        fi
      done
    fi
  else
    warn_c "TARGET_RANGER_HOST not set — skipping Ranger baseline"
  fi

  echo ""
  echo "Inventory files saved to: ${BACKUP_DIR}/"
  pass "Phase 1 complete"
}

###############################################################################
# PHASE 2: EXPORT SENTRY PERMISSIONS
###############################################################################
phase_export() {
  banner "Phase 2: Export Sentry Permissions (authzmigrator)"

  if [[ -z "${AUTHZ_TARBALL}" || ! -f "${AUTHZ_TARBALL}" ]]; then
    echo "ERROR: AUTHZ_TARBALL not set or file not found: '${AUTHZ_TARBALL}'"
    echo "       Download authz_export.tar.gz from Cloudera Support."
    fail_c "authz_export.tar.gz not available"
    return 1
  fi

  if ${DRY_RUN}; then
    echo "  [DRY-RUN] Would upload authz_export.tar.gz to ${SOURCE_SENTRY_HOST}"
    echo "  [DRY-RUN] Would configure and run authz_export.sh"
    return 0
  fi

  echo ">>> Uploading authzmigrator to Sentry host..."
  scp ${SOURCE_SSH_OPTS} -i "${SOURCE_SSH_KEY}" \
    "${AUTHZ_TARBALL}" \
    "${SOURCE_SSH_USER}@${SOURCE_SENTRY_HOST}:/tmp/authz_export.tar.gz"

  echo ">>> Configuring and running authzmigrator on ${SOURCE_SENTRY_HOST}..."
  ssh_src "sudo bash -s" <<REMOTE_EXPORT
set -euo pipefail

cd /opt
rm -rf authzmigrator_work
mkdir -p authzmigrator_work && cd authzmigrator_work
tar xzf /tmp/authz_export.tar.gz

# Locate Sentry process directory
SENTRY_PROC=\$(ls -td /var/run/cloudera-scm-agent/process/*-sentry-SENTRY_SERVER 2>/dev/null | head -1)
if [[ -z "\${SENTRY_PROC}" ]]; then
  echo "ERROR: Cannot find Sentry process directory"
  exit 1
fi
echo "Sentry process dir: \${SENTRY_PROC}"

cd authzmigrator 2>/dev/null || cd */  # handle nested dir

# Copy configs
cp "\${SENTRY_PROC}/sentry-site.xml" config/sentry-site.xml
cp "\${SENTRY_PROC}/core-site.xml"   config/core-site.xml

# Patch sentry-site.xml — update DB credentials, remove credential provider
python3 -c "
import xml.etree.ElementTree as ET
tree = ET.parse('config/sentry-site.xml')
root = tree.getroot()
for prop in root.findall('property'):
    name = prop.find('name').text
    if name == 'sentry.store.jdbc.user':
        prop.find('value').text = '${SOURCE_SENTRY_DB_USER}'
    elif name == 'sentry.store.jdbc.password':
        prop.find('value').text = '${SOURCE_SENTRY_DB_PASS}'
    elif name == 'hadoop.security.credential.provider.path':
        root.remove(prop)
tree.write('config/sentry-site.xml', xml_declaration=True, encoding='UTF-8')
"

# Patch core-site.xml — set fs.defaultFS to file:///, remove credential provider
python3 -c "
import xml.etree.ElementTree as ET
tree = ET.parse('config/core-site.xml')
root = tree.getroot()
for prop in root.findall('property'):
    name = prop.find('name').text
    if name == 'fs.defaultFS':
        prop.find('value').text = 'file:///'
    elif name == 'hadoop.security.credential.provider.path':
        root.remove(prop)
tree.write('config/core-site.xml', xml_declaration=True, encoding='UTF-8')
"

# Patch authorization-migration-site.xml
python3 -c "
import xml.etree.ElementTree as ET
tree = ET.parse('config/authorization-migration-site.xml')
root = tree.getroot()
props = {p.find('name').text: p for p in root.findall('property')}

def set_prop(name, value):
    if name in props:
        props[name].find('value').text = value
    else:
        prop = ET.SubElement(root, 'property')
        ET.SubElement(prop, 'name').text = name
        ET.SubElement(prop, 'value').text = value

set_prop('authorization.migration.export.target_services', '${EXPORT_SERVICES}')
set_prop('authorization.migration.export.output_file', '/opt/backup/permissions.json')
set_prop('authorization.migration.skip.owner.policy', '${SKIP_OWNER_POLICY}')
set_prop('authorization.migration.role.permissions', '${EXPORT_ROLE_PERMISSIONS}')
tree.write('config/authorization-migration-site.xml', xml_declaration=True, encoding='UTF-8')
"

# Ensure output dir exists
mkdir -p /opt/backup

# Run export
echo ">>> Running authz_export.sh..."
sh authz_export.sh 2>&1

echo ">>> Export complete."
ls -lh /opt/backup/permissions.json
REMOTE_EXPORT

  echo ">>> Downloading permissions.json..."
  mkdir -p "${BACKUP_DIR}"
  scp ${SOURCE_SSH_OPTS} -i "${SOURCE_SSH_KEY}" \
    "${SOURCE_SSH_USER}@${SOURCE_SENTRY_HOST}:/opt/backup/permissions.json" \
    "${PERMISSIONS_FILE}"

  echo ">>> Validating exported permissions..."
  python3 <<PY
import json, sys

with open("${PERMISSIONS_FILE}") as f:
    data = json.load(f)

rgm = data.get("roleGroupMapping", {})
print(f"Role-Group mappings: {len(rgm)}")
if len(rgm) == 0:
    print("WARNING: No role-group mappings found!", file=sys.stderr)
    print("  Check authorization.migration.role.permissions=true in config", file=sys.stderr)

db_policies = data.get("dbPolicies", [])
policies    = data.get("policies", [])
kafka       = data.get("kafkaPolicies", [])
all_p       = db_policies or policies
print(f"Hive/Impala policies: {len(all_p)}")
print(f"Kafka policies: {len(kafka)}")

by_scope = {}
for p in all_p:
    scope = p.get("resource", {}).get("authorizableType", "unknown") if "resource" in p else p.get("serviceType", "unknown")
    by_scope[scope] = by_scope.get(scope, 0) + 1
for scope, cnt in sorted(by_scope.items()):
    print(f"  {scope}: {cnt}")

groups = set()
for p in all_p:
    for item_list_key in ("policyItems", "allowExceptions", "denyPolicyItems"):
        for item in p.get(item_list_key, []):
            for g in item.get("groups", []):
                groups.add(g)
    for perm in p.get("permissions", []):
        for pr in perm.get("principals", []):
            if pr.get("principalType") == "ROLE":
                groups.update(rgm.get(pr["principalName"], []))

print(f"Unique groups referenced: {len(groups)}")
for g in sorted(groups)[:20]:
    print(f"  - {g}")
if len(groups) > 20:
    print(f"  ... and {len(groups) - 20} more")

if len(all_p) == 0 and len(kafka) == 0:
    print("WARNING: No policies exported!", file=sys.stderr)
    sys.exit(1)
PY

  pass "Permissions exported: ${PERMISSIONS_FILE}"
  pass "Phase 2 complete"
}

###############################################################################
# PHASE 2.5: CONSOLIDATE POLICIES
###############################################################################
phase_consolidate() {
  banner "Phase 2.5: Consolidate Sentry Policies"

  local script_dir
  script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  local consolidate_py="${script_dir}/consolidate_policies.py"

  if [[ ! -f "${consolidate_py}" ]]; then
    fail_c "consolidate_policies.py not found at ${consolidate_py}"
    return 1
  fi

  local perms_file="${PERMISSIONS_FILE}"
  if [[ ! -f "${perms_file}" ]]; then
    perms_file=$(ls -t /opt/backup/sentry_ranger_migration_*/permissions.json 2>/dev/null | head -1)
  fi

  local consol_dir="${BACKUP_DIR}/consolidation"
  mkdir -p "${consol_dir}"

  local consol_json="${consol_dir}/consolidated_policies.json"
  local consol_report="${consol_dir}/consolidation_report.md"
  local consol_csv="${consol_dir}/consolidation_report.csv"

  local py_args=()

  if [[ -n "${perms_file}" && -f "${perms_file}" ]]; then
    echo ">>> Using permissions.json: ${perms_file}"
    py_args+=(--input "${perms_file}")
  else
    echo ">>> permissions.json not found — looking for CSV fallback..."
    local access_csv objects_csv
    access_csv=$(find "${BACKUP_DIR}" -name "sentry_access.csv" 2>/dev/null | head -1)
    objects_csv=$(find "${BACKUP_DIR}" -name "sentry_objects.csv" 2>/dev/null | head -1)

    if [[ -z "${access_csv}" ]]; then
      warn_c "No permissions.json or sentry_access.csv found in ${BACKUP_DIR}"
      echo "  Provide --input or --from-csv data and re-run."
      return 1
    fi

    echo ">>> Using CSV: ${access_csv}"
    py_args+=(--from-csv --access-csv "${access_csv}")
    [[ -n "${objects_csv}" ]] && py_args+=(--objects-csv "${objects_csv}")
  fi

  py_args+=(
    --output "${consol_json}"
    --report "${consol_report}"
    --report-csv "${consol_csv}"
    --hive-service "${TARGET_HIVE_SERVICE}"
  )

  if ${DRY_RUN}; then
    py_args+=(--dry-run)
  fi

  echo ">>> Running consolidation..."
  echo "    python3 ${consolidate_py} ${py_args[*]}"
  echo ""

  if python3 "${consolidate_py}" "${py_args[@]}"; then
    pass "Consolidation complete"
    echo ""
    echo "  Report:    ${consol_report}"
    echo "  CSV:       ${consol_csv}"
    if ! ${DRY_RUN}; then
      echo "  Ranger JSON: ${consol_json}"
      PERMISSIONS_FILE="${consol_json}"
      echo ""
      echo "  PERMISSIONS_FILE updated to consolidated output."
      echo "  Subsequent import phase will use: ${PERMISSIONS_FILE}"
    fi
  else
    fail_c "Consolidation failed"
    return 1
  fi
}

###############################################################################
# PHASE 3: IMPORT INTO RANGER
###############################################################################
phase_import() {
  banner "Phase 3: Import into Ranger"

  local perms_file="${PERMISSIONS_FILE}"
  if [[ ! -f "${perms_file}" ]]; then
    perms_file=$(ls -t /opt/backup/sentry_ranger_migration_*/permissions.json 2>/dev/null | head -1)
    if [[ -z "${perms_file}" || ! -f "${perms_file}" ]]; then
      echo "ERROR: permissions.json not found. Run --phase export first."
      fail_c "permissions.json missing"
      return 1
    fi
  fi

  echo "Using permissions file: ${perms_file}"

  if ${DRY_RUN}; then
    echo "  [DRY-RUN] Would upload ${perms_file} to HDFS"
    echo "  [DRY-RUN] Would import into Ranger at ${TARGET_RANGER_HOST}"
    return 0
  fi

  # Upload to target host
  echo ">>> Copying permissions.json to target cluster..."
  scp ${TARGET_SSH_OPTS} -i "${TARGET_SSH_KEY}" \
    "${perms_file}" \
    "${TARGET_SSH_USER}@${TARGET_RANGER_HOST}:/tmp/permissions.json"

  # Upload to HDFS
  echo ">>> Uploading to HDFS..."
  ssh_tgt "${TARGET_RANGER_HOST}" "
    hdfs dfs -mkdir -p /user/sentry/export-permissions
    hdfs dfs -put -f /tmp/permissions.json /user/sentry/export-permissions/permissions.json
    hdfs dfs -setfacl -m -R user:ranger:rwx /user/sentry/export-permissions/permissions.json
    echo 'HDFS upload complete:'
    hdfs dfs -ls /user/sentry/export-permissions/
  "

  # Import via REST API
  echo ">>> Importing policies via Ranger REST API..."
  local resp
  resp=$(ranger_api POST \
    "/service/plugins/policies/importPoliciesFromFile?serviceName=${TARGET_HIVE_SERVICE}&updateIfExists=true" \
    -H "Content-Type: multipart/form-data" \
    -F "file=@/tmp/permissions.json" 2>&1) || true
  echo "Import response: ${resp}"

  # Verify
  echo ""
  echo ">>> Verifying import..."
  local post_export="${BACKUP_DIR}/ranger_${TARGET_HIVE_SERVICE}_post_import.json"
  ranger_api GET \
    "/service/plugins/policies/exportJson?serviceName=${TARGET_HIVE_SERVICE}" \
    -o "${post_export}" 2>/dev/null || true

  if [[ -s "${post_export}" ]]; then
    python3 <<PY
import json, glob, os

post = json.load(open("${post_export}"))
post_cnt = len(post.get("policies", []))

baselines = sorted(glob.glob("${BACKUP_DIR}/ranger_${TARGET_HIVE_SERVICE}_baseline.json"))
if not baselines:
    baselines = sorted(glob.glob("/opt/backup/sentry_ranger_migration_*/ranger_${TARGET_HIVE_SERVICE}_baseline.json"))

if baselines:
    base = json.load(open(baselines[-1]))
    base_cnt = len(base.get("policies", []))
else:
    base_cnt = 0

print(f"Hive baseline policies:    {base_cnt}")
print(f"Hive post-import policies: {post_cnt}")
print(f"New policies added:        {post_cnt - base_cnt}")
PY
    pass "Policies imported into Ranger"
  else
    warn_c "Could not verify import — check Ranger UI manually"
  fi

  pass "Phase 3 complete"
}

###############################################################################
# PHASE 5: VALIDATION & COMPARISON
###############################################################################
phase_validate() {
  banner "Phase 5: Validation & Comparison"

  local val_dir="${BACKUP_DIR}"
  if [[ ! -d "${val_dir}" ]]; then
    val_dir=$(ls -td /opt/backup/sentry_ranger_migration_* 2>/dev/null | head -1)
  fi
  mkdir -p "${val_dir}"

  local perms_file="${val_dir}/permissions.json"
  if [[ ! -f "${perms_file}" ]]; then
    perms_file=$(ls -t /opt/backup/sentry_ranger_migration_*/permissions.json 2>/dev/null | head -1)
  fi

  MD_REPORT="${val_dir}/migration_validation_report.md"
  CSV_REPORT="${val_dir}/migration_validation_report.csv"

  init_md_report "${MD_REPORT}" "Sentry→Ranger Migration Validation" \
    "Source=${SOURCE_SENTRY_HOST}" \
    "Target Ranger=${TARGET_RANGER_HOST}" \
    "Timestamp=$(date '+%Y-%m-%d %H:%M:%S')"
  csv_header "${CSV_REPORT}" "Section" "Check" "Expected" "Actual" "Result"

  # --- 5.1 Policy count comparison ---
  echo ""
  echo ">>> 5.1 Policy count comparison..."
  md_section "${MD_REPORT}" "Policy Count Comparison"

  if [[ -f "${perms_file}" ]]; then
    python3 - "${perms_file}" "${val_dir}" "${TARGET_HIVE_SERVICE}" "${TARGET_KAFKA_SERVICE}" "${MD_REPORT}" "${CSV_REPORT}" <<'VALIDATE_PY'
import json, glob, os, sys

perms_file    = sys.argv[1]
val_dir       = sys.argv[2]
hive_svc      = sys.argv[3]
kafka_svc     = sys.argv[4]
md_file       = sys.argv[5]
csv_file      = sys.argv[6]

with open(perms_file) as f:
    sentry = json.load(f)

sentry_by_type = {}
for p in sentry.get("policies", []):
    svc = p.get("serviceType", "unknown")
    sentry_by_type[svc] = sentry_by_type.get(svc, 0) + 1

ranger_files = glob.glob(os.path.join(val_dir, "ranger_*_post_import.json"))
if not ranger_files:
    ranger_files = []
    for d in sorted(glob.glob("/opt/backup/sentry_ranger_migration_*")):
        ranger_files.extend(glob.glob(os.path.join(d, "ranger_*_post_import.json")))

ranger_counts = {}
for f in ranger_files:
    svc = os.path.basename(f).replace("ranger_","").replace("_post_import.json","")
    with open(f) as fh:
        data = json.load(fh)
    ranger_counts[svc] = len(data.get("policies", []))

with open(md_file, "a") as mf:
    mf.write("| Service Type | Sentry Policies | Ranger Policies | Delta |\n")
    mf.write("|---|---|---|---|\n")

    all_svcs = sorted(set(list(sentry_by_type.keys()) + list(ranger_counts.keys())))
    for svc in all_svcs:
        s = sentry_by_type.get(svc, 0)
        r = ranger_counts.get(svc, 0)
        delta = r - s
        mf.write(f"| {svc} | {s} | {r} | {delta:+d} |\n")
        print(f"  {svc:<20} Sentry: {s:>5}  Ranger: {r:>5}  Delta: {delta:+d}")

with open(csv_file, "a") as cf:
    all_svcs = sorted(set(list(sentry_by_type.keys()) + list(ranger_counts.keys())))
    for svc in all_svcs:
        s = sentry_by_type.get(svc, 0)
        r = ranger_counts.get(svc, 0)
        result = "PASS" if r >= s else "WARN"
        cf.write(f"Policy Count,{svc},{s},{r},{result}\n")

# Detailed policy listing
with open(md_file, "a") as mf:
    mf.write("\n## Exported Policy Details\n\n")
    mf.write("| Service | Policy Name | Resources | Groups | Actions |\n")
    mf.write("|---|---|---|---|---|\n")

    for p in sentry.get("policies", []):
        name     = p.get("name", "unnamed")
        svc_type = p.get("serviceType", "unknown")
        resources = p.get("resources", {})
        res_str  = "; ".join(f"{k}={','.join(v.get('values',[]))}" for k, v in resources.items())
        groups   = set()
        actions  = set()
        for item in p.get("policyItems", []):
            for g in item.get("groups", []): groups.add(g)
            for a in item.get("accesses", []):
                if a.get("isAllowed"): actions.add(a.get("type",""))
        g_str = ", ".join(sorted(groups))
        a_str = ", ".join(sorted(actions))
        mf.write(f"| {svc_type} | {name} | {res_str} | {g_str} | {a_str} |\n")

VALIDATE_PY
    pass "Policy count comparison complete"
  else
    warn_c "permissions.json not found — cannot compare counts"
  fi

  # --- 5.2 Ranger policy export for manual review ---
  echo ""
  echo ">>> 5.2 Exporting current Ranger policies for review..."
  md_section "${MD_REPORT}" "Ranger Policy Snapshots"

  if [[ -n "${TARGET_RANGER_HOST}" ]] && ! ${DRY_RUN}; then
    for svc in "${TARGET_HIVE_SERVICE}" "${TARGET_HDFS_SERVICE}" "${TARGET_KAFKA_SERVICE}"; do
      local out="${val_dir}/ranger_${svc}_current.json"
      ranger_api GET "/service/plugins/policies/exportJson?serviceName=${svc}" -o "${out}" 2>/dev/null || true
      if [[ -s "${out}" ]]; then
        local cnt
        cnt=$(python3 -c "import json; print(len(json.load(open('${out}')).get('policies',[])))" 2>/dev/null || echo 0)
        pass "Ranger ${svc}: ${cnt} policies (snapshot saved)"
        md_table_row "${MD_REPORT}" "${svc}" "${cnt} policies"
        csv_row "${CSV_REPORT}" "Ranger Current" "${svc}" "" "${cnt}" "OK"
      fi
    done
  fi

  # --- 5.3 Group/user mapping check ---
  echo ""
  echo ">>> 5.3 Checking user/group sync in Ranger..."
  md_section "${MD_REPORT}" "User/Group Sync"

  if [[ -n "${TARGET_RANGER_HOST}" ]] && ! ${DRY_RUN}; then
    local groups_json
    groups_json=$(ranger_api GET "/service/xusers/groups?pageSize=500" 2>/dev/null) || true
    if [[ -n "${groups_json}" ]]; then
      local group_count
      group_count=$(echo "${groups_json}" | python3 -c "import json,sys; print(json.load(sys.stdin).get('totalCount',0))" 2>/dev/null || echo "?")
      pass "Ranger has ${group_count} groups synced"
      md_table_header "${MD_REPORT}" "Metric" "Value"
      md_table_row "${MD_REPORT}" "Synced Groups" "${group_count}"
      csv_row "${CSV_REPORT}" "User/Group Sync" "Group count" "" "${group_count}" "INFO"
    fi
  fi

  # --- 5.4 Audit log check ---
  echo ""
  echo ">>> 5.4 Checking Ranger audit logs..."
  md_section "${MD_REPORT}" "Audit Status"

  if [[ -n "${TARGET_RANGER_HOST}" ]] && ! ${DRY_RUN}; then
    local audit_json
    audit_json=$(ranger_api GET "/service/assets/accessAudit?pageSize=5" 2>/dev/null) || true
    if [[ -n "${audit_json}" ]]; then
      local audit_total
      audit_total=$(echo "${audit_json}" | python3 -c "import json,sys; print(json.load(sys.stdin).get('totalCount',0))" 2>/dev/null || echo "?")
      pass "Ranger audit has ${audit_total} entries"
      md_table_header "${MD_REPORT}" "Metric" "Value"
      md_table_row "${MD_REPORT}" "Total Audit Entries" "${audit_total}"
      csv_row "${CSV_REPORT}" "Audit" "Entry count" "" "${audit_total}" "INFO"
    else
      warn_c "Could not retrieve audit data"
    fi
  fi

  # --- Summary ---
  echo ""
  md_section "${MD_REPORT}" "Validation Summary"
  md_table_header "${MD_REPORT}" "Result" "Count"
  md_table_row "${MD_REPORT}" "PASS" "${_PASS}"
  md_table_row "${MD_REPORT}" "WARN" "${_WARN}"
  md_table_row "${MD_REPORT}" "FAIL" "${_FAIL}"

  csv_row "${CSV_REPORT}" "Summary" "PASS" "" "${_PASS}" ""
  csv_row "${CSV_REPORT}" "Summary" "WARN" "" "${_WARN}" ""
  csv_row "${CSV_REPORT}" "Summary" "FAIL" "" "${_FAIL}" ""

  echo ""
  echo "================================================================"
  echo "  Validation Results:  PASS=${_PASS}  WARN=${_WARN}  FAIL=${_FAIL}"
  echo "================================================================"
  echo ""
  echo "Reports:"
  echo "  Markdown : ${MD_REPORT}"
  echo "  CSV      : ${CSV_REPORT}"
  echo "  Files    : ${val_dir}/"
}

###############################################################################
# MAIN
###############################################################################
usage() {
  cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Options:
  --phase <phase>   Run a specific phase (see below); default: all
  --dry-run         Show what would be done without making changes
  --backup-dir <d>  Override backup directory
  -h, --help        Show this help

Phases:
  inventory     Phase 1:   Capture Sentry and Ranger inventory
  export        Phase 2:   Export Sentry permissions via authzmigrator
  consolidate   Phase 2.5: Consolidate policies (consolidate_policies.py)
  import        Phase 3:   Import permissions into Ranger
  validate      Phase 5:   Validate and compare policies
  all           Run all phases sequentially
EOF
}

main() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --phase)     PHASE="$2"; shift 2 ;;
      --dry-run)   DRY_RUN=true; shift ;;
      --backup-dir) BACKUP_DIR="$2"; shift 2 ;;
      -h|--help)   usage; exit 0 ;;
      *) echo "Unknown option: $1"; usage; exit 1 ;;
    esac
  done

  mkdir -p "${BACKUP_DIR}"

  banner "Sentry → Ranger Migration"
  echo "  Phase     : ${PHASE}"
  echo "  Dry-run   : ${DRY_RUN}"
  echo "  Backup dir: ${BACKUP_DIR}"
  echo "  Timestamp : ${_TSTAMP}"
  echo ""

  if ${DRY_RUN}; then
    echo "  *** DRY-RUN MODE — no changes will be made ***"
    echo ""
  fi

  case "${PHASE}" in
    inventory)   phase_inventory ;;
    export)      phase_export ;;
    consolidate) phase_consolidate ;;
    import)      phase_import ;;
    validate)    phase_validate ;;
    all)
      phase_inventory
      phase_export
      phase_consolidate
      phase_import
      phase_validate
      ;;
    *)
      echo "Unknown phase: ${PHASE}"
      usage
      exit 1
      ;;
  esac

  echo ""
  banner "Migration Complete"
  echo "  Results: PASS=${_PASS}  WARN=${_WARN}  FAIL=${_FAIL}"
  echo "  Backup : ${BACKUP_DIR}/"
  echo ""
}

main "$@"
