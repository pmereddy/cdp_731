#!/usr/bin/env bash
###############################################################################
# 02_gather_rds_info.sh
#
# Gather detailed information about the MySQL/MariaDB RDS instance that will
# back the CDP metadata databases.
#
# Requires:
#   - AWS CLI v2 with appropriate IAM permissions
#   - mysql client installed on the bastion / jump host
#   - .env populated with RDS_INSTANCE_ID, DB_HOST, DB_ADMIN_USER, DB_ADMIN_PASS
###############################################################################
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"
load_env

require_vars RDS_INSTANCE_ID AWS_REGION DB_HOST DB_PORT DB_ADMIN_USER DB_ADMIN_PASS

OUT_DIR="${SCRIPT_DIR}/output"
mkdir -p "${OUT_DIR}"
REPORT="${OUT_DIR}/rds_instance_report.txt"

banner "Phase 1b: Gather RDS Instance Information"

# ---------------------------------------------------------------------------
# Part 1 -- AWS RDS API metadata
# ---------------------------------------------------------------------------
log_step "Querying AWS RDS API for instance metadata"

RDS_JSON=$(aws rds describe-db-instances \
  --region "${AWS_REGION}" \
  --db-instance-identifier "${RDS_INSTANCE_ID}" \
  --output json 2>/dev/null) || { log_error "Failed to describe RDS instance ${RDS_INSTANCE_ID}"; exit 1; }

{
  echo "================================================================"
  echo "RDS Instance Report -- $(date)"
  echo "================================================================"
  echo ""

  echo "--- Instance Identifier ---"
  echo "${RDS_JSON}" | python3 -c "
import sys, json
db = json.load(sys.stdin)['DBInstances'][0]
fields = [
    ('DB Instance ID',      db.get('DBInstanceIdentifier','')),
    ('Engine',              db.get('Engine','')),
    ('Engine Version',      db.get('EngineVersion','')),
    ('Instance Class',      db.get('DBInstanceClass','')),
    ('Storage (GiB)',       db.get('AllocatedStorage','')),
    ('Storage Type',        db.get('StorageType','')),
    ('IOPS',                db.get('Iops','N/A')),
    ('Multi-AZ',            db.get('MultiAZ','')),
    ('Availability Zone',   db.get('AvailabilityZone','')),
    ('Endpoint',            db.get('Endpoint',{}).get('Address','')),
    ('Port',                db.get('Endpoint',{}).get('Port','')),
    ('Master Username',     db.get('MasterUsername','')),
    ('DB Name',             db.get('DBName','N/A')),
    ('VPC ID',              db.get('DBSubnetGroup',{}).get('VpcId','')),
    ('Subnet Group',        db.get('DBSubnetGroup',{}).get('DBSubnetGroupName','')),
    ('Publicly Accessible', db.get('PubliclyAccessible','')),
    ('Encryption',          db.get('StorageEncrypted','')),
    ('KMS Key',             db.get('KmsKeyId','N/A')),
    ('Backup Retention',    str(db.get('BackupRetentionPeriod','')) + ' days'),
    ('Status',              db.get('DBInstanceStatus','')),
]
for label, val in fields:
    print(f'  {label:25s}: {val}')

print()
print('--- Security Groups ---')
for sg in db.get('VpcSecurityGroups', []):
    print(f\"  {sg['VpcSecurityGroupId']:25s}  Status: {sg['Status']}\")

print()
print('--- Parameter Group ---')
for pg in db.get('DBParameterGroups', []):
    print(f\"  {pg['DBParameterGroupName']:25s}  Apply: {pg['ParameterApplyStatus']}\")

print()
print('--- Option Group ---')
for og in db.get('OptionGroupMemberships', []):
    print(f\"  {og['OptionGroupName']:25s}  Status: {og['Status']}\")

print()
print('--- Tags ---')
tags = db.get('TagList', [])
if tags:
    for t in tags:
        print(f\"  {t['Key']:25s}: {t['Value']}\")
else:
    print('  (none)')
"
} | tee "${REPORT}"

# ---------------------------------------------------------------------------
# Part 2 -- Database-level details via mysql client
# ---------------------------------------------------------------------------
log_step "Querying MySQL/MariaDB for database details"

if ! command -v mysql &>/dev/null; then
  log_warn "mysql client not found. Skipping database-level queries."
  log_warn "Install with:  sudo dnf install -y mysql"
else
  {
    echo ""
    echo "--- Databases ---"
    mysql -h "${DB_HOST}" -P "${DB_PORT}" -u "${DB_ADMIN_USER}" -p"${DB_ADMIN_PASS}" \
      --batch --skip-column-names -e "SHOW DATABASES;" 2>/dev/null || log_warn "Could not list databases"

    echo ""
    echo "--- Character Sets ---"
    mysql -h "${DB_HOST}" -P "${DB_PORT}" -u "${DB_ADMIN_USER}" -p"${DB_ADMIN_PASS}" \
      --batch -e "
        SELECT schema_name, default_character_set_name, default_collation_name
        FROM information_schema.schemata
        ORDER BY schema_name;
      " 2>/dev/null || log_warn "Could not query character sets"

    echo ""
    echo "--- Users ---"
    mysql -h "${DB_HOST}" -P "${DB_PORT}" -u "${DB_ADMIN_USER}" -p"${DB_ADMIN_PASS}" \
      --batch -e "
        SELECT user, host, plugin
        FROM mysql.user
        WHERE user NOT IN ('mysql.sys','mysql.session','mysql.infoschema','rdsadmin','rds_superuser_role')
        ORDER BY user, host;
      " 2>/dev/null || log_warn "Could not query users"

    echo ""
    echo "--- Global Variables (relevant) ---"
    mysql -h "${DB_HOST}" -P "${DB_PORT}" -u "${DB_ADMIN_USER}" -p"${DB_ADMIN_PASS}" \
      --batch -e "
        SHOW VARIABLES WHERE Variable_name IN (
          'max_connections','max_allowed_packet',
          'character_set_server','collation_server',
          'innodb_buffer_pool_size','innodb_flush_method',
          'innodb_file_per_table','innodb_log_file_size',
          'transaction_isolation','log_bin','server_id',
          'lower_case_table_names','default_storage_engine'
        );
      " 2>/dev/null || log_warn "Could not query global variables"

  } | tee -a "${REPORT}"
fi

log_info "RDS report written to ${REPORT}"

banner "Phase 1b Complete"
