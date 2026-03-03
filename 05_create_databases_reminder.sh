#!/usr/bin/env bash
###############################################################################
# 05_create_databases_reminder.sh
#
# Generate the SQL statements needed to create CDP databases, users, and
# grants on the MySQL/MariaDB RDS instance.  The SQL is printed to stdout
# and saved to a file for manual review before execution.
#
# This script does NOT execute SQL automatically -- it is a reminder and
# helper that produces ready-to-run SQL.
#
# Databases created:
#   scm, amon, rman, hue, hivemetastore, oozie, ranger
#
# All databases use:  CHARACTER SET utf8  /  COLLATE utf8_general_ci
# All users use:      mysql_native_password authentication plugin
###############################################################################
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"
load_env

require_vars DB_HOST DB_PORT DB_ADMIN_USER DB_ADMIN_PASS \
             SCM_DB_PASS AMON_DB_PASS RMAN_DB_PASS HUE_DB_PASS \
             HIVE_DB_PASS OOZIE_DB_PASS RANGER_DB_PASS

OUT_DIR="${SCRIPT_DIR}/output"
mkdir -p "${OUT_DIR}"
SQL_FILE="${OUT_DIR}/create_cdp_databases.sql"

banner "Phase 4: Database Creation Reminder"

# ---------------------------------------------------------------------------
# Service-to-database mapping
# ---------------------------------------------------------------------------
# 2. Define the values using a consistent variable prefix (e.g., DB_MAP_<key>)
DB_MAP_scm="SCM_DB_PASS"
DB_MAP_amon="AMON_DB_PASS"
DB_MAP_rman="RMAN_DB_PASS"
DB_MAP_hue="HUE_DB_PASS"
DB_MAP_hivemetastore="HIVE_DB_PASS"
DB_MAP_oozie="OOZIE_DB_PASS"
DB_MAP_ranger="RANGER_DB_PASS"

USER_MAP_scm="scm"
USER_MAP_amon="amon"
USER_MAP_rman="rman"
USER_MAP_hue="hue"
USER_MAP_hivemetastore="hive"
USER_MAP_oozie="oozie"
USER_MAP_ranger="ranger"

# ---------------------------------------------------------------------------
# Generate SQL
# ---------------------------------------------------------------------------
{
  cat <<'HEADER'
-- ==========================================================================
-- CDP 7.3.1.600 SP3 CHF1 -- Database & User Creation Script
--
-- IMPORTANT:
--   1. Review all statements before executing.
--   2. All databases use CHARACTER SET utf8 / COLLATE utf8_general_ci.
--   3. All users use mysql_native_password (required by Cloudera).
--   4. Adjust passwords to match your security policy.
--   5. Connect as an admin user:
--        mysql -h <RDS_ENDPOINT> -P 3306 -u admin -p
-- ==========================================================================

HEADER

  for db in scm amon rman hue hivemetastore oozie ranger; do
    user_var="USER_MAP_${db}"
    pass_var="DB_MAP_${db}"
    user="${!user_var}"
    passo="${!pass_var}"
    pass="${!passo}"

    cat <<SQL
-- ---------------------------------------------------
-- Database: ${db}  |  User: ${user}
-- ---------------------------------------------------
CREATE DATABASE IF NOT EXISTS \`${db}\`
  DEFAULT CHARACTER SET utf8
  DEFAULT COLLATE utf8_general_ci;

CREATE USER IF NOT EXISTS '${user}'@'localhost'
  IDENTIFIED BY '${pass}';

CREATE USER IF NOT EXISTS '${user}'@'%'
  IDENTIFIED BY '${pass}';

ALTER USER '${user}'@'localhost'
  IDENTIFIED BY '${pass}';

ALTER USER '${user}'@'%'
  IDENTIFIED BY '${pass}';

GRANT ALL PRIVILEGES ON \`${db}\`.* TO '${user}'@'localhost';
GRANT ALL PRIVILEGES ON \`${db}\`.* TO '${user}'@'%';

SQL
  done

  echo "FLUSH PRIVILEGES;"
  echo ""
  echo "-- Verification queries"
  echo "SELECT user, host, plugin FROM mysql.user"
  echo "  WHERE user IN ('scm','amon','rman','hue','hive','oozie','ranger')"
  echo "  ORDER BY user, host;"
  echo ""
  echo "SELECT schema_name, default_character_set_name, default_collation_name"
  echo "  FROM information_schema.schemata"
  echo "  WHERE schema_name IN ('scm','amon','rman','hue','hivemetastore','oozie','ranger');"
  echo ""

} > "${SQL_FILE}"

# ---------------------------------------------------------------------------
# Display the SQL
# ---------------------------------------------------------------------------
log_step "Generated SQL Statements"
cat "${SQL_FILE}"

log_info "SQL saved to: ${SQL_FILE}"
echo ""

# ---------------------------------------------------------------------------
# Reminder checklist
# ---------------------------------------------------------------------------
log_step "DATABASE CREATION CHECKLIST"

cat <<'CHECKLIST'

  Before running the SQL above, verify the following:

  [ ] 1. RDS instance is running and accessible from the CM server host
         Test:  mysql -h <DB_HOST> -P 3306 -u admin -p

  [ ] 2. RDS parameter group has the following settings:
         - transaction_isolation = READ-COMMITTED
         - innodb_flush_method = O_DIRECT  (if not RDS-managed)
         - max_connections >= 550
         - max_allowed_packet >= 16M
         - default_storage_engine = InnoDB
         - character_set_server = utf8
         - collation_server = utf8_general_ci
         - log_bin = ON  (recommended)

  [ ] 3. All database passwords are secure and recorded in a vault

  [ ] 4. The MySQL JDBC driver (mysql-connector-java-8.x.jar) is installed
         on the CM server host and all hosts running services that need DB:
           /usr/share/java/mysql-connector-java.jar

  To execute the SQL:
    mysql -h ${DB_HOST} -P ${DB_PORT} -u ${DB_ADMIN_USER} -p < output/create_cdp_databases.sql

CHECKLIST

echo ""
log_warn "This script does NOT execute SQL automatically."
log_warn "Review the generated SQL file and run it manually."

banner "Phase 4 Complete"
