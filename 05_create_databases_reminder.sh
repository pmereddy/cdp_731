#!/usr/bin/env bash
###############################################################################
# 06_install_cloudera_manager.sh
#
# Install Cloudera Manager 7.13.1 components on the cluster:
#   - CM Server host:  cloudera-manager-daemons, cloudera-manager-agent,
#                      cloudera-manager-server
#   - All other hosts: cloudera-manager-daemons, cloudera-manager-agent
#
# Then:
#   - Install MySQL JDBC driver if not present
#   - Run scm_prepare_database.sh on the CM server host
#   - Configure agent config.ini (server_host)
#   - Apply OS tuning (swappiness, THP)
#   - Start CM Server and Agents
###############################################################################
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"
load_env

require_vars ALL_HOSTS CM_SERVER_HOST SSH_USER SSH_KEY \
             DB_HOST DB_PORT SCM_DB_PASS

banner "Phase 5: Install Cloudera Manager 7.13.1"

check_ssh_connectivity

# ---------------------------------------------------------------------------
# Step 1: Install CM packages on CM Server host
# ---------------------------------------------------------------------------
log_step "Step 1: Install CM Server packages on ${CM_SERVER_HOST}"

ssh_cmd "${CM_SERVER_HOST}" "sudo bash -s" <<'SERVER_INSTALL'
set -euo pipefail

echo "Installing cloudera-manager-daemons, cloudera-manager-agent, cloudera-manager-server ..."
dnf install -y cloudera-manager-daemons cloudera-manager-agent cloudera-manager-server \
  2>/dev/null || yum install -y cloudera-manager-daemons cloudera-manager-agent cloudera-manager-server

echo ""
echo "Installed packages:"
rpm -qa | grep -i cloudera-manager || echo "  (none found -- check for errors above)"
SERVER_INSTALL

# ---------------------------------------------------------------------------
# Step 2: Install CM Agent + Daemons on all other hosts
# ---------------------------------------------------------------------------
log_step "Step 2: Install CM Agent + Daemons on worker/data hosts"

AGENT_INSTALL_SCRIPT='
set -euo pipefail
echo "Installing cloudera-manager-daemons and cloudera-manager-agent ..."
dnf install -y cloudera-manager-daemons cloudera-manager-agent \
  2>/dev/null || yum install -y cloudera-manager-daemons cloudera-manager-agent
echo ""
echo "Installed packages:"
rpm -qa | grep -i cloudera-manager || echo "  (none found)"
'

read -ra _hosts <<< "${ALL_HOSTS}"
for h in "${_hosts[@]}"; do
  if [[ "${h}" == "${CM_SERVER_HOST}" ]]; then
    log_info "Skipping ${h} (CM server -- already installed)"
    continue
  fi
  log_host "Installing agent on ${h}"
  ssh_cmd "${h}" "sudo bash -s" <<< "${AGENT_INSTALL_SCRIPT}" || log_warn "Install issues on ${h}"
done

# ---------------------------------------------------------------------------
# Step 3: Install MySQL JDBC driver (if not present)
# ---------------------------------------------------------------------------
log_step "Step 3: Verify MySQL JDBC driver on CM Server host"

ssh_cmd "${CM_SERVER_HOST}" "sudo bash -s" <<'JDBC_CHECK'
set -euo pipefail

JDBC_PATH="/usr/share/java/mysql-connector-java.jar"
if [ -f "${JDBC_PATH}" ]; then
  echo "  JDBC driver already present: ${JDBC_PATH}"
  ls -l "${JDBC_PATH}"
else
  echo "  JDBC driver NOT found at ${JDBC_PATH}"
  echo ""
  echo "  ACTION REQUIRED: Download and install the MySQL JDBC driver."
  echo "  Example:"
  echo "    wget https://dev.mysql.com/get/Downloads/Connector-J/mysql-connector-j-8.0.33.tar.gz"
  echo "    tar xzf mysql-connector-j-8.0.33.tar.gz"
  echo "    sudo mkdir -p /usr/share/java"
  echo "    sudo cp mysql-connector-j-8.0.33/mysql-connector-j-8.0.33.jar /usr/share/java/mysql-connector-java.jar"
  echo ""
  exit 1
fi
JDBC_CHECK

# ---------------------------------------------------------------------------
# Step 4: Prepare SCM database
# ---------------------------------------------------------------------------
log_step "Step 4: Prepare SCM database (scm_prepare_database.sh)"

log_info "Running scm_prepare_database.sh on ${CM_SERVER_HOST} ..."

ssh_cmd "${CM_SERVER_HOST}" "sudo bash -s" <<PREPARE_DB
set -euo pipefail

/opt/cloudera/cm/schema/scm_prepare_database.sh mysql scm scm '${SCM_DB_PASS}' -h '${DB_HOST}' --port '${DB_PORT}'

echo ""
echo "Database properties:"
cat /etc/cloudera-scm-server/db.properties
PREPARE_DB

# ---------------------------------------------------------------------------
# Step 5: Apply OS tuning on all hosts
# ---------------------------------------------------------------------------
log_step "Step 5: Apply OS tuning (swappiness, THP) on all hosts"

TUNING_SCRIPT='
set -euo pipefail

# --- Swappiness ---
CURRENT_SWAP=$(sysctl -n vm.swappiness)
if [ "${CURRENT_SWAP}" -gt 1 ]; then
  sysctl -w vm.swappiness=1
  if ! grep -q "^vm.swappiness" /etc/sysctl.conf; then
    echo "vm.swappiness=1" >> /etc/sysctl.conf
  else
    sed -i "s/^vm.swappiness=.*/vm.swappiness=1/" /etc/sysctl.conf
  fi
  echo "  vm.swappiness set to 1"
else
  echo "  vm.swappiness already ${CURRENT_SWAP}"
fi

# --- THP ---
if [ -f /sys/kernel/mm/transparent_hugepage/enabled ]; then
  echo never > /sys/kernel/mm/transparent_hugepage/enabled 2>/dev/null || true
  echo never > /sys/kernel/mm/transparent_hugepage/defrag  2>/dev/null || true
fi

# Persistent THP disable via systemd
if [ ! -f /etc/systemd/system/disable-thp.service ]; then
  cat > /etc/systemd/system/disable-thp.service <<SVC
[Unit]
Description=Disable Transparent Huge Pages (THP)
After=network.target

[Service]
Type=oneshot
ExecStart=/bin/bash -c "echo never > /sys/kernel/mm/transparent_hugepage/enabled; echo never > /sys/kernel/mm/transparent_hugepage/defrag"
RemainAfterExit=yes

[Install]
WantedBy=multi-user.target
SVC
  systemctl daemon-reload
  systemctl enable --now disable-thp.service
  echo "  THP disable service installed and started"
else
  echo "  THP disable service already exists"
fi
'

run_sudo_on_all_hosts "${TUNING_SCRIPT}" "OS-TUNING"

# ---------------------------------------------------------------------------
# Step 6: Configure agent config.ini on all hosts
# ---------------------------------------------------------------------------
log_step "Step 6: Configure CM Agent (server_host) on all hosts"

CONFIG_SCRIPT="
set -euo pipefail

CFG='/etc/cloudera-scm-agent/config.ini'

if [ ! -f \"\${CFG}\" ]; then
  echo '  config.ini not found -- agent may not be installed'
  exit 1
fi

# Backup
cp -a \"\${CFG}\" \"\${CFG}.bak.\$(date +%Y%m%d_%H%M%S)\"

# Set server_host
sed -i \"s/^server_host=.*/server_host=${CM_SERVER_HOST}/\" \"\${CFG}\"

echo '  Updated config.ini:'
grep -n '^server_host=' \"\${CFG}\"
"

run_sudo_on_all_hosts "${CONFIG_SCRIPT}" "CONFIG-AGENT"

# ---------------------------------------------------------------------------
# Step 7: Start CM Server
# ---------------------------------------------------------------------------
log_step "Step 7: Start Cloudera Manager Server"

ssh_cmd "${CM_SERVER_HOST}" "sudo bash -s" <<'START_SERVER'
set -euo pipefail

systemctl enable cloudera-scm-server
systemctl start cloudera-scm-server

echo "  Waiting for CM Server to start (checking port 7180) ..."
for i in $(seq 1 60); do
  if ss -tlnp | grep -q ":7180 "; then
    echo "  CM Server is listening on port 7180 after ~${i}0 seconds"
    break
  fi
  sleep 10
done

if ! ss -tlnp | grep -q ":7180 "; then
  echo "  WARNING: CM Server is not yet listening on port 7180."
  echo "  Check logs: tail -f /var/log/cloudera-scm-server/cloudera-scm-server.log"
fi

systemctl status cloudera-scm-server --no-pager
START_SERVER

# ---------------------------------------------------------------------------
# Step 8: Start CM Agents on all hosts
# ---------------------------------------------------------------------------
log_step "Step 8: Start Cloudera Manager Agents on all hosts"

AGENT_START='
set -euo pipefail
systemctl enable cloudera-scm-agent
systemctl start cloudera-scm-agent
systemctl status cloudera-scm-agent --no-pager || true
'

run_sudo_on_all_hosts "${AGENT_START}" "START-AGENT"

# ---------------------------------------------------------------------------
# Step 9: Verification
# ---------------------------------------------------------------------------
log_step "Step 9: Verify CM packages across the cluster"

run_on_all_hosts "sudo rpm -qa | grep -i cloudera-manager" "VERIFY-RPM"

log_info "Cloudera Manager installation complete."
log_info "Access the CM Admin Console at: http://${CM_SERVER_HOST}:7180"
log_info "Default credentials: admin / admin"

banner "Phase 5 Complete"
