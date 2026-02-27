#!/usr/bin/env bash
###############################################################################
# 035_fixconfig.sh
#
# Apply OS-level configuration required by CDP on all cluster hosts:
#   1. Disable Transparent Huge Pages (THP) -- runtime + persistent
#   2. Disable SELinux -- runtime + persistent
#   3. Disable firewalld
#   4. Install and enable chronyd (NTP)
#   5. Install rpcbind and perl if missing
#   6. Install OpenJDK 17 and set JAVA_HOME if not already present
###############################################################################
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"
load_env

require_vars ALL_HOSTS SSH_USER SSH_KEY

banner "Fix OS Configuration on All Hosts"

check_ssh_connectivity

# ---------------------------------------------------------------------------
# The configuration script sent to each host
# ---------------------------------------------------------------------------
FIX_SCRIPT='
set -euo pipefail

echo "================================================================"
echo "Host: $(hostname -f)"
echo "================================================================"

###########################################################################
# 1. Disable Transparent Huge Pages (THP)
###########################################################################
echo ""
echo "--- 1. Transparent Huge Pages ---"

if [ -f /sys/kernel/mm/transparent_hugepage/enabled ]; then
  CURRENT=$(cat /sys/kernel/mm/transparent_hugepage/enabled)
  echo "  Before: ${CURRENT}"

  echo never > /sys/kernel/mm/transparent_hugepage/enabled  2>/dev/null || true
  echo never > /sys/kernel/mm/transparent_hugepage/defrag   2>/dev/null || true

  echo "  After:  $(cat /sys/kernel/mm/transparent_hugepage/enabled)"
fi

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
  echo "  [OK] disable-thp.service created and enabled"
else
  echo "  [OK] disable-thp.service already exists"
fi

###########################################################################
# 2. Disable SELinux
###########################################################################
echo ""
echo "--- 2. SELinux ---"

CURRENT_SE=$(getenforce 2>/dev/null || echo "Unknown")
echo "  Current: ${CURRENT_SE}"

if [ "${CURRENT_SE}" != "Disabled" ]; then
  setenforce 0 2>/dev/null || true
  echo "  Runtime: set to Permissive (immediate)"
fi

if [ -f /etc/selinux/config ]; then
  CONFIGURED=$(grep "^SELINUX=" /etc/selinux/config | cut -d= -f2)
  if [ "${CONFIGURED}" != "disabled" ]; then
    sed -i "s/^SELINUX=.*/SELINUX=disabled/" /etc/selinux/config
    echo "  Persistent: /etc/selinux/config set to disabled"
    echo "  NOTE: Full disable takes effect after reboot"
  else
    echo "  Persistent: already set to disabled"
  fi
fi

###########################################################################
# 3. Disable firewalld
###########################################################################
echo ""
echo "--- 3. Firewalld ---"

if systemctl is-active --quiet firewalld 2>/dev/null; then
  systemctl stop firewalld
  echo "  Stopped firewalld"
else
  echo "  firewalld is already stopped"
fi

if systemctl is-enabled --quiet firewalld 2>/dev/null; then
  systemctl disable firewalld
  echo "  Disabled firewalld at boot"
else
  echo "  firewalld is already disabled at boot"
fi

systemctl mask firewalld 2>/dev/null || true
echo "  [OK] firewalld masked"

###########################################################################
# 4. Install and configure chronyd
###########################################################################
echo ""
echo "--- 4. Chronyd (NTP) ---"

if ! rpm -q chrony &>/dev/null; then
  echo "  Installing chrony ..."
  dnf install -y chrony
  echo "  [OK] chrony installed"
else
  echo "  [OK] chrony already installed ($(rpm -q chrony))"
fi

if ! systemctl is-active --quiet chronyd; then
  systemctl enable --now chronyd
  echo "  [OK] chronyd started and enabled"
else
  echo "  [OK] chronyd is already running"
fi

echo "  Sync status:"
chronyc tracking 2>/dev/null | grep -E "Reference ID|System time|RMS offset|Leap status" | sed "s/^/    /"

###########################################################################
# 5. Install rpcbind and perl
###########################################################################
echo ""
echo "--- 5. Required packages: rpcbind, perl ---"

for pkg in rpcbind perl; do
  if rpm -q "${pkg}" &>/dev/null; then
    echo "  [OK] ${pkg} already installed ($(rpm -q ${pkg}))"
  else
    echo "  Installing ${pkg} ..."
    dnf install -y "${pkg}"
    echo "  [OK] ${pkg} installed ($(rpm -q ${pkg}))"
  fi
done

if ! systemctl is-active --quiet rpcbind; then
  systemctl enable --now rpcbind
  echo "  [OK] rpcbind service started and enabled"
else
  echo "  [OK] rpcbind service already running"
fi

###########################################################################
# 6. Install OpenJDK 17 and set JAVA_HOME
###########################################################################
echo ""
echo "--- 6. OpenJDK 17 ---"

if java -version 2>&1 | grep -q "\"17\."; then
  echo "  [OK] Java 17 already installed"
  java -version 2>&1 | head -1 | sed "s/^/    /"
else
  echo "  Installing java-17-openjdk, java-17-openjdk-devel, java-17-openjdk-headless ..."
  dnf install -y java-17-openjdk java-17-openjdk-devel java-17-openjdk-headless
  echo "  [OK] OpenJDK 17 installed"
fi

# Set as default if multiple versions exist
if command -v java &>/dev/null; then
  JAVA17_BIN=$(find /usr/lib/jvm/java-17-openjdk-*/bin/java 2>/dev/null | head -1)
  if [ -n "${JAVA17_BIN}" ]; then
    alternatives --set java "${JAVA17_BIN}" 2>/dev/null || true
  fi
fi

# Persistent JAVA_HOME via profile drop-in
if [ ! -f /etc/profile.d/java17.sh ]; then
  cat > /etc/profile.d/java17.sh <<PROF
export JAVA_HOME=\$(dirname \$(dirname \$(readlink -f /usr/bin/java)))
export PATH=\$JAVA_HOME/bin:\$PATH
PROF
  chmod 644 /etc/profile.d/java17.sh
  echo "  [OK] /etc/profile.d/java17.sh created"
else
  echo "  [OK] /etc/profile.d/java17.sh already exists"
fi

source /etc/profile.d/java17.sh 2>/dev/null || true

echo "  JAVA_HOME=${JAVA_HOME:-not set}"
java -version 2>&1 | head -1 | sed "s/^/    /"
keytool -help 2>&1 | head -1 | sed "s/^/    keytool: /" || echo "    keytool: not found"

###########################################################################
# Summary
###########################################################################
echo ""
echo "================================================================"
echo "  THP:       $(cat /sys/kernel/mm/transparent_hugepage/enabled 2>/dev/null)"
echo "  SELinux:   $(getenforce 2>/dev/null || echo N/A) (config: $(grep ^SELINUX= /etc/selinux/config 2>/dev/null | cut -d= -f2))"
echo "  Firewalld: $(systemctl is-active firewalld 2>/dev/null || echo inactive)"
echo "  Chronyd:   $(systemctl is-active chronyd 2>/dev/null || echo inactive)"
echo "  rpcbind:   $(rpm -q rpcbind 2>/dev/null || echo missing)"
echo "  perl:      $(rpm -q perl 2>/dev/null || echo missing)"
echo "  Java:      $(java -version 2>&1 | head -1)"
echo "  JAVA_HOME: ${JAVA_HOME:-not set}"
echo "================================================================"
'

# ---------------------------------------------------------------------------
# Execute on all hosts
# ---------------------------------------------------------------------------
read -ra _hosts <<< "${ALL_HOSTS}"
for h in "${_hosts[@]}"; do
  log_host "Fixing configuration on ${h}"
  ssh_cmd "${h}" "sudo bash -s" <<< "${FIX_SCRIPT}" 2>&1
  echo ""
done

log_info "OS configuration applied on all hosts."
log_info "NOTE: SELinux disable requires a reboot to fully take effect."

banner "035_fixconfig Complete"
