#!/usr/bin/env bash
###############################################################################
# 03_precheck.sh
#
# Run pre-installation checks on all cluster hosts to verify readiness for
# CDP 7.3.1.600 SP3 CHF1 on RHEL 9.4.
#
# Checks performed:
#   - Disk mounts, free space, filesystem types
#   - OS configuration (swappiness, THP, SELinux, chronyd, ulimits)
#   - Required packages (iproute, rpcbind, perl, python3)
#   - Java version and JAVA_HOME
#   - JDBC driver presence
#   - Yum/DNF repositories
#   - Network (hostname resolution, NTP sync)
#   - Existing Cloudera remnants
###############################################################################
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"
load_env

require_vars ALL_HOSTS SSH_USER SSH_KEY

OUT_DIR="${SCRIPT_DIR}/output"
mkdir -p "${OUT_DIR}"
REPORT="${OUT_DIR}/precheck_report.txt"

banner "Phase 2: Pre-Installation Checks"

check_ssh_connectivity

: > "${REPORT}"

# ---------------------------------------------------------------------------
# The pre-check script sent to each host
# ---------------------------------------------------------------------------
PRECHECK_SCRIPT='
set -uo pipefail

PASS=0
WARN=0
FAIL=0

pass() { echo "  [PASS] $*"; ((PASS++)) || true; }
warn() { echo "  [WARN] $*"; ((WARN++)) || true; }
fail() { echo "  [FAIL] $*"; ((FAIL++)) || true; }

echo "================================================================"
echo "Host: $(hostname -f)"
echo "Date: $(date)"
echo "================================================================"

# --- OS Version ---
echo ""
echo "--- OS Version ---"
if [ -f /etc/redhat-release ]; then
  OS_VER=$(cat /etc/redhat-release)
  echo "  ${OS_VER}"
  if echo "${OS_VER}" | grep -q "release 9"; then
    pass "RHEL 9 detected"
  else
    warn "Expected RHEL 9.x, found: ${OS_VER}"
  fi
else
  fail "Not a RHEL system"
fi

# --- Kernel ---
echo ""
echo "--- Kernel ---"
echo "  $(uname -r)"

# --- Hostname Resolution ---
echo ""
echo "--- Hostname Resolution ---"
FQDN=$(hostname -f 2>/dev/null || echo "UNRESOLVABLE")
SHORT=$(hostname -s 2>/dev/null || echo "UNRESOLVABLE")
echo "  FQDN:  ${FQDN}"
echo "  Short: ${SHORT}"
if [ "${FQDN}" != "${SHORT}" ] && [ "${FQDN}" != "UNRESOLVABLE" ]; then
  pass "FQDN resolves correctly"
else
  fail "FQDN does not resolve or equals short hostname"
fi

# Forward + reverse DNS
HOST_IP=$(hostname -i 2>/dev/null | awk "{print \$1}")
echo "  IP: ${HOST_IP}"

# --- Disk Mounts & Free Space ---
echo ""
echo "--- Disk Mounts & Free Space ---"
df -hT | grep -vE "tmpfs|devtmpfs|overlay"
echo ""
echo "  Block devices:"
lsblk -o NAME,SIZE,TYPE,FSTYPE,MOUNTPOINT 2>/dev/null || lsblk

ROOT_FREE=$(df -BG / | tail -1 | awk "{print \$4}" | tr -d "G")
if [ "${ROOT_FREE}" -ge 20 ]; then
  pass "Root filesystem has ${ROOT_FREE}G free (>=20G)"
else
  fail "Root filesystem has only ${ROOT_FREE}G free (<20G required)"
fi

# Check /opt or /opt/cloudera mount
OPT_FREE=$(df -BG /opt 2>/dev/null | tail -1 | awk "{print \$4}" | tr -d "G" || echo "0")
if [ "${OPT_FREE}" -ge 50 ]; then
  pass "/opt has ${OPT_FREE}G free (>=50G for parcels)"
else
  warn "/opt has only ${OPT_FREE}G free (recommend >=50G for parcels)"
fi

# Check noatime
echo ""
echo "  Mount options (checking noatime):"
MOUNTS_WITHOUT_NOATIME=$(mount | grep -E "^/dev" | grep -v noatime | grep -v "tmpfs" || true)
if [ -n "${MOUNTS_WITHOUT_NOATIME}" ]; then
  warn "Some data mounts lack noatime:"
  echo "${MOUNTS_WITHOUT_NOATIME}" | head -5
else
  pass "All device mounts have noatime"
fi

# --- Swappiness ---
echo ""
echo "--- vm.swappiness ---"
SWAPPINESS=$(sysctl -n vm.swappiness)
echo "  Current value: ${SWAPPINESS}"
if [ "${SWAPPINESS}" -le 1 ]; then
  pass "vm.swappiness=${SWAPPINESS} (<=1)"
else
  fail "vm.swappiness=${SWAPPINESS} -- must be 0 or 1 for CDP"
fi

# --- Transparent Huge Pages ---
echo ""
echo "--- Transparent Huge Pages ---"
THP_ENABLED=$(cat /sys/kernel/mm/transparent_hugepage/enabled 2>/dev/null || echo "N/A")
echo "  enabled: ${THP_ENABLED}"
if echo "${THP_ENABLED}" | grep -q "\[never\]"; then
  pass "THP disabled"
else
  fail "THP is NOT disabled -- set to [never]"
fi

THP_DEFRAG=$(cat /sys/kernel/mm/transparent_hugepage/defrag 2>/dev/null || echo "N/A")
echo "  defrag:  ${THP_DEFRAG}"

# --- SELinux ---
echo ""
echo "--- SELinux ---"
SE_STATUS=$(getenforce 2>/dev/null || echo "Unknown")
echo "  Status: ${SE_STATUS}"
if [ "${SE_STATUS}" = "Disabled" ] || [ "${SE_STATUS}" = "Permissive" ]; then
  pass "SELinux is ${SE_STATUS}"
else
  warn "SELinux is ${SE_STATUS} -- Cloudera recommends Permissive or Disabled"
fi

# --- Firewall ---
echo ""
echo "--- Firewall ---"
FW_STATUS=$(systemctl is-active firewalld 2>/dev/null || echo "inactive")
echo "  firewalld: ${FW_STATUS}"
if [ "${FW_STATUS}" = "inactive" ]; then
  pass "firewalld is inactive"
else
  warn "firewalld is active -- ensure CDP ports are open"
fi

# --- Chronyd (NTP) ---
echo ""
echo "--- Chronyd / NTP ---"
CHRONY_STATUS=$(systemctl is-active chronyd 2>/dev/null || echo "inactive")
echo "  chronyd: ${CHRONY_STATUS}"
if [ "${CHRONY_STATUS}" = "active" ]; then
  pass "chronyd is running"
  chronyc tracking 2>/dev/null | grep -E "Leap status|System time|RMS offset" || true
else
  fail "chronyd is NOT running -- time synchronisation is required"
fi

# --- Java ---
echo ""
echo "--- Java ---"
if command -v java &>/dev/null; then
  JAVA_VER=$(java -version 2>&1 | head -1)
  echo "  ${JAVA_VER}"
  if echo "${JAVA_VER}" | grep -qE "\"(11|17)\."; then
    pass "Java 11 or 17 detected"
  else
    warn "Expected Java 11 or 17; found: ${JAVA_VER}"
  fi
else
  fail "Java not found in PATH"
fi
echo "  JAVA_HOME=${JAVA_HOME:-not set}"

# --- Python ---
echo ""
echo "--- Python ---"
if command -v python3 &>/dev/null; then
  PY_VER=$(python3 --version 2>&1)
  echo "  ${PY_VER}"
  pass "python3 found"
else
  fail "python3 not found"
fi

# --- Required Packages ---
echo ""
echo "--- Required Packages ---"
for pkg in iproute rpcbind perl psmisc; do
  if rpm -q "${pkg}" &>/dev/null; then
    pass "${pkg} installed ($(rpm -q ${pkg}))"
  else
    fail "${pkg} NOT installed -- required by CDP"
  fi
done

# --- JDBC Driver ---
echo ""
echo "--- MySQL JDBC Driver ---"
if [ -f /usr/share/java/mysql-connector-java.jar ]; then
  pass "JDBC driver found at /usr/share/java/mysql-connector-java.jar"
  ls -l /usr/share/java/mysql-connector-java.jar
else
  warn "MySQL JDBC driver not found at /usr/share/java/mysql-connector-java.jar"
  echo "  Install with: download mysql-connector-java-8.x.jar and copy to /usr/share/java/mysql-connector-java.jar"
fi

# --- Yum / DNF Repos ---
echo ""
echo "--- YUM/DNF Repositories ---"
dnf repolist 2>/dev/null || yum repolist 2>/dev/null
echo ""
echo "  Checking for stale mirrorlist entries:"
grep -R "^mirrorlist=" /etc/yum.repos.d/*.repo 2>/dev/null && warn "Active mirrorlist entries found" || pass "No active mirrorlist entries"

# --- Existing Cloudera Artifacts ---
echo ""
echo "--- Existing Cloudera Artifacts ---"
FOUND_RPM=$(rpm -qa 2>/dev/null | grep -i cloudera || true)
if [ -n "${FOUND_RPM}" ]; then
  warn "Existing Cloudera RPMs found:"
  echo "${FOUND_RPM}"
else
  pass "No existing Cloudera RPMs"
fi

FOUND_DIRS=""
for d in /opt/cloudera /var/lib/cloudera-scm-agent /var/lib/cloudera-scm-server /etc/cloudera-scm-agent /etc/cloudera-scm-server; do
  [ -d "${d}" ] && FOUND_DIRS="${FOUND_DIRS} ${d}"
done
if [ -n "${FOUND_DIRS}" ]; then
  warn "Existing Cloudera directories found: ${FOUND_DIRS}"
else
  pass "No existing Cloudera directories"
fi

# --- systemctl Cloudera units ---
FOUND_UNITS=$(systemctl list-unit-files 2>/dev/null | grep cloudera || true)
if [ -n "${FOUND_UNITS}" ]; then
  warn "Existing Cloudera systemd units:"
  echo "${FOUND_UNITS}"
fi

# --- Summary ---
echo ""
echo "================================================================"
echo "SUMMARY: PASS=${PASS}  WARN=${WARN}  FAIL=${FAIL}"
echo "================================================================"
'

# ---------------------------------------------------------------------------
# Execute on all hosts
# ---------------------------------------------------------------------------
read -ra _hosts <<< "${ALL_HOSTS}"
for h in "${_hosts[@]}"; do
  log_host "Running pre-checks on ${h}"
  {
    ssh_cmd "${h}" "sudo bash -s" <<< "${PRECHECK_SCRIPT}" 2>&1
  } | tee -a "${REPORT}"
  echo ""
done

log_info "Pre-check report written to ${REPORT}"
log_info "Review any [FAIL] or [WARN] items before proceeding."

banner "Phase 2 Complete"
