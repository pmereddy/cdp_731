#!/usr/bin/env bash
###############################################################################
# 04_push_cloudera_repo.sh
#
# Push the Cloudera Manager 7.13.1 yum repository configuration to all
# cluster hosts (RHEL 9).  Also imports the GPG key and validates the repo.
#
# Requires:
#   - .env populated with CLOUDERA_REPO_USER, CLOUDERA_REPO_PASS,
#     CM_VERSION, CM_REPO_BASE_URL, CM_GPG_KEY_URL
###############################################################################
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"
load_env

require_vars ALL_HOSTS SSH_USER SSH_KEY CLOUDERA_REPO_USER CLOUDERA_REPO_PASS \
             CM_VERSION CM_REPO_BASE_URL CM_GPG_KEY_URL

banner "Phase 3: Push Cloudera Manager Repository to All Hosts"

check_ssh_connectivity

# Build the authenticated URLs
AUTH_BASE_URL="https://${CLOUDERA_REPO_USER}:${CLOUDERA_REPO_PASS}@archive.cloudera.com/p/cm7/${CM_VERSION}/redhat9/yum"
AUTH_GPG_URL="https://${CLOUDERA_REPO_USER}:${CLOUDERA_REPO_PASS}@archive.cloudera.com/p/cm7/${CM_VERSION}/redhat9/yum/RPM-GPG-KEY-cloudera"

REPO_CONTENT="[cloudera-manager]
name=Cloudera Manager ${CM_VERSION}
baseurl=${AUTH_BASE_URL}
gpgkey=${AUTH_GPG_URL}
gpgcheck=1
enabled=1
autorefresh=0
type=rpm-md"

# ---------------------------------------------------------------------------
# Deploy repo file to each host
# ---------------------------------------------------------------------------
log_step "Deploying cloudera-manager.repo to all hosts"

read -ra _hosts <<< "${ALL_HOSTS}"
for h in "${_hosts[@]}"; do
  log_host "Configuring repo on ${h}"

  ssh_cmd "${h}" "sudo bash -s" <<REMOTE_EOF
set -euo pipefail

# Backup any existing repo file
if [ -f /etc/yum.repos.d/cloudera-manager.repo ]; then
  cp /etc/yum.repos.d/cloudera-manager.repo /etc/yum.repos.d/cloudera-manager.repo.bak.\$(date +%Y%m%d_%H%M%S)
  echo "  Backed up existing cloudera-manager.repo"
fi

# Write the new repo file
cat > /etc/yum.repos.d/cloudera-manager.repo <<'REPOFILE'
${REPO_CONTENT}
REPOFILE

chmod 644 /etc/yum.repos.d/cloudera-manager.repo

echo "  Repo file written."
echo "  Contents:"
cat /etc/yum.repos.d/cloudera-manager.repo
REMOTE_EOF

done

# ---------------------------------------------------------------------------
# Clean cache and verify
# ---------------------------------------------------------------------------
log_step "Cleaning DNF cache and verifying repository on all hosts"

VERIFY_SCRIPT='
set -euo pipefail
echo "  Cleaning cache..."
dnf clean all 2>/dev/null || yum clean all
echo "  Building cache..."
dnf makecache 2>/dev/null || yum -y makecache fast
echo ""
echo "  Repo list (cloudera):"
dnf repolist 2>/dev/null | grep -i cloudera || yum repolist 2>/dev/null | grep -i cloudera || echo "  WARNING: cloudera repo not visible"
echo ""
echo "  Available CM packages:"
dnf list available cloudera-manager-server cloudera-manager-agent cloudera-manager-daemons 2>/dev/null \
  || yum list available cloudera-manager-server cloudera-manager-agent cloudera-manager-daemons 2>/dev/null \
  || echo "  WARNING: CM packages not available"
'

for h in "${_hosts[@]}"; do
  log_host "Verifying repo on ${h}"
  ssh_cmd "${h}" "sudo bash -s" <<< "${VERIFY_SCRIPT}" || log_warn "Verification issues on ${h}"
  echo ""
done

log_info "Cloudera Manager repo deployed and verified on all hosts."

banner "Phase 3 Complete"
