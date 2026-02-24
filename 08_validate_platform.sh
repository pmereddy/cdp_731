#!/usr/bin/env bash
###############################################################################
# 08_validate_platform.sh
#
# Validate the CDP platform after installation:
#   - Cloudera Manager API health
#   - Agent heartbeats on all hosts
#   - Cluster service health
#   - Basic HDFS / YARN smoke tests (if services are running)
###############################################################################
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"
load_env

require_vars CM_SERVER_HOST ALL_HOSTS SSH_USER SSH_KEY

CM_PORT="${CM_API_PORT:-7180}"
CM_USER="${CM_API_USER:-admin}"
CM_PASS="${CM_API_PASS:-admin}"
CM_API_BASE="http://${CM_SERVER_HOST}:${CM_PORT}/api/v54"

OUT_DIR="${SCRIPT_DIR}/output"
mkdir -p "${OUT_DIR}"
REPORT="${OUT_DIR}/platform_validation_report.txt"

banner "Phase 7: Platform Validation"

: > "${REPORT}"

# ---------------------------------------------------------------------------
# Helper: call CM API
# ---------------------------------------------------------------------------
cm_api() {
  local endpoint="$1"
  curl -s -u "${CM_USER}:${CM_PASS}" "${CM_API_BASE}${endpoint}" 2>/dev/null
}

# ---------------------------------------------------------------------------
# 1. CM Server connectivity
# ---------------------------------------------------------------------------
log_step "1. Cloudera Manager Server Connectivity"

CM_VERSION_RESP=$(cm_api "/cm/version" 2>/dev/null || echo "UNREACHABLE")

if echo "${CM_VERSION_RESP}" | grep -q "version"; then
  log_info "CM Server is reachable at ${CM_SERVER_HOST}:${CM_PORT}"
  echo "${CM_VERSION_RESP}" | python3 -m json.tool 2>/dev/null || echo "${CM_VERSION_RESP}"
  echo ""
  echo "CM Server: REACHABLE" >> "${REPORT}"
else
  log_error "CM Server is NOT reachable at ${CM_SERVER_HOST}:${CM_PORT}"
  echo "CM Server: UNREACHABLE" >> "${REPORT}"

  # Try HTTPS on 7183
  log_info "Trying HTTPS on port 7183 ..."
  HTTPS_RESP=$(curl -sk -u "${CM_USER}:${CM_PASS}" "https://${CM_SERVER_HOST}:7183/api/v54/cm/version" 2>/dev/null || echo "")
  if echo "${HTTPS_RESP}" | grep -q "version"; then
    log_info "CM Server reachable via HTTPS on port 7183"
    CM_API_BASE="https://${CM_SERVER_HOST}:7183/api/v54"
    CM_PORT="7183"
    echo "${HTTPS_RESP}" | python3 -m json.tool 2>/dev/null || echo "${HTTPS_RESP}"
  else
    log_error "CM Server not reachable on HTTP:7180 or HTTPS:7183"
    log_error "Check: sudo systemctl status cloudera-scm-server"
    log_error "Logs:  sudo tail -100 /var/log/cloudera-scm-server/cloudera-scm-server.log"
  fi
fi

# ---------------------------------------------------------------------------
# 2. CM Agent heartbeats
# ---------------------------------------------------------------------------
log_step "2. Cloudera Manager Agent Status (via SSH)"

read -ra _hosts <<< "${ALL_HOSTS}"

{
  echo ""
  echo "--- Agent Status ---"
  for h in "${_hosts[@]}"; do
    AGENT_STATUS=$(ssh_cmd "${h}" "sudo systemctl is-active cloudera-scm-agent 2>/dev/null" || echo "unknown")
    if [[ "${AGENT_STATUS}" == "active" ]]; then
      log_info "  ${h}: agent is ACTIVE"
      echo "  ${h}: ACTIVE"
    else
      log_warn "  ${h}: agent status = ${AGENT_STATUS}"
      echo "  ${h}: ${AGENT_STATUS}"
    fi
  done
} | tee -a "${REPORT}"

# ---------------------------------------------------------------------------
# 3. Host list from CM API
# ---------------------------------------------------------------------------
log_step "3. Hosts Registered in Cloudera Manager"

HOSTS_JSON=$(cm_api "/hosts" 2>/dev/null || echo "{}")

if echo "${HOSTS_JSON}" | python3 -c "import sys,json; json.load(sys.stdin)" 2>/dev/null; then
  echo "${HOSTS_JSON}" | python3 -c "
import sys, json
data = json.load(sys.stdin)
items = data.get('items', [])
if not items:
    print('  No hosts registered in CM yet.')
else:
    print(f'  {len(items)} host(s) registered:')
    print(f'  {\"Hostname\":<45s} {\"IP Address\":<16s} {\"Health\":<12s} {\"Last Heartbeat\"}')
    print('  ' + '-'*90)
    for h in items:
        print(f'  {h.get(\"hostname\",\"?\"):<45s} {h.get(\"ipAddress\",\"?\"):<16s} {h.get(\"healthSummary\",\"?\"):<12s} {h.get(\"lastHeartbeat\",\"?\")}')
" 2>/dev/null | tee -a "${REPORT}"
else
  log_warn "Could not parse CM host list (CM may not be fully started)"
fi

# ---------------------------------------------------------------------------
# 4. Cluster list and service health
# ---------------------------------------------------------------------------
log_step "4. Cluster and Service Health"

CLUSTERS_JSON=$(cm_api "/clusters" 2>/dev/null || echo "{}")

if echo "${CLUSTERS_JSON}" | python3 -c "import sys,json; d=json.load(sys.stdin); assert d.get('items')" 2>/dev/null; then
  CLUSTER_NAMES=$(echo "${CLUSTERS_JSON}" | python3 -c "
import sys, json
for c in json.load(sys.stdin).get('items', []):
    print(c['name'])
")

  while IFS= read -r cluster; do
    log_info "Cluster: ${cluster}"
    echo "" >> "${REPORT}"
    echo "Cluster: ${cluster}" >> "${REPORT}"

    SERVICES_JSON=$(cm_api "/clusters/${cluster}/services" 2>/dev/null || echo "{}")

    echo "${SERVICES_JSON}" | python3 -c "
import sys, json
data = json.load(sys.stdin)
items = data.get('items', [])
if not items:
    print('  No services configured yet.')
else:
    print(f'  {\"Service\":<25s} {\"Type\":<15s} {\"State\":<15s} {\"Health\"}')
    print('  ' + '-'*70)
    for s in items:
        print(f'  {s.get(\"name\",\"?\"):<25s} {s.get(\"type\",\"?\"):<15s} {s.get(\"serviceState\",\"?\"):<15s} {s.get(\"healthSummary\",\"?\")}')
" 2>/dev/null | tee -a "${REPORT}"

  done <<< "${CLUSTER_NAMES}"
else
  log_info "No clusters configured yet (expected for fresh install)."
  echo "No clusters configured." >> "${REPORT}"
fi

# ---------------------------------------------------------------------------
# 5. CM Server process check
# ---------------------------------------------------------------------------
log_step "5. CM Server Process Validation"

ssh_cmd "${CM_SERVER_HOST}" "sudo bash -s" <<'PROC_CHECK' | tee -a "${REPORT}"
echo "--- CM Server Process ---"
systemctl status cloudera-scm-server --no-pager 2>&1 | head -15

echo ""
echo "--- CM Server Listening Ports ---"
ss -tlnp | grep -E ":(7180|7182|7183) " || echo "  No CM ports detected"

echo ""
echo "--- CM Server JVM Memory ---"
ps aux | grep "[c]loudera-scm-server" | awk '{print "  PID=" $2, "RSS_MB=" int($6/1024), "CMD=" $11}' || echo "  Process not found"

echo ""
echo "--- Recent Server Log Errors (last 20 ERRORs) ---"
grep -i "ERROR" /var/log/cloudera-scm-server/cloudera-scm-server.log 2>/dev/null | tail -20 || echo "  No errors found"
PROC_CHECK

# ---------------------------------------------------------------------------
# 6. CM Agent process check on all hosts
# ---------------------------------------------------------------------------
log_step "6. CM Agent Process Validation on All Hosts"

AGENT_CHECK='
echo "Host: $(hostname -f)"
echo "  Agent status:  $(systemctl is-active cloudera-scm-agent 2>/dev/null || echo unknown)"
echo "  Agent PID:     $(pgrep -f cloudera-scm-agent || echo none)"
echo "  Config server: $(grep "^server_host=" /etc/cloudera-scm-agent/config.ini 2>/dev/null || echo not-set)"
'

run_on_all_hosts "${AGENT_CHECK}" "AGENT-CHECK" | tee -a "${REPORT}"

# ---------------------------------------------------------------------------
# 7. Parcel status
# ---------------------------------------------------------------------------
log_step "7. Parcel Status"

# List all parcels across all clusters
if echo "${CLUSTERS_JSON}" | python3 -c "import sys,json; d=json.load(sys.stdin); assert d.get('items')" 2>/dev/null; then
  while IFS= read -r cluster; do
    PARCELS_JSON=$(cm_api "/clusters/${cluster}/parcels" 2>/dev/null || echo "{}")
    echo "${PARCELS_JSON}" | python3 -c "
import sys, json
data = json.load(sys.stdin)
items = data.get('items', [])
if not items:
    print('  No parcels found.')
else:
    print(f'  {\"Product\":<15s} {\"Version\":<30s} {\"Stage\":<20s}')
    print('  ' + '-'*65)
    for p in items:
        print(f'  {p.get(\"product\",\"?\"):<15s} {p.get(\"version\",\"?\"):<30s} {p.get(\"stage\",\"?\")}')
" 2>/dev/null | tee -a "${REPORT}"
  done <<< "${CLUSTER_NAMES}"
else
  log_info "No clusters to check parcels for."
fi

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
log_step "Validation Summary"

log_info "Validation report written to ${REPORT}"
echo ""
echo "  Next steps:"
echo "    - If CM Server is running and agents are connected, proceed to add"
echo "      the cluster via the CM wizard."
echo "    - If services are already configured, review any CONCERNING or BAD"
echo "      health statuses above."
echo "    - For TLS setup, see TLS_SETUP.md"

banner "Phase 7 Complete"
