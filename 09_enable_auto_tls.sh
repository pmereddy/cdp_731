#!/usr/bin/env bash
###############################################################################
# 09_enable_auto_tls.sh
#
# Enable Auto-TLS on an existing CDP 7.3.1 cluster using Cloudera Manager's
# internal CA (Use Case 1).
#
# This script:
#   1. Verifies CM is reachable on HTTP
#   2. Triggers the GenerateCMCA API to enable Auto-TLS
#   3. Waits for the command to complete
#   4. Restarts CM Server
#   5. Verifies CM is reachable on HTTPS (port 7183)
#   6. Updates .env with the new API port
#
# After this script completes:
#   - Restart Cloudera Management Services from CM Console
#   - Restart all cluster services from CM Console
###############################################################################
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"
load_env

require_vars CM_SERVER_HOST ALL_HOSTS SSH_USER SSH_KEY CM_API_USER CM_API_PASS

CM_PORT="${CM_API_PORT:-7180}"
CM_USER="${CM_API_USER}"
CM_PASS="${CM_API_PASS}"

banner "Enable Auto-TLS (Use Case 1: CM Internal CA)"

# ---------------------------------------------------------------------------
# Step 1: Verify CM is reachable
# ---------------------------------------------------------------------------
log_step "Step 1: Verify Cloudera Manager is reachable"

CM_HTTP="http://${CM_SERVER_HOST}:${CM_PORT}"
CM_HTTPS="https://${CM_SERVER_HOST}:7183"

VERSION_RESP=$(curl -s -u "${CM_USER}:${CM_PASS}" "${CM_HTTP}/api/v54/cm/version" 2>/dev/null || echo "")

if echo "${VERSION_RESP}" | python3 -c "import sys,json; json.load(sys.stdin)" 2>/dev/null; then
  log_info "CM is reachable at ${CM_HTTP}"
  echo "${VERSION_RESP}" | python3 -m json.tool
else
  # Maybe already on HTTPS?
  VERSION_RESP=$(curl -sk -u "${CM_USER}:${CM_PASS}" "${CM_HTTPS}/api/v54/cm/version" 2>/dev/null || echo "")
  if echo "${VERSION_RESP}" | python3 -c "import sys,json; json.load(sys.stdin)" 2>/dev/null; then
    log_info "CM is already reachable on HTTPS at ${CM_HTTPS}"
    log_info "Auto-TLS may already be enabled. Skipping to validation."
    CM_HTTP="${CM_HTTPS}"
    CM_PORT="7183"
  else
    log_error "CM is not reachable on HTTP:${CM_PORT} or HTTPS:7183"
    exit 1
  fi
fi

# ---------------------------------------------------------------------------
# Step 2: Check if Auto-TLS is already enabled
# ---------------------------------------------------------------------------
log_step "Step 2: Check current TLS status"

TLS_STATUS=$(curl -sk -u "${CM_USER}:${CM_PASS}" "${CM_HTTP}/api/v54/cm/config" 2>/dev/null | \
  python3 -c "
import sys, json
data = json.load(sys.stdin)
for item in data.get('items', []):
    if item.get('name') == 'AUTO_TLS':
        print(item.get('value', 'false'))
        sys.exit(0)
print('false')
" 2>/dev/null || echo "false")

if [[ "${TLS_STATUS}" == "true" ]]; then
  log_info "Auto-TLS is already enabled."
  log_info "Skipping to validation."
else
  log_info "Auto-TLS is NOT yet enabled. Proceeding."

  # -------------------------------------------------------------------------
  # Step 3: Build SSH credential payload
  # -------------------------------------------------------------------------
  log_step "Step 3: Enable Auto-TLS via CM API"

  # Determine SSH private key content for the API call
  SSH_KEY_EXPANDED="${SSH_KEY/#\~/$HOME}"
  if [[ ! -f "${SSH_KEY_EXPANDED}" ]]; then
    log_error "SSH private key not found at ${SSH_KEY_EXPANDED}"
    exit 1
  fi
  SSH_KEY_CONTENT=$(cat "${SSH_KEY_EXPANDED}")

  log_info "Calling /cm/commands/generateCMCA ..."
  log_warn "This will generate the internal CA and deploy certs to all hosts."
  log_warn "CM Server will need to be restarted afterward."

  GENERATE_RESP=$(curl -s -u "${CM_USER}:${CM_PASS}" \
    -X POST \
    -H "Content-Type: application/json" \
    "${CM_HTTP}/api/v54/cm/commands/generateCMCA" \
    -d "$(python3 -c "
import json, sys
payload = {
    'sshPort': 22,
    'userName': '${SSH_USER}',
    'privateKey': '''${SSH_KEY_CONTENT}''',
    'location': '/opt/cloudera/AutoTLS',
    'customCA': False,
    'interpretAsFilenames': False,
    'configureAllServices': True,
    'addlTrustedCaCerts': ''
}
print(json.dumps(payload))
")" 2>/dev/null)

  if echo "${GENERATE_RESP}" | python3 -c "import sys,json; d=json.load(sys.stdin); assert 'id' in d" 2>/dev/null; then
    CMD_ID=$(echo "${GENERATE_RESP}" | python3 -c "import sys,json; print(json.load(sys.stdin)['id'])")
    log_info "GenerateCMCA command started. Command ID: ${CMD_ID}"
  else
    log_error "Failed to start GenerateCMCA command."
    log_error "Response: ${GENERATE_RESP}"
    log_error ""
    log_error "If API-based enablement fails, use the CM Admin Console instead:"
    log_error "  Administration > Security > Enable Auto-TLS"
    exit 1
  fi

  # -------------------------------------------------------------------------
  # Step 4: Wait for the command to complete
  # -------------------------------------------------------------------------
  log_step "Step 4: Waiting for Auto-TLS command to complete"

  for i in $(seq 1 60); do
    sleep 10
    CMD_STATUS=$(curl -s -u "${CM_USER}:${CM_PASS}" \
      "${CM_HTTP}/api/v54/commands/${CMD_ID}" 2>/dev/null)

    ACTIVE=$(echo "${CMD_STATUS}" | python3 -c "import sys,json; print(json.load(sys.stdin).get('active', True))" 2>/dev/null || echo "true")
    SUCCESS=$(echo "${CMD_STATUS}" | python3 -c "import sys,json; print(json.load(sys.stdin).get('success', False))" 2>/dev/null || echo "false")

    if [[ "${ACTIVE}" == "False" ]]; then
      if [[ "${SUCCESS}" == "True" ]]; then
        log_info "Auto-TLS command completed successfully after ~$((i * 10)) seconds."
      else
        log_error "Auto-TLS command failed."
        echo "${CMD_STATUS}" | python3 -m json.tool 2>/dev/null || echo "${CMD_STATUS}"
        exit 1
      fi
      break
    fi

    log_info "  Still running ... (${i}/60)"
  done

  # -------------------------------------------------------------------------
  # Step 5: Restart CM Server
  # -------------------------------------------------------------------------
  log_step "Step 5: Restart Cloudera Manager Server"

  ssh_cmd "${CM_SERVER_HOST}" "sudo systemctl restart cloudera-scm-server"
  log_info "CM Server restart initiated. Waiting for HTTPS port 7183 ..."

  for i in $(seq 1 60); do
    sleep 10
    if curl -sk -o /dev/null -w "%{http_code}" "https://${CM_SERVER_HOST}:7183/api/v54/cm/version" -u "${CM_USER}:${CM_PASS}" 2>/dev/null | grep -q "200"; then
      log_info "CM Server is up on HTTPS port 7183 after ~$((i * 10)) seconds."
      break
    fi
    log_info "  Waiting ... (${i}/60)"
  done
fi

# ---------------------------------------------------------------------------
# Step 6: Validation
# ---------------------------------------------------------------------------
log_step "Step 6: Validate Auto-TLS"

echo ""
echo "--- CM HTTPS endpoint ---"
curl -sk -u "${CM_USER}:${CM_PASS}" "https://${CM_SERVER_HOST}:7183/api/v54/cm/version" 2>/dev/null | \
  python3 -m json.tool 2>/dev/null || log_warn "CM HTTPS not responding yet"

echo ""
echo "--- CM Server certificate ---"
echo | openssl s_client -connect "${CM_SERVER_HOST}:7183" 2>/dev/null | \
  openssl x509 -noout -subject -issuer -dates 2>/dev/null || log_warn "Could not retrieve cert"

echo ""
echo "--- Agent cert files on each host ---"
read -ra _hosts <<< "${ALL_HOSTS}"
for h in "${_hosts[@]}"; do
  log_host "Checking agent certs on ${h}"
  ssh_cmd "${h}" "
    if [ -d /var/lib/cloudera-scm-agent/agent-cert ]; then
      ls -la /var/lib/cloudera-scm-agent/agent-cert/ 2>/dev/null
      echo ''
      grep -E '^(use_tls|verify_cert_file)' /etc/cloudera-scm-agent/config.ini 2>/dev/null
    else
      echo '  Agent cert directory not found yet'
    fi
  " || log_warn "Could not check ${h}"
done

# ---------------------------------------------------------------------------
# Reminder
# ---------------------------------------------------------------------------
log_step "Post Auto-TLS Steps (Manual)"

cat <<'REMINDER'

  Auto-TLS enablement is complete. Now do the following in the CM Console:

  [ ] 1. Access CM at https://<CM_HOST>:7183
         (browser will warn about self-signed CA -- this is expected)

  [ ] 2. Restart Cloudera Management Services:
         Cloudera Management Service > Actions > Restart

  [ ] 3. Restart all cluster services:
         Cluster > Actions > Restart

  [ ] 4. Verify all services are healthy and showing green status

  [ ] 5. Update .env: set CM_API_PORT="7183"

  [ ] 6. After TLS is confirmed working, proceed to enable Kerberos
         using 10_kerberos_prerequisites.sh

REMINDER

banner "09_enable_auto_tls Complete"
