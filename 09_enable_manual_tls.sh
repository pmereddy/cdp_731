#!/usr/bin/env bash
###############################################################################
# 09_enable_manual_tls.sh
#
# Enable Manual TLS on an existing CDP 7.3.1 cluster.
#
# This is the manual-TLS counterpart to 09_enable_auto_tls.sh.
# Use this when you need enterprise CA-signed or wildcard certificates
# instead of the CM-managed internal CA.
#
# This script:
#   1.  Creates TLS directory on all hosts
#   2.  Generates keystores + CSRs on all hosts
#   3.  Signs certificates (self-sign or pause for external CA)
#   4.  Distributes CA certs to all hosts
#   5.  Imports CA certs into JDK truststore on all hosts
#   6.  Imports signed certs into host keystores
#   7.  Exports private keys for agent use
#   8.  Creates symbolic links (server.jks, agent.pem, agent.key)
#   9.  Creates agent password files
#   10. Configures CM Agent config.ini (use_tls, verify_cert_file, client_*)
#   11. Enables HTTPS for CM Admin Console via CM API
#   12. Restarts CM Server and all Agents
#   13. Validates TLS on CM and all hosts
#
# After this script completes:
#   - Restart Cloudera Management Services from CM Console
#   - Restart all cluster services from CM Console
#   - Enable per-service TLS in CM (see MANUAL_TLS_SETUP.md)
###############################################################################
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"
load_env

require_vars CM_SERVER_HOST ALL_HOSTS SSH_USER SSH_KEY \
             KEYSTORE_PASS TLS_CERT_DIR JAVA_HOME \
             TLS_ORG TLS_OU TLS_CITY TLS_STATE TLS_COUNTRY \
             CM_API_USER CM_API_PASS

TRUSTSTORE_PASS="${TRUSTSTORE_PASS:-changeit}"
CM_PORT="${CM_API_PORT:-7180}"
CM_USER="${CM_API_USER}"
CM_PASS="${CM_API_PASS}"

banner "Enable Manual TLS for CDP 7.3.1"

check_ssh_connectivity

OUT_DIR="${SCRIPT_DIR}/output/tls"
mkdir -p "${OUT_DIR}"

# ---------------------------------------------------------------------------
# Determine CA mode
# ---------------------------------------------------------------------------
if [[ -n "${ROOT_CA_CERT:-}" && -f "${ROOT_CA_CERT}" ]]; then
  CA_MODE="external"
  log_info "CA Mode: EXTERNAL (using provided root CA at ${ROOT_CA_CERT})"
else
  CA_MODE="selfsign"
  log_info "CA Mode: SELF-SIGNED (ROOT_CA_CERT not set or not found)"
  log_info "A self-signed root CA will be generated for dev/test use."
fi

# ===================================================================
# Step 1: Create TLS directory on all hosts
# ===================================================================
log_step "Step 1: Create TLS directory on all hosts"

run_sudo_on_all_hosts "
  mkdir -p ${TLS_CERT_DIR}
  chown root:root ${TLS_CERT_DIR}
  chmod 755 ${TLS_CERT_DIR}
  echo '  [OK] ${TLS_CERT_DIR} ready'
" "TLS-DIR"

# ===================================================================
# Step 2: Generate keystores + CSRs on all hosts
# ===================================================================
log_step "Step 2: Generate keystores and CSRs on all hosts"

read -ra _hosts <<< "${ALL_HOSTS}"
for h in "${_hosts[@]}"; do
  log_host "Generating keystore + CSR on ${h}"
  ssh_cmd "${h}" "sudo bash -s" <<KEYGEN
set -euo pipefail

HOST_FQDN=\$(hostname -f)
KS="${TLS_CERT_DIR}/\${HOST_FQDN}.jks"
CSR="${TLS_CERT_DIR}/\${HOST_FQDN}.csr"

if [ -f "\${KS}" ]; then
  echo "  Keystore already exists: \${KS} -- skipping generation"
else
  ${JAVA_HOME}/bin/keytool -genkeypair \
    -alias \${HOST_FQDN} \
    -keyalg RSA \
    -keysize 2048 \
    -keystore \${KS} \
    -dname "CN=\${HOST_FQDN},OU=${TLS_OU},O=${TLS_ORG},L=${TLS_CITY},ST=${TLS_STATE},C=${TLS_COUNTRY}" \
    -ext san=dns:\${HOST_FQDN} \
    -storepass '${KEYSTORE_PASS}' \
    -keypass '${KEYSTORE_PASS}'
  echo "  [OK] Keystore generated: \${KS}"
fi

${JAVA_HOME}/bin/keytool -certreq \
  -alias \${HOST_FQDN} \
  -keystore \${KS} \
  -file \${CSR} \
  -ext san=dns:\${HOST_FQDN} \
  -ext EKU=serverAuth,clientAuth \
  -storepass '${KEYSTORE_PASS}'
echo "  [OK] CSR generated: \${CSR}"
KEYGEN
done

# ===================================================================
# Step 3: Sign certificates
# ===================================================================
log_step "Step 3: Sign certificates"

if [[ "${CA_MODE}" == "selfsign" ]]; then
  # --- Generate self-signed root CA on the CM server host ---
  log_info "Generating self-signed root CA on ${CM_SERVER_HOST} ..."

  ssh_cmd "${CM_SERVER_HOST}" "sudo bash -s" <<SELF_CA
set -euo pipefail

CA_KEY="${TLS_CERT_DIR}/rootca.key"
CA_CERT="${TLS_CERT_DIR}/rootca.pem"

if [ -f "\${CA_CERT}" ] && [ -f "\${CA_KEY}" ]; then
  echo "  Root CA already exists -- reusing"
else
  openssl req -x509 -newkey rsa:4096 -sha256 -days 3650 -nodes \
    -keyout \${CA_KEY} \
    -out \${CA_CERT} \
    -subj "/CN=CDP-Root-CA,O=${TLS_ORG},C=${TLS_COUNTRY}" \
    2>/dev/null
  chmod 400 \${CA_KEY}
  echo "  [OK] Self-signed root CA generated"
fi

echo "  CA Subject:"
openssl x509 -in \${CA_CERT} -noout -subject -dates
SELF_CA

  # --- Sign each host's CSR with the root CA ---
  log_info "Signing host CSRs with self-signed CA ..."

  for h in "${_hosts[@]}"; do
    log_host "Signing CSR for ${h}"

    HOST_FQDN=$(ssh_cmd "${h}" "hostname -f")
    CSR_FILE="${TLS_CERT_DIR}/${HOST_FQDN}.csr"
    CERT_FILE="${TLS_CERT_DIR}/${HOST_FQDN}.pem"

    # Copy CSR from remote host to CM server, sign, copy cert back
    ssh_cmd "${h}" "sudo cat ${CSR_FILE}" > "${OUT_DIR}/${HOST_FQDN}.csr"

    # Copy CSR to CM server host for signing
    scp ${SSH_OPTS} -i "${SSH_KEY}" "${OUT_DIR}/${HOST_FQDN}.csr" \
      "${SSH_USER}@${CM_SERVER_HOST}:/tmp/${HOST_FQDN}.csr"

    # Sign on CM server host
    ssh_cmd "${CM_SERVER_HOST}" "sudo bash -s" <<SIGN_CSR
set -euo pipefail

# Create SAN extension file
cat > /tmp/${HOST_FQDN}.ext <<EXTEOF
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage = digitalSignature, nonRepudiation, keyEncipherment, dataEncipherment
extendedKeyUsage = serverAuth, clientAuth
subjectAltName = DNS:${HOST_FQDN}
EXTEOF

openssl x509 -req -sha256 -days 1095 \
  -in /tmp/${HOST_FQDN}.csr \
  -CA ${TLS_CERT_DIR}/rootca.pem \
  -CAkey ${TLS_CERT_DIR}/rootca.key \
  -CAcreateserial \
  -out /tmp/${HOST_FQDN}.pem \
  -extfile /tmp/${HOST_FQDN}.ext \
  2>/dev/null

echo "  [OK] Certificate signed for ${HOST_FQDN}"
openssl x509 -in /tmp/${HOST_FQDN}.pem -noout -subject -dates
SIGN_CSR

    # Copy signed cert back to the target host
    scp ${SSH_OPTS} -i "${SSH_KEY}" \
      "${SSH_USER}@${CM_SERVER_HOST}:/tmp/${HOST_FQDN}.pem" \
      "${OUT_DIR}/${HOST_FQDN}.pem"

    scp ${SSH_OPTS} -i "${SSH_KEY}" \
      "${OUT_DIR}/${HOST_FQDN}.pem" \
      "${SSH_USER}@${h}:/tmp/${HOST_FQDN}.pem"

    ssh_cmd "${h}" "sudo cp /tmp/${HOST_FQDN}.pem ${TLS_CERT_DIR}/${HOST_FQDN}.pem && rm -f /tmp/${HOST_FQDN}.pem"
  done

else
  # --- External CA mode: pause for manual signing ---
  log_warn "EXTERNAL CA MODE"
  log_warn "CSRs have been generated on each host at:"
  log_warn "  ${TLS_CERT_DIR}/<hostname>.csr"
  echo ""
  echo "  Steps to complete:"
  echo "    1. Collect CSRs from each host"
  echo "    2. Submit to your enterprise CA for signing"
  echo "    3. Request PEM format with serverAuth + clientAuth EKU"
  echo "    4. Place signed certs on each host at:"
  echo "         ${TLS_CERT_DIR}/<hostname>.pem"
  echo "    5. Place root CA at: ${TLS_CERT_DIR}/rootca.pem  on all hosts"
  echo "    6. Place intermediate CA at: ${TLS_CERT_DIR}/intca.pem  (if applicable)"
  echo ""

  pause_prompt "Press ENTER after all signed certificates are in place ..."
fi

# ===================================================================
# Step 4: Distribute CA cert(s) to all hosts
# ===================================================================
log_step "Step 4: Distribute CA certificates to all hosts"

# Fetch root CA from CM server host
ssh_cmd "${CM_SERVER_HOST}" "sudo cat ${TLS_CERT_DIR}/rootca.pem" > "${OUT_DIR}/rootca.pem"

if [[ ! -s "${OUT_DIR}/rootca.pem" ]]; then
  log_error "Root CA not found at ${TLS_CERT_DIR}/rootca.pem on ${CM_SERVER_HOST}"
  exit 1
fi

# Fetch intermediate CA if it exists
ssh_cmd "${CM_SERVER_HOST}" "sudo cat ${TLS_CERT_DIR}/intca.pem 2>/dev/null" > "${OUT_DIR}/intca.pem" || true

for h in "${_hosts[@]}"; do
  log_host "Distributing CA certs to ${h}"
  scp ${SSH_OPTS} -i "${SSH_KEY}" "${OUT_DIR}/rootca.pem" "${SSH_USER}@${h}:/tmp/rootca.pem"
  ssh_cmd "${h}" "sudo cp /tmp/rootca.pem ${TLS_CERT_DIR}/rootca.pem && sudo chmod 644 ${TLS_CERT_DIR}/rootca.pem && rm -f /tmp/rootca.pem"

  if [[ -s "${OUT_DIR}/intca.pem" ]]; then
    scp ${SSH_OPTS} -i "${SSH_KEY}" "${OUT_DIR}/intca.pem" "${SSH_USER}@${h}:/tmp/intca.pem"
    ssh_cmd "${h}" "sudo cp /tmp/intca.pem ${TLS_CERT_DIR}/intca.pem && sudo chmod 644 ${TLS_CERT_DIR}/intca.pem && rm -f /tmp/intca.pem"
  fi
done

# ===================================================================
# Step 5: Import CA certs into JDK truststore on all hosts
# ===================================================================
log_step "Step 5: Import CA certs into JDK truststore"

TRUSTSTORE_IMPORT="
set -euo pipefail
TS='${JAVA_HOME}/lib/security/cacerts'

if ! ${JAVA_HOME}/bin/keytool -list -keystore \${TS} -storepass '${TRUSTSTORE_PASS}' -alias cdp-rootca &>/dev/null; then
  ${JAVA_HOME}/bin/keytool -importcert -alias cdp-rootca \
    -keystore \${TS} -file ${TLS_CERT_DIR}/rootca.pem \
    -storepass '${TRUSTSTORE_PASS}' -noprompt
  echo '  [OK] Root CA imported into JDK truststore'
else
  echo '  [OK] Root CA already in JDK truststore'
fi

if [ -f ${TLS_CERT_DIR}/intca.pem ]; then
  if ! ${JAVA_HOME}/bin/keytool -list -keystore \${TS} -storepass '${TRUSTSTORE_PASS}' -alias cdp-intca &>/dev/null; then
    ${JAVA_HOME}/bin/keytool -importcert -alias cdp-intca \
      -keystore \${TS} -file ${TLS_CERT_DIR}/intca.pem \
      -storepass '${TRUSTSTORE_PASS}' -noprompt
    echo '  [OK] Intermediate CA imported into JDK truststore'
  else
    echo '  [OK] Intermediate CA already in JDK truststore'
  fi
fi
"

run_sudo_on_all_hosts "${TRUSTSTORE_IMPORT}" "JDK-TRUSTSTORE"

# ===================================================================
# Step 6: Import signed certs into host keystores
# ===================================================================
log_step "Step 6: Import signed certs into host keystores"

for h in "${_hosts[@]}"; do
  log_host "Importing certs on ${h}"
  ssh_cmd "${h}" "sudo bash -s" <<IMPORT_CERT
set -euo pipefail
HOST_FQDN=\$(hostname -f)
KS="${TLS_CERT_DIR}/\${HOST_FQDN}.jks"

# Import root CA
if ! ${JAVA_HOME}/bin/keytool -list -keystore \${KS} -storepass '${KEYSTORE_PASS}' -alias cdp-rootca &>/dev/null; then
  ${JAVA_HOME}/bin/keytool -importcert -alias cdp-rootca \
    -keystore \${KS} -file ${TLS_CERT_DIR}/rootca.pem \
    -storepass '${KEYSTORE_PASS}' -noprompt
fi

# Import intermediate CA if present
if [ -f ${TLS_CERT_DIR}/intca.pem ]; then
  if ! ${JAVA_HOME}/bin/keytool -list -keystore \${KS} -storepass '${KEYSTORE_PASS}' -alias cdp-intca &>/dev/null; then
    ${JAVA_HOME}/bin/keytool -importcert -alias cdp-intca \
      -keystore \${KS} -file ${TLS_CERT_DIR}/intca.pem \
      -storepass '${KEYSTORE_PASS}' -noprompt
  fi
fi

# Import signed host cert (replaces the self-signed one)
${JAVA_HOME}/bin/keytool -importcert -alias \${HOST_FQDN} \
  -keystore \${KS} -file ${TLS_CERT_DIR}/\${HOST_FQDN}.pem \
  -storepass '${KEYSTORE_PASS}' -noprompt 2>/dev/null || echo "  (cert may already be imported)"

echo "  [OK] Keystore contents:"
${JAVA_HOME}/bin/keytool -list -keystore \${KS} -storepass '${KEYSTORE_PASS}' | grep -i alias
IMPORT_CERT
done

# ===================================================================
# Step 7: Export private keys for agent use
# ===================================================================
log_step "Step 7: Export private keys for agent use"

for h in "${_hosts[@]}"; do
  log_host "Exporting private key on ${h}"
  ssh_cmd "${h}" "sudo bash -s" <<EXPORT_KEY
set -euo pipefail
HOST_FQDN=\$(hostname -f)
KS="${TLS_CERT_DIR}/\${HOST_FQDN}.jks"
P12="${TLS_CERT_DIR}/\${HOST_FQDN}-key.p12"
KEY="${TLS_CERT_DIR}/\${HOST_FQDN}.key"

if [ -f "\${KEY}" ]; then
  echo "  Private key already exists -- skipping"
else
  ${JAVA_HOME}/bin/keytool -importkeystore \
    -srckeystore \${KS} \
    -destkeystore \${P12} \
    -deststoretype PKCS12 \
    -srcalias \${HOST_FQDN} \
    -srcstorepass '${KEYSTORE_PASS}' \
    -deststorepass '${KEYSTORE_PASS}' 2>/dev/null

  openssl pkcs12 -in \${P12} -nocerts \
    -out \${KEY} \
    -passin pass:'${KEYSTORE_PASS}' \
    -passout pass:'${KEYSTORE_PASS}' 2>/dev/null

  chmod 400 \${KEY}
  chown cloudera-scm:cloudera-scm \${KEY} 2>/dev/null || true
  rm -f \${P12}
  echo "  [OK] Private key exported: \${KEY}"
fi
EXPORT_KEY
done

# ===================================================================
# Step 8: Create symbolic links
# ===================================================================
log_step "Step 8: Create symbolic links on all hosts"

SYMLINK_SCRIPT='
set -euo pipefail
HOST_FQDN=$(hostname -f)
cd '"${TLS_CERT_DIR}"'

ln -sf ${HOST_FQDN}.jks server.jks
ln -sf ${HOST_FQDN}.pem agent.pem
ln -sf ${HOST_FQDN}.key agent.key

echo "  server.jks -> $(readlink server.jks)"
echo "  agent.pem  -> $(readlink agent.pem)"
echo "  agent.key  -> $(readlink agent.key)"
'

run_sudo_on_all_hosts "${SYMLINK_SCRIPT}" "SYMLINKS"

# ===================================================================
# Step 9: Create agent password files
# ===================================================================
log_step "Step 9: Create agent password files on all hosts"

PWFILE_SCRIPT="
set -euo pipefail
echo '${KEYSTORE_PASS}' > /etc/cloudera-scm-agent/agentkey.pw
chown root:root /etc/cloudera-scm-agent/agentkey.pw
chmod 440 /etc/cloudera-scm-agent/agentkey.pw
echo '  [OK] /etc/cloudera-scm-agent/agentkey.pw created'
"

run_sudo_on_all_hosts "${PWFILE_SCRIPT}" "AGENT-PW"

# ===================================================================
# Step 10: Configure CM Agent config.ini
# ===================================================================
log_step "Step 10: Configure CM Agent config.ini on all hosts"

AGENT_CONFIG="
set -euo pipefail
CFG='/etc/cloudera-scm-agent/config.ini'
cp -a \"\${CFG}\" \"\${CFG}.bak.\$(date +%Y%m%d_%H%M%S)\"

set_ini() {
  local key=\"\$1\" val=\"\$2\"
  if grep -q \"^\${key}=\" \"\${CFG}\" 2>/dev/null; then
    sed -i \"s|^\${key}=.*|\${key}=\${val}|\" \"\${CFG}\"
  elif grep -q \"^#.*\${key}=\" \"\${CFG}\" 2>/dev/null; then
    sed -i \"s|^#.*\${key}=.*|\${key}=\${val}|\" \"\${CFG}\"
  else
    echo \"\${key}=\${val}\" >> \"\${CFG}\"
  fi
}

set_ini use_tls 1
set_ini verify_cert_file ${TLS_CERT_DIR}/rootca.pem
set_ini client_cert_file ${TLS_CERT_DIR}/agent.pem
set_ini client_key_file  ${TLS_CERT_DIR}/agent.key
set_ini client_keypw_file /etc/cloudera-scm-agent/agentkey.pw

echo '  Updated config.ini:'
grep -nE '^(use_tls|verify_cert_file|client_cert_file|client_key_file|client_keypw_file|server_host)=' \"\${CFG}\"
"

run_sudo_on_all_hosts "${AGENT_CONFIG}" "AGENT-CONFIG"

# ===================================================================
# Step 11: Enable HTTPS for CM Admin Console via API
# ===================================================================
log_step "Step 11: Enable HTTPS for CM Admin Console"

CM_HTTP="http://${CM_SERVER_HOST}:${CM_PORT}"

log_info "Configuring CM Server TLS settings via API ..."

# Set keystore path and password, enable TLS
curl -s -u "${CM_USER}:${CM_PASS}" \
  -X PUT \
  -H "Content-Type: application/json" \
  "${CM_HTTP}/api/v54/cm/config" \
  -d "{
    \"items\": [
      { \"name\": \"WEB_TLS\",                  \"value\": \"true\" },
      { \"name\": \"KEYSTORE_PATH\",            \"value\": \"${TLS_CERT_DIR}/server.jks\" },
      { \"name\": \"KEYSTORE_PASSWORD\",         \"value\": \"${KEYSTORE_PASS}\" },
      { \"name\": \"TRUSTSTORE_PATH\",           \"value\": \"${JAVA_HOME}/lib/security/cacerts\" },
      { \"name\": \"TRUSTSTORE_PASSWORD\",       \"value\": \"${TRUSTSTORE_PASS}\" },
      { \"name\": \"AGENT_TLS\",                 \"value\": \"true\" },
      { \"name\": \"NEED_AGENT_VALIDATION\",     \"value\": \"true\" }
    ]
  }" 2>/dev/null | python3 -m json.tool 2>/dev/null || log_warn "API config update may have partially failed -- verify in CM Console"

log_info "CM TLS settings applied via API."
log_info "If the API call failed, configure manually in CM Console:"
log_info "  Administration > Settings > Security"
log_info "  - Use TLS Encryption for Admin Console: checked"
log_info "  - Keystore File Location: ${TLS_CERT_DIR}/server.jks"
log_info "  - Keystore File Password: <KEYSTORE_PASS>"
log_info "  - Use TLS Encryption for Agents: checked"
log_info "  - Use TLS Authentication of Agents to Server: checked"
log_info "  - Truststore File: ${JAVA_HOME}/lib/security/cacerts"
log_info "  - Truststore Password: <TRUSTSTORE_PASS>"

# ===================================================================
# Step 12: Restart CM Server and all Agents
# ===================================================================
log_step "Step 12: Restart CM Server and all Agents"

log_info "Restarting CM Server on ${CM_SERVER_HOST} ..."
ssh_cmd "${CM_SERVER_HOST}" "sudo systemctl restart cloudera-scm-server"

log_info "Waiting for CM Server on HTTPS port 7183 ..."
for i in $(seq 1 60); do
  sleep 10
  if curl -sk -o /dev/null -w "%{http_code}" "https://${CM_SERVER_HOST}:7183/api/v54/cm/version" -u "${CM_USER}:${CM_PASS}" 2>/dev/null | grep -q "200"; then
    log_info "CM Server is up on HTTPS:7183 after ~$((i * 10)) seconds."
    break
  fi
  log_info "  Waiting ... (${i}/60)"
done

log_info "Restarting all CM Agents ..."

AGENT_RESTART='
set -euo pipefail
systemctl restart cloudera-scm-agent
echo "  Agent status: $(systemctl is-active cloudera-scm-agent)"
'

run_sudo_on_all_hosts "${AGENT_RESTART}" "RESTART-AGENT"

# ===================================================================
# Step 13: Validation
# ===================================================================
log_step "Step 13: Validate Manual TLS"

echo ""
echo "--- CM HTTPS endpoint ---"
curl -sk -u "${CM_USER}:${CM_PASS}" "https://${CM_SERVER_HOST}:7183/api/v54/cm/version" 2>/dev/null | \
  python3 -m json.tool 2>/dev/null || log_warn "CM HTTPS not responding yet"

echo ""
echo "--- CM Server certificate ---"
echo | openssl s_client -connect "${CM_SERVER_HOST}:7183" 2>/dev/null | \
  openssl x509 -noout -subject -issuer -dates 2>/dev/null || log_warn "Could not retrieve cert"

echo ""
echo "--- Certificate chain validation ---"
for h in "${_hosts[@]}"; do
  log_host "Validating certs on ${h}"
  ssh_cmd "${h}" "
    openssl verify -CAfile ${TLS_CERT_DIR}/rootca.pem ${TLS_CERT_DIR}/agent.pem 2>&1 || echo '  VERIFY FAILED'
    echo '  Agent config:'
    grep -E '^(use_tls|verify_cert_file|client_cert_file)=' /etc/cloudera-scm-agent/config.ini 2>/dev/null
    echo '  Agent status: '\$(systemctl is-active cloudera-scm-agent 2>/dev/null)
  " || log_warn "Could not validate ${h}"
done

# ---------------------------------------------------------------------------
# Post-TLS Reminder
# ---------------------------------------------------------------------------
log_step "Post Manual-TLS Steps"

cat <<'REMINDER'

  Manual TLS is now enabled for CM Server and Agents. Next steps:

  [ ] 1. Access CM at https://<CM_HOST>:7183

  [ ] 2. Restart Cloudera Management Services:
         Cloudera Management Service > Actions > Restart

  [ ] 3. In CM Console, configure Management Service truststore:
         Cloudera Management Service > Configuration > Security
         - TLS/SSL Client Truststore File Location: <JAVA_HOME>/lib/security/cacerts
         - Truststore Password: <TRUSTSTORE_PASS>

  [ ] 4. Restart all cluster services:
         Cluster > Actions > Restart

  [ ] 5. Enable TLS for individual CDP services in CM Console:
         (HDFS, YARN, Hive, Impala, HBase, Hue, Oozie, Ranger, Spark, Kafka)
         See MANUAL_TLS_SETUP.md for per-service configuration.

  [ ] 6. Update .env: set CM_API_PORT="7183"

  [ ] 7. After TLS is confirmed, proceed to Kerberos:
         ./10_kerberos_prerequisites.sh

REMINDER

banner "09_enable_manual_tls Complete"
