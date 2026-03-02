# Manual TLS Setup for CDP 7.3.1 -- Quick Reference

Concise pre-requisites and steps for enabling manual TLS on Cloudera Manager
and all CDP services. Use this alongside `09_enable_manual_tls.sh`.

> **When to use Manual TLS vs Auto-TLS:**
> - **Auto-TLS** (`09_enable_auto_tls.sh`): CM generates and manages all certs.
>   Simplest. Good for dev/test or when your org has no PKI requirements.
> - **Manual TLS** (`09_enable_manual_tls.sh`): You control keystore generation,
>   CA signing, and cert deployment. Required when using enterprise CA-signed
>   or wildcard certificates.

---

## Pre-Requisites

### 1. Software

| Requirement | How to Verify |
|-------------|--------------|
| OpenJDK 17 installed on all hosts | `java -version` (should show 17.x) |
| `keytool` available | `keytool -help 2>&1 | head -1` |
| `openssl` available | `openssl version` |
| CM Server + all Agents running | `systemctl status cloudera-scm-server` |
| All hosts resolve by FQDN | `hostname -f` returns fully qualified name |

### 2. Network

| Port | Purpose | Direction |
|------|---------|-----------|
| 7183 | CM Admin Console (HTTPS) | Client to CM Server |
| 7182 | CM Agent to Server (TLS) | All agents to CM Server |

Ensure AWS Security Groups allow 7183 inbound from your admin network.

### 3. .env Configuration

Fill in these variables in `.env` before running the script:

```bash
KEYSTORE_PASS="<strong-password>"     # Same for keystore and key (CM requirement)
TRUSTSTORE_PASS="changeit"            # JDK default, or your custom password
TLS_CERT_DIR="/opt/cloudera/security/pki"

# Certificate subject fields
TLS_ORG="YourOrg"
TLS_OU="Engineering"
TLS_CITY="YourCity"
TLS_STATE="YourState"
TLS_COUNTRY="US"

# CA certificates -- leave ROOT_CA_CERT empty to self-sign (dev/test)
ROOT_CA_CERT=""                       # Path on CM host to root CA PEM
ROOT_CA_KEY=""                        # Path on CM host to root CA key
INT_CA_CERT=""                        # Path on CM host to intermediate CA PEM
```

### 4. Certificate Authority

**Option A -- Self-Signed CA (dev/test):**
The script generates a self-signed root CA automatically when `ROOT_CA_CERT`
is empty. Browsers will show a warning.

**Option B -- Enterprise CA (production):**
1. Set `ROOT_CA_CERT` and optionally `INT_CA_CERT` in `.env`
2. The script generates CSRs on every host
3. The script pauses so you can sign the CSRs with your CA
4. Place signed certs back and the script continues

---

## Steps Overview

The script `09_enable_manual_tls.sh` performs these steps:

```
Step 1   Create TLS directory on all hosts
Step 2   Generate keystores + CSRs on all hosts
Step 3   Sign certificates (self-sign or pause for external CA)
Step 4   Distribute CA certs to all hosts
Step 5   Import CA certs into JDK truststore on all hosts
Step 6   Import signed certs into host keystores
Step 7   Export private keys for agent use
Step 8   Create symbolic links (server.jks, agent.pem, agent.key)
Step 9   Create agent password files
Step 10  Configure CM Agent config.ini (use_tls, verify_cert_file, client certs)
Step 11  Enable HTTPS for CM Admin Console via API
Step 12  Restart CM Server and all Agents
Step 13  Validate TLS on CM and all hosts
```

---

## Running the Script

```bash
cd cdp_install/
vi .env                          # Fill in TLS variables

./09_enable_manual_tls.sh        # Execute
```

After the script completes:
1. Access CM at `https://<CM_HOST>:7183`
2. Restart Cloudera Management Services from CM Console
3. Restart all cluster services from CM Console
4. Enable TLS for individual CDP services (HDFS, YARN, Hive, etc.) via
   CM Console -- see "Per-Service TLS" below

---

## Per-Service TLS Configuration (CM Console)

After CM-level TLS is working, enable TLS per service in the CM Console
under each service's **Configuration > Security** section.

### Common properties for every service:

| Property | Value |
|----------|-------|
| Enable TLS/SSL | Checked |
| TLS/SSL Server JKS Keystore File | `/opt/cloudera/security/pki/server.jks` |
| TLS/SSL Server JKS Keystore Password | `<KEYSTORE_PASS>` |
| TLS/SSL Client Trust Store File | `$JAVA_HOME/lib/security/cacerts` |
| TLS/SSL Client Trust Store Password | `<TRUSTSTORE_PASS>` |

### Recommended service order:

1. ZooKeeper
2. HDFS (NameNode, DataNode, HttpFS)
3. YARN (ResourceManager, NodeManager)
4. Hive (HiveServer2, Metastore)
5. Impala (Daemon, StateStore, Catalog)
6. HBase
7. Hue
8. Oozie
9. Ranger
10. Spark History Server
11. Kafka

Restart each service after enabling TLS. CM will prompt for stale configs.

---

## Validation Commands

```bash
# CM HTTPS
echo | openssl s_client -connect <CM_HOST>:7183 2>/dev/null | \
  openssl x509 -noout -subject -issuer -dates

# Certificate chain
openssl verify -CAfile /opt/cloudera/security/pki/rootca.pem \
  /opt/cloudera/security/pki/agent.pem

# Key-cert match (MD5 hashes must be identical)
openssl rsa -in /opt/cloudera/security/pki/agent.key -noout -modulus \
  -passin pass:"<KEYSTORE_PASS>" | openssl md5
openssl x509 -in /opt/cloudera/security/pki/agent.pem -noout -modulus | openssl md5

# Keystore contents
keytool -list -v -keystore /opt/cloudera/security/pki/server.jks \
  -storepass "<KEYSTORE_PASS>" | grep -E "Alias|Owner|Issuer|Valid"

# Agent heartbeats -- check in CM Console: Hosts > All Hosts
```

---

## File Locations After Setup

| File | Path | On |
|------|------|----|
| Host keystore | `/opt/cloudera/security/pki/<hostname>.jks` | All |
| Keystore symlink | `/opt/cloudera/security/pki/server.jks` | All |
| Signed certificate | `/opt/cloudera/security/pki/<hostname>.pem` | All |
| Agent cert symlink | `/opt/cloudera/security/pki/agent.pem` | All |
| Private key | `/opt/cloudera/security/pki/<hostname>.key` | All |
| Agent key symlink | `/opt/cloudera/security/pki/agent.key` | All |
| Root CA | `/opt/cloudera/security/pki/rootca.pem` | All |
| Intermediate CA | `/opt/cloudera/security/pki/intca.pem` | All |
| JDK truststore | `$JAVA_HOME/lib/security/cacerts` | All |
| Agent config | `/etc/cloudera-scm-agent/config.ini` | All |
| Agent key password | `/etc/cloudera-scm-agent/agentkey.pw` | All |

---

## Troubleshooting

| Symptom | Likely Cause | Fix |
|---------|-------------|-----|
| CM won't start on 7183 | Wrong keystore path or password | Check `/var/log/cloudera-scm-server/cloudera-scm-server.log` |
| Agent heartbeat lost | `use_tls` not set or cert mismatch | Check `/var/log/cloudera-scm-agent/cloudera-scm-agent.log` |
| "WrongHost" error | `server_host` in config.ini uses IP | Change to FQDN |
| "certificate verify failed" | Root CA not in agent's verify path | Check `verify_cert_file` path |
| Key password != keystore password | CM requires they match | Re-generate keystore with matching passwords |
