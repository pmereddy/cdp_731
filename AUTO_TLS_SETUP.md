# Enabling Auto-TLS on CDP 7.3.1 / Cloudera Manager 7.13.1

Auto-TLS automates certificate generation, deployment, and configuration for
Cloudera Manager and all CDP services. Once enabled, any new hosts or services
added to the cluster are automatically TLS-enabled.

> **Order of operations:** Enable Auto-TLS **before** enabling Kerberos.
> The Kerberos wizard requires TLS to be active for secure keytab distribution.

---

## Table of Contents

1. [Use Case Options](#1-use-case-options)
2. [Use Case 1 -- CM as Internal CA (Simplest)](#2-use-case-1----cm-as-internal-ca-simplest)
3. [Use Case 2 -- CM with Existing Root CA](#3-use-case-2----cm-with-existing-root-ca)
4. [Use Case 3 -- Existing Enterprise Certificates](#4-use-case-3----existing-enterprise-certificates)
5. [Post-Enablement Validation](#5-post-enablement-validation)
6. [Auto-TLS File Locations](#6-auto-tls-file-locations)
7. [Certificate Rotation](#7-certificate-rotation)
8. [Troubleshooting](#8-troubleshooting)

---

## 1. Use Case Options

| Use Case | Who Signs Certs? | Best For | Complexity |
|----------|-----------------|----------|------------|
| **1. CM Internal CA** | Cloudera Manager | Dev/test, quick setup | Lowest |
| **2. Existing Root CA** | Your enterprise CA signs CM's intermediate CA | Production with enterprise PKI | Medium |
| **3. Existing Certificates** | You provide pre-signed certs for every host | Strict PKI policies | Highest |

For an AWS IaaS deployment, **Use Case 1** is the fastest path. Use Case 2 or 3
if your organization requires certificates signed by a corporate CA.

---

## 2. Use Case 1 -- CM as Internal CA (Simplest)

CM generates its own root CA and signs all host certificates automatically.

### Prerequisites

- CM Server and all Agents are running and communicating
- SSH access from CM Server to all hosts (same `SSH_USER` used during install)
- CM Admin Console accessible at `http://<CM_HOST>:7180`

### Steps

1. **Log in** to Cloudera Manager Admin Console

2. Navigate to **Administration > Security**

3. On the **Status** tab, click **Enable Auto-TLS**

4. On the **Enable Auto-TLS** page:
   - Select **Use Cloudera Manager to generate internal CA and corresponding certificates**
   - Under "Enable TLS for", select **All existing and future clusters**
   - Leave "Trusted CA Certificates Location" **empty** (unless you have additional CAs to trust)

5. Click **Next**

6. Enter **SSH credentials** for deploying certificates to all hosts:
   - SSH Username: `root` or another sudo-capable user
   - Authentication Method: password or private key
   - If using a non-root user, it must have passwordless sudo

7. Click **Next** -- CM will:
   - Generate a root CA certificate
   - Generate a keystore and CSR for each host
   - Sign all CSRs with the internal CA
   - Deploy certificates, keystores, and truststores to all hosts
   - Update CM Server and Agent configurations
   - Configure TLS for all services

8. When prompted, **restart Cloudera Manager Server**:
   ```bash
   sudo systemctl restart cloudera-scm-server
   ```

9. After CM restarts, access the Admin Console at **https://<CM_HOST>:7183**
   (the browser will warn about the self-signed CA -- this is expected)

10. **Restart Cloudera Management Services** from the CM Console

11. **Restart all cluster services** (CM will show stale configurations)

### Script

Run `09_enable_auto_tls.sh` to automate the API-based enablement.

---

## 3. Use Case 2 -- CM with Existing Root CA

CM generates an intermediate CA, which you get signed by your enterprise root CA.

### Steps

1. Navigate to **Administration > Security > Enable Auto-TLS**

2. Select **Use an existing Root CA**

3. CM generates an intermediate CA CSR -- download it

4. Submit the CSR to your enterprise CA for signing

5. Upload the signed intermediate CA certificate back to CM

6. CM uses the signed intermediate CA to sign all host certificates

7. Proceed with SSH credentials and restart as in Use Case 1

---

## 4. Use Case 3 -- Existing Enterprise Certificates

You provide pre-generated certificates and keys for every host.

### Steps

1. Generate a key pair and CSR for each host:
   ```bash
   openssl req -newkey rsa:2048 -sha256 -nodes \
     -keyout <hostname>.key \
     -out <hostname>.csr \
     -subj "/CN=<hostname>,O=YourOrg,C=US" \
     -addext "subjectAltName=DNS:<hostname>"
   ```

2. Get all CSRs signed by your enterprise CA (with server + client auth EKU)

3. Place signed certs and keys on the CM Server host (e.g., under `/tmp/auto-tls/certs/`)

4. Upload certificates to CM via the API:
   ```bash
   curl -u admin:admin -X POST \
     -H "Content-Type: application/json" \
     "https://<CM_HOST>:7183/api/v54/cm/commands/addCustomCerts" \
     -d '{
       "interpretAsFilenames": true,
       "hostCerts": [
         {
           "hostname": "<host1-fqdn>",
           "certificate": "/tmp/auto-tls/certs/<host1>.pem",
           "key": "/tmp/auto-tls/certs/<host1>.key"
         }
       ]
     }'
   ```
   Repeat for each host or include all hosts in one call.

5. Deploy certificates to each host via CM API or the wizard

6. Restart CM Server, Management Services, and all cluster services

---

## 5. Post-Enablement Validation

### Verify CM HTTPS

```bash
# Should connect on 7183 (HTTPS)
curl -sk https://<CM_HOST>:7183/api/v54/cm/version

# Check certificate details
echo | openssl s_client -connect <CM_HOST>:7183 2>/dev/null | \
  openssl x509 -noout -subject -issuer -dates
```

### Verify Agent TLS

```bash
# On any host -- check agent config
grep -E "^(use_tls|verify_cert_file)" /etc/cloudera-scm-agent/config.ini

# Check agent cert files exist
ls -la /var/lib/cloudera-scm-agent/agent-cert/
```

### Verify Service TLS

In CM Console: check each service's configuration for TLS-related properties.
All should be auto-configured after Auto-TLS enablement.

### Verify from CM API

```bash
# List hosts and their health (should use HTTPS now)
curl -sk -u admin:admin https://<CM_HOST>:7183/api/v54/hosts | \
  python3 -m json.tool
```

---

## 6. Auto-TLS File Locations

After Auto-TLS is enabled, certificates are stored on each host at:

```
/var/lib/cloudera-scm-agent/agent-cert/
```

| File | Description |
|------|-------------|
| `cm-auto-global_cacerts.pem` | CA cert + trusted certs (PEM) |
| `cm-auto-global_truststore.jks` | CA cert + trusted certs (JKS) |
| `cm-auto-in_cluster_ca_cert.pem` | Cluster CA cert (PEM) |
| `cm-auto-in_cluster_truststore.jks` | Cluster CA cert (JKS) |
| `cm-auto-host_key_cert_chain.pem` | Host cert + private key (PEM) |
| `cm-auto-host_cert_chain.pem` | Host cert chain (PEM) |
| `cm-auto-host_key.pem` | Host private key (PEM) |
| `cm-auto-host_keystore.jks` | Host keystore (JKS) |
| `cm-auto-host_key.pw` | Host key password |

The truststore password can be retrieved from:
```
https://<CM_HOST>:7183/api/v54/certs/truststorePassword
```

---

## 7. Certificate Rotation

### Use Case 1 (Internal CA)

1. Navigate to **Administration > Security**
2. Click **Rotate Auto-TLS Certificates**
3. Follow the wizard
4. Restart CM Server, Management Services, and all clusters

### Use Case 3 (Custom Certs)

1. Generate new certificates
2. Upload via `/cm/commands/addCustomCerts` API
3. Deploy via `/hosts/{hostId}/commands/generateHostCerts` API per host
4. Restart all services

---

## 8. Troubleshooting

### CM does not start on port 7183 after enablement

```bash
sudo tail -100 /var/log/cloudera-scm-server/cloudera-scm-server.log
# Look for keystore errors, port binding issues
```

### Agents lose heartbeat after TLS

```bash
sudo tail -100 /var/log/cloudera-scm-agent/cloudera-scm-agent.log
# Common: certificate verify failed, WrongHost
# Fix: ensure agent config.ini has correct server_host (FQDN, not IP)
```

### Browser shows certificate warning

Expected for Use Case 1 (self-signed CA). Import the CA cert from
`/var/lib/cloudera-scm-agent/agent-cert/cm-auto-in_cluster_ca_cert.pem`
into your browser's trust store to suppress the warning.
