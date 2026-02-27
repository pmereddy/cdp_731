# Installing OpenJDK 17 on RHEL 9.4 for CDP 7.3.1

CDP 7.3.1 and Cloudera Manager 7.13.1 require Java 17 (or 11). This guide
installs the Red Hat build of OpenJDK 17 on all cluster hosts via the
standard RHEL 9 repos -- no external downloads needed.

---

## Quick Install (all hosts at once)

If your `.env` is already configured, run this from the `cdp_install/` directory:

```bash
source common.sh
load_env

run_sudo_on_all_hosts '
set -euo pipefail

dnf install -y java-17-openjdk java-17-openjdk-devel java-17-openjdk-headless

alternatives --set java /usr/lib/jvm/java-17-openjdk-17.*/bin/java 2>/dev/null || \
  alternatives --config java <<< "1" 2>/dev/null || true

cat > /etc/profile.d/java17.sh <<PROF
export JAVA_HOME=\$(dirname \$(dirname \$(readlink -f /usr/bin/java)))
export PATH=\$JAVA_HOME/bin:\$PATH
PROF
chmod 644 /etc/profile.d/java17.sh
source /etc/profile.d/java17.sh

echo "--- Installed ---"
java -version 2>&1
echo "JAVA_HOME=${JAVA_HOME}"
' "INSTALL-JAVA17"
```

---

## Step-by-Step (single host)

### 1. Install packages

```bash
sudo dnf install -y java-17-openjdk java-17-openjdk-devel java-17-openjdk-headless
```

| Package | Contents |
|---------|----------|
| `java-17-openjdk` | JRE (runtime) |
| `java-17-openjdk-devel` | JDK (compiler, keytool, jarsigner) -- needed for TLS keystore operations |
| `java-17-openjdk-headless` | Minimal JRE without GUI libs |

### 2. Verify installation

```bash
java -version
```

Expected output:

```
openjdk version "17.0.x" 2025-xx-xx LTS
OpenJDK Runtime Environment (Red_Hat-17.0.x...) (build 17.0.x+...)
OpenJDK 64-Bit Server VM (Red_Hat-17.0.x...) (build 17.0.x+..., mixed mode, sharing)
```

### 3. Set as default (if multiple Java versions exist)

```bash
sudo alternatives --config java
```

Select the entry pointing to `/usr/lib/jvm/java-17-openjdk-17.*/bin/java`.

To also set the default `javac`:

```bash
sudo alternatives --config javac
```

### 4. Set JAVA_HOME system-wide

Create a profile drop-in so every user and every service inherits `JAVA_HOME`:

```bash
sudo bash -c 'cat > /etc/profile.d/java17.sh <<PROF
export JAVA_HOME=\$(dirname \$(dirname \$(readlink -f /usr/bin/java)))
export PATH=\$JAVA_HOME/bin:\$PATH
PROF'

sudo chmod 644 /etc/profile.d/java17.sh
```

Load it in the current session:

```bash
source /etc/profile.d/java17.sh
```

Verify:

```bash
echo $JAVA_HOME
# /usr/lib/jvm/java-17-openjdk-17.0.x.0.x-x.el9.x86_64

which java
# /usr/lib/jvm/java-17-openjdk-17.0.x.../bin/java

which keytool
# /usr/lib/jvm/java-17-openjdk-17.0.x.../bin/keytool
```

### 5. Verify keytool (needed for TLS)

```bash
keytool -help 2>&1 | head -3
```

### 6. Verify JDK truststore location

On Java 17 (RHEL 9) the truststore is at a different path than Java 8:

```bash
ls -l $JAVA_HOME/lib/security/cacerts
```

> **Note:** On Java 8 the path was `$JAVA_HOME/jre/lib/security/cacerts`.
> On Java 11+ / 17+ it is `$JAVA_HOME/lib/security/cacerts`. All TLS
> scripts and CM configurations must use the new path.

---

## Update .env

After installation, make sure your `.env` has the correct `JAVA_HOME`:

```bash
JAVA_HOME="/usr/lib/jvm/java-17-openjdk"
```

The symlink `/usr/lib/jvm/java-17-openjdk` points to the full versioned directory
and is stable across minor updates.

---

## Verification checklist

Run on each host to confirm:

```bash
java -version 2>&1 | head -1          # openjdk version "17.0.x"
javac -version                         # javac 17.0.x
echo $JAVA_HOME                        # /usr/lib/jvm/java-17-openjdk-17...
ls $JAVA_HOME/lib/security/cacerts     # truststore exists
keytool -list -cacerts 2>&1 | head -3  # keytool can read truststore
```
