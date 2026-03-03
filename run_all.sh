#!/usr/bin/env bash
###############################################################################
# run_all.sh -- Run a command on all cluster hosts and display results
#
# Usage:
#   ./run_all.sh <command>                  # run with SSH key (default)
#   ./run_all.sh -w <command>               # prompt for SSH password
#   ./run_all.sh -s <command>               # run with sudo
#   ./run_all.sh -e /path/to/.env <command> # use alternate .env
#   ./run_all.sh -p <command>               # run in parallel
#   ./run_all.sh -h                         # show help
#
# Examples:
#   ./run_all.sh "hostname -f && uptime"
#   ./run_all.sh -w "hostname -f"
#   ./run_all.sh -ws "systemctl status cloudera-scm-agent"
#   ./run_all.sh -s "cat /etc/cloudera-scm-agent/config.ini | grep server_host"
#   ./run_all.sh -wsp "df -h | grep -v tmpfs"
###############################################################################
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
USE_SUDO=false
PARALLEL=false
USE_PASSWORD=false
SSH_PASS=""
ENV_FILE="${SCRIPT_DIR}/.env"

# ---------------------------------------------------------------------------
# Colours
# ---------------------------------------------------------------------------
if [[ -t 1 ]]; then
  C_RED='\033[0;31m'; C_GRN='\033[0;32m'; C_YEL='\033[0;33m'
  C_CYN='\033[0;36m'; C_BLD='\033[1m';    C_RST='\033[0m'
else
  C_RED=''; C_GRN=''; C_YEL=''; C_CYN=''; C_BLD=''; C_RST=''
fi

# ---------------------------------------------------------------------------
# Usage
# ---------------------------------------------------------------------------
usage() {
  cat <<EOF
Usage: $(basename "$0") [OPTIONS] <command>

Run a command on all cluster hosts defined in .env and display results.

Options:
  -w            Use password auth (prompts for SSH password)
  -s            Run the command with sudo
  -p            Run on all hosts in parallel (output may interleave)
  -e FILE       Use a specific .env file (default: ./.env)
  -h            Show this help message

Examples:
  $(basename "$0") "hostname -f"
  $(basename "$0") -w "hostname -f"
  $(basename "$0") -ws "systemctl status cloudera-scm-agent"
  $(basename "$0") -s "yum list installed | grep cloudera"
  $(basename "$0") -wsp "free -h"
  $(basename "$0") -e /tmp/other.env "uptime"
EOF
  exit 0
}

# ---------------------------------------------------------------------------
# Parse options
# ---------------------------------------------------------------------------
while getopts ":wspe:h" opt; do
  case ${opt} in
    w) USE_PASSWORD=true ;;
    s) USE_SUDO=true ;;
    p) PARALLEL=true ;;
    e) ENV_FILE="${OPTARG}" ;;
    h) usage ;;
    \?) echo "Unknown option: -${OPTARG}" >&2; usage ;;
    :)  echo "Option -${OPTARG} requires an argument." >&2; usage ;;
  esac
done
shift $((OPTIND - 1))

if [[ $# -eq 0 ]]; then
  echo -e "${C_RED}Error: No command specified.${C_RST}" >&2
  echo ""
  usage
fi

REMOTE_CMD="$*"

# ---------------------------------------------------------------------------
# Load .env
# ---------------------------------------------------------------------------
if [[ ! -f "${ENV_FILE}" ]]; then
  echo -e "${C_RED}Error: .env file not found at ${ENV_FILE}${C_RST}" >&2
  exit 1
fi
# shellcheck disable=SC1090
source "${ENV_FILE}"

for var in ALL_HOSTS SSH_USER; do
  if [[ -z "${!var:-}" ]]; then
    echo -e "${C_RED}Error: ${var} is not set in ${ENV_FILE}${C_RST}" >&2
    exit 1
  fi
done

if [[ "${USE_PASSWORD}" == "true" ]]; then
  if ! command -v sshpass &>/dev/null; then
    echo -e "${C_RED}Error: 'sshpass' is required for password auth but not found.${C_RST}" >&2
    echo -e "${C_RED}Install it with: sudo dnf install -y sshpass  (or:  sudo yum install -y sshpass)${C_RST}" >&2
    exit 1
  fi
  read -rsp "SSH password for ${SSH_USER}: " SSH_PASS < /dev/tty
  echo ""
  export SSHPASS="${SSH_PASS}"
  SSH_OPTS="${SSH_OPTS:--o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10 -o PubkeyAuthentication=no}"
else
  if [[ -z "${SSH_KEY:-}" ]]; then
    echo -e "${C_RED}Error: SSH_KEY is not set in ${ENV_FILE} (use -w for password auth)${C_RST}" >&2
    exit 1
  fi
  SSH_OPTS="${SSH_OPTS:--o BatchMode=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10}"
fi

read -ra HOSTS <<< "${ALL_HOSTS}"
HOST_COUNT=${#HOSTS[@]}

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------
AUTH_MODE="key"
[[ "${USE_PASSWORD}" == "true" ]] && AUTH_MODE="password"

echo ""
echo -e "${C_BLD}Command:${C_RST}  ${REMOTE_CMD}"
echo -e "${C_BLD}Sudo:${C_RST}     ${USE_SUDO}"
echo -e "${C_BLD}Auth:${C_RST}     ${AUTH_MODE}"
echo -e "${C_BLD}Hosts:${C_RST}    ${HOST_COUNT}"
echo -e "${C_BLD}Parallel:${C_RST} ${PARALLEL}"
echo ""

# ---------------------------------------------------------------------------
# Run on a single host and capture results
# ---------------------------------------------------------------------------
run_on_host() {
  local host="$1"
  local cmd="$2"
  local use_sudo="$3"
  local exit_code=0
  local ssh_cmd_prefix

  if [[ "${USE_PASSWORD}" == "true" ]]; then
    ssh_cmd_prefix="sshpass -e ssh ${SSH_OPTS}"
  else
    ssh_cmd_prefix="ssh ${SSH_OPTS} -i ${SSH_KEY}"
  fi

  if [[ "${use_sudo}" == "true" ]]; then
    # shellcheck disable=SC2086
    output=$(${ssh_cmd_prefix} "${SSH_USER}@${host}" "sudo bash -c '${cmd}'" 2>&1) || exit_code=$?
  else
    # shellcheck disable=SC2086
    output=$(${ssh_cmd_prefix} "${SSH_USER}@${host}" "${cmd}" 2>&1) || exit_code=$?
  fi

  # Print results with host header
  if [[ ${exit_code} -eq 0 ]]; then
    echo -e "${C_CYN}┌──────────────────────────────────────────────────────────────${C_RST}"
    echo -e "${C_CYN}│${C_RST} ${C_GRN}${C_BLD}${host}${C_RST}  ${C_GRN}[OK]${C_RST}"
    echo -e "${C_CYN}├──────────────────────────────────────────────────────────────${C_RST}"
  else
    echo -e "${C_CYN}┌──────────────────────────────────────────────────────────────${C_RST}"
    echo -e "${C_CYN}│${C_RST} ${C_RED}${C_BLD}${host}${C_RST}  ${C_RED}[FAILED exit=${exit_code}]${C_RST}"
    echo -e "${C_CYN}├──────────────────────────────────────────────────────────────${C_RST}"
  fi

  if [[ -n "${output}" ]]; then
    echo "${output}" | sed "s/^/${C_CYN}│${C_RST}  /"
  else
    echo -e "${C_CYN}│${C_RST}  ${C_YEL}(no output)${C_RST}"
  fi

  echo -e "${C_CYN}└──────────────────────────────────────────────────────────────${C_RST}"
  echo ""

  return ${exit_code}
}

# ---------------------------------------------------------------------------
# Execute
# ---------------------------------------------------------------------------
TOTAL_OK=0
TOTAL_FAIL=0
FAILED_HOSTS=()

if [[ "${PARALLEL}" == "true" ]]; then
  # Parallel execution using background jobs
  TMPDIR=$(mktemp -d)
  trap 'rm -rf "${TMPDIR}"' EXIT

  for h in "${HOSTS[@]}"; do
    (
      run_on_host "${h}" "${REMOTE_CMD}" "${USE_SUDO}"
      echo $? > "${TMPDIR}/${h}.rc"
    ) &
  done

  wait

  for h in "${HOSTS[@]}"; do
    rc=$(cat "${TMPDIR}/${h}.rc" 2>/dev/null || echo "1")
    if [[ "${rc}" -eq 0 ]]; then
      ((TOTAL_OK++)) || true
    else
      ((TOTAL_FAIL++)) || true
      FAILED_HOSTS+=("${h}")
    fi
  done
else
  # Sequential execution
  for h in "${HOSTS[@]}"; do
    if run_on_host "${h}" "${REMOTE_CMD}" "${USE_SUDO}"; then
      ((TOTAL_OK++)) || true
    else
      ((TOTAL_FAIL++)) || true
      FAILED_HOSTS+=("${h}")
    fi
  done
fi

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo -e "${C_BLD}═══════════════════════════════════════════════════════════════${C_RST}"
echo -e "${C_BLD} Summary${C_RST}"
echo -e "${C_BLD}═══════════════════════════════════════════════════════════════${C_RST}"
echo -e "  Total:   ${HOST_COUNT}"
echo -e "  ${C_GRN}OK:${C_RST}      ${TOTAL_OK}"
echo -e "  ${C_RED}Failed:${C_RST}  ${TOTAL_FAIL}"

if [[ ${TOTAL_FAIL} -gt 0 ]]; then
  echo -e "  ${C_RED}Failed hosts:${C_RST}"
  for fh in "${FAILED_HOSTS[@]}"; do
    echo -e "    ${C_RED}- ${fh}${C_RST}"
  done
fi

echo ""
