#!/usr/bin/env bash
###############################################################################
# common.sh -- Shared functions for CDP 7.3.1 installation scripts
#
# Source this file at the top of every numbered script:
#   SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
#   source "${SCRIPT_DIR}/common.sh"
###############################################################################
set -euo pipefail

# ---------------------------------------------------------------------------
# Colour helpers (no-op when stdout is not a terminal)
# ---------------------------------------------------------------------------
if [[ -t 1 ]]; then
  _RED='\033[0;31m'; _YEL='\033[0;33m'; _GRN='\033[0;32m'
  _CYN='\033[0;36m'; _RST='\033[0m'
else
  _RED=''; _YEL=''; _GRN=''; _CYN=''; _RST=''
fi

log_info()  { echo -e "${_GRN}[INFO]${_RST}  $(date '+%F %T')  $*"; }
log_warn()  { echo -e "${_YEL}[WARN]${_RST}  $(date '+%F %T')  $*" >&2; }
log_error() { echo -e "${_RED}[ERROR]${_RST} $(date '+%F %T')  $*" >&2; }
log_step()  { echo -e "\n${_CYN}===== $* =====${_RST}"; }
log_host()  { echo -e "${_CYN}---- $* ----${_RST}"; }

# ---------------------------------------------------------------------------
# Load .env
# ---------------------------------------------------------------------------
load_env() {
  local env_file="${1:-${SCRIPT_DIR}/.env}"
  if [[ ! -f "${env_file}" ]]; then
    log_error ".env file not found at ${env_file}"
    log_error "Copy .env.template to .env and fill in all values."
    exit 1
  fi
  # shellcheck disable=SC1090
  source "${env_file}"
  log_info "Loaded environment from ${env_file}"
}

# ---------------------------------------------------------------------------
# Validate that required variables are set (non-empty)
# ---------------------------------------------------------------------------
require_vars() {
  local missing=0
  for var in "$@"; do
    if [[ -z "${!var:-}" ]]; then
      log_error "Required variable ${var} is not set in .env"
      missing=1
    fi
  done
  if [[ ${missing} -eq 1 ]]; then
    exit 1
  fi
}

# ---------------------------------------------------------------------------
# SSH wrapper
# ---------------------------------------------------------------------------
ssh_cmd() {
  local host="$1"; shift
  # shellcheck disable=SC2086
  ssh ${SSH_OPTS} -i "${SSH_KEY}" "${SSH_USER}@${host}" "$@"
}

ssh_sudo() {
  local host="$1"; shift
  ssh_cmd "${host}" "sudo bash -s" <<< "$@"
}

# ---------------------------------------------------------------------------
# Run a command on every host in ALL_HOSTS (read as array)
# Usage:  run_on_all_hosts "hostname -f && df -h"
# ---------------------------------------------------------------------------
run_on_all_hosts() {
  local cmd="$1"
  local label="${2:-EXEC}"
  read -ra _hosts <<< "${ALL_HOSTS}"
  for h in "${_hosts[@]}"; do
    log_host "${label} on ${h}"
    ssh_cmd "${h}" "${cmd}" || log_warn "Command failed on ${h}"
  done
}

# ---------------------------------------------------------------------------
# Run a heredoc script with sudo on every host
# Usage:  run_sudo_on_all_hosts "set -euxo pipefail; echo hello"
# ---------------------------------------------------------------------------
run_sudo_on_all_hosts() {
  local script="$1"
  local label="${2:-SUDO}"
  read -ra _hosts <<< "${ALL_HOSTS}"
  for h in "${_hosts[@]}"; do
    log_host "${label} on ${h}"
    ssh_cmd "${h}" "sudo bash -s" <<< "${script}" || log_warn "Command failed on ${h}"
  done
}

# ---------------------------------------------------------------------------
# Check SSH connectivity to all hosts
# ---------------------------------------------------------------------------
check_ssh_connectivity() {
  log_step "Checking SSH connectivity"
  local fail=0
  read -ra _hosts <<< "${ALL_HOSTS}"
  for h in "${_hosts[@]}"; do
    if ssh_cmd "${h}" "echo OK" &>/dev/null; then
      log_info "  ${h} -- reachable"
    else
      log_error "  ${h} -- UNREACHABLE"
      fail=1
    fi
  done
  if [[ ${fail} -eq 1 ]]; then
    log_error "Some hosts are unreachable. Fix SSH before continuing."
    exit 1
  fi
  log_info "All hosts reachable via SSH."
}

# ---------------------------------------------------------------------------
# Separator / banner
# ---------------------------------------------------------------------------
banner() {
  echo ""
  echo "###############################################################################"
  echo "#  $*"
  echo "###############################################################################"
  echo ""
}

# ---------------------------------------------------------------------------
# Prompt the operator to continue (for reminder/interactive scripts)
# ---------------------------------------------------------------------------
pause_prompt() {
  local msg="${1:-Press ENTER to continue or Ctrl-C to abort...}"
  echo ""
  read -rp "${msg}" < /dev/tty
}
