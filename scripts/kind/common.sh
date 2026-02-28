#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
K8S_DIR="${ROOT_DIR}/k8s/kind"

CLUSTER_NAME="${CLUSTER_NAME:-task-queue-local}"
NAMESPACE="${NAMESPACE:-task-queue}"
APP_IMAGE="${APP_IMAGE:-task-queue-sample-app:kind}"
APP_REPLICAS="${APP_REPLICAS:-4}"
APP_HOST_PORT="${APP_HOST_PORT:-8080}"
POSTGRES_HOST_PORT="${POSTGRES_HOST_PORT:-54032}"
PROMETHEUS_HOST_PORT="${PROMETHEUS_HOST_PORT:-9090}"
GRAFANA_HOST_PORT="${GRAFANA_HOST_PORT:-3000}"

require_cmd() {
  local cmd="$1"
  if ! command -v "${cmd}" >/dev/null 2>&1; then
    echo "Missing required command: ${cmd}" >&2
    exit 1
  fi
}

cluster_exists() {
  kind get clusters | grep -qx "${CLUSTER_NAME}"
}

validate_host_port() {
  local value="$1"
  local var_name="$2"
  if ! [[ "${value}" =~ ^[0-9]+$ ]] || ((value < 1 || value > 65535)); then
    echo "${var_name} must be an integer in range 1..65535, got '${value}'" >&2
    exit 1
  fi
}

assert_host_port_free() {
  local port="$1"
  local var_name="$2"
  if ! command -v lsof >/dev/null 2>&1; then
    return 0
  fi
  if lsof -nP -iTCP:"${port}" -sTCP:LISTEN >/dev/null 2>&1; then
    echo "Host port ${port} is already in use." >&2
    echo "Set ${var_name} to another port, for example:" >&2
    echo "  ${var_name}=$((port + 1)) scripts/kind/up.sh" >&2
    exit 1
  fi
}

generate_cluster_config() {
  local output_file="$1"
  sed \
    -e "s/hostPort: 8080/hostPort: ${APP_HOST_PORT}/" \
    -e "s/hostPort: 54032/hostPort: ${POSTGRES_HOST_PORT}/" \
    -e "s/hostPort: 9090/hostPort: ${PROMETHEUS_HOST_PORT}/" \
    -e "s/hostPort: 3000/hostPort: ${GRAFANA_HOST_PORT}/" \
    "${K8S_DIR}/cluster-config.yaml" > "${output_file}"
}
