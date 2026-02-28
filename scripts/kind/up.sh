#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "${SCRIPT_DIR}/common.sh"

require_cmd kind
require_cmd kubectl
require_cmd docker

if ! cluster_exists; then
  validate_host_port "${APP_HOST_PORT}" "APP_HOST_PORT"
  validate_host_port "${POSTGRES_HOST_PORT}" "POSTGRES_HOST_PORT"
  validate_host_port "${PROMETHEUS_HOST_PORT}" "PROMETHEUS_HOST_PORT"
  validate_host_port "${GRAFANA_HOST_PORT}" "GRAFANA_HOST_PORT"

  assert_host_port_free "${APP_HOST_PORT}" "APP_HOST_PORT"
  assert_host_port_free "${POSTGRES_HOST_PORT}" "POSTGRES_HOST_PORT"
  assert_host_port_free "${PROMETHEUS_HOST_PORT}" "PROMETHEUS_HOST_PORT"
  assert_host_port_free "${GRAFANA_HOST_PORT}" "GRAFANA_HOST_PORT"

  cluster_config_file="$(mktemp "${TMPDIR:-/tmp}/kind-cluster-config.XXXXXX.yaml")"
  trap 'rm -f "${cluster_config_file}"' EXIT
  generate_cluster_config "${cluster_config_file}"

  echo "Creating kind cluster '${CLUSTER_NAME}'..."
  kind create cluster --name "${CLUSTER_NAME}" --config "${cluster_config_file}"
else
  echo "kind cluster '${CLUSTER_NAME}' already exists"
fi

"${SCRIPT_DIR}/redeploy.sh"

cat <<EOF

Cluster is ready.
App endpoint:      http://localhost:${APP_HOST_PORT}
Postgres endpoint: localhost:${POSTGRES_HOST_PORT} (db/user/password: task_queue)
Prometheus:        http://localhost:${PROMETHEUS_HOST_PORT}
Grafana:           http://localhost:${GRAFANA_HOST_PORT}
Logs in Grafana:   Explore -> Loki

Useful commands:
  kubectl -n ${NAMESPACE} get pods -o wide
  ${SCRIPT_DIR}/scale.sh 8
EOF
