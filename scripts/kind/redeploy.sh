#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "${SCRIPT_DIR}/common.sh"

require_cmd kind
require_cmd kubectl
require_cmd docker

if ! cluster_exists; then
  echo "kind cluster '${CLUSTER_NAME}' does not exist. Run ${SCRIPT_DIR}/up.sh first." >&2
  exit 1
fi

echo "Building image '${APP_IMAGE}'..."
docker build -t "${APP_IMAGE}" -f "${ROOT_DIR}/task-queue-sample-app/Dockerfile" "${ROOT_DIR}"

echo "Loading image into kind cluster '${CLUSTER_NAME}'..."
kind load docker-image "${APP_IMAGE}" --name "${CLUSTER_NAME}"

echo "Applying manifests..."
kubectl apply -k "${K8S_DIR}"

echo "Updating sample-app image..."
kubectl -n "${NAMESPACE}" set image deployment/task-queue-sample-app "sample-app=${APP_IMAGE}"

echo "Temporarily scaling sample-app to 0 while infrastructure rolls out..."
kubectl -n "${NAMESPACE}" scale deployment/task-queue-sample-app --replicas=0

echo "Waiting for rollout..."
kubectl -n "${NAMESPACE}" rollout status deployment/task-queue-postgres --timeout=180s
kubectl -n "${NAMESPACE}" rollout status deployment/task-queue-postgres-exporter --timeout=180s
kubectl -n "${NAMESPACE}" rollout status deployment/task-queue-prometheus --timeout=180s
kubectl -n "${NAMESPACE}" rollout status deployment/task-queue-loki --timeout=180s
kubectl -n "${NAMESPACE}" rollout status daemonset/task-queue-alloy --timeout=180s
kubectl -n "${NAMESPACE}" rollout status deployment/task-queue-grafana --timeout=180s

echo "Bootstrapping sample-app schema with a single replica..."
kubectl -n "${NAMESPACE}" scale deployment/task-queue-sample-app --replicas=1
kubectl -n "${NAMESPACE}" rollout status deployment/task-queue-sample-app --timeout=300s

echo "Scaling sample-app to requested replicas (${APP_REPLICAS})..."
kubectl -n "${NAMESPACE}" scale deployment/task-queue-sample-app --replicas="${APP_REPLICAS}"
kubectl -n "${NAMESPACE}" rollout status deployment/task-queue-sample-app --timeout=300s

kubectl -n "${NAMESPACE}" get pods -o wide
