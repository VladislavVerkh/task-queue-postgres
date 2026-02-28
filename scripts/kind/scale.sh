#!/usr/bin/env bash

set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <replicas>" >&2
  exit 1
fi

REPLICAS="$1"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "${SCRIPT_DIR}/common.sh"

require_cmd kubectl

if ! [[ "${REPLICAS}" =~ ^[0-9]+$ ]]; then
  echo "replicas must be a non-negative integer" >&2
  exit 1
fi

kubectl -n "${NAMESPACE}" scale deployment/task-queue-sample-app --replicas="${REPLICAS}"
kubectl -n "${NAMESPACE}" rollout status deployment/task-queue-sample-app --timeout=300s
kubectl -n "${NAMESPACE}" get pods -o wide
