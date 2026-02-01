#!/usr/bin/env bash
set -euo pipefail

CLUSTER_NAME="${CLUSTER_NAME:-lsm-example}"
NAMESPACE="lsm-example"
IMAGE="lsmengine-server:dev"

if ! command -v kind >/dev/null; then
  echo "kind is required" >&2
  exit 1
fi
if ! command -v kubectl >/dev/null; then
  echo "kubectl is required" >&2
  exit 1
fi
if ! command -v openssl >/dev/null; then
  echo "openssl is required" >&2
  exit 1
fi

kind get clusters | grep -q "^${CLUSTER_NAME}$" || kind create cluster --name "${CLUSTER_NAME}"

docker build -f docker/Dockerfile.server -t "${IMAGE}" .
kind load docker-image "${IMAGE}" --name "${CLUSTER_NAME}"

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "${TMP_DIR}"' EXIT

openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout "${TMP_DIR}/tls.key" \
  -out "${TMP_DIR}/tls.crt" \
  -subj "/CN=lsm.local" \
  -days 1 >/dev/null 2>&1

kubectl apply -f examples/k8s-envoy/namespace.yaml

kubectl -n "${NAMESPACE}" delete secret envoy-tls >/dev/null 2>&1 || true
kubectl -n "${NAMESPACE}" create secret tls envoy-tls \
  --cert="${TMP_DIR}/tls.crt" \
  --key="${TMP_DIR}/tls.key"

kubectl apply -k examples/k8s-envoy

if ! kubectl -n "${NAMESPACE}" rollout status deployment/lsm-server --timeout=120s; then
  echo "rollout failed; dumping diagnostics" >&2
  kubectl -n "${NAMESPACE}" get pods -o wide || true
  kubectl -n "${NAMESPACE}" describe pod -l app=lsm-server || true
  kubectl -n "${NAMESPACE}" logs deployment/lsm-server -c envoy --tail=200 || true
  kubectl -n "${NAMESPACE}" logs deployment/lsm-server -c lsm --tail=200 || true
  kubectl -n "${NAMESPACE}" get events --sort-by=.lastTimestamp || true
  exit 1
fi

if [[ "${SKIP_PORT_FORWARD:-}" == "1" ]]; then
  exit 0
fi

echo "Port forwarding https://127.0.0.1:8443 -> service/lsm-server"
echo "Try: curl -k https://127.0.0.1:8443/healthz"
kubectl -n "${NAMESPACE}" port-forward service/lsm-server 8443:8443
