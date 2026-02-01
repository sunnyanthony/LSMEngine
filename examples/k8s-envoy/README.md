# Envoy Sidecar Example (kind)

This example runs the LSM server behind Envoy in a single pod. Envoy terminates
TLS and forwards traffic to the LSM HTTP handler.

## Prereqs
- Docker
- kind
- kubectl
- openssl

## Steps
1) Create a kind cluster and build the server image:
```bash
./scripts/kind-envoy-example.sh
```

2) Verify:
```bash
curl -k https://127.0.0.1:8443/healthz
curl -k https://127.0.0.1:8443/stats
```

## Notes
- The app server does not implement TLS; Envoy terminates TLS and forwards to
  `127.0.0.1:8080`.
- This is a minimal example to show how users can place TLS/auth at the proxy.
