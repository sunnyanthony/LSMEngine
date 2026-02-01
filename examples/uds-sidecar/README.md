# UDS Sidecar Example

This sidecar listens on a Unix domain socket and forwards write events to a
webhook endpoint.

## Build
```sh
docker build -t lsm-uds-sidecar:dev .
```

## Run
```sh
docker run --rm -e LSM_WEBHOOK_URL="http://host.docker.internal:8081/webhook" \
  -v /tmp/lsm:/var/run/lsm lsm-uds-sidecar:dev
```

## Environment
- `LSM_EVENTS_SOCKET` (default `/var/run/lsm/events.sock`)
- `LSM_WEBHOOK_URL` (optional)
