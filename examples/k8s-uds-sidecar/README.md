# K8s UDS Sidecar Example

This example shows a pod where the LSM container emits write events to a Unix
domain socket, and a sidecar container forwards them to a webhook.

## Notes
- Both containers mount the same `emptyDir` volume at `/var/run/lsm`.
- Replace the image names with your registry builds.
- Your LSM app must be configured to emit write events to
  `/var/run/lsm/events.sock`.
- Set `LSM_WEBHOOK_URL` on the sidecar to your endpoint.
