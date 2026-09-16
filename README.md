# geoBoundaryBot

Confirming geoBoundaries submissions are in the proper format. And destroying all humans on the weekend.

## Deploying with Helm

The Helm chart lives in `charts/geoboundarybot/`. It deploys:

- A **CronJob** that runs the nightly build pipeline (`builder/run.py`)
- A **Dask** scheduler and worker pool (workers scale to 0 when idle)
- An ephemeral **PostGIS** database for passing build outputs to CGAZ
- A **Cloudflare Tunnel** for external access
- A **PVC** for the geoBoundaries data repo

### Prerequisites

- Kubernetes cluster with a storage class that supports `ReadWriteMany`
- `helm` v3
- A Cloudflare Tunnel token (see [Cloudflare Tunnel](#cloudflare-tunnel) below)
- (Optional) An S3-compatible bucket for build output storage (see [S3 uploads](#s3-uploads) below)

### Install

```sh
# Add the Dask Helm repo (required for the subchart dependency)
helm repo add dask https://helm.dask.org/
helm dependency build charts/geoboundarybot/

# Create the Cloudflare Tunnel secret (see below)
kubectl create secret generic cloudflared-token \
  --from-literal=token=<your-tunnel-token>

# Install the chart with release name "gb"
helm install gb charts/geoboundarybot/
```

> **Note:** The Dask worker PVC mount uses the release name `gb` by convention.
> If you use a different release name, update `dask.worker.mounts` in your values override
> to match (e.g. `claimName: <release-name>-data`).

### Configuration

Override any value at install time with `--set` or `-f values-override.yaml`.
Key values:

| Value | Default | Description |
|---|---|---|
| `build.nightly.schedule` | `"0 2 * * *"` | Nightly CronJob schedule (2 AM UTC) |
| `build.release.version` | `v6` | Version prefix for release builds (override per release) |
| `build.<nightly\|release>.lockWaitSeconds` | `0` / `21600` | How long to wait for the shared build lock (0 = fail fast) |
| `build.dask.workers` | `4` | Number of Dask workers during builds |
| `build.sync.dataRepo` | `wmgeolab/geoBoundaries` | Git URL for the data repo |
| `volume.storageClass` | `""` | Storage class for the data PVC |
| `volume.size` | `100Gi` | Size of the data PVC |
| `builddb.storageClass` | `""` | Storage class for the ephemeral PostGIS PVC |
| `builddb.size` | `50Gi` | Size of the ephemeral PostGIS PVC |
| `s3.enabled` | `false` | Enable S3 uploads from workers |
| `s3.endpoint` | `""` | S3-compatible endpoint URL |
| `s3.bucket` | `""` | Bucket name |
| `s3.credentialsSecret` | `gb-s3-credentials` | Secret name for S3 credentials |
| `cloudflared.replicas` | `2` | Cloudflare Tunnel replica count |
| `cloudflared.tokenSecret` | `cloudflared-token` | Secret name for the tunnel token |

### Cloudflare Tunnel

The chart deploys a [Cloudflare Tunnel](https://developers.cloudflare.com/cloudflare-one/connections/connect-networks/) to expose services without a public ingress.

1. Create a tunnel in the [Cloudflare Zero Trust dashboard](https://one.dash.cloudflare.com/)
2. Copy the tunnel token
3. Create the Kubernetes secret before installing the chart:

   ```sh
   kubectl create secret generic cloudflared-token \
     --from-literal=token=eyJhIjoiNj...
   ```

4. Configure DNS and ingress rules in the Cloudflare dashboard -- routing is managed there, not in the cluster

The tunnel deployment includes a `/ready` liveness probe and runs 2 replicas by default for availability.

### S3 uploads

Workers can upload build outputs to any S3-compatible bucket (AWS S3, Cloudflare R2, MinIO, etc.). The relative path structure is preserved so the bucket can be served directly as a drop-in replacement for the file server:

```
gbOpen/USA/ADM0/geoBoundaries-USA-ADM0.geojson
gbOpen/USA/ADM0/geoBoundaries-USA-ADM0_simplified.geojson
gbOpen/USA/ADM0/geoBoundaries-USA-ADM0-metaData.json
gbOpen/USA/ADM0/geoBoundaries-USA-ADM0-all.zip
...
```

To enable:

1. Create the credentials secret:

   ```sh
   kubectl create secret generic gb-s3-credentials \
     --from-literal=access-key-id=<your-access-key> \
     --from-literal=secret-access-key=<your-secret-key>
   ```

2. Set the S3 values at install time:

   ```sh
   helm install gb charts/geoboundarybot/ \
     --set s3.enabled=true \
     --set s3.endpoint=https://<account>.r2.cloudflarestorage.com \
     --set s3.bucket=geoboundaries
   ```

S3 credentials are only mounted by the build driver pod and passed to Dask workers as task arguments -- the workers themselves do not mount the secret.

When `s3.enabled` is `false` (the default), no S3 env vars are injected and uploads are skipped entirely.

### Manual builds

The chart renders two CronJobs, `gb-build-nightly` (scheduled) and
`gb-build-release` (suspended, manual only). To trigger a build by hand:

```sh
# extra nightly run
kubectl create job --from=cronjob/gb-build-nightly gb-build-manual

# release: set the version first, then create the job
helm upgrade gb charts/geoboundarybot/ --reuse-values --set build.release.version=v7
kubectl create job --from=cronjob/gb-build-release gb-build-v7
```

Follow the logs:

```sh
kubectl logs -f job/gb-build-manual
```

Clean up when done:

```sh
kubectl delete job gb-build-manual
```

### Build lock

Only one build can run at a time. Both CronJobs share the Dask cluster, the
data PVC and the ephemeral PostGIS, and `concurrencyPolicy: Forbid` only
prevents overlap within a single CronJob. So the driver holds a Kubernetes
Lease named `gb-build-lock` for the whole run, renewed every minute.

- A nightly that finds the lock held exits 1 immediately (`lockWaitSeconds: 0`); the next nightly picks it up.
- A release that finds the lock held waits for the running build to finish, up to `build.release.lockWaitSeconds` (default 6h).
- A driver that crashes stops renewing; the lease is treated as free after 5 minutes.

Inspect or force-clear the lock:

```sh
kubectl get lease gb-build-lock -o yaml
kubectl delete lease gb-build-lock   # only if no build pod is actually running
```

### Build pipeline overview

The CronJob runs `python -m builder.run`, which orchestrates:

0. **Lock** -- acquires the shared build Lease (see above)
1. **Sync** -- pulls the latest geoBoundaries data repo onto the PVC
2. **PostGIS** -- creates an ephemeral PostGIS Deployment, Service, and PVC via kr8s
3. **Scale up** -- scales Dask workers from 0 to the configured count
4. **Build** -- discovers boundary ZIPs and fans out builds across Dask workers; each worker pushes its output geometries to PostGIS and uploads files to S3 (if configured)
5. **Scale down** -- scales Dask workers back to 0 (runs even on failure)
6. **CGAZ** -- launches a one-shot Kubernetes Job that reads boundaries from PostGIS
7. **Verify** -- on release runs, checks every uploaded object exists in the bucket
8. **Promote** -- on release runs, writes `current.json` pointing at the new version
9. **Teardown** -- deletes the ephemeral PostGIS resources and releases the build lock
10. **Exit** -- exits 0 on success, 1 on any failure (marks the CronJob as failed)
