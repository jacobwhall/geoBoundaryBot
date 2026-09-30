"""Kubernetes-aware build driver for the geoBoundaries pipeline.

Entrypoint: `python -m builder.run`

Orchestrates the full build:
  0. Acquire the shared build lock (a Kubernetes Lease) so the nightly and
     release CronJobs can never run concurrently
  1. Pull latest changes from the data repo
  2. Create an ephemeral PostGIS database for build outputs
  3. Scale up Dask workers
  4. Discover boundaries and fan out builds across Dask
     (each worker pushes its output geometries to PostGIS and uploads
      its build outputs to S3 under {GB_RELEASE_VERSION}/...)
  5. Scale down Dask workers
  6. Publish per-product API indexes to S3
  7. Launch a single-pod CGAZ Job that reads from PostGIS
  8. If GB_PROMOTE_CURRENT=true, verify every uploaded object exists in
     the bucket
  9. If GB_PROMOTE_CURRENT=true, write current.json to the bucket root
  10. Tear down the ephemeral database
  11. Release the build lock
  Exit 0 on success, 1 on failure
"""

import json
import mimetypes
import boto3
from botocore.config import Config as BotoConfig
import logging
import os
import subprocess
import tempfile
import threading
from datetime import datetime, timedelta, timezone
from kr8s import NotFoundError, ServerError
from kr8s.objects import Deployment, PersistentVolumeClaim, Service, new_class
import sys
from pathlib import Path
import time
from builder.paths import REPO_DIR
from builder.paths import SOURCE_DATA
from builder.paths import RELEASE_DATA, TMP_DIR
from sqlalchemy import create_engine, text
from builder.builder_class import builder
from builder.paths import ISO_CSV, LICENSES_CSV, RELEASE_DATA
import geopandas as gpd
import pandas as pd
from sqlalchemy import create_engine
from dask.distributed import Client, as_completed
from kr8s.objects import Job

log = logging.getLogger(__name__)

PRODUCTS = ["gbOpen", "gbHumanitarian", "gbAuthoritative"]

# kr8s ships no Lease class; build a sync one matching the other objects above.
Lease = new_class("Lease", "coordination.k8s.io/v1", asyncio=False)


# ---------------------------------------------------------------------------
# Step 0 — Shared build lock (Kubernetes Lease)
# ---------------------------------------------------------------------------


class BuildLockHeldError(Exception):
    """Raised when the build lock is held by another run and we won't wait."""


def _utcnow():
    return datetime.now(timezone.utc).replace(microsecond=0)


def _k8s_time(dt):
    # Lease acquireTime/renewTime are MicroTime, which the API server only
    # accepts with exactly six fractional digits.
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _status_code(exc):
    resp = getattr(exc, "response", None)
    return getattr(resp, "status_code", None)


class BuildLock:
    """Mutex across build drivers, backed by a coordination.k8s.io Lease.

    The nightly and release CronJobs share the Dask cluster, the data PVC and
    the ephemeral PostGIS, so only one driver may run at a time.  The Lease is
    created atomically (409 on a race) and taken over with a resourceVersion
    precondition (409 on a race).  A background thread renews it; a lease
    whose renewTime is older than leaseDurationSeconds is considered free so
    a crashed driver cannot wedge the pipeline.
    """

    def __init__(
        self,
        name,
        namespace,
        holder,
        lease_seconds=300,
        renew_every=60,
        wait_timeout=0,
        poll_every=30,
    ):
        self.name = name
        self.namespace = namespace
        self.holder = holder
        self.lease_seconds = lease_seconds
        self.renew_every = renew_every
        self.wait_timeout = wait_timeout
        self.poll_every = poll_every
        self.lost = False
        self._lease = None
        self._stop = threading.Event()
        self._thread = None

    # -- helpers -----------------------------------------------------------

    def _spec(self, acquire_time):
        return {
            "holderIdentity": self.holder,
            "leaseDurationSeconds": self.lease_seconds,
            "acquireTime": _k8s_time(acquire_time),
            "renewTime": _k8s_time(_utcnow()),
        }

    @staticmethod
    def _is_free(lease):
        spec = lease.spec or {}
        if not spec.get("holderIdentity"):
            return True
        renew = spec.get("renewTime")
        if not renew:
            return True
        renewed_at = datetime.fromisoformat(renew.replace("Z", "+00:00"))
        duration = int(spec.get("leaseDurationSeconds") or 0)
        return renewed_at + timedelta(seconds=duration) < _utcnow()

    def _try_create(self):
        lease = Lease(
            {
                "apiVersion": "coordination.k8s.io/v1",
                "kind": "Lease",
                "metadata": {"name": self.name, "namespace": self.namespace},
                "spec": self._spec(_utcnow()),
            }
        )
        try:
            lease.create()
        except ServerError as e:
            if _status_code(e) == 409:
                return None  # someone else created it first
            raise
        return lease

    def _try_takeover(self, lease):
        rv = lease.metadata.get("resourceVersion")
        try:
            lease.patch(
                {
                    "metadata": {"resourceVersion": rv},
                    "spec": self._spec(_utcnow()),
                }
            )
        except ServerError as e:
            if _status_code(e) == 409:
                return None  # lost the race; caller re-reads
            raise
        return lease

    # -- public API --------------------------------------------------------

    def acquire(self):
        deadline = time.monotonic() + self.wait_timeout
        logged_wait = False
        while True:
            try:
                lease = Lease.get(self.name, namespace=self.namespace)
            except NotFoundError:
                lease = self._try_create()
                if lease is not None:
                    break
                continue

            if self._is_free(lease):
                lease = self._try_takeover(lease)
                if lease is not None:
                    break
                continue

            spec = lease.spec or {}
            holder = spec.get("holderIdentity")
            renew = spec.get("renewTime")
            if self.wait_timeout <= 0 or time.monotonic() >= deadline:
                raise BuildLockHeldError(
                    f"Build lock {self.name} is held by {holder} "
                    f"(last renewed {renew})"
                )
            if not logged_wait:
                log.info(
                    "Build lock %s held by %s (renewed %s); waiting up to %ds",
                    self.name,
                    holder,
                    renew,
                    self.wait_timeout,
                )
                logged_wait = True
            time.sleep(self.poll_every)

        self._lease = lease
        log.info("Acquired build lock %s as %s", self.name, self.holder)
        self._thread = threading.Thread(
            target=self._renew_loop, name="build-lock-renew", daemon=True
        )
        self._thread.start()

    def _renew_loop(self):
        while not self._stop.wait(self.renew_every):
            try:
                self._lease.refresh()
                current = (self._lease.spec or {}).get("holderIdentity")
                if current != self.holder:
                    log.error(
                        "Build lock %s was taken over by %s — this run was "
                        "presumed dead; its results may be clobbered",
                        self.name,
                        current,
                    )
                    self.lost = True
                    return
                self._lease.patch({"spec": {"renewTime": _k8s_time(_utcnow())}})
            except Exception:
                log.warning("Failed to renew build lock %s", self.name, exc_info=True)

    def release(self):
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
        if self._lease is None or self.lost:
            return
        try:
            self._lease.patch({"spec": {"holderIdentity": ""}})
            log.info("Released build lock %s", self.name)
        except Exception:
            log.warning("Failed to release build lock %s", self.name, exc_info=True)

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, *exc):
        self.release()
        return False


# ---------------------------------------------------------------------------
# Step 1 — Repo sync
# ---------------------------------------------------------------------------


def sync_repo(path, remote, branch="main", lfs=False):
    """Clone or pull a git repository on the mounted PVC."""

    path = Path(path)

    # Mark directory safe so git doesn't reject cross-user ownership on the PVC
    subprocess.run(
        ["git", "config", "--global", "--add", "safe.directory", str(path)],
        check=True,
    )

    if (path / ".git").is_dir():
        log.info("Pulling %s", path)
        subprocess.run(["git", "fetch", "--all"], cwd=path, check=True)
        subprocess.run(
            ["git", "reset", "--hard", f"origin/{branch}"], cwd=path, check=True
        )
    else:
        log.info("Cloning %s → %s", remote, path)
        path.mkdir(parents=True, exist_ok=True)
        subprocess.run(["git", "clone", remote, str(path)], check=True)

    if lfs:
        subprocess.run(["git", "lfs", "install"], cwd=path, check=True)
        subprocess.run(["git", "lfs", "pull"], cwd=path, check=True)


def sync_data_repo():
    """Sync the geoBoundaries data repo on the mounted PVC."""

    data_remote = os.environ.get(
        "GB_DATA_REPO", "https://github.com/wmgeolab/geoBoundaries.git"
    )
    branch = os.environ.get("GB_DATA_BRANCH", "main")
    lfs = os.environ.get("GB_DATA_LFS", "true").lower() == "true"

    sync_repo(REPO_DIR, data_remote, branch=branch, lfs=lfs)


# ---------------------------------------------------------------------------
# Step 2/7 — Ephemeral PostGIS via kr8s
# ---------------------------------------------------------------------------


def _delete_and_wait(obj, timeout=60):
    """Delete a k8s resource and wait for it to be fully gone."""
    try:
        obj.delete()
        log.info("Deleting %s/%s", obj.kind, obj.name)
    except Exception:
        return  # doesn't exist

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            obj.refresh()
        except Exception:
            return  # gone
        time.sleep(2)
    log.warning("Timed out waiting for %s/%s deletion", obj.kind, obj.name)


def create_build_db(timeout=120):
    """Create an ephemeral PostGIS PVC + Deployment + Service for build outputs.

    Returns the connection URL.
    """

    release = os.environ["GB_RELEASE_NAME"]
    ns = os.environ.get("GB_NAMESPACE", "default")
    image = os.environ.get("GB_POSTGIS_IMAGE", "postgis/postgis:17-3.5")
    storage_class = os.environ.get("GB_BUILDDB_STORAGE_CLASS", "")
    storage_size = os.environ.get("GB_BUILDDB_STORAGE_SIZE", "50Gi")
    name = f"{release}-builddb"

    labels = {
        "app.kubernetes.io/instance": release,
        "app.kubernetes.io/component": "builddb",
    }

    pvc_spec = {
        "accessModes": ["ReadWriteOnce"],
        "resources": {"requests": {"storage": storage_size}},
    }
    if storage_class:
        pvc_spec["storageClassName"] = storage_class

    pvc = PersistentVolumeClaim(
        {
            "apiVersion": "v1",
            "kind": "PersistentVolumeClaim",
            "metadata": {"name": name, "namespace": ns, "labels": labels},
            "spec": pvc_spec,
        }
    )

    deploy = Deployment(
        {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": name, "namespace": ns, "labels": labels},
            "spec": {
                "replicas": 1,
                "selector": {"matchLabels": labels},
                "template": {
                    "metadata": {"labels": labels},
                    "spec": {
                        "securityContext": {
                            "runAsUser": 999,
                            "runAsGroup": 999,
                            "fsGroup": 999,
                        },
                        "containers": [
                            {
                                "name": "postgis",
                                "image": image,
                                "env": [
                                    {"name": "POSTGRES_DB", "value": "geoboundaries"},
                                    {"name": "POSTGRES_USER", "value": "gb"},
                                    {"name": "POSTGRES_PASSWORD", "value": "builddb"},
                                    {
                                        "name": "PGDATA",
                                        "value": "/var/lib/postgresql/data/pgdata",
                                    },
                                ],
                                "ports": [{"containerPort": 5432}],
                                "readinessProbe": {
                                    "exec": {
                                        "command": [
                                            "pg_isready",
                                            "-U",
                                            "gb",
                                            "-d",
                                            "geoboundaries",
                                        ],
                                    },
                                    "initialDelaySeconds": 5,
                                    "periodSeconds": 5,
                                },
                                "volumeMounts": [
                                    {
                                        "name": "pgdata",
                                        "mountPath": "/var/lib/postgresql/data",
                                    }
                                ],
                            }
                        ],
                        "volumes": [
                            {
                                "name": "pgdata",
                                "persistentVolumeClaim": {"claimName": name},
                            }
                        ],
                    },
                },
            },
        }
    )

    svc = Service(
        {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": name, "namespace": ns, "labels": labels},
            "spec": {
                "selector": labels,
                "ports": [{"port": 5432, "targetPort": 5432}],
            },
        }
    )

    log.info(
        "Creating build database: %s (%s on %s)",
        name,
        storage_size,
        storage_class or "default storage class",
    )

    # Clean up leftover resources from any previous run (order matters:
    # Deployment must be gone before PVC, or the pvc-protection finalizer
    # keeps the PVC in Terminating state).
    _delete_and_wait(deploy)
    _delete_and_wait(svc)
    _delete_and_wait(pvc)

    pvc.create()
    deploy.create()
    svc.create()

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        deploy.refresh()
        ready = deploy.status.get("readyReplicas", 0) or 0
        if ready >= 1:
            log.info("Build database ready")
            break
        log.info("Waiting for build database…")
        time.sleep(5)
    else:
        raise TimeoutError(f"Build database did not become ready within {timeout}s")

    db_url = f"postgresql://gb:builddb@{name}:5432/geoboundaries"

    # Service endpoints may lag behind readyReplicas; retry initial connection
    for attempt in range(6):
        try:
            _init_build_db_schema(db_url)
            break
        except Exception:
            if attempt == 5:
                raise
            log.info("DB not accepting connections yet, retrying in 5s…")
            time.sleep(5)

    return db_url


def _init_build_db_schema(db_url):
    """Enable PostGIS and create the boundaries table."""

    engine = create_engine(db_url)
    with engine.begin() as conn:
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis"))
        conn.execute(
            text("""
            CREATE TABLE IF NOT EXISTS boundaries (
                id SERIAL PRIMARY KEY,
                product TEXT NOT NULL,
                iso TEXT NOT NULL,
                adm_level TEXT NOT NULL,
                shape_name TEXT,
                shape_id TEXT,
                shape_group TEXT,
                shape_type TEXT,
                geom geometry(Geometry, 4326)
            )
        """)
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS idx_boundaries_lookup "
                "ON boundaries (product, iso, adm_level)"
            )
        )
    engine.dispose()
    log.info("Build database schema initialized")


def teardown_build_db():
    """Delete the ephemeral PostGIS PVC, Deployment, and Service."""

    release = os.environ["GB_RELEASE_NAME"]
    ns = os.environ.get("GB_NAMESPACE", "default")
    name = f"{release}-builddb"

    for cls in (Deployment, Service, PersistentVolumeClaim):
        try:
            obj = cls.get(name, namespace=ns)
            obj.delete()
            log.info("Deleted %s/%s", cls.kind, name)
        except Exception:
            log.warning("Could not delete %s/%s", cls.kind, name, exc_info=True)


# ---------------------------------------------------------------------------
# Step 3/5 — Dask scaling via kr8s
# ---------------------------------------------------------------------------


def scale_dask_workers(replicas, timeout=300):
    """Scale the Dask worker Deployment via the Kubernetes API."""

    release = os.environ["GB_RELEASE_NAME"]
    ns = os.environ.get("GB_NAMESPACE", "default")
    name = f"{release}-dask-worker"

    log.info("Scaling %s to %d replicas", name, replicas)
    deploy = Deployment.get(name, namespace=ns)
    deploy.scale(replicas)

    if replicas > 0:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            deploy.refresh()
            ready = deploy.status.get("readyReplicas", 0) or 0
            if ready >= replicas:
                log.info("%s: %d/%d replicas ready", name, ready, replicas)
                return
            log.info("%s: %d/%d replicas ready, waiting…", name, ready, replicas)
            time.sleep(10)
        raise TimeoutError(
            f"{name} did not reach {replicas} ready replicas within {timeout}s"
        )


# ---------------------------------------------------------------------------
# Step 4 — Boundary builds via Dask
# ---------------------------------------------------------------------------


def discover_boundaries(products=None):
    """Scan sourceData/ for ZIP files and return (product, iso, adm) tuples."""

    products = products or PRODUCTS
    boundaries = []
    for product in products:
        source_dir = SOURCE_DATA / product
        if not source_dir.is_dir():
            log.warning("Source directory not found: %s", source_dir)
            continue
        for zip_file in sorted(source_dir.glob("*.zip")):
            parts = zip_file.stem.split("_", 1)
            if len(parts) == 2:
                boundaries.append((product, parts[0], parts[1]))
            else:
                log.warning("Skipping malformed filename: %s", zip_file.name)
    return boundaries


def _s3_client(s3_config):
    """Build a boto3 S3 client with standard retries for transient errors."""

    return boto3.client(
        "s3",
        endpoint_url=s3_config["endpoint"],
        aws_access_key_id=s3_config["access_key_id"],
        aws_secret_access_key=s3_config["secret_access_key"],
        config=BotoConfig(retries={"max_attempts": 5, "mode": "standard"}),
    )


def upload_to_s3(output_dir, key_prefix, s3_config):
    """Upload all build outputs for a boundary to S3-compatible storage.

    Keys mirror the release directory structure so the bucket can be
    served directly as a drop-in replacement for the file server:
        {product}/{ISO}/{ADM}/geoBoundaries-{ISO}-{ADM}.geojson
        {product}/{ISO}/{ADM}/geoBoundaries-{ISO}-{ADM}-metaData.json
        ...

    Args:
        output_dir: Path to the directory containing build outputs.
        key_prefix: Prefix for S3 keys (e.g. "gbOpen/USA/ADM1").
        s3_config: Dict with endpoint, access_key_id, secret_access_key, bucket.

    Returns a manifest: a list of {"key", "size"} dicts, one per uploaded
    file, with the full bucket key (including the global prefix).  Raises
    if the output directory is missing or empty, or if any upload fails
    after retries.
    """

    if not output_dir.is_dir():
        raise FileNotFoundError(f"Build output directory missing: {output_dir}")

    s3 = _s3_client(s3_config)
    bucket = s3_config["bucket"]
    global_prefix = s3_config.get("prefix", "")

    manifest = []
    for file_path in output_dir.rglob("*"):
        if not file_path.is_file():
            continue
        key = f"{key_prefix}/{file_path.relative_to(output_dir)}"
        if global_prefix:
            key = f"{global_prefix}/{key}"
        content_type, _ = mimetypes.guess_type(str(file_path))
        extra_args = {}
        if content_type:
            extra_args["ContentType"] = content_type
        s3.upload_file(str(file_path), bucket, key, ExtraArgs=extra_args)
        manifest.append({"key": key, "size": file_path.stat().st_size})

    if not manifest:
        raise FileNotFoundError(f"Build output directory is empty: {output_dir}")

    log.info(
        "Uploaded %d file(s) for %s to s3://%s/", len(manifest), key_prefix, bucket
    )
    return manifest


def build_boundary(
    product: str,
    iso: str,
    adm: str,
    db_url: str,
    s3_config: dict | None = None,
) -> dict:
    """Process a single boundary.  Runs on a Dask worker.

    After a successful build:
      - pushes the output geometry to PostGIS for CGAZ
      - uploads all output files to S3 (if configured)

    Both side effects are treated as build stages: a failure in either
    marks the boundary as failed (failed_stage "writePostGIS" /
    "uploadToS3") so strict mode aborts instead of promoting a release
    with missing CGAZ input or missing bucket objects.
    """

    tmpdir = tempfile.mkdtemp(prefix=f"gb-{product}-{iso}-{adm}-")
    tmpdir = Path(tmpdir)

    iso_df = pd.read_csv(ISO_CSV)
    license_df = pd.read_csv(LICENSES_CSV)
    valid_isos = iso_df["Alpha-3code"].tolist()
    valid_licenses = license_df["license_name"].tolist()
    b = builder(iso, adm, product, valid_isos, valid_licenses, tmpdir=tmpdir)

    result = {"product": product, "iso": iso, "adm": adm}
    for stage_name, stage_fn in [
        ("checkExistence", b.checkExistence),
        ("checkSourceValidity", b.checkSourceValidity),
        ("checkBuildTabularMetaData", b.checkBuildTabularMetaData),
        ("checkBuildGeometryFiles", b.checkBuildGeometryFiles),
        ("calculateGeomMeta", b.calculateGeomMeta),
        ("constructFiles", b.constructFiles),
    ]:
        try:
            stage_result = stage_fn()
        except Exception as e:
            result["status"] = "error"
            result["failed_stage"] = stage_name
            result["error"] = str(e)
            return result
        if isinstance(stage_result, str) and "ERROR" in stage_result.upper():
            result["status"] = "error"
            result["failed_stage"] = stage_name
            result["error"] = stage_result
            return result

    # Read back the exact metadata artifact that was written to the release
    # tree.  This record is also used to build the aggregate API index, which
    # keeps aggregate responses identical to per-boundary responses.
    metadata_path = b.targetPath / f"geoBoundaries-{iso}-{adm}-metaData.json"
    try:
        with metadata_path.open(encoding="utf-8") as metadata_file:
            metadata = json.load(metadata_file)
        if not isinstance(metadata, dict):
            raise ValueError("metadata root must be a JSON object")
        if metadata.get("boundaryISO") != iso:
            raise ValueError(
                f"boundaryISO must be {iso!r}, got {metadata.get('boundaryISO')!r}"
            )
        if metadata.get("boundaryType") != adm:
            raise ValueError(
                f"boundaryType must be {adm!r}, got {metadata.get('boundaryType')!r}"
            )
    except Exception as e:
        log.error("Metadata load failed for %s/%s_%s: %s", product, iso, adm, e)
        result["status"] = "error"
        result["failed_stage"] = "loadMetadata"
        result["error"] = str(e)
        return result

    result["metadata"] = metadata

    # Push the built boundary to PostGIS for downstream CGAZ consumption.
    try:
        geojson_path = (
            b.targetPath / f"geoBoundaries-{iso}-{adm}.geojson"
        )
        if geojson_path.exists():
            gdf = gpd.read_file(geojson_path)
            gdf = gdf.to_crs(epsg=4326)
            gdf["product"] = product
            gdf["iso"] = iso
            gdf["adm_level"] = adm
            gdf = gdf.rename(columns={
                "geometry": "geom",
                "shapeName": "shape_name",
                "shapeID": "shape_id",
                "shapeGroup": "shape_group",
                "shapeType": "shape_type",
            }).set_geometry("geom")

            engine = create_engine(db_url)
            gdf.to_postgis(
                "boundaries",
                engine,
                if_exists="append",
                index=False,
            )
            engine.dispose()
    except Exception as e:
        log.error("PostGIS write failed for %s/%s_%s: %s", product, iso, adm, e)
        result["status"] = "error"
        result["failed_stage"] = "writePostGIS"
        result["error"] = str(e)
        return result

    # Upload all outputs to S3-compatible storage.
    result["uploaded"] = []
    if s3_config:
        try:
            result["uploaded"] = upload_to_s3(
                b.targetPath, f"{product}/{iso}/{adm}", s3_config
            )
        except Exception as e:
            log.error("S3 upload failed for %s/%s_%s: %s", product, iso, adm, e)
            result["status"] = "error"
            result["failed_stage"] = "uploadToS3"
            result["error"] = str(e)
            return result

    result["status"] = "ok"
    return result


def run_boundary_builds(scheduler_url, db_url, s3_config=None, version="nightly"):
    """Connect to Dask, discover boundaries, fan out work.

    Returns (successes, failures): two lists of the per-boundary result
    dicts returned by `build_boundary`.
    """

    log.info("Connecting to Dask scheduler at %s", scheduler_url)
    client = Client(scheduler_url)
    log.info("Dashboard: %s", client.dashboard_link)

    boundaries = discover_boundaries()
    log.info("Discovered %d boundaries to build", len(boundaries))

    if not boundaries:
        log.info("Nothing to build.")
        return [], []

    futures = {
        client.submit(
            build_boundary,
            product,
            iso,
            adm,
            db_url,
            s3_config=s3_config,
            key=f"{version}-{product}-{iso}-{adm}",
        ): (product, iso, adm)
        for product, iso, adm in boundaries
    }

    successes = []
    failures = []
    t0 = time.monotonic()

    for future, result in as_completed(futures, with_results=True):
        tag = f"{result['product']}/{result['iso']}_{result['adm']}"
        if result["status"] == "ok":
            successes.append(result)
            log.info("OK  %s", tag)
        else:
            failures.append(result)
            log.error(
                "FAIL %s stage=%s: %s",
                tag,
                result.get("failed_stage"),
                result.get("error"),
            )

    elapsed = time.monotonic() - t0
    log.info(
        "Boundary builds: %d succeeded, %d failed in %.0fs",
        len(successes),
        len(failures),
        elapsed,
    )
    return successes, failures


def _adm_sort_key(adm):
    """Sort ADM levels numerically while keeping malformed values last."""

    if isinstance(adm, str) and adm.startswith("ADM") and adm[3:].isdigit():
        return int(adm[3:])
    return sys.maxsize


def upload_api_indexes(successes, s3_config):
    """Write one aggregate API index per product.

    Each index is a deterministic JSON array at
    `{version}/{product}/index.json`.  Empty products still receive an empty
    index so every documented product has a complete aggregate endpoint.

    Returns a manifest compatible with `verify_release_objects` and raises if
    a successful result is missing valid metadata or an upload fails.
    """

    records_by_product = {product: [] for product in PRODUCTS}
    for result in successes:
        product = result.get("product")
        metadata = result.get("metadata")
        if product not in records_by_product:
            raise ValueError(f"Unknown product in successful build: {product!r}")
        if not isinstance(metadata, dict):
            raise ValueError(
                f"Successful build {product}/{result.get('iso')}_{result.get('adm')} "
                "has no metadata object"
            )
        if metadata.get("boundaryISO") != result.get("iso"):
            raise ValueError(
                f"Metadata ISO mismatch for {product}/{result.get('iso')}_"
                f"{result.get('adm')}"
            )
        if metadata.get("boundaryType") != result.get("adm"):
            raise ValueError(
                f"Metadata ADM mismatch for {product}/{result.get('iso')}_"
                f"{result.get('adm')}"
            )
        records_by_product[product].append(metadata)

    s3 = _s3_client(s3_config)
    bucket = s3_config["bucket"]
    prefix = s3_config.get("prefix", "")
    manifest = []

    for product in PRODUCTS:
        records = sorted(
            records_by_product[product],
            key=lambda record: (
                record["boundaryISO"],
                _adm_sort_key(record["boundaryType"]),
                record["boundaryType"],
            ),
        )
        body = (json.dumps(records, indent=2) + "\n").encode("utf-8")
        key = f"{product}/index.json"
        if prefix:
            key = f"{prefix}/{key}"
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=body,
            ContentType="application/json",
            CacheControl="public, max-age=300",
        )
        manifest.append({"key": key, "size": len(body)})
        log.info(
            "Published API index with %d record(s) to s3://%s/%s",
            len(records),
            bucket,
            key,
        )

    return manifest


def verify_release_objects(successes, s3_config, index_objects=None):
    """Confirm every object uploaded by successful builds exists in the bucket.

    Lists the whole `{prefix}/` tree once and checks each key in each
    success's upload manifest for presence and matching size.  Raises
    RuntimeError on any discrepancy so the caller never promotes a
    version with missing or truncated files.
    """

    if not successes:
        raise RuntimeError("No successful boundary builds; refusing to promote")

    prefix = s3_config.get("prefix", "")
    list_prefix = f"{prefix}/" if prefix else ""
    bucket = s3_config["bucket"]

    log.info("Listing s3://%s/%s for verification…", bucket, list_prefix)
    s3 = _s3_client(s3_config)
    in_bucket = {}
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=list_prefix):
        for obj in page.get("Contents", []):
            in_bucket[obj["Key"]] = obj["Size"]

    problems = []
    expected = 0
    for r in successes:
        tag = f"{r['product']}/{r['iso']}_{r['adm']}"
        manifest = r.get("uploaded") or []
        if not manifest:
            problems.append(f"{tag}: build succeeded but uploaded nothing")
            continue
        for entry in manifest:
            expected += 1
            key, size = entry["key"], entry["size"]
            if key not in in_bucket:
                problems.append(f"{tag}: missing s3://{bucket}/{key}")
            elif in_bucket[key] != size:
                problems.append(
                    f"{tag}: size mismatch s3://{bucket}/{key} "
                    f"(local {size}, bucket {in_bucket[key]})"
                )

    for entry in index_objects or []:
        expected += 1
        key, size = entry["key"], entry["size"]
        if key not in in_bucket:
            problems.append(f"API index: missing s3://{bucket}/{key}")
        elif in_bucket[key] != size:
            problems.append(
                f"API index: size mismatch s3://{bucket}/{key} "
                f"(local {size}, bucket {in_bucket[key]})"
            )

    if problems:
        bar = "=" * 72
        log.error(bar)
        log.error("  RELEASE VERIFICATION FAILED — refusing to promote")
        log.error(
            "  %d problem(s) across %d expected object(s)", len(problems), expected
        )
        log.error(bar)
        for msg in problems:
            log.error("  %s", msg)
        log.error(bar)
        raise RuntimeError(
            f"{len(problems)} object(s) missing or mismatched "
            f"in s3://{bucket}/{list_prefix}"
        )

    log.info(
        "Verified %d object(s) for %d boundaries in s3://%s/%s",
        expected,
        len(successes),
        bucket,
        list_prefix,
    )


def promote_to_current(version, s3_config):
    """Flip the `current` pointer in the bucket to `version`.

    Writes /current.json at the bucket root with {"version": version}.
    Short Cache-Control so a release becomes visible quickly even
    without an explicit CDN purge.
    """

    s3 = _s3_client(s3_config)
    body = (json.dumps({"version": version}) + "\n").encode("utf-8")
    s3.put_object(
        Bucket=s3_config["bucket"],
        Key="current.json",
        Body=body,
        ContentType="application/json",
        CacheControl="public, max-age=60",
    )
    log.info("Promoted current → %s (wrote s3://%s/current.json)",
             version, s3_config["bucket"])


def log_failure_summary(failures, strict):
    """Print a prominent, human-readable banner listing failed boundaries."""

    header = "BUILD FAILURES (strict mode — aborting)" if strict else (
        "UPSTREAM DATA ISSUES (non-strict mode — continuing)"
    )
    bar = "=" * 72

    log.error(bar)
    log.error("  %s", header)
    log.error("  %d boundary build(s) failed", len(failures))
    log.error(bar)
    for f in failures:
        tag = f"{f['product']}/{f['iso']}_{f['adm']}"
        log.error("  %-30s [%s]", tag, f.get("failed_stage"))
        log.error("    %s", f.get("error"))
    log.error(bar)


# ---------------------------------------------------------------------------
# Step 6 — CGAZ Job via kr8s
# ---------------------------------------------------------------------------


def run_cgaz_job(db_url, timeout=7200):
    """Create a one-shot Kubernetes Job for CGAZ processing and wait for it."""

    release = os.environ["GB_RELEASE_NAME"]
    ns = os.environ.get("GB_NAMESPACE", "default")
    image = os.environ["GB_IMAGE"]
    job_name = f"{release}-cgaz-{int(time.time())}"

    manifest = {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": job_name,
            "namespace": ns,
            "labels": {
                "app.kubernetes.io/name": "geoboundarybot",
                "app.kubernetes.io/component": "cgaz",
            },
        },
        "spec": {
            "backoffLimit": 1,
            "ttlSecondsAfterFinished": 3600,
            "template": {
                "spec": {
                    "restartPolicy": "Never",
                    "containers": [
                        {
                            "name": "cgaz",
                            "image": image,
                            "command": [
                                "python",
                                "-m",
                                "builder.cgaz_builder",
                                "-vv",
                            ],
                            "env": [
                                {
                                    "name": "GB_REPO_DIR",
                                    "value": "/data/geoBoundaries",
                                },
                                {
                                    "name": "DATABASE_URL",
                                    "value": db_url,
                                },
                            ],
                            "volumeMounts": [
                                {
                                    "name": "data",
                                    "mountPath": "/data/geoBoundaries",
                                },
                            ],
                        },
                    ],
                    "volumes": [
                        {
                            "name": "data",
                            "persistentVolumeClaim": {
                                "claimName": f"{release}-data",
                            },
                        },
                    ],
                },
            },
        },
    }

    log.info("Creating CGAZ job: %s", job_name)
    job = Job(manifest)
    job.create()

    log.info("Waiting for CGAZ job (timeout %ds)…", timeout)
    job.wait(["condition=Complete", "condition=Failed"], timeout=timeout)
    job.refresh()

    if (job.status.get("succeeded") or 0) >= 1:
        log.info("CGAZ job %s completed successfully", job_name)
        return True

    log.error("CGAZ job %s failed", job_name)
    return False


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    dask_workers = int(os.environ.get("DASK_WORKERS", "4"))
    scheduler = os.environ.get("DASK_SCHEDULER", "tcp://localhost:8786")
    version = os.environ.get("GB_RELEASE_VERSION", "nightly")
    strict = os.environ.get("GB_STRICT", "false").lower() == "true"
    promote = os.environ.get("GB_PROMOTE_CURRENT", "false").lower() == "true"

    # Promoting `current` implies strict — we never want to flip the pointer
    # at a build that had upstream data issues.
    if promote and not strict:
        log.info("GB_PROMOTE_CURRENT=true → forcing strict mode")
        strict = True

    log.info(
        "Release: version=%s strict=%s promote_current=%s",
        version, strict, promote,
    )

    # Build S3 config from env vars (None if not configured).
    # The release version becomes the top-level key prefix so versions
    # coexist in the bucket: v6/gbOpen/KEN/ADM1/..., nightly/gbOpen/...
    s3_config = None
    s3_endpoint = os.environ.get("S3_ENDPOINT_URL")
    s3_bucket = os.environ.get("S3_BUCKET")
    if s3_endpoint and s3_bucket:
        s3_config = {
            "endpoint": s3_endpoint,
            "bucket": s3_bucket,
            "prefix": version,
            "access_key_id": os.environ["AWS_ACCESS_KEY_ID"],
            "secret_access_key": os.environ["AWS_SECRET_ACCESS_KEY"],
        }
        log.info("S3 uploads enabled → s3://%s/%s/", s3_bucket, version)
    else:
        log.info("S3 uploads disabled (S3_ENDPOINT_URL / S3_BUCKET not set)")

    if promote and s3_config is None:
        log.error("GB_PROMOTE_CURRENT=true requires S3 to be configured")
        sys.exit(1)

    # 0. Acquire the shared build lock. Held across the whole run — the
    #    git reset in step 1 is itself unsafe while another build is running.
    release = os.environ["GB_RELEASE_NAME"]
    ns = os.environ.get("GB_NAMESPACE", "default")
    lock = BuildLock(
        name=f"{release}-build-lock",
        namespace=ns,
        holder=os.environ.get("GB_POD_NAME") or os.environ.get("HOSTNAME", "unknown"),
        wait_timeout=int(os.environ.get("GB_LOCK_WAIT_SECONDS", "0")),
    )
    log.info("=== Step 0: Acquiring build lock ===")
    try:
        lock.acquire()
    except BuildLockHeldError as e:
        bar = "=" * 72
        log.error(bar)
        log.error("  ANOTHER BUILD IS RUNNING — aborting")
        log.error("  %s", e)
        log.error("  Set build.<nightly|release>.lockWaitSeconds to wait instead.")
        log.error(bar)
        sys.exit(1)

    try:
        _run_pipeline(version, strict, promote, dask_workers, scheduler, s3_config)
    finally:
        # 11. Release the build lock (always)
        log.info("=== Step 11: Releasing build lock ===")
        lock.release()

    log.info("=== Build pipeline complete ===")


def _run_pipeline(version, strict, promote, dask_workers, scheduler, s3_config):
    """Steps 1–10. Must only run while the build lock is held."""

    # 1. Sync data repo
    log.info("=== Step 1: Syncing data repository ===")
    sync_data_repo()

    # 2. Create ephemeral build database
    log.info("=== Step 2: Creating build database ===")
    db_url = create_build_db()

    try:
        # 3. Scale up Dask
        log.info("=== Step 3: Scaling Dask workers to %d ===", dask_workers)
        scale_dask_workers(dask_workers)

        try:
            # 4. Run boundary builds
            log.info("=== Step 4: Running boundary builds ===")
            successes, failures = run_boundary_builds(
                scheduler, db_url, s3_config, version=version
            )

            if failures:
                log_failure_summary(failures, strict=strict)
                if strict:
                    sys.exit(1)
        finally:
            # 5. Scale down Dask (always, even on failure)
            log.info("=== Step 5: Scaling Dask workers to 0 ===")
            try:
                scale_dask_workers(0)
            except Exception:
                log.exception("Failed to scale down Dask workers")

        # 6. Publish aggregate API indexes for both nightly and releases.
        #    A promoted release cannot proceed unless all product indexes were
        #    written and then verified alongside the boundary objects.
        index_objects = []
        if s3_config:
            log.info("=== Step 6: Publishing aggregate API indexes ===")
            index_objects = upload_api_indexes(successes, s3_config)

        # 7. CGAZ
        log.info("=== Step 7: Running CGAZ job ===")
        cgaz_ok = run_cgaz_job(db_url)
        if not cgaz_ok:
            log.error("CGAZ job failed")
            sys.exit(1)

        # 8. Verify uploads, then promote `current` pointer (release runs only)
        if promote:
            log.info("=== Step 8: Verifying release objects in S3 ===")
            verify_release_objects(successes, s3_config, index_objects)
            log.info("=== Step 9: Promoting current → %s ===", version)
            promote_to_current(version, s3_config)
    finally:
        # 10. Tear down build database (always)
        log.info("=== Step 10: Tearing down build database ===")
        try:
            teardown_build_db()
        except Exception:
            log.exception("Failed to tear down build database")


if __name__ == "__main__":
    main()
