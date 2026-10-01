"""Kubernetes-aware build driver for the geoBoundaries pipeline.

Entrypoint: `python -m builder.run`

Orchestrates the full build:
  0. Acquire the shared build lock (a Kubernetes Lease) so the nightly and
     release CronJobs can never run concurrently
  1. Pull latest changes from the data repo, and make sure the LSIB base
     layer CGAZ needs is cached in the bucket (copied from Git LFS if not)
  2. Create an ephemeral PostGIS database for build outputs
  3. Scale up Dask workers
  4. Discover boundaries and fan out builds across Dask
     (each worker pushes its output geometries to PostGIS and uploads
      its build outputs to S3 under {GB_RELEASE_VERSION}/...).
     The per-country CGAZ work rides along on the same cluster: one task
     loads LSIB into PostGIS, and each gbOpen country gets a task once its
     ADM0-2 builds finish (see builder.cgaz_builder).  The driver feeds
     Dask one task per free worker thread: LSIB, then builds biggest-first,
     then CGAZ countries, so CGAZ fills workers as the builds tail off
  5. Scale down Dask workers
  6. Publish per-product API indexes to S3
  7. Launch a single-pod CGAZ Job that merges the per-country parts from
     PostGIS into global layers and uploads them under
     {GB_RELEASE_VERSION}/CGAZ/
  8. If GB_PROMOTE_CURRENT=true, verify every uploaded object exists in
     the bucket
  9. If GB_PROMOTE_CURRENT=true, write current.json to the bucket root
  10. Tear down the ephemeral database
  11. Release the build lock
  Exit 0 on success, 1 on failure
"""

import hashlib
import heapq
import itertools
import json
from botocore.exceptions import ClientError
import logging
import os
import shutil
import subprocess
import tempfile
import threading
from datetime import datetime, timedelta, timezone
import kr8s
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
from builder.paths import ISO_CSV, LICENSES_CSV, LSIB_GEOJSON, RELEASE_DATA
import geopandas as gpd
import pandas as pd
import requests
from sqlalchemy import create_engine
from dask.distributed import Client, as_completed
from kr8s.objects import Job
from builder import cgaz_builder
from builder.s3 import s3_client as _s3_client
from builder.s3 import s3_config_from_env, upload_to_s3

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


def _builddb_labels(release):
    return {
        "app.kubernetes.io/instance": release,
        "app.kubernetes.io/component": "builddb",
    }


def _delete_builddb_pvcs(release, ns, wait=True):
    """Delete every build-database PVC for this release, found by label."""
    for pvc in kr8s.get(
        "persistentvolumeclaims", namespace=ns, label_selector=_builddb_labels(release)
    ):
        if wait:
            _delete_and_wait(pvc)
        else:
            pvc.delete()
            log.info("Deleted %s/%s", pvc.kind, pvc.name)


def create_build_db(timeout=120):
    """Create an ephemeral PostGIS PVC + Deployment + Service for build outputs.

    Returns the connection URL.
    """

    release = os.environ["GB_RELEASE_NAME"]
    ns = os.environ.get("GB_NAMESPACE", "default")
    image = os.environ.get("GB_POSTGIS_IMAGE", "postgis/postgis:17-3.5")
    storage_class = os.environ.get("GB_BUILDDB_STORAGE_CLASS", "")
    storage_size = os.environ.get("GB_BUILDDB_STORAGE_SIZE", "50Gi")
    # Every Dask thread can hold a connection at once (each build writes its
    # boundary, each CGAZ task reads and writes parts), and Postgres's
    # default of 100 is well under 48 workers x 4 threads.
    max_connections = os.environ.get("GB_BUILDDB_MAX_CONNECTIONS", "500")
    cpu = os.environ.get("GB_BUILDDB_CPU", "4")
    memory = os.environ.get("GB_BUILDDB_MEMORY", "16Gi")
    # A quarter of the memory request, per the usual Postgres advice.
    shared_buffers = os.environ.get("GB_BUILDDB_SHARED_BUFFERS", "4GB")
    settings = {
        "max_connections": max_connections,
        # The database lasts one run, and a crash mid-run means rerunning
        # anyway, so skip the crash-safety work that's slow on NFS: no
        # commit-time syncs, no full-page images in the WAL.  (The tables
        # are also UNLOGGED, so they skip the WAL entirely.)
        "fsync": "off",
        "synchronous_commit": "off",
        "full_page_writes": "off",
        # Big boundary geometries are TOASTed; lz4 compresses them far
        # faster than the default pglz.
        "default_toast_compression": "lz4",
        "shared_buffers": shared_buffers,
        "max_wal_size": "16GB",
        "checkpoint_timeout": "30min",
    }
    name = f"{release}-builddb"
    # Unique per run: reusing a claim name before the old PV is reclaimed lets
    # the vcluster syncer bind the new claim to the dying volume, leaving it
    # Pending forever once the provisioner deletes that volume.
    pvc_name = f"{name}-{int(time.time())}"

    labels = _builddb_labels(release)

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
            "metadata": {"name": pvc_name, "namespace": ns, "labels": labels},
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
                                "args": [
                                    "postgres",
                                    *(
                                        arg
                                        for key, value in settings.items()
                                        for arg in ("-c", f"{key}={value}")
                                    ),
                                ],
                                "resources": {
                                    "requests": {"cpu": cpu, "memory": memory},
                                },
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
                                "persistentVolumeClaim": {"claimName": pvc_name},
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
        pvc_name,
        storage_size,
        storage_class or "default storage class",
    )

    # Clean up leftover resources from any previous run (order matters:
    # Deployment must be gone before PVC, or the pvc-protection finalizer
    # keeps the PVC in Terminating state).
    _delete_and_wait(deploy)
    _delete_and_wait(svc)
    _delete_builddb_pvcs(release, ns)

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


BOUNDARIES_COLUMNS = [
    "product",
    "iso",
    "adm_level",
    "shape_name",
    "shape_iso",
    "shape_id",
    "shape_group",
    "shape_type",
    "geom",
]


def _init_build_db_schema(db_url):
    """Enable PostGIS and create the boundaries and CGAZ tables."""

    engine = create_engine(db_url)
    with engine.begin() as conn:
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis"))
        conn.execute(
            text("""
            CREATE UNLOGGED TABLE IF NOT EXISTS boundaries (
                id SERIAL PRIMARY KEY,
                product TEXT NOT NULL,
                iso TEXT NOT NULL,
                adm_level TEXT NOT NULL,
                shape_name TEXT,
                shape_iso TEXT,
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
        cgaz_builder.init_schema(conn)
    engine.dispose()
    log.info("Build database schema initialized")


def teardown_build_db():
    """Delete the ephemeral PostGIS PVC, Deployment, and Service."""

    release = os.environ["GB_RELEASE_NAME"]
    ns = os.environ.get("GB_NAMESPACE", "default")
    name = f"{release}-builddb"

    for cls in (Deployment, Service):
        try:
            obj = cls.get(name, namespace=ns)
            obj.delete()
            log.info("Deleted %s/%s", cls.kind, name)
        except Exception:
            log.warning("Could not delete %s/%s", cls.kind, name, exc_info=True)
    try:
        _delete_builddb_pvcs(release, ns, wait=False)
    except Exception:
        log.warning("Could not delete build database PVCs", exc_info=True)


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
    """Scan sourceData/ for ZIP files.

    Returns (product, iso, adm, size) tuples, where size is the ZIP's size
    in bytes.
    """

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
                boundaries.append(
                    (product, parts[0], parts[1], zip_file.stat().st_size)
                )
            else:
                log.warning("Skipping malformed filename: %s", zip_file.name)
    return boundaries


class _StageTimer:
    """Wall-clock seconds spent in each build stage, in the order they ran."""

    def __init__(self):
        self.timings = {}
        self._stage = None
        self._t0 = self._start = time.perf_counter()

    def start(self, stage):
        """Close out the running stage, if any, and start timing `stage`."""
        now = time.perf_counter()
        if self._stage is not None:
            self.timings[self._stage] = round(now - self._start, 1)
        self._stage, self._start = stage, now

    def stop(self):
        self.start(None)
        self.timings["total"] = round(time.perf_counter() - self._t0, 1)


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

    The result's "timings" maps each stage that ran to its duration in
    seconds, plus "total"; a failed build stops at the stage that failed.
    """

    timer = _StageTimer()
    timer.start("setup")
    tmpdir = tempfile.mkdtemp(prefix=f"gb-{product}-{iso}-{adm}-")
    tmpdir = Path(tmpdir)
    # Workers are long-lived within a run, so each boundary's build tree
    # has to go as soon as it's uploaded or /tmp fills the node's disk.
    try:
        iso_df = pd.read_csv(ISO_CSV)
        license_df = pd.read_csv(LICENSES_CSV)
        valid_isos = iso_df["Alpha-3code"].tolist()
        valid_licenses = license_df["license_name"].tolist()
        b = builder(iso, adm, product, valid_isos, valid_licenses, tmpdir=tmpdir)

        result = {
            "product": product,
            "iso": iso,
            "adm": adm,
            "timings": timer.timings,
        }
        for stage_name, stage_fn in [
            ("checkExistence", b.checkExistence),
            ("checkSourceValidity", b.checkSourceValidity),
            ("checkBuildTabularMetaData", b.checkBuildTabularMetaData),
            ("checkBuildGeometryFiles", b.checkBuildGeometryFiles),
            ("calculateGeomMeta", b.calculateGeomMeta),
            ("constructFiles", b.constructFiles),
        ]:
            timer.start(stage_name)
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
        timer.start("loadMetadata")
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
        timer.start("writePostGIS")
        try:
            geojson_path = (
                b.targetPath / f"geoBoundaries-{iso}-{adm}.geojson"
            )
            if geojson_path.exists():
                # constructFiles has already parsed the released GeoJSON.
                gdf = getattr(b, "releaseGeom", None)
                if gdf is None:
                    gdf = gpd.read_file(geojson_path)
                gdf = gdf.to_crs(epsg=4326)
                gdf["product"] = product
                gdf["iso"] = iso
                gdf["adm_level"] = adm
                gdf = gdf.rename(columns={
                    "geometry": "geom",
                    "shapeName": "shape_name",
                    "shapeISO": "shape_iso",
                    "shapeID": "shape_id",
                    "shapeGroup": "shape_group",
                    "shapeType": "shape_type",
                }).set_geometry("geom")
                # to_postgis appends every column, so drop anything the table
                # doesn't define rather than failing the whole boundary.
                gdf = gdf[[c for c in BOUNDARIES_COLUMNS if c in gdf.columns]]

                # Replace rather than append: Dask reruns a task whose worker
                # died, and duplicate rows would overlap in CGAZ.
                engine = create_engine(db_url)
                try:
                    with engine.begin() as conn:
                        conn.execute(
                            text(
                                "DELETE FROM boundaries WHERE product = :product "
                                "AND iso = :iso AND adm_level = :adm"
                            ),
                            {"product": product, "iso": iso, "adm": adm},
                        )
                        gdf.to_postgis(
                            "boundaries",
                            conn,
                            if_exists="append",
                            index=False,
                        )
                finally:
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
            timer.start("uploadToS3")
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
    finally:
        timer.start("cleanup")
        shutil.rmtree(tmpdir, ignore_errors=True)
        # `result` holds this same dict, so the returned timings are complete.
        timer.stop()


# What the driver hands to Dask first.  LSIB leads, since no CGAZ country can
# start without it; then every build, biggest source first; then the CGAZ
# countries, biggest first, which fill workers as the builds tail off.
_RANK = {"lsib": 0, "build": 1, "cgaz": 2}


class BuildQueue:
    """The driver's priority queue of Dask work, CGAZ dependencies included.

    Dask only orders tasks within each worker's own queue, and hands every
    task to a worker the moment it's submitted (our per-country task groups
    are far too small for its scheduler-side queuing).  Submitting
    everything up front can leave a 20-minute build waiting on one worker
    behind trivial ones while other workers sit idle.  So the driver keeps
    the queue here and submits one task per free worker thread, always the
    next by _RANK, then source size.

    A CGAZ country is queued once LSIB has loaded and all of that country's
    gbOpen ADM0-2 builds have finished, successfully or not.
    """

    def __init__(
        self, boundaries, db_url, lsib_url, s3_config=None, version="nightly"
    ):
        self._heap = []
        self._seq = itertools.count()
        self._db_url = db_url
        self._version = version
        self.lsib_ok = None
        # Per gbOpen country: the ADM0-2 builds still running, the results
        # of those that finished, and their summed source size.
        self._waiting = {}
        self._finished = {}
        self._cgaz_size = {}

        self._push(
            "lsib",
            0,
            cgaz_builder.prepare_lsib,
            (lsib_url, db_url),
            key=f"{version}-cgaz-lsib",
            tag="CGAZ/LSIB",
        )
        for product, iso, adm, size in boundaries:
            self._push(
                "build",
                size,
                build_boundary,
                (product, iso, adm, db_url),
                {"s3_config": s3_config},
                key=f"{version}-{product}-{iso}-{adm}",
                tag=f"{product}/{iso}_{adm}",
                boundary=(product, iso, adm),
            )
            if product == "gbOpen" and adm in cgaz_builder.LEVELS:
                self._waiting.setdefault(iso, set()).add(adm)
                self._cgaz_size[iso] = self._cgaz_size.get(iso, 0) + size

    def __len__(self):
        return len(self._heap)

    @property
    def cgaz_countries(self):
        return len(self._cgaz_size)

    def _push(self, kind, size, fn, args, kwargs=None, **task):
        task.update(kind=kind, fn=fn, args=args, kwargs=kwargs or {})
        heapq.heappush(self._heap, (_RANK[kind], -size, next(self._seq), task))

    def pop(self):
        """Remove and return the most important queued task."""
        return heapq.heappop(self._heap)[-1]

    def finished(self, task, result):
        """Record a finished task, queueing any CGAZ country it unblocks."""

        if task["kind"] == "lsib":
            self.lsib_ok = result.get("status") == "ok"
            ready = [iso for iso, adms in self._waiting.items() if not adms]
        elif task["kind"] == "build":
            product, iso, adm = task["boundary"]
            if product != "gbOpen" or adm not in cgaz_builder.LEVELS:
                return
            self._waiting[iso].discard(adm)
            self._finished.setdefault(iso, []).append(
                {"adm": adm, "status": result.get("status")}
            )
            lsib_done = self.lsib_ok is not None
            ready = [iso] if lsib_done and not self._waiting[iso] else []
        else:
            return

        for iso in ready:
            del self._waiting[iso]
            builds = self._finished.pop(iso)
            # Without LSIB there's nothing to clip to; the run fails on that.
            if not self.lsib_ok:
                continue
            self._push(
                "cgaz",
                self._cgaz_size[iso],
                cgaz_builder.build_cgaz_country,
                (iso, self._db_url, builds),
                key=f"{self._version}-cgaz-{iso}",
                tag=f"CGAZ/{iso}",
            )


def run_boundary_builds(
    scheduler_url, db_url, lsib_url, s3_config=None, version="nightly", strict=False
):
    """Connect to Dask, discover boundaries, and run the builds together
    with the per-country CGAZ work that reads their output.

    Returns (successes, failures, cgaz_results): the per-boundary result
    dicts returned by `build_boundary`, and the results of the CGAZ tasks.
    In strict mode, if any build failed, the CGAZ work still running or
    queued once the last build finishes is abandoned, since the run is
    going to abort anyway.
    """

    boundaries = discover_boundaries()
    log.info("Discovered %d boundaries to build", len(boundaries))

    if not boundaries:
        log.info("Nothing to build.")
        return [], [], []

    queue = BuildQueue(boundaries, db_url, lsib_url, s3_config, version)
    log.info("CGAZ will cover %d gbOpen countries", queue.cgaz_countries)

    log.info("Connecting to Dask scheduler at %s", scheduler_url)
    with Client(scheduler_url) as client:
        log.info("Dashboard: %s", client.dashboard_link)
        return _drain_build_queue(client, queue, len(boundaries), strict)


def _drain_build_queue(client, queue, n_builds, strict):
    running = {}
    dispatched = itertools.count()
    # raise_errors=False: a task that raised (e.g. its worker was OOM-killed
    # too often) is logged as a failure instead of crashing the driver.
    completed = as_completed(with_results=True, raise_errors=False)

    def top_up():
        # Re-read every time, since workers can die and be replaced mid-run.
        slots = max(sum(client.nthreads().values()), 1)
        while queue and len(running) < slots:
            task = queue.pop()
            future = client.submit(
                task["fn"],
                *task["args"],
                key=task["key"],
                # Only matters if Dask doubles tasks up on one worker.
                priority=-next(dispatched),
                **task["kwargs"],
            )
            running[future] = task
            completed.add(future)

    successes = []
    failures = []
    cgaz_results = []
    builds_left = n_builds
    t0 = time.monotonic()

    top_up()
    for future, result in completed:
        task = running.pop(future)
        if future.status == "error":
            result = {"status": "error", "error": repr(result[1])}
            if task["kind"] == "build":
                product, iso, adm = task["boundary"]
                result.update(product=product, iso=iso, adm=adm, failed_stage="dask")
        queue.finished(task, result)

        if task["kind"] == "build":
            total = result.get("timings", {}).get("total", 0)
            if result["status"] == "ok":
                successes.append(result)
                log.info("OK  %s (%.0fs)", task["tag"], total)
            else:
                failures.append(result)
                log.error(
                    "FAIL %s stage=%s (%.0fs): %s",
                    task["tag"],
                    result.get("failed_stage"),
                    total,
                    result.get("error"),
                )

            builds_left -= 1
            if builds_left == 0:
                log.info(
                    "Boundary builds: %d succeeded, %d failed in %.0fs",
                    len(successes),
                    len(failures),
                    time.monotonic() - t0,
                )
                log_build_timings(successes + failures)
                if strict and failures:
                    client.cancel(list(running))
                    log.info("Abandoned outstanding CGAZ work (strict mode)")
                    break
        else:
            result["tag"] = task["tag"]
            cgaz_results.append(result)
            total = result.get("timings", {}).get("total", 0)
            if result["status"] == "error":
                log.error(
                    "FAIL %s (%.0fs): %s", task["tag"], total, result.get("error")
                )
            elif result["status"] == "skipped":
                log.info("SKIP %s: %s", task["tag"], result.get("reason"))
            else:
                log.info("OK  %s (%.0fs)", task["tag"], total)

        top_up()

    log.info(
        "CGAZ tasks: %d ok, %d skipped, %d failed (%.0fs since the builds started)",
        sum(r["status"] == "ok" for r in cgaz_results),
        sum(r["status"] == "skipped" for r in cgaz_results),
        sum(r["status"] == "error" for r in cgaz_results),
        time.monotonic() - t0,
    )
    return successes, failures, cgaz_results


def log_build_timings(results, slowest=15):
    """Log where build time went: per-stage totals, then the slowest builds."""

    timed = [r for r in results if r.get("timings", {}).get("total") is not None]
    if not timed:
        return

    stage_totals = {}
    for r in timed:
        for stage, secs in r["timings"].items():
            if stage != "total":
                stage_totals[stage] = stage_totals.get(stage, 0) + secs
    grand_total = sum(stage_totals.values()) or 1
    log.info("Build time by stage, summed over %d builds:", len(timed))
    for stage, secs in sorted(stage_totals.items(), key=lambda kv: -kv[1]):
        log.info("  %-26s %8.0fs  %5.1f%%", stage, secs, 100 * secs / grand_total)

    log.info("Slowest %d builds (seconds per stage):", min(slowest, len(timed)))
    for r in sorted(timed, key=lambda r: -r["timings"]["total"])[:slowest]:
        stages = "  ".join(
            f"{stage}={secs:.0f}"
            for stage, secs in r["timings"].items()
            if stage != "total"
        )
        log.info(
            "  %-22s %6.0fs  %s",
            f"{r['product']}/{r['iso']}_{r['adm']}",
            r["timings"]["total"],
            stages,
        )


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


def verify_release_objects(successes, s3_config, index_objects=None, cgaz_objects=None):
    """Confirm every object uploaded by successful builds exists in the bucket.

    Lists the whole `{prefix}/` tree once and checks each key in each
    success's upload manifest, plus the API indexes and CGAZ layers, for
    presence and matching size.  Raises
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

    if cgaz_objects is not None and not cgaz_objects:
        problems.append("CGAZ: merge job uploaded nothing")

    for label, entries in (("API index", index_objects), ("CGAZ", cgaz_objects)):
        for entry in entries or []:
            expected += 1
            key, size = entry["key"], entry["size"]
            if key not in in_bucket:
                problems.append(f"{label}: missing s3://{bucket}/{key}")
            elif in_bucket[key] != size:
                problems.append(
                    f"{label}: size mismatch s3://{bucket}/{key} "
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


def check_cgaz_results(cgaz_results, strict):
    """Whether the CGAZ merge can go ahead, given the Dask CGAZ results.

    A failed LSIB task is always fatal, since there's nothing to lay the
    countries out on.  A failed country falls back to its LSIB outline in
    every layer, which only non-strict runs accept.
    """

    lsib = next((r for r in cgaz_results if r.get("tag") == "CGAZ/LSIB"), None)
    if lsib is None or lsib["status"] != "ok":
        log.error(
            "CGAZ LSIB base layer failed: %s",
            lsib.get("error") if lsib else "task never finished",
        )
        return False

    failed = [r for r in cgaz_results if r["status"] == "error"]
    if failed:
        bar = "=" * 72
        log.error(bar)
        log.error(
            "  %d CGAZ country task(s) failed (%s)",
            len(failed),
            "strict mode — aborting" if strict else "using LSIB outlines instead",
        )
        log.error(bar)
        for r in failed:
            log.error("  %-30s %s", r["tag"], r.get("error"))
        log.error(bar)
        if strict:
            return False
    return True


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
# LSIB base layer for CGAZ
# ---------------------------------------------------------------------------

# The image only carries the Git LFS pointer for the ~400 MB LSIB file. The
# object itself is cached in the bucket, outside any release prefix, and
# copied there from LFS the first time a build finds it missing.
LSIB_KEY = f"reference/lsib/{LSIB_GEOJSON.name}"
LSIB_LFS_REPO = os.environ.get(
    "GB_LSIB_LFS_REPO", "https://github.com/wmgeolab/geoBoundaryBot.git"
)


def _read_lfs_pointer(path):
    """Return (sha256, size) from the Git LFS pointer file at `path`."""

    # A real pointer is ~130 bytes; anything bigger means LFS was smudged
    # into the image and there's no pointer to read.
    if path.stat().st_size > 1024:
        raise ValueError(f"{path} is not a Git LFS pointer")
    fields = dict(
        line.split(" ", 1) for line in path.read_text().splitlines() if " " in line
    )
    oid = fields.get("oid", "")
    if not oid.startswith("sha256:") or "size" not in fields:
        raise ValueError(f"{path} is not a Git LFS pointer")
    return oid.removeprefix("sha256:"), int(fields["size"])


def _lfs_download_url(oid, size):
    """Ask the LFS server for a short-lived download URL for one object."""

    resp = requests.post(
        f"{LSIB_LFS_REPO.removesuffix('/')}/info/lfs/objects/batch",
        data=json.dumps({
            "operation": "download",
            "transfers": ["basic"],
            "objects": [{"oid": oid, "size": size}],
        }),
        headers={
            "Accept": "application/vnd.git-lfs+json",
            "Content-Type": "application/vnd.git-lfs+json",
        },
        timeout=60,
    )
    resp.raise_for_status()
    obj = resp.json()["objects"][0]
    if "error" in obj:
        raise RuntimeError(f"LFS server refused object {oid}: {obj['error']}")
    return obj["actions"]["download"]["href"]


def stage_lsib(s3_config):
    """Make sure the LSIB file is in the bucket, copying it from LFS if not.

    The bucket copy is tagged with the pointer's sha256, so updating the
    pointer in dta/ replaces it on the next run.
    """

    oid, size = _read_lfs_pointer(LSIB_GEOJSON)
    s3 = _s3_client(s3_config)
    bucket = s3_config["bucket"]

    try:
        head = s3.head_object(Bucket=bucket, Key=LSIB_KEY)
    except ClientError as e:
        if e.response["Error"]["Code"] not in ("404", "NoSuchKey", "NotFound"):
            raise
    else:
        if head.get("Metadata", {}).get("sha256") == oid:
            log.info("LSIB already cached at s3://%s/%s", bucket, LSIB_KEY)
            return
        log.warning(
            "s3://%s/%s doesn't match the LFS pointer, replacing it", bucket, LSIB_KEY
        )

    log.info(
        "Copying LSIB (%d bytes) from %s to s3://%s/%s",
        size, LSIB_LFS_REPO, bucket, LSIB_KEY,
    )
    digest = hashlib.sha256()
    with tempfile.NamedTemporaryFile(suffix=".geojson") as tmp:
        with requests.get(_lfs_download_url(oid, size), stream=True, timeout=60) as resp:
            resp.raise_for_status()
            for chunk in resp.iter_content(chunk_size=1 << 20):
                tmp.write(chunk)
                digest.update(chunk)
        tmp.flush()
        if digest.hexdigest() != oid:
            raise RuntimeError(
                f"LSIB download has sha256 {digest.hexdigest()}, expected {oid}"
            )
        s3.upload_file(
            tmp.name,
            bucket,
            LSIB_KEY,
            ExtraArgs={"ContentType": "application/geo+json", "Metadata": {"sha256": oid}},
        )
    log.info("Cached LSIB at s3://%s/%s", bucket, LSIB_KEY)


def lsib_url(s3_config, expires=7200):
    """A URL the LSIB Dask task can download the file from without credentials.

    The task is queued ahead of every build, so it normally starts within
    seconds; `expires` leaves room for it to be rerun if its worker dies.
    """

    if s3_config is None:
        # No bucket to cache in, so hand CGAZ the (one-hour) LFS URL directly.
        return _lfs_download_url(*_read_lfs_pointer(LSIB_GEOJSON))
    return _s3_client(s3_config).generate_presigned_url(
        "get_object",
        Params={"Bucket": s3_config["bucket"], "Key": LSIB_KEY},
        ExpiresIn=expires,
    )


# ---------------------------------------------------------------------------
# Step 7 — CGAZ merge Job via kr8s
# ---------------------------------------------------------------------------


def _cgaz_job_env(db_url, version, s3_config):
    """Env for the CGAZ merge pod: the build database, plus S3 if enabled."""

    env = [
        {"name": "DATABASE_URL", "value": db_url},
        {"name": "GB_RELEASE_VERSION", "value": version},
    ]
    if s3_config is not None:
        # Credentials come from the same Secret the driver uses, by reference.
        secret = os.environ["GB_S3_CREDENTIALS_SECRET"]
        env += [
            {"name": "S3_ENDPOINT_URL", "value": s3_config["endpoint"]},
            {"name": "S3_BUCKET", "value": s3_config["bucket"]},
            {
                "name": "AWS_ACCESS_KEY_ID",
                "valueFrom": {
                    "secretKeyRef": {"name": secret, "key": "access-key-id"}
                },
            },
            {
                "name": "AWS_SECRET_ACCESS_KEY",
                "valueFrom": {
                    "secretKeyRef": {"name": secret, "key": "secret-access-key"}
                },
            },
        ]
    return env


def run_cgaz_job(db_url, version, s3_config, timeout=14400):
    """Run the one-shot CGAZ merge Job and wait for it.

    The Job reads the per-country parts the Dask tasks left in PostGIS,
    merges them into global layers and uploads them under
    {version}/CGAZ/.  It needs nothing from the data PVC.
    """

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
            # CGAZ failures so far have been deterministic (bad input, OOM),
            # so a retry just burns another hour.
            "backoffLimit": 0,
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
                            "env": _cgaz_job_env(db_url, version, s3_config),
                            # Room for the global mapshaper merges (see
                            # MERGE_HEAP in cgaz_builder).
                            "resources": {
                                "requests": {"cpu": "4", "memory": "32Gi"},
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

    # Poll rather than job.wait(): kr8s's wait returns silently when the API
    # server closes the watch (30–60 min), which read as a failure mid-run.
    log.info("Waiting for CGAZ job (timeout %ds)…", timeout)
    deadline = time.monotonic() + timeout
    while True:
        job.refresh()
        finished = {
            c["type"]
            for c in job.status.get("conditions", [])
            if c["type"] in ("Complete", "Failed") and c["status"] == "True"
        }
        if "Complete" in finished:
            log.info("CGAZ job %s completed successfully", job_name)
            return True
        if "Failed" in finished:
            log.error("CGAZ job %s failed", job_name)
            return False
        if time.monotonic() > deadline:
            break
        time.sleep(30)

    # Don't leave it running unsupervised: a retried build would start
    # another CGAZ job alongside it.
    log.error("CGAZ job %s still running after %ds; deleting it", job_name, timeout)
    job.delete(propagation_policy="Background")
    return False


def read_cgaz_outputs(db_url):
    """The {"key", "size"} manifest the CGAZ merge Job recorded in PostGIS."""

    engine = create_engine(db_url)
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text("SELECT key, size FROM cgaz_outputs ORDER BY key")
            ).all()
    finally:
        engine.dispose()
    return [{"key": key, "size": size} for key, size in rows]


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
    s3_config = s3_config_from_env(version)
    if s3_config:
        log.info("S3 uploads enabled → s3://%s/%s/", s3_config["bucket"], version)
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

    # Before the long build, so a missing LSIB fails the run early.
    if s3_config:
        log.info("Staging LSIB base layer for CGAZ")
        stage_lsib(s3_config)

    # 2. Create ephemeral build database
    log.info("=== Step 2: Creating build database ===")
    db_url = create_build_db()

    try:
        # 3. Scale up Dask
        log.info("=== Step 3: Scaling Dask workers to %d ===", dask_workers)
        scale_dask_workers(dask_workers)

        try:
            # 4. Run boundary builds, with the per-country CGAZ work alongside
            log.info("=== Step 4: Running boundary builds + per-country CGAZ ===")
            successes, failures, cgaz_results = run_boundary_builds(
                scheduler,
                db_url,
                lsib_url(s3_config),
                s3_config,
                version=version,
                strict=strict,
            )

            if failures:
                log_failure_summary(failures, strict=strict)
                if strict:
                    sys.exit(1)
            if not check_cgaz_results(cgaz_results, strict):
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

        # 7. CGAZ merge
        log.info("=== Step 7: Running CGAZ merge job ===")
        cgaz_ok = run_cgaz_job(db_url, version, s3_config)
        if not cgaz_ok:
            log.error("CGAZ job failed")
            sys.exit(1)
        cgaz_objects = read_cgaz_outputs(db_url) if s3_config else []

        # 8. Verify uploads, then promote `current` pointer (release runs only)
        if promote:
            log.info("=== Step 8: Verifying release objects in S3 ===")
            verify_release_objects(
                successes, s3_config, index_objects, cgaz_objects=cgaz_objects
            )
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
