"""Kubernetes-aware build driver for the nightly pipeline.

Entrypoint: `python -m builder.run`

Orchestrates the full build:
  1. Pull latest changes from the data repo
  2. Create an ephemeral PostGIS database for build outputs
  3. Scale up Dask workers
  4. Discover boundaries and fan out builds across Dask
     (each worker pushes its output geometries to PostGIS)
  5. Scale down Dask workers
  6. Launch a single-pod CGAZ Job that reads from PostGIS
  7. Tear down the ephemeral database
  8. Exit 0 on success, 1 on failure
"""

import mimetypes
import boto3
import logging
import os
import subprocess
from kr8s.objects import Deployment, PersistentVolumeClaim, Service
import sys
from pathlib import Path
import time
from builder.paths import REPO_DIR
from builder.paths import SOURCE_DATA
from builder.paths import RELEASE_DATA
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
        subprocess.run(["git", "clone", "--depth", "1", remote, str(path)], check=True)

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


def upload_to_s3(product, iso, adm, s3_config):
    """Upload all build outputs for a boundary to S3-compatible storage.

    Keys mirror the release directory structure so the bucket can be
    served directly as a drop-in replacement for the file server:
        {product}/{ISO}/{ADM}/geoBoundaries-{ISO}-{ADM}.geojson
        {product}/{ISO}/{ADM}/geoBoundaries-{ISO}-{ADM}-metaData.json
        ...
    """

    output_dir = RELEASE_DATA / product / iso / adm
    if not output_dir.is_dir():
        log.warning("No output dir for %s/%s_%s, skipping S3 upload", product, iso, adm)
        return

    s3 = boto3.client(
        "s3",
        endpoint_url=s3_config["endpoint"],
        aws_access_key_id=s3_config["access_key_id"],
        aws_secret_access_key=s3_config["secret_access_key"],
    )
    bucket = s3_config["bucket"]
    prefix = s3_config.get("prefix", "")

    for file_path in output_dir.rglob("*"):
        if not file_path.is_file():
            continue
        key = str(file_path.relative_to(RELEASE_DATA))
        if prefix:
            key = f"{prefix}/{key}"
        content_type, _ = mimetypes.guess_type(str(file_path))
        extra_args = {}
        if content_type:
            extra_args["ContentType"] = content_type
        s3.upload_file(str(file_path), bucket, key, ExtraArgs=extra_args)

    log.info("Uploaded %s/%s_%s to s3://%s/", product, iso, adm, bucket)


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
    """

    iso_df = pd.read_csv(ISO_CSV)
    license_df = pd.read_csv(LICENSES_CSV)
    valid_isos = iso_df["Alpha-3code"].tolist()
    valid_licenses = license_df["license_name"].tolist()
    b = builder(iso, adm, product, valid_isos, valid_licenses)

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

    # Push the built boundary to PostGIS for downstream CGAZ consumption.
    try:
        geojson_path = (
            RELEASE_DATA / product / iso / adm / f"geoBoundaries-{iso}-{adm}.geojson"
        )
        if geojson_path.exists():
            gdf = gpd.read_file(geojson_path)
            gdf = gdf.to_crs(epsg=4326)
            gdf["product"] = product
            gdf["iso"] = iso
            gdf["adm_level"] = adm
            gdf = gdf.rename(columns={"geometry": "geom"}).set_geometry("geom")

            engine = create_engine(db_url)
            gdf.to_postgis(
                "boundaries",
                engine,
                if_exists="append",
                index=False,
            )
            engine.dispose()
    except Exception as e:
        log.warning("PostGIS write failed for %s/%s_%s: %s", product, iso, adm, e)

    # Upload all outputs to S3-compatible storage.
    if s3_config:
        try:
            upload_to_s3(product, iso, adm, s3_config)
        except Exception as e:
            log.warning("S3 upload failed for %s/%s_%s: %s", product, iso, adm, e)

    result["status"] = "ok"
    return result


def run_boundary_builds(scheduler_url, db_url, s3_config=None):
    """Connect to Dask, discover boundaries, fan out work. Returns (ok, fail) counts."""

    log.info("Connecting to Dask scheduler at %s", scheduler_url)
    client = Client(scheduler_url)
    log.info("Dashboard: %s", client.dashboard_link)

    boundaries = discover_boundaries()
    log.info("Discovered %d boundaries to build", len(boundaries))

    if not boundaries:
        log.info("Nothing to build.")
        return 0, 0

    futures = {
        client.submit(
            build_boundary,
            product,
            iso,
            adm,
            db_url,
            s3_config=s3_config,
            key=f"{product}-{iso}-{adm}",
        ): (product, iso, adm)
        for product, iso, adm in boundaries
    }

    succeeded, failed = 0, 0
    t0 = time.monotonic()

    for future, result in as_completed(futures, with_results=True):
        tag = f"{result['product']}/{result['iso']}_{result['adm']}"
        if result["status"] == "ok":
            succeeded += 1
            log.info("OK  %s", tag)
        else:
            failed += 1
            log.error(
                "FAIL %s stage=%s: %s",
                tag,
                result.get("failed_stage"),
                result.get("error"),
            )

    elapsed = time.monotonic() - t0
    log.info(
        "Boundary builds: %d succeeded, %d failed in %.0fs",
        succeeded,
        failed,
        elapsed,
    )
    return succeeded, failed


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

    # Build S3 config from env vars (None if not configured)
    s3_config = None
    s3_endpoint = os.environ.get("S3_ENDPOINT_URL")
    s3_bucket = os.environ.get("S3_BUCKET")
    if s3_endpoint and s3_bucket:
        s3_config = {
            "endpoint": s3_endpoint,
            "bucket": s3_bucket,
            "prefix": os.environ.get("S3_KEY_PREFIX", ""),
            "access_key_id": os.environ["AWS_ACCESS_KEY_ID"],
            "secret_access_key": os.environ["AWS_SECRET_ACCESS_KEY"],
        }
        log.info("S3 uploads enabled → s3://%s/", s3_bucket)
    else:
        log.info("S3 uploads disabled (S3_ENDPOINT_URL / S3_BUCKET not set)")

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
            succeeded, failed = run_boundary_builds(scheduler, db_url, s3_config)

            if failed:
                log.error("%d boundary builds failed", failed)
                sys.exit(1)
        finally:
            # 5. Scale down Dask (always, even on failure)
            log.info("=== Step 5: Scaling Dask workers to 0 ===")
            try:
                scale_dask_workers(0)
            except Exception:
                log.exception("Failed to scale down Dask workers")

        # 6. CGAZ
        log.info("=== Step 6: Running CGAZ job ===")
        cgaz_ok = run_cgaz_job(db_url)
        if not cgaz_ok:
            log.error("CGAZ job failed")
            sys.exit(1)
    finally:
        # 7. Tear down build database (always)
        log.info("=== Step 7: Tearing down build database ===")
        try:
            teardown_build_db()
        except Exception:
            log.exception("Failed to tear down build database")

    log.info("=== Build pipeline complete ===")


if __name__ == "__main__":
    main()
