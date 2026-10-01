"""S3-compatible storage helpers shared by the build driver, the Dask
workers and the CGAZ merge Job."""

import logging
import mimetypes
import os

import boto3
from botocore.config import Config as BotoConfig

log = logging.getLogger(__name__)


def s3_config_from_env(version):
    """Build the S3 config dict from env vars, or None if S3 isn't configured.

    The release version becomes the top-level key prefix so versions
    coexist in the bucket: v6/gbOpen/KEN/ADM1/..., nightly/gbOpen/...
    """

    endpoint = os.environ.get("S3_ENDPOINT_URL")
    bucket = os.environ.get("S3_BUCKET")
    if not (endpoint and bucket):
        return None
    return {
        "endpoint": endpoint,
        "bucket": bucket,
        "prefix": version,
        "access_key_id": os.environ["AWS_ACCESS_KEY_ID"],
        "secret_access_key": os.environ["AWS_SECRET_ACCESS_KEY"],
    }


def s3_client(s3_config):
    """Build a boto3 S3 client with standard retries for transient errors."""

    return boto3.client(
        "s3",
        endpoint_url=s3_config["endpoint"],
        aws_access_key_id=s3_config["access_key_id"],
        aws_secret_access_key=s3_config["secret_access_key"],
        # R2 only accepts SigV4. Requests default to it anyway, but presigned
        # URLs fall back to legacy SigV2 unless it's set explicitly.
        region_name="auto",
        config=BotoConfig(
            signature_version="s3v4",
            retries={"max_attempts": 5, "mode": "standard"},
        ),
    )


def upload_to_s3(output_dir, key_prefix, s3_config):
    """Upload every file under `output_dir` to S3-compatible storage.

    Keys mirror the release directory structure so the bucket can be
    served directly as a drop-in replacement for the file server:
        {product}/{ISO}/{ADM}/geoBoundaries-{ISO}-{ADM}.geojson
        {product}/{ISO}/{ADM}/geoBoundaries-{ISO}-{ADM}-metaData.json
        CGAZ/geoBoundariesCGAZ_ADM1.geojson
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

    s3 = s3_client(s3_config)
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
