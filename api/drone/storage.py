"""GCS storage helper — generates pre-signed PUT URLs for photo uploads.

Signed URL TTL: 15 minutes (FR-06).
Bucket name is read from GCS_DRONE_BUCKET env var.
"""
import datetime
import os

from google.cloud import storage


def generate_upload_urls(capture_id: str, filenames: list[str]) -> list[dict]:
    """Return one signed PUT URL per filename.

    Args:
        capture_id: UUID of the capture row.
        filenames:  List of original photo filenames (e.g. DJI_0001.JPG).

    Returns:
        List of {"filename": str, "url": str} dicts.
    """
    bucket_name = os.environ["GCS_DRONE_BUCKET"]
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    ttl = datetime.timedelta(minutes=15)
    urls = []
    for name in filenames:
        blob = bucket.blob(f"captures/{capture_id}/photos/{name}")
        url = blob.generate_signed_url(
            version="v4",
            expiration=ttl,
            method="PUT",
            content_type="image/jpeg",
        )
        urls.append({"filename": name, "url": url})
    return urls


def tiles_url(capture_id: str) -> str:
    """Return the public GCS URL prefix for Potree tiles."""
    bucket_name = os.environ["GCS_DRONE_BUCKET"]
    return f"https://storage.googleapis.com/{bucket_name}/captures/{capture_id}/tiles/"
