"""Unit tests for api/drone/storage.py — GCS pre-signed URL generation.

Coverage target: ≥ 85%
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


def _make_gcs_mock():
    mock_client = MagicMock()
    mock_bucket = MagicMock()
    mock_blob = MagicMock()
    mock_client.bucket.return_value = mock_bucket
    mock_bucket.blob.return_value = mock_blob
    mock_blob.generate_signed_url.return_value = "https://signed-url/photo.JPG"
    return mock_client, mock_bucket, mock_blob


def test_generate_upload_urls_returns_one_per_filename():
    mock_client, _, _ = _make_gcs_mock()
    with patch("api.drone.storage.storage.Client", return_value=mock_client):
        from api.drone.storage import generate_upload_urls  # noqa: PLC0415
        urls = generate_upload_urls("cap-uuid", ["A.JPG", "B.JPG", "C.JPG"])
    assert len(urls) == 3


def test_generate_upload_urls_response_shape():
    mock_client, _, _ = _make_gcs_mock()
    with patch("api.drone.storage.storage.Client", return_value=mock_client):
        from api.drone.storage import generate_upload_urls  # noqa: PLC0415
        urls = generate_upload_urls("cap-uuid", ["A.JPG"])
    assert urls[0]["filename"] == "A.JPG"
    assert "url" in urls[0]


def test_generate_upload_urls_uses_correct_blob_path():
    mock_client, mock_bucket, mock_blob = _make_gcs_mock()
    with patch("api.drone.storage.storage.Client", return_value=mock_client):
        from api.drone.storage import generate_upload_urls  # noqa: PLC0415
        generate_upload_urls("cap-uuid-123", ["photo.JPG"])
    mock_bucket.blob.assert_called_with("captures/cap-uuid-123/photos/photo.JPG")


def test_generate_upload_urls_uses_put_method():
    mock_client, _, mock_blob = _make_gcs_mock()
    with patch("api.drone.storage.storage.Client", return_value=mock_client):
        from api.drone.storage import generate_upload_urls  # noqa: PLC0415
        generate_upload_urls("cap-uuid", ["a.JPG"])
    call_kwargs = mock_blob.generate_signed_url.call_args[1]
    assert call_kwargs.get("method") == "PUT"


def test_tiles_url_format():
    import os  # noqa: PLC0415
    os.environ["GCS_DRONE_BUCKET"] = "my-bucket"
    from api.drone.storage import tiles_url  # noqa: PLC0415
    url = tiles_url("cap-abc")
    assert "my-bucket" in url
    assert "cap-abc" in url
    assert url.endswith("tiles/")


def test_generate_upload_urls_empty_filenames():
    mock_client, _, _ = _make_gcs_mock()
    with patch("api.drone.storage.storage.Client", return_value=mock_client):
        from api.drone.storage import generate_upload_urls  # noqa: PLC0415
        urls = generate_upload_urls("cap-uuid", [])
    assert urls == []
