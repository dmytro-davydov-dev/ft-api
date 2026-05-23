"""Unit tests for GET /sites/{id}/captures/{id} — status + tile URL.

Coverage target: api/drone/captures.py GET route
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

SITE_ID = "site-uuid-1"
CAP_ID = "cap-uuid-1"
BASE = f"/api/v1/drone/sites/{SITE_ID}/captures/{CAP_ID}"
_VALID_CLAIM = {"uid": "user-1", "customerId": "cust-abc"}
_SITE = {"id": SITE_ID, "customer_id": "cust-abc"}


def _cap(status: str, tiles_prefix=None, metadata=None):
    return {
        "id": CAP_ID, "site_id": SITE_ID, "customer_id": "cust-abc",
        "status": status, "captured_at": "2026-04-20T09:00:00Z",
        "photo_count": 312, "tiles_gcs_prefix": tiles_prefix, "metadata": metadata or {},
    }


def _auth_header():
    return {"Authorization": "Bearer token-user-1"}


def _make_db(cap):
    db = MagicMock()

    def _table(name):
        t = MagicMock()
        s = MagicMock()
        t.select.return_value = s
        s.eq.return_value = s
        if name == "sites":
            s.execute.return_value = MagicMock(data=[_SITE])
        elif name == "captures":
            s.execute.return_value = MagicMock(data=[cap])
        return t

    db.table.side_effect = _table
    return db


@pytest.fixture(autouse=True)
def _patch_auth(auth_mock):
    auth_mock.verify_id_token.side_effect = None
    auth_mock.verify_id_token.return_value = _VALID_CLAIM
    yield


def test_get_capture_returns_200(client):
    db = _make_db(_cap("processing"))
    with patch("api.drone.captures.get_supabase_client", return_value=db):
        resp = client.get(BASE, headers=_auth_header())
    assert resp.status_code == 200


def test_get_capture_response_shape(client):
    db = _make_db(_cap("processing"))
    with patch("api.drone.captures.get_supabase_client", return_value=db):
        data = client.get(BASE, headers=_auth_header()).get_json()
    required = {"capture_id", "site_id", "captured_at", "status", "photo_count", "tiles_url", "metadata"}
    assert required.issubset(data.keys())


def test_tiles_url_null_when_not_ready(client):
    for status in ("pending", "uploading", "processing", "tiling", "error"):
        db = _make_db(_cap(status))
        with patch("api.drone.captures.get_supabase_client", return_value=db):
            data = client.get(BASE, headers=_auth_header()).get_json()
        assert data["tiles_url"] is None, f"tiles_url should be null for status={status}"


def test_tiles_url_present_when_ready(client):
    db = _make_db(_cap("ready"))
    with patch("api.drone.captures.get_supabase_client", return_value=db), \
         patch("api.drone.captures.storage.tiles_url", return_value="https://storage.googleapis.com/bucket/captures/cap-uuid-1/tiles/"):
        data = client.get(BASE, headers=_auth_header()).get_json()
    assert data["tiles_url"] is not None
    assert "tiles" in data["tiles_url"]


def test_gsd_present_in_metadata(client):
    meta = {"gsd_cm": 2.4, "odm_version": "3.4.0"}
    db = _make_db(_cap("ready", metadata=meta))
    with patch("api.drone.captures.get_supabase_client", return_value=db), \
         patch("api.drone.captures.storage.tiles_url", return_value="https://tiles/"):
        data = client.get(BASE, headers=_auth_header()).get_json()
    assert data["metadata"]["gsd_cm"] == 2.4


def test_error_status_has_error_in_metadata(client):
    meta = {"error": "too_few_features", "detail": "Ensure 70%+ image overlap"}
    db = _make_db(_cap("error", metadata=meta))
    with patch("api.drone.captures.get_supabase_client", return_value=db):
        data = client.get(BASE, headers=_auth_header()).get_json()
    assert data["status"] == "error"
    assert "error" in data["metadata"]


def test_cross_tenant_capture_returns_404(client, auth_mock):
    auth_mock.verify_id_token.return_value = {"uid": "user-x", "customerId": "cust-other"}
    db = MagicMock()

    def _table(name):
        t = MagicMock()
        s = MagicMock()
        t.select.return_value = s
        s.eq.return_value = s
        s.execute.return_value = MagicMock(data=[])
        return t

    db.table.side_effect = _table
    with patch("api.drone.captures.get_supabase_client", return_value=db):
        resp = client.get(BASE, headers={"Authorization": "Bearer token-user-x"})
    assert resp.status_code == 404


def test_unauthenticated_returns_401(client, auth_mock):
    from firebase_admin.auth import InvalidIdTokenError  # noqa: PLC0415
    auth_mock.verify_id_token.side_effect = InvalidIdTokenError("bad")
    resp = client.get(BASE, headers={"Authorization": "Bearer bad"})
    assert resp.status_code == 401
