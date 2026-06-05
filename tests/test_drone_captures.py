"""Unit tests for POST /sites/{id}/captures — capture record + upload URLs.

Coverage target: api/drone/captures.py ≥ 90%
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

SITE_ID = "site-uuid-1"
BASE = f"/api/v1/drone/sites/{SITE_ID}/captures"
_VALID_CLAIM = {"uid": "user-1", "customerId": "cust-abc"}
_SITE = {"id": SITE_ID, "customer_id": "cust-abc"}
_CAP = {
    "id": "cap-uuid-1", "site_id": SITE_ID, "customer_id": "cust-abc",
    "status": "pending", "captured_at": "2026-04-20T09:00:00Z",
    "photo_count": 2, "metadata": {}, "created_at": "2026-05-01T00:00:00Z",
}
_SIGNED_URLS = [{"filename": "A.JPG", "url": "https://signed/A"}, {"filename": "B.JPG", "url": "https://signed/B"}]


def _auth_header():
    return {"Authorization": "Bearer token-user-1"}


def _make_db(site_rows=None, cap_rows=None, insert_row=None):
    db = MagicMock()
    tbl = MagicMock()
    db.table.return_value = tbl

    sel = MagicMock()
    tbl.select.return_value = sel
    sel.eq.return_value = sel
    sel.execute.return_value = MagicMock(data=site_rows if site_rows is not None else [_SITE])

    ins = MagicMock()
    tbl.insert.return_value = ins
    ins.execute.return_value = MagicMock(data=[insert_row or _CAP])

    upd = MagicMock()
    tbl.update.return_value = upd
    upd.eq.return_value = upd
    upd.execute.return_value = MagicMock(data=[])

    return db


@pytest.fixture(autouse=True)
def _patch_auth(auth_mock):
    auth_mock.verify_id_token.side_effect = None
    auth_mock.verify_id_token.return_value = _VALID_CLAIM
    yield


_VALID_BODY = {"captured_at": "2026-04-20T09:00:00Z", "photo_count": 2, "filenames": ["A.JPG", "B.JPG"]}


def test_create_capture_returns_201(client):
    db = _make_db()
    with patch("api.drone.captures.get_supabase_client", return_value=db), \
         patch("api.drone.captures.storage.generate_upload_urls", return_value=_SIGNED_URLS):
        resp = client.post(BASE, json=_VALID_BODY, headers=_auth_header())
    assert resp.status_code == 201


def test_create_capture_response_shape(client):
    db = _make_db()
    with patch("api.drone.captures.get_supabase_client", return_value=db), \
         patch("api.drone.captures.storage.generate_upload_urls", return_value=_SIGNED_URLS):
        data = client.post(BASE, json=_VALID_BODY, headers=_auth_header()).get_json()
    assert data["capture_id"] == _CAP["id"]
    assert data["status"] == "pending"
    assert len(data["upload_urls"]) == 2
    assert data["upload_urls"][0]["filename"] == "A.JPG"


def test_create_capture_returns_one_url_per_filename(client):
    filenames = [f"img_{i}.JPG" for i in range(5)]
    signed = [{"filename": f, "url": f"https://signed/{f}"} for f in filenames]
    db = _make_db()
    cap = {**_CAP, "photo_count": 5}
    db.table.return_value.insert.return_value.execute.return_value = MagicMock(data=[cap])
    with patch("api.drone.captures.get_supabase_client", return_value=db), \
         patch("api.drone.captures.storage.generate_upload_urls", return_value=signed):
        resp = client.post(BASE, json={
            "captured_at": "2026-04-20T09:00:00Z", "photo_count": 5, "filenames": filenames,
        }, headers=_auth_header())
    assert resp.status_code == 201
    assert len(resp.get_json()["upload_urls"]) == 5


def test_create_capture_exceeds_max_photo_count(client):
    db = _make_db()
    with patch("api.drone.captures.get_supabase_client", return_value=db):
        resp = client.post(BASE, json={
            "captured_at": "2026-04-20T09:00:00Z",
            "photo_count": 501,
            "filenames": [f"f{i}.JPG" for i in range(501)],
        }, headers=_auth_header())
    assert resp.status_code == 422


def test_create_capture_filenames_mismatch_photo_count(client):
    db = _make_db()
    with patch("api.drone.captures.get_supabase_client", return_value=db):
        resp = client.post(BASE, json={
            "captured_at": "2026-04-20T09:00:00Z",
            "photo_count": 3,
            "filenames": ["A.JPG", "B.JPG"],
        }, headers=_auth_header())
    assert resp.status_code == 422


def test_create_capture_different_tenant_succeeds_isolated_by_customer_id(client, auth_mock):
    # Sites are authoritative in Firestore; Supabase isolation is via customer_id on captures.
    # A user from a different tenant can create a capture — it will be tagged with their customer_id.
    auth_mock.verify_id_token.return_value = {"uid": "user-x", "customerId": "cust-other"}
    cap_other = {**_CAP, "customer_id": "cust-other"}
    db = _make_db(insert_row=cap_other)
    with patch("api.drone.captures.get_supabase_client", return_value=db), \
         patch("api.drone.captures.storage.generate_upload_urls", return_value=_SIGNED_URLS):
        resp = client.post(BASE, json=_VALID_BODY, headers={"Authorization": "Bearer token-user-x"})
    assert resp.status_code == 201


def test_create_capture_missing_captured_at_returns_422(client):
    db = _make_db()
    with patch("api.drone.captures.get_supabase_client", return_value=db):
        resp = client.post(BASE, json={"photo_count": 2, "filenames": ["A.JPG", "B.JPG"]}, headers=_auth_header())
    assert resp.status_code == 422


def test_create_capture_unauthenticated_returns_401(client, auth_mock):
    from firebase_admin.auth import InvalidIdTokenError  # noqa: PLC0415
    auth_mock.verify_id_token.side_effect = InvalidIdTokenError("bad")
    resp = client.post(BASE, json=_VALID_BODY, headers={"Authorization": "Bearer bad"})
    assert resp.status_code == 401
