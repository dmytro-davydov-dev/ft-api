"""Unit tests for drone site endpoints — POST /sites, GET /sites, DELETE /sites/{id}.

Coverage target: api/drone/sites.py ≥ 90%
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

BASE = "/api/v1/drone/sites"
_VALID_CLAIM = {"uid": "user-1", "customerId": "cust-abc"}
_SITE = {"id": "site-uuid", "customer_id": "cust-abc", "name": "Tower A", "created_at": "2026-05-01T00:00:00Z"}


def _auth_header(claim=_VALID_CLAIM):
    return {"Authorization": f"Bearer token-{claim['uid']}"}


def _make_db(*, insert_row=None, sites_rows=None, captures_rows=None):
    db = MagicMock()

    def _tbl(name):
        t = MagicMock()
        sel = MagicMock()
        t.select.return_value = sel
        sel.eq.return_value = sel
        sel.order.return_value = sel
        if name == "sites":
            sel.execute.return_value = MagicMock(data=sites_rows if sites_rows is not None else [_SITE])
        else:
            sel.execute.return_value = MagicMock(data=captures_rows if captures_rows is not None else [])
        ins = MagicMock()
        t.insert.return_value = ins
        ins.execute.return_value = MagicMock(data=[insert_row or _SITE])
        upd = MagicMock()
        t.update.return_value = upd
        upd.eq.return_value = upd
        upd.execute.return_value = MagicMock(data=[])
        dlt = MagicMock()
        t.delete.return_value = dlt
        dlt.eq.return_value = dlt
        dlt.execute.return_value = MagicMock(data=[])
        return t

    db.table.side_effect = _tbl
    return db


@pytest.fixture(autouse=True)
def _patch_auth(auth_mock):
    auth_mock.verify_id_token.side_effect = None
    auth_mock.verify_id_token.return_value = _VALID_CLAIM
    yield


# ---------------------------------------------------------------------------
# POST /sites
# ---------------------------------------------------------------------------


def test_create_site_returns_201(client):
    db = _make_db()
    with patch("api.drone.sites.get_supabase_client", return_value=db):
        resp = client.post(BASE, json={"name": "Tower A"}, headers=_auth_header())
    assert resp.status_code == 201


def test_create_site_response_shape(client):
    db = _make_db()
    with patch("api.drone.sites.get_supabase_client", return_value=db):
        data = client.post(BASE, json={"name": "Tower A"}, headers=_auth_header()).get_json()
    assert data["site_id"] == _SITE["id"]
    assert data["name"] == "Tower A"
    assert "capture_count" in data
    assert "created_at" in data


def test_create_site_missing_name_returns_422(client):
    db = _make_db()
    with patch("api.drone.sites.get_supabase_client", return_value=db):
        resp = client.post(BASE, json={}, headers=_auth_header())
    assert resp.status_code == 422


def test_create_site_empty_name_returns_422(client):
    db = _make_db()
    with patch("api.drone.sites.get_supabase_client", return_value=db):
        resp = client.post(BASE, json={"name": "  "}, headers=_auth_header())
    assert resp.status_code == 422


def test_create_site_unauthenticated_returns_401(client, auth_mock):
    from firebase_admin.auth import InvalidIdTokenError  # noqa: PLC0415
    auth_mock.verify_id_token.side_effect = InvalidIdTokenError("bad")
    resp = client.post(BASE, json={"name": "X"}, headers={"Authorization": "Bearer bad"})
    assert resp.status_code == 401


# ---------------------------------------------------------------------------
# GET /sites
# ---------------------------------------------------------------------------


def test_list_sites_returns_200(client):
    db = _make_db(sites_rows=[_SITE])
    with patch("api.drone.sites.get_supabase_client", return_value=db):
        resp = client.get(BASE, headers=_auth_header())
    assert resp.status_code == 200


def test_list_sites_response_has_sites_key(client):
    db = _make_db(sites_rows=[_SITE])
    with patch("api.drone.sites.get_supabase_client", return_value=db):
        data = client.get(BASE, headers=_auth_header()).get_json()
    assert "sites" in data
    assert isinstance(data["sites"], list)


def test_list_sites_empty_when_no_sites(client):
    db = _make_db(sites_rows=[])
    with patch("api.drone.sites.get_supabase_client", return_value=db):
        data = client.get(BASE, headers=_auth_header()).get_json()
    assert data["sites"] == []


def test_list_sites_site_shape(client):
    db = _make_db(sites_rows=[_SITE])
    with patch("api.drone.sites.get_supabase_client", return_value=db):
        data = client.get(BASE, headers=_auth_header()).get_json()
    site = data["sites"][0]
    assert {"site_id", "name", "capture_count", "last_capture_at"}.issubset(site.keys())


# ---------------------------------------------------------------------------
# DELETE /sites/<id>
# ---------------------------------------------------------------------------


def test_delete_site_returns_200(client):
    db = _make_db(sites_rows=[_SITE])
    with patch("api.drone.sites.get_supabase_client", return_value=db):
        resp = client.delete(f"{BASE}/{_SITE['id']}", headers=_auth_header())
    assert resp.status_code == 200


def test_delete_site_with_captures_returns_409(client):
    db = _make_db(captures_rows=[{"id": "cap-uuid"}])
    with patch("api.drone.sites.get_supabase_client", return_value=db):
        resp = client.delete(f"{BASE}/{_SITE['id']}", headers=_auth_header())
    assert resp.status_code == 409


def test_delete_nonexistent_site_returns_404(client):
    db = _make_db(sites_rows=[])
    with patch("api.drone.sites.get_supabase_client", return_value=db):
        resp = client.delete(f"{BASE}/no-such-uuid", headers=_auth_header())
    assert resp.status_code == 404
