"""Unit tests for Supabase RLS tenant isolation — sites & captures tables.

These tests verify that the application layer enforces tenant isolation
(customer_id filtering). RLS policies in 001_drone_schema.sql enforce the
same at the DB layer; these tests cover the ft-api code path.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

BASE_SITES = "/api/v1/drone/sites"
_CUST_A = "cust-alpha"
_CUST_B = "cust-beta"
_VALID_CLAIM_A = {"uid": "user-a", "customerId": _CUST_A}
_VALID_CLAIM_B = {"uid": "user-b", "customerId": _CUST_B}

_SITE_ROW = {"id": "site-uuid-1", "customer_id": _CUST_A, "name": "Site A", "created_at": "2026-05-01T00:00:00Z"}
_SITE_ROW_B = {"id": "site-uuid-2", "customer_id": _CUST_B, "name": "Site B", "created_at": "2026-05-01T00:00:00Z"}


def _auth_header(claim: dict) -> dict:
    return {"Authorization": f"Bearer token-{claim['uid']}"}


def _make_db(sites_rows=None, captures_rows=None):
    db = MagicMock()

    def _tbl(name):
        t = MagicMock()
        sel = MagicMock()
        t.select.return_value = sel
        sel.eq.return_value = sel
        sel.order.return_value = sel
        if name == "sites":
            sel.execute.return_value = MagicMock(data=sites_rows if sites_rows is not None else [_SITE_ROW])
        else:
            sel.execute.return_value = MagicMock(data=captures_rows if captures_rows is not None else [])
        ins = MagicMock()
        t.insert.return_value = ins
        ins.execute.return_value = MagicMock(data=[_SITE_ROW])
        return t

    db.table.side_effect = _tbl
    return db


@pytest.fixture(autouse=True)
def _patch_auth(auth_mock):
    auth_mock.verify_id_token.side_effect = None
    auth_mock.verify_id_token.return_value = _VALID_CLAIM_A
    yield auth_mock


def test_tenant_a_cannot_see_tenant_b_sites(client, auth_mock):
    """GET /sites filters by customer_id from JWT — tenant B sites are invisible to tenant A."""
    db = _make_db(sites_rows=[_SITE_ROW])

    with patch("api.drone.sites.get_supabase_client", return_value=db):
        resp = client.get(BASE_SITES, headers=_auth_header(_VALID_CLAIM_A))

    assert resp.status_code == 200
    data = resp.get_json()
    site_ids = [s["site_id"] for s in data["sites"]]
    assert _SITE_ROW["id"] in site_ids
    assert _SITE_ROW_B["id"] not in site_ids


def test_tenant_b_cannot_access_tenant_a_site(client, auth_mock):
    """GET /sites/{id}/captures/{id} returns 404 when site belongs to another tenant."""
    auth_mock.verify_id_token.return_value = _VALID_CLAIM_B
    db = _make_db(sites_rows=[])  # site not found for tenant B

    with patch("api.drone.captures.get_supabase_client", return_value=db):
        resp = client.get(
            f"{BASE_SITES}/{_SITE_ROW['id']}/captures/cap-uuid",
            headers=_auth_header(_VALID_CLAIM_B),
        )
    assert resp.status_code == 404


def test_captures_inherit_customer_id(client, auth_mock):
    """POST /captures always stores the JWT customer_id, never body-supplied one."""
    cap_row = {
        "id": "cap-uuid-1", "site_id": _SITE_ROW["id"],
        "customer_id": _CUST_A, "status": "pending",
        "captured_at": "2026-05-01T09:00:00Z", "photo_count": 2,
        "metadata": {}, "created_at": "2026-05-01T00:00:00Z",
    }

    inserted = {}

    def _tbl(name):
        t = MagicMock()
        sel = MagicMock()
        t.select.return_value = sel
        sel.eq.return_value = sel
        sel.execute.return_value = MagicMock(data=[_SITE_ROW] if name == "sites" else [])
        ins = MagicMock()
        t.insert.return_value = ins
        def _ins_exec(row):
            inserted.update(row)
            return MagicMock(data=[cap_row])
        ins.execute.return_value = MagicMock(data=[cap_row])

        def _capture_insert(row):
            inserted.update(row)
            m = MagicMock()
            m.execute.return_value = MagicMock(data=[cap_row])
            return m
        t.insert.side_effect = _capture_insert
        return t

    db = MagicMock()
    db.table.side_effect = _tbl

    with patch("api.drone.captures.get_supabase_client", return_value=db), \
         patch("api.drone.captures.storage.generate_upload_urls", return_value=[
             {"filename": "a.JPG", "url": "https://signed/a"},
             {"filename": "b.JPG", "url": "https://signed/b"},
         ]):
        resp = client.post(
            f"{BASE_SITES}/{_SITE_ROW['id']}/captures",
            json={
                "captured_at": "2026-05-01T09:00:00Z",
                "photo_count": 2,
                "filenames": ["a.JPG", "b.JPG"],
                "customer_id": _CUST_B,  # should be ignored
            },
            headers=_auth_header(_VALID_CLAIM_A),
        )

    assert resp.status_code == 201
    assert inserted.get("customer_id") == _CUST_A
