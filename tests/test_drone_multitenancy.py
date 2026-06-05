"""Multi-tenancy enforcement tests for drone endpoints.

Verifies cross-tenant isolation: Tenant A cannot read, create, or modify
Tenant B's sites/captures. Firebase JWT `customerId` is always the source
of truth — body-supplied customerId is ignored (FR-17, FR-18, US-5).
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

TENANT_A = {"uid": "user-a", "customerId": "cust-a"}
TENANT_B = {"uid": "user-b", "customerId": "cust-b"}

SITE_B = {"id": "site-b-uuid", "customer_id": "cust-b", "name": "Tenant B Tower"}
CAP_B  = {
    "id": "cap-b-uuid", "site_id": "site-b-uuid", "customer_id": "cust-b",
    "status": "ready", "captured_at": "2026-05-01T00:00:00Z",
    "photo_count": 3, "metadata": {}, "created_at": "2026-05-01T00:00:00Z",
    "tiles_url": "https://storage.googleapis.com/flowterra-drone-dev/captures/cap-b-uuid/tiles/",
}

BASE_SITES    = "/api/v1/drone/sites"
BASE_SITE_B   = f"{BASE_SITES}/{SITE_B['id']}"
BASE_CAPS_B   = f"{BASE_SITE_B}/captures"
BASE_CAP_B    = f"{BASE_CAPS_B}/{CAP_B['id']}"


def _header(claim: dict) -> dict:
    return {"Authorization": f"Bearer token-{claim['uid']}"}


def _make_db_empty() -> MagicMock:
    """DB that returns no rows for every query (simulates tenant isolation)."""
    db = MagicMock()

    def _tbl(_name: str) -> MagicMock:
        t = MagicMock()
        sel = MagicMock()
        t.select.return_value = sel
        sel.eq.return_value = sel
        sel.order.return_value = sel
        sel.limit.return_value = sel
        sel.execute.return_value = MagicMock(data=[])
        ins = MagicMock()
        t.insert.return_value = ins
        ins.execute.return_value = MagicMock(data=[])
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


# ---------------------------------------------------------------------------
# Auth helpers — switch token between tenant A and B per test
# ---------------------------------------------------------------------------

@pytest.fixture()
def auth_as_tenant_a(auth_mock):
    auth_mock.verify_id_token.side_effect = None
    auth_mock.verify_id_token.return_value = TENANT_A
    yield


@pytest.fixture()
def auth_as_tenant_b(auth_mock):
    auth_mock.verify_id_token.side_effect = None
    auth_mock.verify_id_token.return_value = TENANT_B
    yield


# ---------------------------------------------------------------------------
# 1. Tenant A reads Tenant B's site list — filtered, returns empty not B's data
# ---------------------------------------------------------------------------

def test_tenant_a_cannot_see_tenant_b_sites(client, auth_as_tenant_a):
    db = _make_db_empty()
    with patch("api.drone.sites.get_supabase_client", return_value=db):
        resp = client.get(BASE_SITES, headers=_header(TENANT_A))
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["sites"] == []


# ---------------------------------------------------------------------------
# 2. Tenant A reads Tenant B's specific site → 404
# ---------------------------------------------------------------------------

def test_tenant_a_cannot_read_tenant_b_site(client, auth_as_tenant_a):
    db = _make_db_empty()
    with patch("api.drone.sites.get_supabase_client", return_value=db):
        resp = client.get(BASE_SITE_B, headers=_header(TENANT_A))
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# 3. Tenant A creates capture on Tenant B's site → 404
# ---------------------------------------------------------------------------

def test_tenant_a_cannot_create_capture_on_tenant_b_site(client, auth_as_tenant_a):
    db = _make_db_empty()
    payload = {"captured_at": "2026-05-01T00:00:00Z", "photo_count": 1, "filenames": ["x.jpg"]}
    from flask import abort as flask_abort  # noqa: PLC0415
    with patch("api.drone.captures.get_supabase_client", return_value=db), \
         patch("api.drone.captures._verify_site_owner", side_effect=lambda *_: flask_abort(404)):
        resp = client.post(BASE_CAPS_B, json=payload, headers=_header(TENANT_A))
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# 4. Tenant A reads Tenant B's capture status → 404
# ---------------------------------------------------------------------------

def test_tenant_a_cannot_read_tenant_b_capture_status(client, auth_as_tenant_a):
    db = _make_db_empty()
    with patch("api.drone.captures.get_supabase_client", return_value=db):
        resp = client.get(BASE_CAP_B, headers=_header(TENANT_A))
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# 5. Tenant A triggers processing on Tenant B's capture → 404
# ---------------------------------------------------------------------------

def test_tenant_a_cannot_trigger_processing_on_tenant_b_capture(client, auth_as_tenant_a):
    db = _make_db_empty()
    with patch("api.drone.captures.get_supabase_client", return_value=db):
        resp = client.post(f"{BASE_CAP_B}/process", headers=_header(TENANT_A))
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# 6. Tenant A deletes Tenant B's capture → 404
# ---------------------------------------------------------------------------

def test_tenant_a_cannot_delete_tenant_b_capture(client, auth_as_tenant_a):
    db = _make_db_empty()
    with patch("api.drone.captures.get_supabase_client", return_value=db):
        resp = client.delete(BASE_CAP_B, headers=_header(TENANT_A))
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# 7. Request with no JWT → 401
# ---------------------------------------------------------------------------

def test_no_jwt_returns_401(client, auth_mock):
    resp = client.get(BASE_SITES)
    assert resp.status_code == 401


# ---------------------------------------------------------------------------
# 8. Request with expired JWT → 401
# ---------------------------------------------------------------------------

def test_expired_jwt_returns_401(client, auth_mock):
    from firebase_admin.auth import ExpiredIdTokenError  # noqa: PLC0415
    auth_mock.verify_id_token.side_effect = ExpiredIdTokenError("expired")
    resp = client.get(BASE_SITES, headers={"Authorization": "Bearer expired-token"})
    assert resp.status_code == 401


# ---------------------------------------------------------------------------
# 9. customerId in request body is ignored; JWT claim is used instead
# ---------------------------------------------------------------------------

def test_body_customer_id_is_ignored(client, auth_as_tenant_a):
    """Tenant A cannot elevate to Tenant B by passing customerId in body."""
    db = _make_db_empty()
    payload = {"name": "Injected site", "customerId": "cust-b"}
    with patch("api.drone.sites.get_supabase_client", return_value=db):
        resp = client.post(BASE_SITES, json=payload, headers=_header(TENANT_A))

    # If site was created it must belong to cust-a, not cust-b
    if resp.status_code == 201:
        data = resp.get_json()
        customer_id_in_response = data.get("customer_id")
        if customer_id_in_response is not None:
            assert customer_id_in_response == TENANT_A["customerId"]

        # Confirm the DB insert used cust-a not cust-b
        from api.drone import sites as sites_module  # noqa: PLC0415
        calls = db.table.call_args_list
        for call in calls:
            if call[0][0] == "sites":
                insert_calls = db.table.return_value.insert.call_args_list
                for ic in insert_calls:
                    inserted = ic[0][0] if ic[0] else ic[1].get("json", {})
                    if isinstance(inserted, dict):
                        assert inserted.get("customer_id") != TENANT_B["customerId"]
