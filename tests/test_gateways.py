"""Unit tests for GET /api/v1/customers/{id}/gateways.

Exit criteria:
  1. Returns HTTP 200 with correctly shaped JSON.
  2. Response includes customerId and gateways list.
  3. Each gateway has: id, label, model, siteId, floor, zoneId, ipAddress,
     status, lastHeartbeat, tagCount.
  4. Status is one of: online, offline, degraded.
  5. Pilot roster has 30 gateways (20 floor 1, 10 floor 2).
  6. Pilot roster includes at least one offline and one degraded gateway.
  7. Mismatched tenant → 403.
  8. Unauthenticated → 401.
"""
from __future__ import annotations

import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

BASE_URL = "/api/v1/customers/cust-abc/gateways"
_VALID_CLAIM = {"uid": "user-1", "customerId": "cust-abc"}


def _make_token(claim: dict) -> str:
    return "token-" + claim.get("uid", "anon")


def _auth_header(claim: dict) -> dict[str, str]:
    return {"Authorization": f"Bearer {_make_token(claim)}"}


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _patch_auth(auth_mock):
    auth_mock.verify_id_token.side_effect = None
    auth_mock.verify_id_token.return_value = _VALID_CLAIM
    yield auth_mock


# ---------------------------------------------------------------------------
# Happy-path tests
# ---------------------------------------------------------------------------


def test_returns_200(client):
    resp = client.get(BASE_URL, headers=_auth_header(_VALID_CLAIM))
    assert resp.status_code == 200


def test_response_has_customer_id(client):
    data = client.get(BASE_URL, headers=_auth_header(_VALID_CLAIM)).get_json()
    assert data["customerId"] == "cust-abc"


def test_response_has_gateways_list(client):
    data = client.get(BASE_URL, headers=_auth_header(_VALID_CLAIM)).get_json()
    assert isinstance(data["gateways"], list)
    assert len(data["gateways"]) > 0


def test_gateway_shape(client):
    data = client.get(BASE_URL, headers=_auth_header(_VALID_CLAIM)).get_json()
    required = {
        "id", "label", "model", "siteId", "floor",
        "zoneId", "ipAddress", "status", "lastHeartbeat", "tagCount",
    }
    for gw in data["gateways"]:
        assert required.issubset(gw.keys()), f"Missing keys in gateway {gw.get('id')}"


def test_status_values(client):
    data = client.get(BASE_URL, headers=_auth_header(_VALID_CLAIM)).get_json()
    valid_statuses = {"online", "offline", "degraded"}
    for gw in data["gateways"]:
        assert gw["status"] in valid_statuses, f"Unexpected status: {gw['status']}"


def test_pilot_has_30_gateways(client):
    data = client.get(BASE_URL, headers=_auth_header(_VALID_CLAIM)).get_json()
    assert len(data["gateways"]) == 30


def test_pilot_floor_counts(client):
    data = client.get(BASE_URL, headers=_auth_header(_VALID_CLAIM)).get_json()
    gws = data["gateways"]
    assert sum(1 for g in gws if g["floor"] == 1) == 20
    assert sum(1 for g in gws if g["floor"] == 2) == 10


def test_has_offline_gateway(client):
    data = client.get(BASE_URL, headers=_auth_header(_VALID_CLAIM)).get_json()
    statuses = {g["status"] for g in data["gateways"]}
    assert "offline" in statuses


def test_has_degraded_gateway(client):
    data = client.get(BASE_URL, headers=_auth_header(_VALID_CLAIM)).get_json()
    statuses = {g["status"] for g in data["gateways"]}
    assert "degraded" in statuses


def test_tag_count_is_non_negative_int(client):
    data = client.get(BASE_URL, headers=_auth_header(_VALID_CLAIM)).get_json()
    for gw in data["gateways"]:
        assert isinstance(gw["tagCount"], int) and gw["tagCount"] >= 0


# ---------------------------------------------------------------------------
# Auth / tenant guard tests
# ---------------------------------------------------------------------------


def test_forbidden_for_wrong_tenant(client, auth_mock):
    auth_mock.verify_id_token.return_value = {"uid": "user-2", "customerId": "cust-other"}
    resp = client.get(BASE_URL, headers=_auth_header({"uid": "user-2"}))
    assert resp.status_code == 403


def test_unauthenticated_returns_401(client, auth_mock):
    from firebase_admin.auth import InvalidIdTokenError  # noqa: PLC0415
    auth_mock.verify_id_token.side_effect = InvalidIdTokenError("bad token")
    resp = client.get(BASE_URL, headers={"Authorization": "Bearer bad"})
    assert resp.status_code == 401
