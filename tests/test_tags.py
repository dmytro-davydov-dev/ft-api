"""Unit tests for GET /api/v1/customers/{id}/tags.

Exit criteria:
  1. Returns HTTP 200 with correctly shaped JSON.
  2. Response includes customerId and tags list.
  3. Each tag has: id, label, type, batteryPct, lastSeen, zoneId, floor, status.
  4. Status is one of: active, inactive, low_battery.
  5. Mismatched tenant → 403.
  6. Unauthenticated → 401.
"""
from __future__ import annotations

import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

BASE_URL = "/api/v1/customers/cust-abc/tags"
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


def test_response_has_tags_list(client):
    data = client.get(BASE_URL, headers=_auth_header(_VALID_CLAIM)).get_json()
    assert isinstance(data["tags"], list)
    assert len(data["tags"]) > 0


def test_tag_shape(client):
    data = client.get(BASE_URL, headers=_auth_header(_VALID_CLAIM)).get_json()
    required = {"id", "label", "type", "batteryPct", "lastSeen", "zoneId", "floor", "status"}
    for tag in data["tags"]:
        assert required.issubset(tag.keys()), f"Missing keys in tag {tag.get('id')}"


def test_status_values(client):
    data = client.get(BASE_URL, headers=_auth_header(_VALID_CLAIM)).get_json()
    valid_statuses = {"active", "inactive", "low_battery"}
    for tag in data["tags"]:
        assert tag["status"] in valid_statuses, f"Unexpected status: {tag['status']}"


def test_has_low_battery_tag(client):
    """Pilot set must include at least one low_battery tag."""
    data = client.get(BASE_URL, headers=_auth_header(_VALID_CLAIM)).get_json()
    statuses = {t["status"] for t in data["tags"]}
    assert "low_battery" in statuses


def test_has_inactive_tag(client):
    """Pilot set must include at least one inactive tag."""
    data = client.get(BASE_URL, headers=_auth_header(_VALID_CLAIM)).get_json()
    statuses = {t["status"] for t in data["tags"]}
    assert "inactive" in statuses


# ---------------------------------------------------------------------------
# Auth / tenant guard tests
# ---------------------------------------------------------------------------


def test_forbidden_for_wrong_tenant(client, auth_mock):
    """Token for cust-other must be rejected for cust-abc's tags."""
    auth_mock.verify_id_token.return_value = {"uid": "user-2", "customerId": "cust-other"}
    resp = client.get(BASE_URL, headers=_auth_header({"uid": "user-2"}))
    assert resp.status_code == 403


def test_unauthenticated_returns_401(client, auth_mock):
    from firebase_admin.auth import InvalidIdTokenError  # noqa: PLC0415
    auth_mock.verify_id_token.side_effect = InvalidIdTokenError("bad token")
    resp = client.get(BASE_URL, headers={"Authorization": "Bearer bad"})
    assert resp.status_code == 401
