"""Unit tests for GET /api/v1/customers/{id}/geofences.

Exit criteria:
  1. Returns HTTP 200 with correctly shaped JSON.
  2. Response includes customerId and geofences list.
  3. Each geofence has: id, name, areaIds, rules, capacityThreshold.
  4. Each rule has: trigger, roles, notify.
  5. Mismatched tenant → 403.
  6. Unauthenticated → 401.
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

BASE_URL = "/api/v1/customers/cust-abc/geofences"
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
    """Make firebase_admin.auth.verify_id_token return controllable claims."""
    auth_mock.verify_id_token.side_effect = None
    auth_mock.verify_id_token.return_value = _VALID_CLAIM
    yield auth_mock


# ---------------------------------------------------------------------------
# Happy-path tests
# ---------------------------------------------------------------------------


def test_list_geofences_200(client):
    """Happy path: authenticated tenant gets geofences list."""
    resp = client.get(BASE_URL, headers=_auth_header(_VALID_CLAIM))

    assert resp.status_code == 200
    body = resp.get_json()
    assert body["customerId"] == "cust-abc"
    assert isinstance(body["geofences"], list)
    assert len(body["geofences"]) > 0


def test_geofence_shape(client):
    """Every geofence must have required fields in correct types."""
    resp = client.get(BASE_URL, headers=_auth_header(_VALID_CLAIM))
    geofences = resp.get_json()["geofences"]

    for geo in geofences:
        assert isinstance(geo["id"],       str),  f"id not str: {geo}"
        assert isinstance(geo["name"],     str),  f"name not str: {geo}"
        assert isinstance(geo["areaIds"],  list), f"areaIds not list: {geo}"
        assert isinstance(geo["rules"],    list), f"rules not list: {geo}"
        # capacityThreshold may be int or None
        assert geo["capacityThreshold"] is None or isinstance(geo["capacityThreshold"], int)


def test_rule_shape(client):
    """Every rule must have trigger, roles, notify."""
    resp = client.get(BASE_URL, headers=_auth_header(_VALID_CLAIM))
    geofences = resp.get_json()["geofences"]

    for geo in geofences:
        for rule in geo["rules"]:
            assert rule["trigger"] in ("enter", "exit"), f"bad trigger: {rule}"
            assert isinstance(rule["roles"],  list), f"roles not list: {rule}"
            assert isinstance(rule["notify"], list), f"notify not list: {rule}"


def test_pilot_geofences_names(client):
    """Pilot data must include the expected named geofences."""
    resp = client.get(BASE_URL, headers=_auth_header(_VALID_CLAIM))
    names = {g["name"] for g in resp.get_json()["geofences"]}

    assert "Server Room (Restricted)" in names
    assert "Reception Entry Zone" in names


# ---------------------------------------------------------------------------
# Auth / tenant guard tests
# ---------------------------------------------------------------------------


def test_wrong_tenant_403(client, auth_mock):
    """Token for cust-other must be rejected for cust-abc's geofences."""
    auth_mock.verify_id_token.return_value = {"uid": "user-2", "customerId": "cust-other"}
    resp = client.get(BASE_URL, headers=_auth_header({"uid": "user-2"}))
    assert resp.status_code == 403


def test_unauthenticated_401(client, auth_mock):
    """Missing token → 401."""
    from firebase_admin.auth import InvalidIdTokenError  # type: ignore[import]
    auth_mock.verify_id_token.side_effect = InvalidIdTokenError("bad token")
    resp = client.get(BASE_URL, headers={"Authorization": "Bearer bad"})
    assert resp.status_code == 401


def test_no_auth_header_401(client, auth_mock):
    """No Authorization header → 401."""
    from firebase_admin.auth import InvalidIdTokenError  # type: ignore[import]
    auth_mock.verify_id_token.side_effect = InvalidIdTokenError("missing")
    resp = client.get(BASE_URL)
    assert resp.status_code == 401
