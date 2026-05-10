"""Unit tests for GET/POST /api/v1/customers/{id}/people.

Exit criteria:
  GET
  1. Returns HTTP 200 with correctly shaped JSON.
  2. Response includes customerId and people list.
  3. Each person has all required fields.
  4. Mismatched tenant → 403.
  5. Unauthenticated → 401.

  POST
  6. Returns HTTP 201 with the new person including a generated id.
  7. Missing required fields → 400.
  8. Mismatched tenant → 403.
"""
from __future__ import annotations

import pytest

BASE_URL = "/api/v1/customers/cust-abc/people"
_VALID_CLAIM = {"uid": "user-1", "customerId": "cust-abc"}

_REQUIRED_PERSON_FIELDS = {
    "id", "firstName", "lastName", "email", "phone",
    "company", "role", "nationality", "tagId", "pictureUrl",
    "supervisor", "emergencyContact",
}

_VALID_BODY = {
    "firstName":        "Test",
    "lastName":         "User",
    "email":            "test.user@example.com",
    "phone":            "+44 7700 900000",
    "company":          "Test Co",
    "role":             "Tester",
    "nationality":      "British",
    "supervisor":       "Test Boss",
    "emergencyContact": "+44 7700 900001",
}


def _auth_header(claim: dict) -> dict[str, str]:
    return {"Authorization": f"Bearer token-{claim.get('uid', 'anon')}"}


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _patch_auth(auth_mock):
    auth_mock.verify_id_token.side_effect = None
    auth_mock.verify_id_token.return_value = _VALID_CLAIM
    yield auth_mock


# ---------------------------------------------------------------------------
# GET tests
# ---------------------------------------------------------------------------


def test_get_returns_200(client):
    resp = client.get(BASE_URL, headers=_auth_header(_VALID_CLAIM))
    assert resp.status_code == 200


def test_get_response_has_customer_id(client):
    data = client.get(BASE_URL, headers=_auth_header(_VALID_CLAIM)).get_json()
    assert data["customerId"] == "cust-abc"


def test_get_response_has_people_list(client):
    data = client.get(BASE_URL, headers=_auth_header(_VALID_CLAIM)).get_json()
    assert isinstance(data["people"], list)
    assert len(data["people"]) > 0


def test_get_person_shape(client):
    data = client.get(BASE_URL, headers=_auth_header(_VALID_CLAIM)).get_json()
    for person in data["people"]:
        assert _REQUIRED_PERSON_FIELDS.issubset(person.keys()), (
            f"Missing keys in person {person.get('id')}"
        )


def test_get_forbidden_for_wrong_tenant(client, auth_mock):
    auth_mock.verify_id_token.return_value = {"uid": "user-2", "customerId": "cust-other"}
    resp = client.get(BASE_URL, headers=_auth_header({"uid": "user-2"}))
    assert resp.status_code == 403


def test_get_unauthenticated_returns_401(client, auth_mock):
    from firebase_admin.auth import InvalidIdTokenError  # noqa: PLC0415
    auth_mock.verify_id_token.side_effect = InvalidIdTokenError("bad token")
    resp = client.get(BASE_URL, headers={"Authorization": "Bearer bad"})
    assert resp.status_code == 401


# ---------------------------------------------------------------------------
# POST tests
# ---------------------------------------------------------------------------


def test_post_returns_201(client):
    resp = client.post(BASE_URL, json=_VALID_BODY, headers=_auth_header(_VALID_CLAIM))
    assert resp.status_code == 201


def test_post_response_has_generated_id(client):
    data = client.post(BASE_URL, json=_VALID_BODY, headers=_auth_header(_VALID_CLAIM)).get_json()
    assert "id" in data
    assert data["id"].startswith("person-")


def test_post_response_shape(client):
    data = client.post(BASE_URL, json=_VALID_BODY, headers=_auth_header(_VALID_CLAIM)).get_json()
    assert _REQUIRED_PERSON_FIELDS.issubset(data.keys())


def test_post_optional_fields_default_to_none(client):
    data = client.post(BASE_URL, json=_VALID_BODY, headers=_auth_header(_VALID_CLAIM)).get_json()
    assert data["tagId"] is None
    assert data["pictureUrl"] is None


def test_post_missing_required_field_returns_400(client):
    body = {k: v for k, v in _VALID_BODY.items() if k != "email"}
    resp = client.post(BASE_URL, json=body, headers=_auth_header(_VALID_CLAIM))
    assert resp.status_code == 400


def test_post_forbidden_for_wrong_tenant(client, auth_mock):
    auth_mock.verify_id_token.return_value = {"uid": "user-2", "customerId": "cust-other"}
    resp = client.post(BASE_URL, json=_VALID_BODY, headers=_auth_header({"uid": "user-2"}))
    assert resp.status_code == 403
