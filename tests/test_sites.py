"""Unit tests for GET /api/v1/customers/{id}/sites.

Exit criteria:
  1. Returns HTTP 200 with customerId and sites list.
  2. Static pilot site fields (id, name, floorplan, floors) are always present.
  3. sitePhotos is merged from Firestore when the document exists.
  4. sitePhotos is absent when no Firestore document exists.
  5. Firestore failure is swallowed — static config is still returned.
  6. Mismatched tenant → 403.
  7. Unauthenticated → 401.
"""
from __future__ import annotations

import sys
from unittest.mock import MagicMock, patch

import pytest

BASE_URL = "/api/v1/customers/cust-abc/sites"
_VALID_CLAIM = {"uid": "user-1", "customerId": "cust-abc"}


def _auth_header(claim: dict) -> dict[str, str]:
    return {"Authorization": f"Bearer token-{claim.get('uid', 'anon')}"}


@pytest.fixture(autouse=True)
def _patch_auth(auth_mock):
    auth_mock.verify_id_token.side_effect = None
    auth_mock.verify_id_token.return_value = _VALID_CLAIM
    yield auth_mock


def _firestore_doc(*, exists: bool, data: dict | None = None):
    """Build a minimal Firestore document stub."""
    doc = MagicMock()
    doc.exists = exists
    doc.to_dict.return_value = data or {}
    return doc


# ---------------------------------------------------------------------------
# Happy-path — no Firestore document
# ---------------------------------------------------------------------------

def test_returns_200_no_firestore_doc(client):
    doc = _firestore_doc(exists=False)
    with patch("routes.v1.sites.fs") as mock_fs:
        mock_fs.client.return_value.document.return_value.get.return_value = doc
        resp = client.get(BASE_URL, headers=_auth_header(_VALID_CLAIM))
    assert resp.status_code == 200


def test_response_shape_no_firestore_doc(client):
    doc = _firestore_doc(exists=False)
    with patch("routes.v1.sites.fs") as mock_fs:
        mock_fs.client.return_value.document.return_value.get.return_value = doc
        data = client.get(BASE_URL, headers=_auth_header(_VALID_CLAIM)).get_json()
    assert data["customerId"] == "cust-abc"
    assert len(data["sites"]) == 1


def test_pilot_site_static_fields(client):
    doc = _firestore_doc(exists=False)
    with patch("routes.v1.sites.fs") as mock_fs:
        mock_fs.client.return_value.document.return_value.get.return_value = doc
        site = client.get(BASE_URL, headers=_auth_header(_VALID_CLAIM)).get_json()["sites"][0]
    assert site["id"] == "site-hq-pilot"
    assert site["name"] == "HQ Pilot Office"
    assert "floorplan" in site
    assert len(site["floors"]) == 2


def test_no_site_photos_when_doc_missing(client):
    doc = _firestore_doc(exists=False)
    with patch("routes.v1.sites.fs") as mock_fs:
        mock_fs.client.return_value.document.return_value.get.return_value = doc
        site = client.get(BASE_URL, headers=_auth_header(_VALID_CLAIM)).get_json()["sites"][0]
    assert "sitePhotos" not in site


# ---------------------------------------------------------------------------
# Happy-path — Firestore document with sitePhotos
# ---------------------------------------------------------------------------

_SAMPLE_PHOTOS = [
    {
        "url": "https://storage.example.com/photo1.png",
        "storagePath": "sites/site-hq-pilot/site-photos/abc-photo1.png",
        "takenAt": "2026-05-19",
        "comment": "Lobby",
        "uploadedAt": "2026-05-19T19:23:24.722Z",
        "filename": "photo1.png",
        "sizeBytes": 1048576,
    }
]


def test_site_photos_merged_from_firestore(client):
    doc = _firestore_doc(exists=True, data={"sitePhotos": _SAMPLE_PHOTOS})
    with patch("routes.v1.sites.fs") as mock_fs:
        mock_fs.client.return_value.document.return_value.get.return_value = doc
        site = client.get(BASE_URL, headers=_auth_header(_VALID_CLAIM)).get_json()["sites"][0]
    assert site["sitePhotos"] == _SAMPLE_PHOTOS


def test_static_fields_preserved_when_photos_merged(client):
    doc = _firestore_doc(exists=True, data={"sitePhotos": _SAMPLE_PHOTOS})
    with patch("routes.v1.sites.fs") as mock_fs:
        mock_fs.client.return_value.document.return_value.get.return_value = doc
        site = client.get(BASE_URL, headers=_auth_header(_VALID_CLAIM)).get_json()["sites"][0]
    assert site["id"] == "site-hq-pilot"
    assert "floorplan" in site
    assert len(site["floors"]) == 2


def test_no_site_photos_key_when_firestore_doc_has_none(client):
    """Firestore doc exists but has no sitePhotos field — key should be absent."""
    doc = _firestore_doc(exists=True, data={"someOtherField": "value"})
    with patch("routes.v1.sites.fs") as mock_fs:
        mock_fs.client.return_value.document.return_value.get.return_value = doc
        site = client.get(BASE_URL, headers=_auth_header(_VALID_CLAIM)).get_json()["sites"][0]
    assert "sitePhotos" not in site


# ---------------------------------------------------------------------------
# Resilience — Firestore failure
# ---------------------------------------------------------------------------

def test_firestore_error_returns_static_config(client):
    with patch("routes.v1.sites.fs") as mock_fs:
        mock_fs.client.side_effect = Exception("Firestore unavailable")
        resp = client.get(BASE_URL, headers=_auth_header(_VALID_CLAIM))
    assert resp.status_code == 200
    site = resp.get_json()["sites"][0]
    assert site["id"] == "site-hq-pilot"
    assert "sitePhotos" not in site


# ---------------------------------------------------------------------------
# Auth guards
# ---------------------------------------------------------------------------

def test_wrong_tenant_returns_403(client, auth_mock):
    wrong_claim = {"uid": "user-2", "customerId": "cust-xyz"}
    auth_mock.verify_id_token.return_value = wrong_claim
    with patch("routes.v1.sites.fs"):
        resp = client.get(
            "/api/v1/customers/cust-abc/sites",
            headers=_auth_header(wrong_claim),
        )
    assert resp.status_code == 403


def test_unauthenticated_returns_401(client):
    resp = client.get(BASE_URL)
    assert resp.status_code == 401
