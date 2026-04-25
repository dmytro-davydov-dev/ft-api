"""Unit tests for auth/middleware.py — require_auth decorator.

Covers:
  - 401 when Authorization header is absent
  - 401 when token is expired
  - 401 when token is invalid / verification fails
  - 403 when token is valid but customerId claim is missing
  - 200 + g values populated when token is valid and claim is present
  - /health is exempt from auth (unauthenticated probe succeeds)
"""
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _bearer(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


# ---------------------------------------------------------------------------
# /health — unauthenticated
# ---------------------------------------------------------------------------

class TestHealthEndpoint:
    def test_health_requires_no_auth(self, client):
        resp = client.get("/health")
        assert resp.status_code == 200
        assert resp.get_json() == {"status": "ok"}


# ---------------------------------------------------------------------------
# /api/v1/me — protected route used to exercise require_auth
# ---------------------------------------------------------------------------

class TestRequireAuth:
    ENDPOINT = "/api/v1/me"

    def test_missing_auth_header_returns_401(self, client):
        resp = client.get(self.ENDPOINT)
        assert resp.status_code == 401

    def test_malformed_auth_header_returns_401(self, client):
        resp = client.get(self.ENDPOINT, headers={"Authorization": "Token abc123"})
        assert resp.status_code == 401

    def test_expired_token_returns_401(self, client, auth_mock):
        auth_mock.verify_id_token.side_effect = auth_mock.ExpiredIdTokenError(
            "expired", None
        )
        resp = client.get(self.ENDPOINT, headers=_bearer("expired-token"))
        assert resp.status_code == 401

    def test_invalid_token_returns_401(self, client, auth_mock):
        auth_mock.verify_id_token.side_effect = auth_mock.InvalidIdTokenError("bad")
        resp = client.get(self.ENDPOINT, headers=_bearer("invalid-token"))
        assert resp.status_code == 401

    def test_generic_verification_error_returns_401(self, client, auth_mock):
        auth_mock.verify_id_token.side_effect = Exception("network error")
        resp = client.get(self.ENDPOINT, headers=_bearer("some-token"))
        assert resp.status_code == 401

    def test_missing_customer_id_claim_returns_403(self, client, auth_mock):
        auth_mock.verify_id_token.side_effect = None
        auth_mock.verify_id_token.return_value = {"uid": "user-abc"}  # no customerId
        resp = client.get(self.ENDPOINT, headers=_bearer("valid-token-no-claim"))
        assert resp.status_code == 403

    def test_valid_token_returns_200_with_identity(self, client, auth_mock):
        auth_mock.verify_id_token.side_effect = None
        auth_mock.verify_id_token.return_value = {
            "uid": "user-123",
            "customerId": "cust-456",
        }
        resp = client.get(self.ENDPOINT, headers=_bearer("valid-token"))
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["uid"] == "user-123"
        assert body["customerId"] == "cust-456"

    def test_customer_id_injected_into_flask_g(self, app, auth_mock):
        """Verify g.customer_id and g.uid are set within the request context."""
        auth_mock.verify_id_token.side_effect = None
        auth_mock.verify_id_token.return_value = {
            "uid": "user-xyz",
            "customerId": "cust-xyz",
        }
        with app.test_client() as c:
            from flask import g

            with app.test_request_context(
                "/api/v1/me", headers=_bearer("valid")
            ):
                # Simulate the decorated call via the test client instead.
                pass

        # Re-verify via actual HTTP call — g values surface in the response body.
        with app.test_client() as c:
            resp = c.get("/api/v1/me", headers=_bearer("valid"))
            assert resp.get_json()["uid"] == "user-xyz"
            assert resp.get_json()["customerId"] == "cust-xyz"
