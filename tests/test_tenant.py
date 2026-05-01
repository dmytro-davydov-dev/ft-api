"""Integration tests for auth/tenant.py — require_tenant middleware.

Exit criteria (from FLO-28):
  ✓ A request with JWT for ``tenant-abc`` cannot retrieve rows belonging
    to ``tenant-xyz`` — confirmed by tenant-mismatch 403.
  ✓ Middleware rejects requests with mismatched URL {id} vs token customerId.

Additional coverage:
  - Missing Authorization header → 401
  - Expired token → 401
  - Invalid token → 401
  - Generic verification error → 401
  - Valid JWT but missing customerId claim → 403
  - Matching tenant → 200 + g.bq_customer_id injected
  - Route without URL {id} segment → auth still enforced, no mismatch check
"""
import sys
from unittest.mock import MagicMock

import pytest
from flask import Blueprint, Flask, g, jsonify


# ---------------------------------------------------------------------------
# Reuse the firebase_admin stubs already installed by conftest.py
# (sys.modules["firebase_admin.auth"] is _auth_mock at this point).
# ---------------------------------------------------------------------------

def _bearer(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


# ---------------------------------------------------------------------------
# Test app factory — registers a synthetic report blueprint that uses
# require_tenant so we exercise the middleware in isolation.
# ---------------------------------------------------------------------------

def _make_tenant_app():
    """Create a minimal Flask app with a report blueprint protected by require_tenant."""
    # Import here so sys.modules patches from conftest are already in place.
    import importlib

    for mod in list(sys.modules):
        if mod.startswith("routes") or mod in ("app", "auth.tenant"):
            del sys.modules[mod]

    from auth.tenant import require_tenant

    report_bp = Blueprint("report", __name__)
    report_bp.before_request(require_tenant("id"))

    @report_bp.get("/api/v1/customers/<id>/reports")
    def list_reports(id: str):  # noqa: A002
        return jsonify(
            {
                "customerId": g.customer_id,
                "bq_customer_id": g.bq_customer_id,
                "uid": g.uid,
            }
        )

    # A route without an {id} URL segment — auth still runs, mismatch check skipped.
    no_id_bp = Blueprint("no_id", __name__)
    no_id_bp.before_request(require_tenant("id"))

    @no_id_bp.get("/api/v1/reports/global")
    def global_reports():
        return jsonify({"customerId": g.customer_id})

    app = Flask(__name__)
    app.config["TESTING"] = True
    app.register_blueprint(report_bp)
    app.register_blueprint(no_id_bp)
    return app


@pytest.fixture()
def tenant_client():
    return _make_tenant_app().test_client()


@pytest.fixture()
def tenant_auth_mock():
    return sys.modules["firebase_admin.auth"]


REPORT_URL = "/api/v1/customers/tenant-abc/reports"
GLOBAL_URL = "/api/v1/reports/global"


# ---------------------------------------------------------------------------
# Auth failures — middleware must reject before any tenant check.
# ---------------------------------------------------------------------------

class TestTenantAuthRejections:
    def test_missing_auth_header_returns_401(self, tenant_client):
        resp = tenant_client.get(REPORT_URL)
        assert resp.status_code == 401

    def test_malformed_bearer_returns_401(self, tenant_client):
        resp = tenant_client.get(REPORT_URL, headers={"Authorization": "Token xyz"})
        assert resp.status_code == 401

    def test_expired_token_returns_401(self, tenant_client, tenant_auth_mock):
        tenant_auth_mock.verify_id_token.side_effect = (
            tenant_auth_mock.ExpiredIdTokenError("exp", None)
        )
        resp = tenant_client.get(REPORT_URL, headers=_bearer("expired"))
        assert resp.status_code == 401

    def test_invalid_token_returns_401(self, tenant_client, tenant_auth_mock):
        tenant_auth_mock.verify_id_token.side_effect = (
            tenant_auth_mock.InvalidIdTokenError("bad")
        )
        resp = tenant_client.get(REPORT_URL, headers=_bearer("bad-token"))
        assert resp.status_code == 401

    def test_generic_verification_error_returns_401(self, tenant_client, tenant_auth_mock):
        tenant_auth_mock.verify_id_token.side_effect = RuntimeError("network")
        resp = tenant_client.get(REPORT_URL, headers=_bearer("some-token"))
        assert resp.status_code == 401

    def test_missing_customer_id_claim_returns_403(self, tenant_client, tenant_auth_mock):
        tenant_auth_mock.verify_id_token.side_effect = None
        tenant_auth_mock.verify_id_token.return_value = {"uid": "user-1"}  # no customerId
        resp = tenant_client.get(REPORT_URL, headers=_bearer("no-claim"))
        assert resp.status_code == 403


# ---------------------------------------------------------------------------
# FLO-28 Exit Criteria 1 & 2 — tenant mismatch enforcement.
# ---------------------------------------------------------------------------

class TestTenantMismatch:
    def test_mismatched_url_id_vs_token_returns_403(self, tenant_client, tenant_auth_mock):
        """JWT is for tenant-xyz but URL contains tenant-abc → 403. (Exit Criteria 2)"""
        tenant_auth_mock.verify_id_token.side_effect = None
        tenant_auth_mock.verify_id_token.return_value = {
            "uid": "user-xyz",
            "customerId": "tenant-xyz",
        }
        resp = tenant_client.get(
            "/api/v1/customers/tenant-abc/reports",
            headers=_bearer("valid-xyz-token"),
        )
        assert resp.status_code == 403

    def test_tenant_abc_cannot_access_tenant_xyz_data(self, tenant_client, tenant_auth_mock):
        """JWT for tenant-abc is rejected on tenant-xyz URL. (Exit Criteria 1)"""
        tenant_auth_mock.verify_id_token.side_effect = None
        tenant_auth_mock.verify_id_token.return_value = {
            "uid": "user-abc",
            "customerId": "tenant-abc",
        }
        resp = tenant_client.get(
            "/api/v1/customers/tenant-xyz/reports",
            headers=_bearer("valid-abc-token"),
        )
        assert resp.status_code == 403


# ---------------------------------------------------------------------------
# Happy path — matching tenant.
# ---------------------------------------------------------------------------

class TestTenantSuccess:
    def test_matching_tenant_returns_200(self, tenant_client, tenant_auth_mock):
        tenant_auth_mock.verify_id_token.side_effect = None
        tenant_auth_mock.verify_id_token.return_value = {
            "uid": "user-abc",
            "customerId": "tenant-abc",
        }
        resp = tenant_client.get(
            "/api/v1/customers/tenant-abc/reports",
            headers=_bearer("valid-token"),
        )
        assert resp.status_code == 200

    def test_bq_customer_id_injected_into_flask_g(self, tenant_client, tenant_auth_mock):
        """g.bq_customer_id must equal the JWT customerId claim, not the URL param."""
        tenant_auth_mock.verify_id_token.side_effect = None
        tenant_auth_mock.verify_id_token.return_value = {
            "uid": "user-abc",
            "customerId": "tenant-abc",
        }
        resp = tenant_client.get(
            "/api/v1/customers/tenant-abc/reports",
            headers=_bearer("valid-token"),
        )
        body = resp.get_json()
        assert body["bq_customer_id"] == "tenant-abc"
        assert body["customerId"] == "tenant-abc"
        assert body["uid"] == "user-abc"

    def test_client_supplied_customer_id_query_param_ignored(
        self, tenant_client, tenant_auth_mock
    ):
        """Query param customerId is ignored — only the JWT claim is authoritative."""
        tenant_auth_mock.verify_id_token.side_effect = None
        tenant_auth_mock.verify_id_token.return_value = {
            "uid": "user-abc",
            "customerId": "tenant-abc",
        }
        # Even if attacker passes ?customerId=tenant-xyz it must have no effect.
        resp = tenant_client.get(
            "/api/v1/customers/tenant-abc/reports?customerId=tenant-xyz",
            headers=_bearer("valid-token"),
        )
        assert resp.status_code == 200
        assert resp.get_json()["bq_customer_id"] == "tenant-abc"

    def test_route_without_id_segment_still_enforces_auth(
        self, tenant_client, tenant_auth_mock
    ):
        """Routes without an {id} URL segment are auth-gated but skip mismatch check."""
        tenant_auth_mock.verify_id_token.side_effect = None
        tenant_auth_mock.verify_id_token.return_value = {
            "uid": "user-abc",
            "customerId": "tenant-abc",
        }
        resp = tenant_client.get(GLOBAL_URL, headers=_bearer("valid-token"))
        assert resp.status_code == 200

    def test_route_without_id_segment_rejects_unauthenticated(
        self, tenant_client, tenant_auth_mock
    ):
        resp = tenant_client.get(GLOBAL_URL)
        assert resp.status_code == 401
