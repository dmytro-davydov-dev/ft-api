"""FLO-33 — Unit tests for GET /api/v1/customers/{id}/reporting/occupancy/floor.

Exit criteria:
  1. Returns HTTP 200 with correctly shaped JSON against seeded BQ data.
  2. Partition pruning enforced: missing date params → 400 before any BQ call.

Additional coverage:
  - Response envelope fields match the spec (customerId, reportType, from, to, rows).
  - Row fields contain floor, hour, tagCount (BigQuery column names).
  - SQL uses @customerId placeholder — tenant ID never interpolated.
  - SQL uses @fromDate / @toDate placeholders (partition pruning).
  - siteId filter injected into SQL when supplied; absent otherwise.
  - Mismatched tenant (URL id vs JWT) → 403.
  - Unauthenticated request → 401.

BqClient is mocked via patch so no real BigQuery calls are made.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

BASE_URL = "/api/v1/customers/cust-abc/reporting/occupancy/floor"

_VALID_CLAIM = {"uid": "user-1", "customerId": "cust-abc"}

# Representative rows BigQuery would return for an occupancy/floor query.
# Shape: floor (INT64 → int), hour (TIMESTAMP → string), tagCount (INT64 → int).
_SEEDED_ROWS = [
    {"floor": 1, "hour": "2026-04-26T09:00:00+00:00", "tagCount": 18},
    {"floor": 2, "hour": "2026-04-26T09:00:00+00:00", "tagCount": 42},
    {"floor": 1, "hour": "2026-04-26T10:00:00+00:00", "tagCount": 21},
]


def _bearer(token: str = "tok") -> dict:
    return {"Authorization": f"Bearer {token}"}


def _setup_auth(auth_mock, claim: dict = _VALID_CLAIM) -> None:
    auth_mock.verify_id_token.side_effect = None
    auth_mock.verify_id_token.return_value = claim


def _mock_bq(rows=None):
    """Context manager: patches BqClient so run_report returns *rows*."""
    mock_client = MagicMock()
    mock_client.run_report.return_value = rows if rows is not None else []
    return patch("routes.v1.report.BqClient", return_value=mock_client)


# ===========================================================================
# Exit criterion 1 — HTTP 200, correct JSON shape, seeded BQ rows
# ===========================================================================


class TestOccupancyFloorShape:
    """Verify response envelope and row fields match the FLO-33 spec."""

    def test_returns_200_with_seeded_rows(self, client, auth_mock):
        _setup_auth(auth_mock)
        with _mock_bq(_SEEDED_ROWS):
            resp = client.get(
                f"{BASE_URL}?from=2026-04-19&to=2026-04-26",
                headers=_bearer(),
            )
        assert resp.status_code == 200

    def test_response_envelope_contains_required_fields(self, client, auth_mock):
        """customerId, reportType, from, to, rows must all be present."""
        _setup_auth(auth_mock)
        with _mock_bq(_SEEDED_ROWS):
            resp = client.get(
                f"{BASE_URL}?from=2026-04-19&to=2026-04-26",
                headers=_bearer(),
            )
        body = resp.get_json()
        for field in ("customerId", "reportType", "from", "to", "rows"):
            assert field in body, f"Missing envelope field: {field}"

    def test_customer_id_in_response_matches_jwt(self, client, auth_mock):
        _setup_auth(auth_mock)
        with _mock_bq(_SEEDED_ROWS):
            resp = client.get(
                f"{BASE_URL}?from=2026-04-19&to=2026-04-26",
                headers=_bearer(),
            )
        assert resp.get_json()["customerId"] == "cust-abc"

    def test_report_type_is_occupancy_floor(self, client, auth_mock):
        _setup_auth(auth_mock)
        with _mock_bq(_SEEDED_ROWS):
            resp = client.get(
                f"{BASE_URL}?from=2026-04-19&to=2026-04-26",
                headers=_bearer(),
            )
        assert resp.get_json()["reportType"] == "occupancy/floor"

    def test_from_and_to_reflected_in_response(self, client, auth_mock):
        _setup_auth(auth_mock)
        with _mock_bq(_SEEDED_ROWS):
            resp = client.get(
                f"{BASE_URL}?from=2026-04-19&to=2026-04-26",
                headers=_bearer(),
            )
        body = resp.get_json()
        assert body["from"] == "2026-04-19"
        assert body["to"] == "2026-04-26"

    def test_rows_contain_expected_count(self, client, auth_mock):
        _setup_auth(auth_mock)
        with _mock_bq(_SEEDED_ROWS):
            resp = client.get(
                f"{BASE_URL}?from=2026-04-19&to=2026-04-26",
                headers=_bearer(),
            )
        body = resp.get_json()
        assert body["count"] == len(_SEEDED_ROWS)
        assert len(body["rows"]) == len(_SEEDED_ROWS)

    def test_rows_have_floor_field(self, client, auth_mock):
        """Each row must include floor (BQ column name, grouped dimension)."""
        _setup_auth(auth_mock)
        with _mock_bq(_SEEDED_ROWS):
            resp = client.get(
                f"{BASE_URL}?from=2026-04-19&to=2026-04-26",
                headers=_bearer(),
            )
        rows = resp.get_json()["rows"]
        assert all("floor" in row for row in rows)

    def test_rows_have_hour_field(self, client, auth_mock):
        """Each row must include hour (TIMESTAMP_TRUNC result)."""
        _setup_auth(auth_mock)
        with _mock_bq(_SEEDED_ROWS):
            resp = client.get(
                f"{BASE_URL}?from=2026-04-19&to=2026-04-26",
                headers=_bearer(),
            )
        rows = resp.get_json()["rows"]
        assert all("hour" in row for row in rows)

    def test_rows_have_tag_count_field(self, client, auth_mock):
        """Each row must include tagCount (COUNT(DISTINCT tagId))."""
        _setup_auth(auth_mock)
        with _mock_bq(_SEEDED_ROWS):
            resp = client.get(
                f"{BASE_URL}?from=2026-04-19&to=2026-04-26",
                headers=_bearer(),
            )
        rows = resp.get_json()["rows"]
        assert all("tagCount" in row for row in rows)

    def test_rows_values_match_seeded_data(self, client, auth_mock):
        _setup_auth(auth_mock)
        with _mock_bq(_SEEDED_ROWS):
            resp = client.get(
                f"{BASE_URL}?from=2026-04-19&to=2026-04-26",
                headers=_bearer(),
            )
        rows = resp.get_json()["rows"]
        assert rows[0]["floor"] == 1
        assert rows[0]["tagCount"] == 18
        assert rows[1]["floor"] == 2
        assert rows[1]["tagCount"] == 42

    def test_empty_result_returns_200_with_empty_rows(self, client, auth_mock):
        _setup_auth(auth_mock)
        with _mock_bq([]):
            resp = client.get(
                f"{BASE_URL}?from=2026-04-19&to=2026-04-26",
                headers=_bearer(),
            )
        body = resp.get_json()
        assert resp.status_code == 200
        assert body["rows"] == []
        assert body["count"] == 0

    def test_clamped_false_within_90_days(self, client, auth_mock):
        _setup_auth(auth_mock)
        with _mock_bq([]):
            resp = client.get(
                f"{BASE_URL}?from=2026-04-19&to=2026-04-26",
                headers=_bearer(),
            )
        assert resp.get_json()["clamped"] is False

    def test_clamped_true_beyond_90_days(self, client, auth_mock):
        _setup_auth(auth_mock)
        with _mock_bq([]):
            resp = client.get(
                f"{BASE_URL}?from=2026-01-01&to=2027-01-01",
                headers=_bearer(),
            )
        body = resp.get_json()
        assert resp.status_code == 200
        assert body["clamped"] is True


# ===========================================================================
# Exit criterion 2 — Partition pruning: missing date params → 400
# ===========================================================================


class TestOccupancyFloorPartitionPruning:
    def test_missing_both_dates_returns_400(self, client, auth_mock):
        _setup_auth(auth_mock)
        resp = client.get(BASE_URL, headers=_bearer())
        assert resp.status_code == 400

    def test_missing_from_returns_400(self, client, auth_mock):
        _setup_auth(auth_mock)
        resp = client.get(f"{BASE_URL}?to=2026-04-26", headers=_bearer())
        assert resp.status_code == 400

    def test_missing_to_returns_400(self, client, auth_mock):
        _setup_auth(auth_mock)
        resp = client.get(f"{BASE_URL}?from=2026-04-19", headers=_bearer())
        assert resp.status_code == 400

    def test_bq_not_called_when_date_missing(self, client, auth_mock):
        _setup_auth(auth_mock)
        mock_client = MagicMock()
        with patch("routes.v1.report.BqClient", return_value=mock_client):
            client.get(BASE_URL, headers=_bearer())
        mock_client.run_report.assert_not_called()

    def test_invalid_date_format_returns_400(self, client, auth_mock):
        _setup_auth(auth_mock)
        resp = client.get(f"{BASE_URL}?from=26-04-2026&to=2026-04-26", headers=_bearer())
        assert resp.status_code == 400

    def test_from_after_to_returns_400(self, client, auth_mock):
        _setup_auth(auth_mock)
        resp = client.get(f"{BASE_URL}?from=2026-04-26&to=2026-04-19", headers=_bearer())
        assert resp.status_code == 400


# ===========================================================================
# SQL parameterisation — no literal interpolation
# ===========================================================================


class TestOccupancyFloorSqlParams:
    """SQL must use @placeholders; tenant ID and dates must never be literals."""

    def _captured_sql(self, client, auth_mock) -> str:
        mock_client = MagicMock()
        mock_client.run_report.return_value = []
        with patch("routes.v1.report.BqClient", return_value=mock_client):
            client.get(
                f"{BASE_URL}?from=2026-04-19&to=2026-04-26",
                headers=_bearer(),
            )
        return mock_client.run_report.call_args[0][0]

    def test_run_report_is_called_once(self, client, auth_mock):
        _setup_auth(auth_mock)
        mock_client = MagicMock()
        mock_client.run_report.return_value = []
        with patch("routes.v1.report.BqClient", return_value=mock_client):
            client.get(
                f"{BASE_URL}?from=2026-04-19&to=2026-04-26",
                headers=_bearer(),
            )
        mock_client.run_report.assert_called_once()

    def test_sql_uses_customer_id_placeholder(self, client, auth_mock):
        _setup_auth(auth_mock)
        sql = self._captured_sql(client, auth_mock)
        assert "@customerId" in sql
        assert "cust-abc" not in sql

    def test_sql_uses_date_placeholders(self, client, auth_mock):
        _setup_auth(auth_mock)
        sql = self._captured_sql(client, auth_mock)
        assert "@fromDate" in sql
        assert "@toDate" in sql

    def test_params_list_is_non_empty(self, client, auth_mock):
        _setup_auth(auth_mock)
        mock_client = MagicMock()
        mock_client.run_report.return_value = []
        with patch("routes.v1.report.BqClient", return_value=mock_client):
            client.get(
                f"{BASE_URL}?from=2026-04-19&to=2026-04-26",
                headers=_bearer(),
            )
        params = mock_client.run_report.call_args[0][1]
        assert len(params) >= 3


# ===========================================================================
# Optional siteId filter
# ===========================================================================


class TestOccupancyFloorSiteFilter:
    def test_site_id_injects_sql_filter_fragment(self, client, auth_mock):
        _setup_auth(auth_mock)
        mock_client = MagicMock()
        mock_client.run_report.return_value = []
        with patch("routes.v1.report.BqClient", return_value=mock_client):
            client.get(
                f"{BASE_URL}?from=2026-04-19&to=2026-04-26&siteId=site-hq",
                headers=_bearer(),
            )
        sql = mock_client.run_report.call_args[0][0]
        assert "@siteId" in sql
        assert "site-hq" not in sql

    def test_no_site_id_omits_filter_from_sql(self, client, auth_mock):
        _setup_auth(auth_mock)
        mock_client = MagicMock()
        mock_client.run_report.return_value = []
        with patch("routes.v1.report.BqClient", return_value=mock_client):
            client.get(
                f"{BASE_URL}?from=2026-04-19&to=2026-04-26",
                headers=_bearer(),
            )
        sql = mock_client.run_report.call_args[0][0]
        assert "@siteId" not in sql


# ===========================================================================
# Tenant isolation
# ===========================================================================


class TestOccupancyFloorTenantIsolation:
    def test_unauthenticated_request_returns_401(self, client):
        resp = client.get(f"{BASE_URL}?from=2026-04-19&to=2026-04-26")
        assert resp.status_code == 401

    def test_mismatched_url_id_vs_jwt_returns_403(self, client, auth_mock):
        _setup_auth(auth_mock, claim={"uid": "u1", "customerId": "cust-abc"})
        resp = client.get(
            "/api/v1/customers/other-tenant/reporting/occupancy/floor"
            "?from=2026-04-19&to=2026-04-26",
            headers=_bearer(),
        )
        assert resp.status_code == 403

    def test_cross_tenant_request_does_not_reach_bq(self, client, auth_mock):
        _setup_auth(auth_mock, claim={"uid": "u1", "customerId": "cust-abc"})
        mock_client = MagicMock()
        with patch("routes.v1.report.BqClient", return_value=mock_client):
            client.get(
                "/api/v1/customers/other-tenant/reporting/occupancy/floor"
                "?from=2026-04-19&to=2026-04-26",
                headers=_bearer(),
            )
        mock_client.run_report.assert_not_called()
