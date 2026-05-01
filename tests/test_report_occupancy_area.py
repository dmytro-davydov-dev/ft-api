"""FLO-32 — Unit tests for GET /api/v1/customers/{id}/reporting/occupancy/area.

Exit criteria:
  1. Returns HTTP 200 with correctly shaped JSON against seeded BQ data.
  2. Partition pruning enforced: missing date params → 400 before any BQ call.

Additional coverage:
  - Response envelope fields match the spec (customerId, reportType, from, to, rows).
  - Row fields contain areaId, hour, tagCount (BigQuery column names).
  - BqClient is called with parameterised @customerId — never string-interpolated.
  - siteId filter is forwarded when supplied.
  - Mismatched tenant (URL id vs JWT) → 403, not 200.
  - Unauthenticated request → 401.

BqClient is mocked via patch so no real BigQuery calls are made.
"""
from __future__ import annotations

import sys
from datetime import date, timedelta
from unittest.mock import MagicMock, call, patch

import pytest

# ---------------------------------------------------------------------------
# Shared fixture helpers (auth token, seeded rows)
# ---------------------------------------------------------------------------

BASE_URL = "/api/v1/customers/cust-abc/reporting/occupancy/area"

_VALID_CLAIM = {"uid": "user-1", "customerId": "cust-abc"}

# Representative rows that BigQuery would return for an occupancy/area query.
# Shape: areaId (STRING), hour (TIMESTAMP → serialised as string by BqClient),
# tagCount (INT64 → int).
_SEEDED_ROWS = [
    {"areaId": "zone-reception",  "hour": "2026-04-26T09:00:00+00:00", "tagCount": 12},
    {"areaId": "zone-open-plan",  "hour": "2026-04-26T09:00:00+00:00", "tagCount": 34},
    {"areaId": "zone-meeting-a",  "hour": "2026-04-26T10:00:00+00:00", "tagCount": 5},
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


class TestOccupancyAreaShape:
    """Verify response envelope and row fields match the FLO-32 spec."""

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
        assert "customerId" in body
        assert "reportType" in body
        assert "from" in body
        assert "to" in body
        assert "rows" in body

    def test_customer_id_in_response_matches_jwt(self, client, auth_mock):
        _setup_auth(auth_mock)
        with _mock_bq(_SEEDED_ROWS):
            resp = client.get(
                f"{BASE_URL}?from=2026-04-19&to=2026-04-26",
                headers=_bearer(),
            )
        assert resp.get_json()["customerId"] == "cust-abc"

    def test_report_type_is_occupancy_area(self, client, auth_mock):
        _setup_auth(auth_mock)
        with _mock_bq(_SEEDED_ROWS):
            resp = client.get(
                f"{BASE_URL}?from=2026-04-19&to=2026-04-26",
                headers=_bearer(),
            )
        assert resp.get_json()["reportType"] == "occupancy/area"

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

    def test_rows_have_area_id_field(self, client, auth_mock):
        """Each row must include areaId (BQ column name)."""
        _setup_auth(auth_mock)
        with _mock_bq(_SEEDED_ROWS):
            resp = client.get(
                f"{BASE_URL}?from=2026-04-19&to=2026-04-26",
                headers=_bearer(),
            )
        rows = resp.get_json()["rows"]
        assert all("areaId" in row for row in rows)

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
        """Row values passed through from BQ must appear unchanged in the response."""
        _setup_auth(auth_mock)
        with _mock_bq(_SEEDED_ROWS):
            resp = client.get(
                f"{BASE_URL}?from=2026-04-19&to=2026-04-26",
                headers=_bearer(),
            )
        rows = resp.get_json()["rows"]
        assert rows[0]["areaId"] == "zone-reception"
        assert rows[0]["tagCount"] == 12
        assert rows[1]["areaId"] == "zone-open-plan"
        assert rows[1]["tagCount"] == 34

    def test_empty_result_returns_200_with_empty_rows(self, client, auth_mock):
        """Zero rows from BQ is a valid 200 (no data for the window)."""
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


# ===========================================================================
# Exit criterion 2 — Partition pruning: missing date params → 400
# ===========================================================================


class TestOccupancyAreaPartitionPruning:
    """Verify the cost-guard rejects requests without a date range."""

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
        """BigQuery must not be called when partition pruning rejects the request."""
        _setup_auth(auth_mock)
        mock_client = MagicMock()
        with patch("routes.v1.report.BqClient", return_value=mock_client):
            client.get(BASE_URL, headers=_bearer())
        mock_client.run_report.assert_not_called()

    def test_invalid_from_date_format_returns_400(self, client, auth_mock):
        _setup_auth(auth_mock)
        resp = client.get(f"{BASE_URL}?from=19-04-2026&to=2026-04-26", headers=_bearer())
        assert resp.status_code == 400

    def test_from_after_to_returns_400(self, client, auth_mock):
        _setup_auth(auth_mock)
        resp = client.get(f"{BASE_URL}?from=2026-04-26&to=2026-04-19", headers=_bearer())
        assert resp.status_code == 400


# ===========================================================================
# Parameterised customerId — SQL injection prevention
# ===========================================================================


class TestOccupancyAreaCustomerIdParam:
    """BqClient must receive customerId as a query parameter, never interpolated.

    Note: we inspect the SQL string rather than the parameter objects because the
    exact type of ScalarQueryParameter varies depending on whether the real
    google-cloud-bigquery library or a test stub is active (set by test_bq_client.py).
    Detailed parameter construction is already unit-tested in TestBqClientTruncation
    and parse_and_clamp_dates suites.
    """

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

    def test_sql_uses_customer_id_placeholder_not_literal(self, client, auth_mock):
        """@customerId placeholder must appear in SQL; literal 'cust-abc' must not."""
        _setup_auth(auth_mock)
        mock_client = MagicMock()
        mock_client.run_report.return_value = []
        with patch("routes.v1.report.BqClient", return_value=mock_client):
            client.get(
                f"{BASE_URL}?from=2026-04-19&to=2026-04-26",
                headers=_bearer(),
            )
        sql_arg = mock_client.run_report.call_args[0][0]
        assert "@customerId" in sql_arg, "SQL must use @customerId placeholder"
        assert "cust-abc" not in sql_arg, "Tenant ID must not be interpolated into SQL"

    def test_sql_uses_date_placeholders_for_partition_pruning(self, client, auth_mock):
        """@fromDate and @toDate must appear in SQL to enforce partition pruning."""
        _setup_auth(auth_mock)
        mock_client = MagicMock()
        mock_client.run_report.return_value = []
        with patch("routes.v1.report.BqClient", return_value=mock_client):
            client.get(
                f"{BASE_URL}?from=2026-04-19&to=2026-04-26",
                headers=_bearer(),
            )
        sql_arg = mock_client.run_report.call_args[0][0]
        assert "@fromDate" in sql_arg, "SQL must use @fromDate parameter"
        assert "@toDate" in sql_arg, "SQL must use @toDate parameter"

    def test_params_list_is_non_empty(self, client, auth_mock):
        """At minimum customerId, fromDate, toDate must be passed as BQ params."""
        _setup_auth(auth_mock)
        mock_client = MagicMock()
        mock_client.run_report.return_value = []
        with patch("routes.v1.report.BqClient", return_value=mock_client):
            client.get(
                f"{BASE_URL}?from=2026-04-19&to=2026-04-26",
                headers=_bearer(),
            )
        params = mock_client.run_report.call_args[0][1]
        assert len(params) >= 3, "Expected at least customerId, fromDate, toDate params"


# ===========================================================================
# Optional siteId filter
# ===========================================================================


class TestOccupancyAreaSiteFilter:
    def test_site_id_injects_sql_filter_fragment(self, client, auth_mock):
        """When siteId is provided the SQL must contain AND siteId = @siteId."""
        _setup_auth(auth_mock)
        mock_client = MagicMock()
        mock_client.run_report.return_value = []
        with patch("routes.v1.report.BqClient", return_value=mock_client):
            client.get(
                f"{BASE_URL}?from=2026-04-19&to=2026-04-26&siteId=site-hq",
                headers=_bearer(),
            )
        sql_arg = mock_client.run_report.call_args[0][0]
        assert "@siteId" in sql_arg, "SQL must contain @siteId parameter when siteId is supplied"
        # The literal value must not be interpolated
        assert "site-hq" not in sql_arg

    def test_no_site_id_omits_site_filter_from_sql(self, client, auth_mock):
        """When siteId is absent the SQL must not contain the siteId filter."""
        _setup_auth(auth_mock)
        mock_client = MagicMock()
        mock_client.run_report.return_value = []
        with patch("routes.v1.report.BqClient", return_value=mock_client):
            client.get(
                f"{BASE_URL}?from=2026-04-19&to=2026-04-26",
                headers=_bearer(),
            )
        sql_arg = mock_client.run_report.call_args[0][0]
        assert "@siteId" not in sql_arg


# ===========================================================================
# Tenant isolation
# ===========================================================================


class TestOccupancyAreaTenantIsolation:
    def test_unauthenticated_request_returns_401(self, client):
        resp = client.get(f"{BASE_URL}?from=2026-04-19&to=2026-04-26")
        assert resp.status_code == 401

    def test_mismatched_url_id_vs_jwt_returns_403(self, client, auth_mock):
        """JWT for cust-abc cannot access /customers/other-tenant/... URL."""
        _setup_auth(auth_mock, claim={"uid": "u1", "customerId": "cust-abc"})
        resp = client.get(
            "/api/v1/customers/other-tenant/reporting/occupancy/area"
            "?from=2026-04-19&to=2026-04-26",
            headers=_bearer(),
        )
        assert resp.status_code == 403

    def test_cross_tenant_request_does_not_reach_bq(self, client, auth_mock):
        """BQ must not be called when tenant check fails."""
        _setup_auth(auth_mock, claim={"uid": "u1", "customerId": "cust-abc"})
        mock_client = MagicMock()
        with patch("routes.v1.report.BqClient", return_value=mock_client):
            client.get(
                "/api/v1/customers/other-tenant/reporting/occupancy/area"
                "?from=2026-04-19&to=2026-04-26",
                headers=_bearer(),
            )
        mock_client.run_report.assert_not_called()
