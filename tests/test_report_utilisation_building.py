"""FLO-34 — Unit tests for GET /api/v1/customers/{id}/reporting/utilisation/building.

Exit criteria:
  1. Returns HTTP 200 with correctly shaped JSON against seeded BQ data.
  2. Partition pruning enforced: missing date params → 400 before any BQ call.

Additional coverage:
  - Response envelope fields: customerId, reportType, from, to, rows.
  - Row fields: day, occupied_hours, total_hours, utilisation_pct.
  - SQL uses @customerId placeholder — tenant ID never interpolated.
  - SQL uses @fromDate / @toDate (partition pruning).
  - siteId filter injected into SQL when supplied; absent otherwise.
  - Mismatched tenant → 403; unauthenticated → 401.

BqClient is mocked via patch so no real BigQuery calls are made.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

BASE_URL = "/api/v1/customers/cust-abc/reporting/utilisation/building"

_VALID_CLAIM = {"uid": "user-1", "customerId": "cust-abc"}

# Representative rows BigQuery would return for utilisation/building.
# Shape: day (DATE → string), occupied_hours (INT64), total_hours (INT64),
# utilisation_pct (FLOAT64 → float).
_SEEDED_ROWS = [
    {"day": "2026-04-24", "occupied_hours": 8,  "total_hours": 24, "utilisation_pct": 33.33},
    {"day": "2026-04-25", "occupied_hours": 12, "total_hours": 24, "utilisation_pct": 50.0},
    {"day": "2026-04-26", "occupied_hours": 6,  "total_hours": 24, "utilisation_pct": 25.0},
]


def _bearer(token: str = "tok") -> dict:
    return {"Authorization": f"Bearer {token}"}


def _setup_auth(auth_mock, claim: dict = _VALID_CLAIM) -> None:
    auth_mock.verify_id_token.side_effect = None
    auth_mock.verify_id_token.return_value = claim


def _mock_bq(rows=None):
    mock_client = MagicMock()
    mock_client.run_report.return_value = rows if rows is not None else []
    return patch("routes.v1.report.BqClient", return_value=mock_client)


# ===========================================================================
# Exit criterion 1 — HTTP 200, correct JSON shape, seeded BQ rows
# ===========================================================================


class TestUtilisationBuildingShape:
    def test_returns_200_with_seeded_rows(self, client, auth_mock):
        _setup_auth(auth_mock)
        with _mock_bq(_SEEDED_ROWS):
            resp = client.get(
                f"{BASE_URL}?from=2026-04-19&to=2026-04-26",
                headers=_bearer(),
            )
        assert resp.status_code == 200

    def test_response_envelope_contains_required_fields(self, client, auth_mock):
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

    def test_report_type_is_utilisation_building(self, client, auth_mock):
        _setup_auth(auth_mock)
        with _mock_bq(_SEEDED_ROWS):
            resp = client.get(
                f"{BASE_URL}?from=2026-04-19&to=2026-04-26",
                headers=_bearer(),
            )
        assert resp.get_json()["reportType"] == "utilisation/building"

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

    def test_rows_have_day_field(self, client, auth_mock):
        """Each row must include day (DATE grouped dimension)."""
        _setup_auth(auth_mock)
        with _mock_bq(_SEEDED_ROWS):
            resp = client.get(
                f"{BASE_URL}?from=2026-04-19&to=2026-04-26",
                headers=_bearer(),
            )
        assert all("day" in row for row in resp.get_json()["rows"])

    def test_rows_have_occupied_hours_field(self, client, auth_mock):
        _setup_auth(auth_mock)
        with _mock_bq(_SEEDED_ROWS):
            resp = client.get(
                f"{BASE_URL}?from=2026-04-19&to=2026-04-26",
                headers=_bearer(),
            )
        assert all("occupied_hours" in row for row in resp.get_json()["rows"])

    def test_rows_have_total_hours_field(self, client, auth_mock):
        _setup_auth(auth_mock)
        with _mock_bq(_SEEDED_ROWS):
            resp = client.get(
                f"{BASE_URL}?from=2026-04-19&to=2026-04-26",
                headers=_bearer(),
            )
        assert all("total_hours" in row for row in resp.get_json()["rows"])

    def test_rows_have_utilisation_pct_field(self, client, auth_mock):
        """Each row must include utilisation_pct (occupied_hours / 24 * 100)."""
        _setup_auth(auth_mock)
        with _mock_bq(_SEEDED_ROWS):
            resp = client.get(
                f"{BASE_URL}?from=2026-04-19&to=2026-04-26",
                headers=_bearer(),
            )
        assert all("utilisation_pct" in row for row in resp.get_json()["rows"])

    def test_rows_values_match_seeded_data(self, client, auth_mock):
        _setup_auth(auth_mock)
        with _mock_bq(_SEEDED_ROWS):
            resp = client.get(
                f"{BASE_URL}?from=2026-04-19&to=2026-04-26",
                headers=_bearer(),
            )
        rows = resp.get_json()["rows"]
        assert rows[0]["day"] == "2026-04-24"
        assert rows[0]["occupied_hours"] == 8
        assert rows[0]["utilisation_pct"] == 33.33
        assert rows[1]["utilisation_pct"] == 50.0

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


class TestUtilisationBuildingPartitionPruning:
    def test_missing_both_dates_returns_400(self, client, auth_mock):
        _setup_auth(auth_mock)
        assert client.get(BASE_URL, headers=_bearer()).status_code == 400

    def test_missing_from_returns_400(self, client, auth_mock):
        _setup_auth(auth_mock)
        assert client.get(f"{BASE_URL}?to=2026-04-26", headers=_bearer()).status_code == 400

    def test_missing_to_returns_400(self, client, auth_mock):
        _setup_auth(auth_mock)
        assert client.get(f"{BASE_URL}?from=2026-04-19", headers=_bearer()).status_code == 400

    def test_bq_not_called_when_date_missing(self, client, auth_mock):
        _setup_auth(auth_mock)
        mock_client = MagicMock()
        with patch("routes.v1.report.BqClient", return_value=mock_client):
            client.get(BASE_URL, headers=_bearer())
        mock_client.run_report.assert_not_called()

    def test_invalid_date_format_returns_400(self, client, auth_mock):
        _setup_auth(auth_mock)
        assert client.get(
            f"{BASE_URL}?from=26-04-2026&to=2026-04-26", headers=_bearer()
        ).status_code == 400

    def test_from_after_to_returns_400(self, client, auth_mock):
        _setup_auth(auth_mock)
        assert client.get(
            f"{BASE_URL}?from=2026-04-26&to=2026-04-19", headers=_bearer()
        ).status_code == 400


# ===========================================================================
# SQL parameterisation — no literal interpolation
# ===========================================================================


class TestUtilisationBuildingSqlParams:
    def _captured_sql(self, client, auth_mock) -> str:
        mock_client = MagicMock()
        mock_client.run_report.return_value = []
        with patch("routes.v1.report.BqClient", return_value=mock_client):
            client.get(
                f"{BASE_URL}?from=2026-04-19&to=2026-04-26",
                headers=_bearer(),
            )
        return mock_client.run_report.call_args[0][0]

    def test_run_report_called_once(self, client, auth_mock):
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

    def test_params_list_non_empty(self, client, auth_mock):
        _setup_auth(auth_mock)
        mock_client = MagicMock()
        mock_client.run_report.return_value = []
        with patch("routes.v1.report.BqClient", return_value=mock_client):
            client.get(
                f"{BASE_URL}?from=2026-04-19&to=2026-04-26",
                headers=_bearer(),
            )
        assert len(mock_client.run_report.call_args[0][1]) >= 3


# ===========================================================================
# Optional siteId filter
# ===========================================================================


class TestUtilisationBuildingSiteFilter:
    def test_site_id_injects_sql_filter(self, client, auth_mock):
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

    def test_no_site_id_omits_filter(self, client, auth_mock):
        _setup_auth(auth_mock)
        mock_client = MagicMock()
        mock_client.run_report.return_value = []
        with patch("routes.v1.report.BqClient", return_value=mock_client):
            client.get(
                f"{BASE_URL}?from=2026-04-19&to=2026-04-26",
                headers=_bearer(),
            )
        assert "@siteId" not in mock_client.run_report.call_args[0][0]


# ===========================================================================
# Tenant isolation
# ===========================================================================


class TestUtilisationBuildingTenantIsolation:
    def test_unauthenticated_returns_401(self, client):
        assert client.get(
            f"{BASE_URL}?from=2026-04-19&to=2026-04-26"
        ).status_code == 401

    def test_mismatched_tenant_returns_403(self, client, auth_mock):
        _setup_auth(auth_mock, claim={"uid": "u1", "customerId": "cust-abc"})
        assert client.get(
            "/api/v1/customers/other-tenant/reporting/utilisation/building"
            "?from=2026-04-19&to=2026-04-26",
            headers=_bearer(),
        ).status_code == 403

    def test_cross_tenant_does_not_reach_bq(self, client, auth_mock):
        _setup_auth(auth_mock, claim={"uid": "u1", "customerId": "cust-abc"})
        mock_client = MagicMock()
        with patch("routes.v1.report.BqClient", return_value=mock_client):
            client.get(
                "/api/v1/customers/other-tenant/reporting/utilisation/building"
                "?from=2026-04-19&to=2026-04-26",
                headers=_bearer(),
            )
        mock_client.run_report.assert_not_called()
