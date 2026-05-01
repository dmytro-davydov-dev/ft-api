"""FLO-35 — Unit tests for GET /api/v1/customers/{id}/reporting/people-day.

Exit criteria:
  1. Returns HTTP 200 with correctly shaped JSON.
  2. X-Truncated: true header returned when row limit is hit.
  3. Partition pruning enforced: missing date params → 400 before any BQ call.

Additional coverage:
  - Row fields: tagId, day, first_seen, last_seen, duration_min.
  - run_raw_events called (not run_report — this is a raw-event endpoint).
  - SQL uses @customerId, @fromDate, @toDate, @limit placeholders.
  - limit query param respected; values above MAX_LIMIT clamped.
  - No X-Truncated header when rows are under the limit.
  - siteId filter injected into SQL when supplied; absent otherwise.
  - Mismatched tenant → 403; unauthenticated → 401.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from report.bq_client import DEFAULT_LIMIT, MAX_LIMIT

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

BASE_URL = "/api/v1/customers/cust-abc/reporting/people-day"

_VALID_CLAIM = {"uid": "user-1", "customerId": "cust-abc"}

# Representative rows BigQuery would return for a people-day query.
# Shape: tagId (STRING), day (DATE → string), first_seen (TIMESTAMP → string),
# last_seen (TIMESTAMP → string), duration_min (INT64 → int).
_SEEDED_ROWS = [
    {
        "tagId": "badge-001",
        "day": "2026-04-26",
        "first_seen": "2026-04-26T08:02:00+00:00",
        "last_seen": "2026-04-26T17:45:00+00:00",
        "duration_min": 583,
    },
    {
        "tagId": "badge-042",
        "day": "2026-04-26",
        "first_seen": "2026-04-26T09:15:00+00:00",
        "last_seen": "2026-04-26T16:30:00+00:00",
        "duration_min": 435,
    },
]


def _bearer(token: str = "tok") -> dict:
    return {"Authorization": f"Bearer {token}"}


def _setup_auth(auth_mock, claim: dict = _VALID_CLAIM) -> None:
    auth_mock.verify_id_token.side_effect = None
    auth_mock.verify_id_token.return_value = claim


def _mock_bq(rows=None, truncated: bool = False):
    """Patch BqClient so run_raw_events returns (rows, truncated)."""
    mock_client = MagicMock()
    mock_client.run_raw_events.return_value = (rows if rows is not None else [], truncated)
    return patch("routes.v1.report.BqClient", return_value=mock_client)


# ===========================================================================
# Exit criterion 1 — HTTP 200, correct JSON shape
# ===========================================================================


class TestPeopleDayShape:
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

    def test_report_type_is_people_day(self, client, auth_mock):
        _setup_auth(auth_mock)
        with _mock_bq(_SEEDED_ROWS):
            resp = client.get(
                f"{BASE_URL}?from=2026-04-19&to=2026-04-26",
                headers=_bearer(),
            )
        assert resp.get_json()["reportType"] == "people-day"

    def test_customer_id_in_response_matches_jwt(self, client, auth_mock):
        _setup_auth(auth_mock)
        with _mock_bq(_SEEDED_ROWS):
            resp = client.get(
                f"{BASE_URL}?from=2026-04-19&to=2026-04-26",
                headers=_bearer(),
            )
        assert resp.get_json()["customerId"] == "cust-abc"

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

    def test_rows_have_tag_id_field(self, client, auth_mock):
        _setup_auth(auth_mock)
        with _mock_bq(_SEEDED_ROWS):
            resp = client.get(
                f"{BASE_URL}?from=2026-04-19&to=2026-04-26",
                headers=_bearer(),
            )
        assert all("tagId" in row for row in resp.get_json()["rows"])

    def test_rows_have_day_field(self, client, auth_mock):
        _setup_auth(auth_mock)
        with _mock_bq(_SEEDED_ROWS):
            resp = client.get(
                f"{BASE_URL}?from=2026-04-19&to=2026-04-26",
                headers=_bearer(),
            )
        assert all("day" in row for row in resp.get_json()["rows"])

    def test_rows_have_first_seen_field(self, client, auth_mock):
        _setup_auth(auth_mock)
        with _mock_bq(_SEEDED_ROWS):
            resp = client.get(
                f"{BASE_URL}?from=2026-04-19&to=2026-04-26",
                headers=_bearer(),
            )
        assert all("first_seen" in row for row in resp.get_json()["rows"])

    def test_rows_have_last_seen_field(self, client, auth_mock):
        _setup_auth(auth_mock)
        with _mock_bq(_SEEDED_ROWS):
            resp = client.get(
                f"{BASE_URL}?from=2026-04-19&to=2026-04-26",
                headers=_bearer(),
            )
        assert all("last_seen" in row for row in resp.get_json()["rows"])

    def test_rows_have_duration_min_field(self, client, auth_mock):
        """Each row must include duration_min (TIMESTAMP_DIFF in minutes)."""
        _setup_auth(auth_mock)
        with _mock_bq(_SEEDED_ROWS):
            resp = client.get(
                f"{BASE_URL}?from=2026-04-19&to=2026-04-26",
                headers=_bearer(),
            )
        assert all("duration_min" in row for row in resp.get_json()["rows"])

    def test_row_values_match_seeded_data(self, client, auth_mock):
        _setup_auth(auth_mock)
        with _mock_bq(_SEEDED_ROWS):
            resp = client.get(
                f"{BASE_URL}?from=2026-04-19&to=2026-04-26",
                headers=_bearer(),
            )
        rows = resp.get_json()["rows"]
        assert rows[0]["tagId"] == "badge-001"
        assert rows[0]["duration_min"] == 583
        assert rows[1]["tagId"] == "badge-042"
        assert rows[1]["duration_min"] == 435

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

    def test_uses_run_raw_events_not_run_report(self, client, auth_mock):
        """people-day is a raw-event endpoint — must call run_raw_events, not run_report."""
        _setup_auth(auth_mock)
        mock_client = MagicMock()
        mock_client.run_raw_events.return_value = ([], False)
        with patch("routes.v1.report.BqClient", return_value=mock_client):
            client.get(
                f"{BASE_URL}?from=2026-04-19&to=2026-04-26",
                headers=_bearer(),
            )
        mock_client.run_raw_events.assert_called_once()
        mock_client.run_report.assert_not_called()


# ===========================================================================
# Exit criterion 2 — X-Truncated header
# ===========================================================================


class TestPeopleDayTruncation:
    def test_x_truncated_true_when_limit_hit(self, client, auth_mock):
        _setup_auth(auth_mock)
        with _mock_bq(_SEEDED_ROWS, truncated=True):
            resp = client.get(
                f"{BASE_URL}?from=2026-04-19&to=2026-04-26",
                headers=_bearer(),
            )
        assert resp.status_code == 200
        assert resp.headers.get("X-Truncated") == "true"

    def test_no_x_truncated_header_when_under_limit(self, client, auth_mock):
        _setup_auth(auth_mock)
        with _mock_bq(_SEEDED_ROWS, truncated=False):
            resp = client.get(
                f"{BASE_URL}?from=2026-04-19&to=2026-04-26",
                headers=_bearer(),
            )
        assert "X-Truncated" not in resp.headers

    def test_custom_limit_forwarded_to_run_raw_events(self, client, auth_mock):
        """Caller-supplied limit must be passed through to BqClient."""
        _setup_auth(auth_mock)
        mock_client = MagicMock()
        mock_client.run_raw_events.return_value = ([], False)
        with patch("routes.v1.report.BqClient", return_value=mock_client):
            client.get(
                f"{BASE_URL}?from=2026-04-19&to=2026-04-26&limit=200",
                headers=_bearer(),
            )
        _, kwargs = mock_client.run_raw_events.call_args
        assert kwargs.get("limit") == 200

    def test_limit_above_max_clamped_to_max(self, client, auth_mock):
        _setup_auth(auth_mock)
        mock_client = MagicMock()
        mock_client.run_raw_events.return_value = ([], False)
        with patch("routes.v1.report.BqClient", return_value=mock_client):
            client.get(
                f"{BASE_URL}?from=2026-04-19&to=2026-04-26&limit=99999",
                headers=_bearer(),
            )
        _, kwargs = mock_client.run_raw_events.call_args
        assert kwargs.get("limit") == MAX_LIMIT

    def test_default_limit_used_when_not_supplied(self, client, auth_mock):
        _setup_auth(auth_mock)
        mock_client = MagicMock()
        mock_client.run_raw_events.return_value = ([], False)
        with patch("routes.v1.report.BqClient", return_value=mock_client):
            client.get(
                f"{BASE_URL}?from=2026-04-19&to=2026-04-26",
                headers=_bearer(),
            )
        _, kwargs = mock_client.run_raw_events.call_args
        assert kwargs.get("limit") == DEFAULT_LIMIT

    def test_x_truncated_still_returns_200(self, client, auth_mock):
        _setup_auth(auth_mock)
        with _mock_bq(_SEEDED_ROWS, truncated=True):
            resp = client.get(
                f"{BASE_URL}?from=2026-04-19&to=2026-04-26",
                headers=_bearer(),
            )
        assert resp.status_code == 200


# ===========================================================================
# Exit criterion 3 — Partition pruning
# ===========================================================================


class TestPeopleDayPartitionPruning:
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
        mock_client.run_raw_events.assert_not_called()

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
# SQL parameterisation
# ===========================================================================


class TestPeopleDaySqlParams:
    def _captured_sql(self, client, auth_mock) -> str:
        mock_client = MagicMock()
        mock_client.run_raw_events.return_value = ([], False)
        with patch("routes.v1.report.BqClient", return_value=mock_client):
            client.get(
                f"{BASE_URL}?from=2026-04-19&to=2026-04-26",
                headers=_bearer(),
            )
        return mock_client.run_raw_events.call_args[0][0]

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

    def test_sql_uses_limit_placeholder(self, client, auth_mock):
        """LIMIT @limit must appear in SQL — never a hard-coded integer."""
        _setup_auth(auth_mock)
        sql = self._captured_sql(client, auth_mock)
        assert "@limit" in sql

    def test_params_list_non_empty(self, client, auth_mock):
        _setup_auth(auth_mock)
        mock_client = MagicMock()
        mock_client.run_raw_events.return_value = ([], False)
        with patch("routes.v1.report.BqClient", return_value=mock_client):
            client.get(
                f"{BASE_URL}?from=2026-04-19&to=2026-04-26",
                headers=_bearer(),
            )
        assert len(mock_client.run_raw_events.call_args[0][1]) >= 3


# ===========================================================================
# Optional siteId filter
# ===========================================================================


class TestPeopleDaySiteFilter:
    def test_site_id_injects_sql_filter(self, client, auth_mock):
        _setup_auth(auth_mock)
        mock_client = MagicMock()
        mock_client.run_raw_events.return_value = ([], False)
        with patch("routes.v1.report.BqClient", return_value=mock_client):
            client.get(
                f"{BASE_URL}?from=2026-04-19&to=2026-04-26&siteId=site-hq",
                headers=_bearer(),
            )
        sql = mock_client.run_raw_events.call_args[0][0]
        assert "@siteId" in sql
        assert "site-hq" not in sql

    def test_no_site_id_omits_filter(self, client, auth_mock):
        _setup_auth(auth_mock)
        mock_client = MagicMock()
        mock_client.run_raw_events.return_value = ([], False)
        with patch("routes.v1.report.BqClient", return_value=mock_client):
            client.get(
                f"{BASE_URL}?from=2026-04-19&to=2026-04-26",
                headers=_bearer(),
            )
        assert "@siteId" not in mock_client.run_raw_events.call_args[0][0]


# ===========================================================================
# Tenant isolation
# ===========================================================================


class TestPeopleDayTenantIsolation:
    def test_unauthenticated_returns_401(self, client):
        assert client.get(
            f"{BASE_URL}?from=2026-04-19&to=2026-04-26"
        ).status_code == 401

    def test_mismatched_tenant_returns_403(self, client, auth_mock):
        _setup_auth(auth_mock, claim={"uid": "u1", "customerId": "cust-abc"})
        assert client.get(
            "/api/v1/customers/other-tenant/reporting/people-day"
            "?from=2026-04-19&to=2026-04-26",
            headers=_bearer(),
        ).status_code == 403

    def test_cross_tenant_does_not_reach_bq(self, client, auth_mock):
        _setup_auth(auth_mock, claim={"uid": "u1", "customerId": "cust-abc"})
        mock_client = MagicMock()
        with patch("routes.v1.report.BqClient", return_value=mock_client):
            client.get(
                "/api/v1/customers/other-tenant/reporting/people-day"
                "?from=2026-04-19&to=2026-04-26",
                headers=_bearer(),
            )
        mock_client.run_raw_events.assert_not_called()
