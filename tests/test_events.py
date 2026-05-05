"""Unit tests for GET /api/v1/customers/{id}/events.

Exit criteria:
  1. Returns HTTP 200 with correctly shaped JSON envelope.
  2. X-Truncated: true header returned when row limit is hit.
  3. Partition pruning enforced: missing date params → 400.

Additional coverage:
  - Row fields: event_ts, tag_id, gateway_id, area_id, zone_id, floor, site_id, rssi, battery_pct.
  - run_raw_events called (raw-event endpoint with LIMIT).
  - SQL uses @customerId, @fromDate, @toDate, @limit placeholders (never literals).
  - siteId filter applied when supplied; absent when not.
  - limit query param respected; values above MAX_LIMIT clamped.
  - Default limit used when not supplied.
  - No X-Truncated header when rows are under the limit.
  - Mismatched tenant → 403; unauthenticated → 401.

BqClient is mocked via patch so no real BigQuery calls are made.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from report.bq_client import DEFAULT_LIMIT, MAX_LIMIT

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

BASE_URL = "/api/v1/customers/cust-abc/events"

_VALID_CLAIM = {"uid": "user-1", "customerId": "cust-abc"}

_SEEDED_ROWS = [
    {
        "event_ts": "2026-05-05T10:01:00+00:00",
        "tag_id": "tag-001",
        "gateway_id": "gw-floor1-a",
        "area_id": "zone-open-plan",
        "zone_id": "zone-open-plan",
        "floor": 1,
        "site_id": "site-hq-pilot",
        "rssi": -65,
        "battery_pct": 82,
    },
    {
        "event_ts": "2026-05-05T10:00:30+00:00",
        "tag_id": "tag-002",
        "gateway_id": "gw-floor1-b",
        "area_id": "zone-reception",
        "zone_id": "zone-reception",
        "floor": 1,
        "site_id": "site-hq-pilot",
        "rssi": -72,
        "battery_pct": 55,
    },
    {
        "event_ts": "2026-05-05T10:00:00+00:00",
        "tag_id": "tag-003",
        "gateway_id": "gw-floor2-a",
        "area_id": "zone-floor2-open",
        "zone_id": "zone-floor2-open",
        "floor": 2,
        "site_id": "site-hq-pilot",
        "rssi": -80,
        "battery_pct": None,
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
    return patch("routes.v1.events.BqClient", return_value=mock_client)


# ===========================================================================
# Exit criterion 1 — HTTP 200, correct JSON shape
# ===========================================================================


class TestEventsShape:
    """Verify response envelope and row fields match the spec."""

    def test_returns_200_with_seeded_rows(self, client, auth_mock):
        _setup_auth(auth_mock)
        with _mock_bq(_SEEDED_ROWS):
            resp = client.get(
                f"{BASE_URL}?from=2026-05-05&to=2026-05-05",
                headers=_bearer(),
            )
        assert resp.status_code == 200

    def test_response_envelope_contains_required_fields(self, client, auth_mock):
        _setup_auth(auth_mock)
        with _mock_bq(_SEEDED_ROWS):
            resp = client.get(
                f"{BASE_URL}?from=2026-05-05&to=2026-05-05",
                headers=_bearer(),
            )
        body = resp.get_json()
        for field in ("customerId", "from", "to", "count", "rows"):
            assert field in body, f"Missing envelope field: {field}"

    def test_customer_id_in_response_matches_jwt(self, client, auth_mock):
        _setup_auth(auth_mock)
        with _mock_bq(_SEEDED_ROWS):
            resp = client.get(
                f"{BASE_URL}?from=2026-05-05&to=2026-05-05",
                headers=_bearer(),
            )
        assert resp.get_json()["customerId"] == "cust-abc"

    def test_from_and_to_reflected_in_response(self, client, auth_mock):
        _setup_auth(auth_mock)
        with _mock_bq(_SEEDED_ROWS):
            resp = client.get(
                f"{BASE_URL}?from=2026-05-01&to=2026-05-05",
                headers=_bearer(),
            )
        body = resp.get_json()
        assert body["from"] == "2026-05-01"
        assert body["to"] == "2026-05-05"

    def test_count_matches_rows_length(self, client, auth_mock):
        _setup_auth(auth_mock)
        with _mock_bq(_SEEDED_ROWS):
            resp = client.get(
                f"{BASE_URL}?from=2026-05-05&to=2026-05-05",
                headers=_bearer(),
            )
        body = resp.get_json()
        assert body["count"] == len(_SEEDED_ROWS)
        assert len(body["rows"]) == len(_SEEDED_ROWS)

    def test_rows_have_event_ts_field(self, client, auth_mock):
        _setup_auth(auth_mock)
        with _mock_bq(_SEEDED_ROWS):
            resp = client.get(f"{BASE_URL}?from=2026-05-05&to=2026-05-05", headers=_bearer())
        assert all("event_ts" in row for row in resp.get_json()["rows"])

    def test_rows_have_tag_id_field(self, client, auth_mock):
        _setup_auth(auth_mock)
        with _mock_bq(_SEEDED_ROWS):
            resp = client.get(f"{BASE_URL}?from=2026-05-05&to=2026-05-05", headers=_bearer())
        assert all("tag_id" in row for row in resp.get_json()["rows"])

    def test_rows_have_rssi_field(self, client, auth_mock):
        _setup_auth(auth_mock)
        with _mock_bq(_SEEDED_ROWS):
            resp = client.get(f"{BASE_URL}?from=2026-05-05&to=2026-05-05", headers=_bearer())
        assert all("rssi" in row for row in resp.get_json()["rows"])

    def test_rows_have_gateway_id_field(self, client, auth_mock):
        _setup_auth(auth_mock)
        with _mock_bq(_SEEDED_ROWS):
            resp = client.get(f"{BASE_URL}?from=2026-05-05&to=2026-05-05", headers=_bearer())
        assert all("gateway_id" in row for row in resp.get_json()["rows"])

    def test_row_values_match_seeded_data(self, client, auth_mock):
        _setup_auth(auth_mock)
        with _mock_bq(_SEEDED_ROWS):
            resp = client.get(f"{BASE_URL}?from=2026-05-05&to=2026-05-05", headers=_bearer())
        rows = resp.get_json()["rows"]
        assert rows[0]["tag_id"] == "tag-001"
        assert rows[0]["rssi"] == -65
        assert rows[0]["floor"] == 1
        assert rows[2]["tag_id"] == "tag-003"
        assert rows[2]["floor"] == 2

    def test_empty_result_returns_200_with_empty_rows(self, client, auth_mock):
        _setup_auth(auth_mock)
        with _mock_bq([]):
            resp = client.get(f"{BASE_URL}?from=2026-05-05&to=2026-05-05", headers=_bearer())
        body = resp.get_json()
        assert resp.status_code == 200
        assert body["rows"] == []
        assert body["count"] == 0

    def test_uses_run_raw_events_not_run_report(self, client, auth_mock):
        """events is a raw-event endpoint — must call run_raw_events, not run_report."""
        _setup_auth(auth_mock)
        mock_client = MagicMock()
        mock_client.run_raw_events.return_value = ([], False)
        with patch("routes.v1.events.BqClient", return_value=mock_client):
            client.get(f"{BASE_URL}?from=2026-05-05&to=2026-05-05", headers=_bearer())
        mock_client.run_raw_events.assert_called_once()
        mock_client.run_report.assert_not_called()


# ===========================================================================
# Exit criterion 2 — X-Truncated header
# ===========================================================================


class TestEventsTruncation:
    def test_x_truncated_true_when_limit_hit(self, client, auth_mock):
        _setup_auth(auth_mock)
        with _mock_bq(_SEEDED_ROWS, truncated=True):
            resp = client.get(f"{BASE_URL}?from=2026-05-05&to=2026-05-05", headers=_bearer())
        assert resp.status_code == 200
        assert resp.headers.get("X-Truncated") == "true"

    def test_no_x_truncated_header_when_under_limit(self, client, auth_mock):
        _setup_auth(auth_mock)
        with _mock_bq(_SEEDED_ROWS, truncated=False):
            resp = client.get(f"{BASE_URL}?from=2026-05-05&to=2026-05-05", headers=_bearer())
        assert "X-Truncated" not in resp.headers

    def test_custom_limit_forwarded_to_run_raw_events(self, client, auth_mock):
        _setup_auth(auth_mock)
        mock_client = MagicMock()
        mock_client.run_raw_events.return_value = ([], False)
        with patch("routes.v1.events.BqClient", return_value=mock_client):
            client.get(f"{BASE_URL}?from=2026-05-05&to=2026-05-05&limit=200", headers=_bearer())
        _, kwargs = mock_client.run_raw_events.call_args
        assert kwargs.get("limit") == 200

    def test_limit_above_max_clamped_to_max(self, client, auth_mock):
        _setup_auth(auth_mock)
        mock_client = MagicMock()
        mock_client.run_raw_events.return_value = ([], False)
        with patch("routes.v1.events.BqClient", return_value=mock_client):
            client.get(f"{BASE_URL}?from=2026-05-05&to=2026-05-05&limit=99999", headers=_bearer())
        _, kwargs = mock_client.run_raw_events.call_args
        assert kwargs.get("limit") == MAX_LIMIT

    def test_default_limit_used_when_not_supplied(self, client, auth_mock):
        _setup_auth(auth_mock)
        mock_client = MagicMock()
        mock_client.run_raw_events.return_value = ([], False)
        with patch("routes.v1.events.BqClient", return_value=mock_client):
            client.get(f"{BASE_URL}?from=2026-05-05&to=2026-05-05", headers=_bearer())
        _, kwargs = mock_client.run_raw_events.call_args
        assert kwargs.get("limit") == DEFAULT_LIMIT


# ===========================================================================
# Exit criterion 3 — Partition pruning
# ===========================================================================


class TestEventsPartitionPruning:
    def test_missing_both_dates_returns_400(self, client, auth_mock):
        _setup_auth(auth_mock)
        assert client.get(BASE_URL, headers=_bearer()).status_code == 400

    def test_missing_from_returns_400(self, client, auth_mock):
        _setup_auth(auth_mock)
        assert client.get(f"{BASE_URL}?to=2026-05-05", headers=_bearer()).status_code == 400

    def test_missing_to_returns_400(self, client, auth_mock):
        _setup_auth(auth_mock)
        assert client.get(f"{BASE_URL}?from=2026-05-05", headers=_bearer()).status_code == 400

    def test_bq_not_called_when_date_missing(self, client, auth_mock):
        _setup_auth(auth_mock)
        mock_client = MagicMock()
        with patch("routes.v1.events.BqClient", return_value=mock_client):
            client.get(BASE_URL, headers=_bearer())
        mock_client.run_raw_events.assert_not_called()

    def test_invalid_date_format_returns_400(self, client, auth_mock):
        _setup_auth(auth_mock)
        assert client.get(
            f"{BASE_URL}?from=05-05-2026&to=2026-05-05", headers=_bearer()
        ).status_code == 400

    def test_from_after_to_returns_400(self, client, auth_mock):
        _setup_auth(auth_mock)
        assert client.get(
            f"{BASE_URL}?from=2026-05-05&to=2026-05-01", headers=_bearer()
        ).status_code == 400


# ===========================================================================
# SQL parameterisation
# ===========================================================================


class TestEventsSqlParams:
    def _captured_sql(self, client, auth_mock) -> str:
        mock_client = MagicMock()
        mock_client.run_raw_events.return_value = ([], False)
        with patch("routes.v1.events.BqClient", return_value=mock_client):
            client.get(
                f"{BASE_URL}?from=2026-05-05&to=2026-05-05",
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
        _setup_auth(auth_mock)
        sql = self._captured_sql(client, auth_mock)
        assert "@limit" in sql

    def test_sql_queries_location_events_table(self, client, auth_mock):
        _setup_auth(auth_mock)
        sql = self._captured_sql(client, auth_mock)
        assert "location_events" in sql

    def test_params_list_contains_at_least_three_entries(self, client, auth_mock):
        _setup_auth(auth_mock)
        mock_client = MagicMock()
        mock_client.run_raw_events.return_value = ([], False)
        with patch("routes.v1.events.BqClient", return_value=mock_client):
            client.get(
                f"{BASE_URL}?from=2026-05-05&to=2026-05-05",
                headers=_bearer(),
            )
        assert len(mock_client.run_raw_events.call_args[0][1]) >= 3


# ===========================================================================
# siteId filter
# ===========================================================================


class TestEventsSiteFilter:
    def test_site_filter_injected_when_site_id_supplied(self, client, auth_mock):
        _setup_auth(auth_mock)
        mock_client = MagicMock()
        mock_client.run_raw_events.return_value = ([], False)
        with patch("routes.v1.events.BqClient", return_value=mock_client):
            client.get(
                f"{BASE_URL}?from=2026-05-05&to=2026-05-05&siteId=site-hq",
                headers=_bearer(),
            )
        sql = mock_client.run_raw_events.call_args[0][0]
        assert "@siteId" in sql

    def test_site_filter_absent_when_site_id_not_supplied(self, client, auth_mock):
        _setup_auth(auth_mock)
        mock_client = MagicMock()
        mock_client.run_raw_events.return_value = ([], False)
        with patch("routes.v1.events.BqClient", return_value=mock_client):
            client.get(
                f"{BASE_URL}?from=2026-05-05&to=2026-05-05",
                headers=_bearer(),
            )
        sql = mock_client.run_raw_events.call_args[0][0]
        assert "@siteId" not in sql


# ===========================================================================
# Tenant isolation
# ===========================================================================


class TestEventsTenantIsolation:
    def test_unauthenticated_returns_401(self, client):
        assert client.get(f"{BASE_URL}?from=2026-05-05&to=2026-05-05").status_code == 401

    def test_mismatched_tenant_returns_403(self, client, auth_mock):
        _setup_auth(auth_mock, claim={"uid": "u1", "customerId": "cust-abc"})
        assert client.get(
            "/api/v1/customers/other-tenant/events?from=2026-05-05&to=2026-05-05",
            headers=_bearer(),
        ).status_code == 403

    def test_cross_tenant_does_not_reach_bq(self, client, auth_mock):
        _setup_auth(auth_mock, claim={"uid": "u1", "customerId": "cust-abc"})
        mock_client = MagicMock()
        with patch("routes.v1.events.BqClient", return_value=mock_client):
            client.get(
                "/api/v1/customers/other-tenant/events?from=2026-05-05&to=2026-05-05",
                headers=_bearer(),
            )
        mock_client.run_raw_events.assert_not_called()
