"""Unit tests for report/bq_client.py — cost-guard middleware.

Covers all FLO-29 exit criteria:
  - A request without a date filter returns HTTP 400
  - Date ranges > 90 days are silently clamped; correct range reflected in response
  - ``X-Truncated: true`` header returned when row count hits the limit

No real BigQuery calls are made; BqClient is tested by mocking
google.cloud.bigquery at the module level (via sys.modules patching in conftest).

Additional unit tests exercise the pure helper functions directly so that
edge-case logic is verified without HTTP overhead.
"""

from __future__ import annotations

import sys
from datetime import date, timedelta
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# google-cloud-bigquery stub — must be installed before report/* is imported
# ---------------------------------------------------------------------------

def _make_bq_mock():
    """Return a minimal google.cloud.bigquery stub."""
    mock = MagicMock()

    # ScalarQueryParameter is used as a constructor; just store args.
    class _Param:
        def __init__(self, name, kind, value):
            self.name = name
            self.kind = kind
            self.value = value

    mock.ScalarQueryParameter = _Param
    mock.QueryJobConfig = MagicMock(side_effect=lambda **kw: MagicMock(**kw))
    return mock


_bq_mock = _make_bq_mock()

# Patch google.cloud.bigquery before any report.* import.
sys.modules.setdefault("google", MagicMock())
sys.modules.setdefault("google.cloud", MagicMock())
sys.modules["google.cloud.bigquery"] = _bq_mock


# ---------------------------------------------------------------------------
# Helpers imported after stubbing
# ---------------------------------------------------------------------------

from report.bq_client import (  # noqa: E402
    DEFAULT_LIMIT,
    MAX_LIMIT,
    MAX_RANGE_DAYS,
    BqClient,
    DateWindow,
    build_raw_event_params,
    build_report_params,
    clamp_limit,
    parse_and_clamp_dates,
)


# ===========================================================================
# parse_and_clamp_dates — pure-function unit tests
# ===========================================================================


class TestParseAndClampDates:
    """Unit tests for parse_and_clamp_dates (no Flask context needed)."""

    # --- happy path ---------------------------------------------------------

    def test_valid_range_within_90_days(self, app):
        with app.app_context():
            w = parse_and_clamp_dates("2026-01-01", "2026-01-31")
        assert w.from_date == date(2026, 1, 1)
        assert w.to_date == date(2026, 1, 31)
        assert w.clamped is False

    def test_single_day_range_not_clamped(self, app):
        with app.app_context():
            w = parse_and_clamp_dates("2026-03-15", "2026-03-15")
        assert w.from_date == w.to_date == date(2026, 3, 15)
        assert w.clamped is False

    def test_exactly_90_days_not_clamped(self, app):
        from_date = date(2026, 1, 1)
        to_date = from_date + timedelta(days=MAX_RANGE_DAYS)
        with app.app_context():
            w = parse_and_clamp_dates(from_date.isoformat(), to_date.isoformat())
        assert w.clamped is False
        assert (w.to_date - w.from_date).days == MAX_RANGE_DAYS

    # --- clamping -----------------------------------------------------------

    def test_range_exceeding_90_days_is_clamped(self, app):
        from_str = "2026-01-01"
        to_str = "2026-06-01"  # ~150 days
        with app.app_context():
            w = parse_and_clamp_dates(from_str, to_str)
        assert w.clamped is True
        assert (w.to_date - w.from_date).days == MAX_RANGE_DAYS
        assert w.from_date == date(2026, 1, 1)

    def test_clamped_to_date_equals_from_plus_90(self, app):
        with app.app_context():
            w = parse_and_clamp_dates("2026-01-01", "2027-01-01")
        assert w.to_date == date(2026, 1, 1) + timedelta(days=MAX_RANGE_DAYS)

    # --- 400 errors (exit criterion 1) -------------------------------------

    def test_missing_both_params_raises_400(self, app):
        with app.test_request_context("/?"):
            from werkzeug.exceptions import BadRequest
            with pytest.raises(Exception) as exc_info:
                parse_and_clamp_dates(None, None)
            assert exc_info.value.code == 400

    def test_missing_from_raises_400(self, app):
        with app.test_request_context("/?to=2026-01-31"):
            with pytest.raises(Exception) as exc_info:
                parse_and_clamp_dates(None, "2026-01-31")
            assert exc_info.value.code == 400

    def test_missing_to_raises_400(self, app):
        with app.test_request_context("/?from=2026-01-01"):
            with pytest.raises(Exception) as exc_info:
                parse_and_clamp_dates("2026-01-01", None)
            assert exc_info.value.code == 400

    def test_invalid_from_date_raises_400(self, app):
        with app.test_request_context("/"):
            with pytest.raises(Exception) as exc_info:
                parse_and_clamp_dates("not-a-date", "2026-01-31")
            assert exc_info.value.code == 400

    def test_invalid_to_date_raises_400(self, app):
        with app.test_request_context("/"):
            with pytest.raises(Exception) as exc_info:
                parse_and_clamp_dates("2026-01-01", "31-01-2026")
            assert exc_info.value.code == 400

    def test_from_after_to_raises_400(self, app):
        with app.test_request_context("/"):
            with pytest.raises(Exception) as exc_info:
                parse_and_clamp_dates("2026-02-01", "2026-01-01")
            assert exc_info.value.code == 400


# ===========================================================================
# clamp_limit — pure-function unit tests
# ===========================================================================


class TestClampLimit:
    def test_none_returns_default_not_clamped(self):
        limit, was_clamped = clamp_limit(None)
        assert limit == DEFAULT_LIMIT
        assert was_clamped is False

    def test_value_within_range_not_clamped(self):
        limit, was_clamped = clamp_limit(500)
        assert limit == 500
        assert was_clamped is False

    def test_value_exceeding_max_is_clamped(self):
        limit, was_clamped = clamp_limit(MAX_LIMIT + 1)
        assert limit == MAX_LIMIT
        assert was_clamped is True

    def test_value_at_max_not_clamped(self):
        limit, was_clamped = clamp_limit(MAX_LIMIT)
        assert limit == MAX_LIMIT
        assert was_clamped is False

    def test_zero_clamped_to_one(self):
        limit, _ = clamp_limit(0)
        assert limit == 1

    def test_negative_clamped_to_one(self):
        limit, _ = clamp_limit(-100)
        assert limit == 1


# ===========================================================================
# Report endpoint HTTP tests — cost-guard integration
# ===========================================================================

# A valid auth mock payload reused across tests.
_VALID_CLAIM = {"uid": "u1", "customerId": "cust-abc"}


def _bearer(token: str = "tok") -> dict:
    return {"Authorization": f"Bearer {token}"}


def _setup_auth(auth_mock):
    """Configure auth_mock to return _VALID_CLAIM for any token."""
    auth_mock.verify_id_token.side_effect = None
    auth_mock.verify_id_token.return_value = _VALID_CLAIM


class TestReportEndpointMissingDates:
    """Exit criterion 1: missing date params → 400."""

    ENDPOINTS = [
        "/api/v1/customers/cust-abc/reporting/occupancy-area",
        "/api/v1/customers/cust-abc/reporting/occupancy-floor",
        "/api/v1/customers/cust-abc/reporting/utilisation-building",
        "/api/v1/customers/cust-abc/reporting/people-day",
        "/api/v1/customers/cust-abc/reporting/alerts",
    ]

    @pytest.mark.parametrize("endpoint", ENDPOINTS)
    def test_no_date_params_returns_400(self, client, auth_mock, endpoint):
        _setup_auth(auth_mock)
        resp = client.get(endpoint, headers=_bearer())
        assert resp.status_code == 400

    @pytest.mark.parametrize("endpoint", ENDPOINTS)
    def test_only_from_returns_400(self, client, auth_mock, endpoint):
        _setup_auth(auth_mock)
        resp = client.get(f"{endpoint}?from=2026-01-01", headers=_bearer())
        assert resp.status_code == 400

    @pytest.mark.parametrize("endpoint", ENDPOINTS)
    def test_only_to_returns_400(self, client, auth_mock, endpoint):
        _setup_auth(auth_mock)
        resp = client.get(f"{endpoint}?to=2026-01-31", headers=_bearer())
        assert resp.status_code == 400


class TestReportEndpointDateClamping:
    """Exit criterion 2: ranges > 90 days silently clamped; correct range in response."""

    BASE = "/api/v1/customers/cust-abc/reporting/occupancy-area"

    def _mock_bq(self, rows=None):
        """Return a context manager that stubs BqClient.run_report."""
        mock_client = MagicMock()
        mock_client.run_report.return_value = rows or []
        return patch("routes.v1.report.BqClient", return_value=mock_client)

    def test_within_90_days_not_clamped(self, client, auth_mock):
        _setup_auth(auth_mock)
        with self._mock_bq():
            resp = client.get(
                f"{self.BASE}?from=2026-01-01&to=2026-01-31",
                headers=_bearer(),
            )
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["from"] == "2026-01-01"
        assert body["to"] == "2026-01-31"
        assert body["clamped"] is False

    def test_over_90_days_clamped_to_from_plus_90(self, client, auth_mock):
        _setup_auth(auth_mock)
        with self._mock_bq():
            resp = client.get(
                f"{self.BASE}?from=2026-01-01&to=2027-01-01",
                headers=_bearer(),
            )
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["from"] == "2026-01-01"
        expected_to = (date(2026, 1, 1) + timedelta(days=MAX_RANGE_DAYS)).isoformat()
        assert body["to"] == expected_to
        assert body["clamped"] is True

    def test_clamped_range_still_returns_200(self, client, auth_mock):
        _setup_auth(auth_mock)
        with self._mock_bq():
            resp = client.get(
                f"{self.BASE}?from=2025-01-01&to=2026-12-31",
                headers=_bearer(),
            )
        assert resp.status_code == 200


class TestReportEndpointTruncation:
    """Exit criterion 3: X-Truncated: true returned when row limit is hit."""

    BASE_PEOPLE = "/api/v1/customers/cust-abc/reporting/people-day"
    BASE_ALERTS = "/api/v1/customers/cust-abc/reporting/alerts"

    def _mock_raw(self, rows, truncated):
        mock_client = MagicMock()
        mock_client.run_raw_events.return_value = (rows, truncated)
        return patch("routes.v1.report.BqClient", return_value=mock_client)

    def test_truncated_true_header_when_limit_hit(self, client, auth_mock):
        _setup_auth(auth_mock)
        fake_rows = [{"tagId": f"t{i}"} for i in range(DEFAULT_LIMIT)]
        with self._mock_raw(fake_rows, truncated=True):
            resp = client.get(
                f"{self.BASE_PEOPLE}?from=2026-01-01&to=2026-01-31",
                headers=_bearer(),
            )
        assert resp.status_code == 200
        assert resp.headers.get("X-Truncated") == "true"

    def test_no_truncated_header_when_under_limit(self, client, auth_mock):
        _setup_auth(auth_mock)
        fake_rows = [{"tagId": "t1"}]
        with self._mock_raw(fake_rows, truncated=False):
            resp = client.get(
                f"{self.BASE_PEOPLE}?from=2026-01-01&to=2026-01-31",
                headers=_bearer(),
            )
        assert resp.status_code == 200
        assert "X-Truncated" not in resp.headers

    def test_truncated_header_on_alerts_endpoint(self, client, auth_mock):
        _setup_auth(auth_mock)
        fake_rows = [{"geofenceId": f"g{i}"} for i in range(DEFAULT_LIMIT)]
        with self._mock_raw(fake_rows, truncated=True):
            resp = client.get(
                f"{self.BASE_ALERTS}?from=2026-01-01&to=2026-01-31",
                headers=_bearer(),
            )
        assert resp.status_code == 200
        assert resp.headers.get("X-Truncated") == "true"

    def test_custom_limit_passed_through(self, client, auth_mock):
        """Caller-supplied limit is respected (within MAX_LIMIT)."""
        _setup_auth(auth_mock)
        mock_client = MagicMock()
        mock_client.run_raw_events.return_value = ([], False)
        with patch("routes.v1.report.BqClient", return_value=mock_client):
            resp = client.get(
                f"{self.BASE_PEOPLE}?from=2026-01-01&to=2026-01-31&limit=200",
                headers=_bearer(),
            )
        assert resp.status_code == 200
        _, kwargs = mock_client.run_raw_events.call_args
        assert kwargs.get("limit") == 200

    def test_limit_above_max_clamped_to_max(self, client, auth_mock):
        _setup_auth(auth_mock)
        mock_client = MagicMock()
        mock_client.run_raw_events.return_value = ([], False)
        with patch("routes.v1.report.BqClient", return_value=mock_client):
            resp = client.get(
                f"{self.BASE_PEOPLE}?from=2026-01-01&to=2026-01-31&limit=99999",
                headers=_bearer(),
            )
        assert resp.status_code == 200
        _, kwargs = mock_client.run_raw_events.call_args
        assert kwargs.get("limit") == MAX_LIMIT


# ===========================================================================
# BqClient.run_raw_events — unit tests for truncation logic
# ===========================================================================


class TestBqClientTruncation:
    """Verify truncation is detected when row count meets the limit."""

    def _make_client(self, result_rows):
        """Return a BqClient whose underlying BQ client returns *result_rows*."""
        client = BqClient.__new__(BqClient)
        bq_mock = MagicMock()
        bq_mock.ScalarQueryParameter = _bq_mock.ScalarQueryParameter
        bq_mock.QueryJobConfig = MagicMock(side_effect=lambda **kw: MagicMock(**kw))

        # Fake query result
        fake_result = [dict(row) for row in result_rows]
        bq_mock.Client.return_value.query.return_value.result.return_value = [
            MagicMock(**{k: v for k, v in row.items()}, __iter__=lambda self: iter(row.items()))
            for row in fake_result
        ]
        # Simpler: just return list of MagicMock that dict() can convert
        result_mocks = []
        for row in fake_result:
            m = MagicMock()
            m.__iter__ = MagicMock(return_value=iter(row.items()))
            result_mocks.append(m)
        bq_mock.Client.return_value.query.return_value.result.return_value = result_mocks

        client._bq = bq_mock
        client._client = bq_mock.Client()
        return client

    def test_truncated_true_when_rows_equal_limit(self):
        limit = 3
        rows = [{"id": i} for i in range(limit)]
        bq_client = self._make_client(rows)

        with patch.object(
            bq_client._client, "query"
        ) as mock_query:
            row_mocks = []
            for row in rows:
                m = MagicMock()
                m.__iter__ = MagicMock(return_value=iter(row.items()))
                row_mocks.append(m)
            mock_query.return_value.result.return_value = row_mocks

            _, truncated = bq_client.run_raw_events("SELECT 1 LIMIT @limit", [], limit=limit)

        assert truncated is True

    def test_truncated_false_when_rows_below_limit(self):
        limit = 10
        rows = [{"id": i} for i in range(3)]  # fewer than limit
        bq_client = self._make_client(rows)

        with patch.object(bq_client._client, "query") as mock_query:
            row_mocks = []
            for row in rows:
                m = MagicMock()
                m.__iter__ = MagicMock(return_value=iter(row.items()))
                row_mocks.append(m)
            mock_query.return_value.result.return_value = row_mocks

            _, truncated = bq_client.run_raw_events("SELECT 1 LIMIT @limit", [], limit=limit)

        assert truncated is False
