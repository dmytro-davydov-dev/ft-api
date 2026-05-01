"""FLO-38 — Consolidated endpoint unit tests for all 5 report routes.

File: api/tests/test_report.py

Coverage (all 9 scenarios required by FLO-38)
----------------------------------------------
| # | Test                          | Assertion                                                   |
|---|-------------------------------|-------------------------------------------------------------|
| 1 | R1 occupancy/area — happy     | HTTP 200, rows[*] contain areaId, hour, tagCount            |
| 2 | R2 occupancy/floor — happy    | HTTP 200, rows[*] contain floor, hour, tagCount             |
| 3 | R3 utilisation/building—happy | HTTP 200, rows[*] contain day, utilisation_pct              |
| 4 | R4 people-day — happy         | HTTP 200, rows[*] contain tagId, first_seen, last_seen,     |
|   |                               |   duration_min                                              |
| 5 | R5 alerts — happy             | HTTP 200, rows[*] contain geofenceId, tagId, event, ts      |
| 6 | Missing date param            | HTTP 400 from cost-guard before any BQ call                 |
| 7 | Cross-tenant isolation        | tenant-abc JWT → tenant-xyz URL returns 403                 |
| 8 | Date range > 90 days          | HTTP 200, response body includes clamped=true               |
| 9 | R4/R5 row limit               | X-Truncated: true header when limit is exceeded             |

BqClient is mocked throughout — no real BigQuery calls are made.
Seeded row shapes mirror what seed_bq.py (FLO-30) inserts into dev BQ.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from report.bq_client import DEFAULT_LIMIT, MAX_LIMIT

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

_CUSTOMER_ID = "tenant-abc"
_VALID_CLAIM = {"uid": "user-1", "customerId": _CUSTOMER_ID}
_DATE_RANGE = "from=2026-04-19&to=2026-04-26"
_WIDE_RANGE = "from=2025-01-01&to=2026-04-30"  # > 90 days → triggers clamp


def _url(slug: str, qs: str = _DATE_RANGE, customer: str = _CUSTOMER_ID) -> str:
    return f"/api/v1/customers/{customer}/reporting/{slug}?{qs}"


def _bearer(token: str = "tok") -> dict:
    return {"Authorization": f"Bearer {token}"}


def _setup_auth(auth_mock, claim: dict = _VALID_CLAIM) -> None:
    auth_mock.verify_id_token.side_effect = None
    auth_mock.verify_id_token.return_value = claim


def _mock_aggregate(rows=None):
    """Patch BqClient so run_report returns *rows*."""
    mock = MagicMock()
    mock.run_report.return_value = rows if rows is not None else []
    return patch("routes.v1.report.BqClient", return_value=mock)


def _mock_raw_events(rows=None, truncated: bool = False):
    """Patch BqClient so run_raw_events returns (rows, truncated)."""
    mock = MagicMock()
    mock.run_raw_events.return_value = (rows if rows is not None else [], truncated)
    return patch("routes.v1.report.BqClient", return_value=mock)


# ---------------------------------------------------------------------------
# Seeded row fixtures — mirror the schema that seed_bq.py inserts into dev BQ
# ---------------------------------------------------------------------------

_ROWS_OCCUPANCY_AREA = [
    {"areaId": "zone-reception",  "hour": "2026-04-26T09:00:00+00:00", "tagCount": 12},
    {"areaId": "zone-open-plan",  "hour": "2026-04-26T09:00:00+00:00", "tagCount": 34},
    {"areaId": "zone-meeting-a",  "hour": "2026-04-26T10:00:00+00:00", "tagCount": 5},
]

_ROWS_OCCUPANCY_FLOOR = [
    {"floor": 1, "hour": "2026-04-26T09:00:00+00:00", "tagCount": 18},
    {"floor": 2, "hour": "2026-04-26T09:00:00+00:00", "tagCount": 42},
    {"floor": 1, "hour": "2026-04-26T10:00:00+00:00", "tagCount": 21},
]

_ROWS_UTILISATION_BUILDING = [
    {"day": "2026-04-24", "occupied_hours": 8,  "total_hours": 24, "utilisation_pct": 33.33},
    {"day": "2026-04-25", "occupied_hours": 12, "total_hours": 24, "utilisation_pct": 50.0},
    {"day": "2026-04-26", "occupied_hours": 6,  "total_hours": 24, "utilisation_pct": 25.0},
]

_ROWS_PEOPLE_DAY = [
    {
        "tagId": "badge-001",
        "day": "2026-04-26",
        "first_seen": "2026-04-26T08:02:00+00:00",
        "last_seen":  "2026-04-26T17:45:00+00:00",
        "duration_min": 583,
    },
    {
        "tagId": "badge-042",
        "day": "2026-04-26",
        "first_seen": "2026-04-26T09:15:00+00:00",
        "last_seen":  "2026-04-26T16:30:00+00:00",
        "duration_min": 435,
    },
]

_ROWS_ALERTS = [
    {
        "geofenceId": "fence-loading-bay",
        "tagId": "badge-001",
        "event": "enter",
        "ts": "2026-04-26T09:15:00+00:00",
    },
    {
        "geofenceId": "fence-loading-bay",
        "tagId": "badge-001",
        "event": "exit",
        "ts": "2026-04-26T09:47:00+00:00",
    },
    {
        "geofenceId": "fence-server-room",
        "tagId": "badge-007",
        "event": "enter",
        "ts": "2026-04-26T14:02:00+00:00",
    },
]


# ===========================================================================
# Test 1 — R1: GET /occupancy/area happy path
# ===========================================================================


class TestR1OccupancyAreaHappyPath:
    """HTTP 200; rows contain areaId, hour, tagCount."""

    def test_returns_200(self, client, auth_mock):
        _setup_auth(auth_mock)
        with _mock_aggregate(_ROWS_OCCUPANCY_AREA):
            resp = client.get(_url("occupancy/area"), headers=_bearer())
        assert resp.status_code == 200

    def test_rows_contain_area_id(self, client, auth_mock):
        _setup_auth(auth_mock)
        with _mock_aggregate(_ROWS_OCCUPANCY_AREA):
            resp = client.get(_url("occupancy/area"), headers=_bearer())
        assert all("areaId" in row for row in resp.get_json()["rows"])

    def test_rows_contain_hour(self, client, auth_mock):
        _setup_auth(auth_mock)
        with _mock_aggregate(_ROWS_OCCUPANCY_AREA):
            resp = client.get(_url("occupancy/area"), headers=_bearer())
        assert all("hour" in row for row in resp.get_json()["rows"])

    def test_rows_contain_tag_count(self, client, auth_mock):
        _setup_auth(auth_mock)
        with _mock_aggregate(_ROWS_OCCUPANCY_AREA):
            resp = client.get(_url("occupancy/area"), headers=_bearer())
        assert all("tagCount" in row for row in resp.get_json()["rows"])

    def test_report_type_correct(self, client, auth_mock):
        _setup_auth(auth_mock)
        with _mock_aggregate(_ROWS_OCCUPANCY_AREA):
            resp = client.get(_url("occupancy/area"), headers=_bearer())
        assert resp.get_json()["reportType"] == "occupancy/area"

    def test_row_values_match_seeded_data(self, client, auth_mock):
        _setup_auth(auth_mock)
        with _mock_aggregate(_ROWS_OCCUPANCY_AREA):
            resp = client.get(_url("occupancy/area"), headers=_bearer())
        rows = resp.get_json()["rows"]
        assert rows[0]["areaId"] == "zone-reception"
        assert rows[0]["tagCount"] == 12


# ===========================================================================
# Test 2 — R2: GET /occupancy/floor happy path
# ===========================================================================


class TestR2OccupancyFloorHappyPath:
    """HTTP 200; rows contain floor, hour, tagCount."""

    def test_returns_200(self, client, auth_mock):
        _setup_auth(auth_mock)
        with _mock_aggregate(_ROWS_OCCUPANCY_FLOOR):
            resp = client.get(_url("occupancy/floor"), headers=_bearer())
        assert resp.status_code == 200

    def test_rows_contain_floor(self, client, auth_mock):
        _setup_auth(auth_mock)
        with _mock_aggregate(_ROWS_OCCUPANCY_FLOOR):
            resp = client.get(_url("occupancy/floor"), headers=_bearer())
        assert all("floor" in row for row in resp.get_json()["rows"])

    def test_rows_contain_hour(self, client, auth_mock):
        _setup_auth(auth_mock)
        with _mock_aggregate(_ROWS_OCCUPANCY_FLOOR):
            resp = client.get(_url("occupancy/floor"), headers=_bearer())
        assert all("hour" in row for row in resp.get_json()["rows"])

    def test_rows_contain_tag_count(self, client, auth_mock):
        _setup_auth(auth_mock)
        with _mock_aggregate(_ROWS_OCCUPANCY_FLOOR):
            resp = client.get(_url("occupancy/floor"), headers=_bearer())
        assert all("tagCount" in row for row in resp.get_json()["rows"])

    def test_report_type_correct(self, client, auth_mock):
        _setup_auth(auth_mock)
        with _mock_aggregate(_ROWS_OCCUPANCY_FLOOR):
            resp = client.get(_url("occupancy/floor"), headers=_bearer())
        assert resp.get_json()["reportType"] == "occupancy/floor"

    def test_row_values_match_seeded_data(self, client, auth_mock):
        _setup_auth(auth_mock)
        with _mock_aggregate(_ROWS_OCCUPANCY_FLOOR):
            resp = client.get(_url("occupancy/floor"), headers=_bearer())
        rows = resp.get_json()["rows"]
        assert rows[0]["floor"] == 1
        assert rows[1]["floor"] == 2
        assert rows[0]["tagCount"] == 18


# ===========================================================================
# Test 3 — R3: GET /utilisation/building happy path
# ===========================================================================


class TestR3UtilisationBuildingHappyPath:
    """HTTP 200; rows contain day, utilisation_pct.

    Note: the BQ SQL uses snake_case column names (day, utilisation_pct).
    The FLO-38 ticket table uses 'date'/'utilisationPct' as shorthand —
    the actual column names in the response are 'day' and 'utilisation_pct'.
    """

    def test_returns_200(self, client, auth_mock):
        _setup_auth(auth_mock)
        with _mock_aggregate(_ROWS_UTILISATION_BUILDING):
            resp = client.get(_url("utilisation/building"), headers=_bearer())
        assert resp.status_code == 200

    def test_rows_contain_day(self, client, auth_mock):
        _setup_auth(auth_mock)
        with _mock_aggregate(_ROWS_UTILISATION_BUILDING):
            resp = client.get(_url("utilisation/building"), headers=_bearer())
        assert all("day" in row for row in resp.get_json()["rows"])

    def test_rows_contain_utilisation_pct(self, client, auth_mock):
        _setup_auth(auth_mock)
        with _mock_aggregate(_ROWS_UTILISATION_BUILDING):
            resp = client.get(_url("utilisation/building"), headers=_bearer())
        assert all("utilisation_pct" in row for row in resp.get_json()["rows"])

    def test_report_type_correct(self, client, auth_mock):
        _setup_auth(auth_mock)
        with _mock_aggregate(_ROWS_UTILISATION_BUILDING):
            resp = client.get(_url("utilisation/building"), headers=_bearer())
        assert resp.get_json()["reportType"] == "utilisation/building"

    def test_row_values_match_seeded_data(self, client, auth_mock):
        _setup_auth(auth_mock)
        with _mock_aggregate(_ROWS_UTILISATION_BUILDING):
            resp = client.get(_url("utilisation/building"), headers=_bearer())
        rows = resp.get_json()["rows"]
        assert rows[0]["day"] == "2026-04-24"
        assert rows[0]["utilisation_pct"] == 33.33
        assert rows[1]["utilisation_pct"] == 50.0


# ===========================================================================
# Test 4 — R4: GET /people-day happy path
# ===========================================================================


class TestR4PeopleDayHappyPath:
    """HTTP 200; rows contain tagId, first_seen, last_seen, duration_min."""

    def test_returns_200(self, client, auth_mock):
        _setup_auth(auth_mock)
        with _mock_raw_events(_ROWS_PEOPLE_DAY):
            resp = client.get(_url("people-day"), headers=_bearer())
        assert resp.status_code == 200

    def test_rows_contain_tag_id(self, client, auth_mock):
        _setup_auth(auth_mock)
        with _mock_raw_events(_ROWS_PEOPLE_DAY):
            resp = client.get(_url("people-day"), headers=_bearer())
        assert all("tagId" in row for row in resp.get_json()["rows"])

    def test_rows_contain_first_seen(self, client, auth_mock):
        _setup_auth(auth_mock)
        with _mock_raw_events(_ROWS_PEOPLE_DAY):
            resp = client.get(_url("people-day"), headers=_bearer())
        assert all("first_seen" in row for row in resp.get_json()["rows"])

    def test_rows_contain_last_seen(self, client, auth_mock):
        _setup_auth(auth_mock)
        with _mock_raw_events(_ROWS_PEOPLE_DAY):
            resp = client.get(_url("people-day"), headers=_bearer())
        assert all("last_seen" in row for row in resp.get_json()["rows"])

    def test_rows_contain_duration_min(self, client, auth_mock):
        _setup_auth(auth_mock)
        with _mock_raw_events(_ROWS_PEOPLE_DAY):
            resp = client.get(_url("people-day"), headers=_bearer())
        assert all("duration_min" in row for row in resp.get_json()["rows"])

    def test_report_type_correct(self, client, auth_mock):
        _setup_auth(auth_mock)
        with _mock_raw_events(_ROWS_PEOPLE_DAY):
            resp = client.get(_url("people-day"), headers=_bearer())
        assert resp.get_json()["reportType"] == "people-day"

    def test_row_values_match_seeded_data(self, client, auth_mock):
        _setup_auth(auth_mock)
        with _mock_raw_events(_ROWS_PEOPLE_DAY):
            resp = client.get(_url("people-day"), headers=_bearer())
        rows = resp.get_json()["rows"]
        assert rows[0]["tagId"] == "badge-001"
        assert rows[0]["duration_min"] == 583
        assert rows[1]["tagId"] == "badge-042"


# ===========================================================================
# Test 5 — R5: GET /alerts happy path
# ===========================================================================


class TestR5AlertsHappyPath:
    """HTTP 200; rows contain geofenceId, tagId, event, ts."""

    def test_returns_200(self, client, auth_mock):
        _setup_auth(auth_mock)
        with _mock_raw_events(_ROWS_ALERTS):
            resp = client.get(_url("alerts"), headers=_bearer())
        assert resp.status_code == 200

    def test_rows_contain_geofence_id(self, client, auth_mock):
        _setup_auth(auth_mock)
        with _mock_raw_events(_ROWS_ALERTS):
            resp = client.get(_url("alerts"), headers=_bearer())
        assert all("geofenceId" in row for row in resp.get_json()["rows"])

    def test_rows_contain_tag_id(self, client, auth_mock):
        _setup_auth(auth_mock)
        with _mock_raw_events(_ROWS_ALERTS):
            resp = client.get(_url("alerts"), headers=_bearer())
        assert all("tagId" in row for row in resp.get_json()["rows"])

    def test_rows_contain_event(self, client, auth_mock):
        _setup_auth(auth_mock)
        with _mock_raw_events(_ROWS_ALERTS):
            resp = client.get(_url("alerts"), headers=_bearer())
        assert all("event" in row for row in resp.get_json()["rows"])

    def test_rows_contain_ts(self, client, auth_mock):
        _setup_auth(auth_mock)
        with _mock_raw_events(_ROWS_ALERTS):
            resp = client.get(_url("alerts"), headers=_bearer())
        assert all("ts" in row for row in resp.get_json()["rows"])

    def test_report_type_correct(self, client, auth_mock):
        _setup_auth(auth_mock)
        with _mock_raw_events(_ROWS_ALERTS):
            resp = client.get(_url("alerts"), headers=_bearer())
        assert resp.get_json()["reportType"] == "alerts"

    def test_row_values_match_seeded_data(self, client, auth_mock):
        _setup_auth(auth_mock)
        with _mock_raw_events(_ROWS_ALERTS):
            resp = client.get(_url("alerts"), headers=_bearer())
        rows = resp.get_json()["rows"]
        assert rows[0]["geofenceId"] == "fence-loading-bay"
        assert rows[0]["event"] == "enter"
        assert rows[2]["tagId"] == "badge-007"


# ===========================================================================
# Test 6 — Missing date param → HTTP 400 (cost-guard)
# Applies to all 5 endpoints.
# ===========================================================================


class TestMissingDateParam:
    """Cost-guard middleware must return 400 before any BQ call when dates are absent."""

    @pytest.mark.parametrize("slug", [
        "occupancy/area",
        "occupancy/floor",
        "utilisation/building",
        "people-day",
        "alerts",
    ])
    def test_missing_both_dates_returns_400(self, slug, client, auth_mock):
        _setup_auth(auth_mock)
        resp = client.get(
            f"/api/v1/customers/{_CUSTOMER_ID}/reporting/{slug}",
            headers=_bearer(),
        )
        assert resp.status_code == 400

    @pytest.mark.parametrize("slug", [
        "occupancy/area",
        "occupancy/floor",
        "utilisation/building",
        "people-day",
        "alerts",
    ])
    def test_missing_from_returns_400(self, slug, client, auth_mock):
        _setup_auth(auth_mock)
        resp = client.get(
            f"/api/v1/customers/{_CUSTOMER_ID}/reporting/{slug}?to=2026-04-26",
            headers=_bearer(),
        )
        assert resp.status_code == 400

    @pytest.mark.parametrize("slug", [
        "occupancy/area",
        "occupancy/floor",
        "utilisation/building",
        "people-day",
        "alerts",
    ])
    def test_missing_to_returns_400(self, slug, client, auth_mock):
        _setup_auth(auth_mock)
        resp = client.get(
            f"/api/v1/customers/{_CUSTOMER_ID}/reporting/{slug}?from=2026-04-19",
            headers=_bearer(),
        )
        assert resp.status_code == 400

    def test_bq_not_called_when_date_missing(self, client, auth_mock):
        """BQ must not be called at all — cost-guard fires first."""
        _setup_auth(auth_mock)
        mock_client = MagicMock()
        with patch("routes.v1.report.BqClient", return_value=mock_client):
            client.get(
                f"/api/v1/customers/{_CUSTOMER_ID}/reporting/occupancy/area",
                headers=_bearer(),
            )
        mock_client.run_report.assert_not_called()
        mock_client.run_raw_events.assert_not_called()


# ===========================================================================
# Test 7 — Cross-tenant isolation: tenant-abc JWT cannot access tenant-xyz URL
# ===========================================================================


class TestCrossTenantIsolation:
    """tenant-abc JWT must be rejected with 403 when accessing tenant-xyz resources."""

    @pytest.mark.parametrize("slug", [
        "occupancy/area",
        "occupancy/floor",
        "utilisation/building",
        "people-day",
        "alerts",
    ])
    def test_mismatched_tenant_returns_403(self, slug, client, auth_mock):
        """JWT for tenant-abc cannot retrieve rows for tenant-xyz."""
        _setup_auth(auth_mock, claim={"uid": "u1", "customerId": "tenant-abc"})
        resp = client.get(
            f"/api/v1/customers/tenant-xyz/reporting/{slug}?{_DATE_RANGE}",
            headers=_bearer(),
        )
        assert resp.status_code == 403

    def test_cross_tenant_does_not_reach_bq(self, client, auth_mock):
        """BQ must not be called when tenant check fails — no quota consumed."""
        _setup_auth(auth_mock, claim={"uid": "u1", "customerId": "tenant-abc"})
        mock_client = MagicMock()
        with patch("routes.v1.report.BqClient", return_value=mock_client):
            client.get(
                f"/api/v1/customers/tenant-xyz/reporting/occupancy/area?{_DATE_RANGE}",
                headers=_bearer(),
            )
        mock_client.run_report.assert_not_called()
        mock_client.run_raw_events.assert_not_called()

    def test_unauthenticated_returns_401(self, client):
        resp = client.get(
            f"/api/v1/customers/{_CUSTOMER_ID}/reporting/occupancy/area?{_DATE_RANGE}"
        )
        assert resp.status_code == 401


# ===========================================================================
# Test 8 — Date range > 90 days → 200 with clamped window in response
# ===========================================================================


class TestDateRangeClamping:
    """A range exceeding 90 days must be clamped silently; response reflects clamped=true."""

    @pytest.mark.parametrize("slug,use_raw", [
        ("occupancy/area", False),
        ("occupancy/floor", False),
        ("utilisation/building", False),
        ("people-day", True),
        ("alerts", True),
    ])
    def test_wide_range_returns_200(self, slug, use_raw, client, auth_mock):
        """Request with > 90-day window must still return 200 (not 400)."""
        _setup_auth(auth_mock)
        ctx = _mock_raw_events() if use_raw else _mock_aggregate()
        with ctx:
            resp = client.get(_url(slug, qs=_WIDE_RANGE), headers=_bearer())
        assert resp.status_code == 200

    @pytest.mark.parametrize("slug,use_raw", [
        ("occupancy/area", False),
        ("occupancy/floor", False),
        ("utilisation/building", False),
        ("people-day", True),
        ("alerts", True),
    ])
    def test_wide_range_response_has_clamped_true(self, slug, use_raw, client, auth_mock):
        """Response body must contain clamped=true when range was trimmed."""
        _setup_auth(auth_mock)
        ctx = _mock_raw_events() if use_raw else _mock_aggregate()
        with ctx:
            resp = client.get(_url(slug, qs=_WIDE_RANGE), headers=_bearer())
        assert resp.get_json()["clamped"] is True

    def test_clamped_to_date_is_at_most_90_days_after_from(self, client, auth_mock):
        """Clamped 'to' date must be at most 90 days after 'from' date."""
        from datetime import date

        _setup_auth(auth_mock)
        with _mock_aggregate():
            resp = client.get(_url("occupancy/area", qs=_WIDE_RANGE), headers=_bearer())
        body = resp.get_json()
        from_date = date.fromisoformat(body["from"])
        to_date = date.fromisoformat(body["to"])
        assert (to_date - from_date).days <= 90


# ===========================================================================
# Test 9 — R4/R5 row limit: X-Truncated: true when limit is exceeded
# ===========================================================================


class TestRowLimitTruncation:
    """X-Truncated: true header must appear on R4 and R5 when the row cap is hit."""

    def test_r4_x_truncated_true_when_limit_hit(self, client, auth_mock):
        _setup_auth(auth_mock)
        with _mock_raw_events(_ROWS_PEOPLE_DAY, truncated=True):
            resp = client.get(_url("people-day"), headers=_bearer())
        assert resp.status_code == 200
        assert resp.headers.get("X-Truncated") == "true"

    def test_r5_x_truncated_true_when_limit_hit(self, client, auth_mock):
        _setup_auth(auth_mock)
        with _mock_raw_events(_ROWS_ALERTS, truncated=True):
            resp = client.get(_url("alerts"), headers=_bearer())
        assert resp.status_code == 200
        assert resp.headers.get("X-Truncated") == "true"

    def test_r4_no_x_truncated_header_when_under_limit(self, client, auth_mock):
        _setup_auth(auth_mock)
        with _mock_raw_events(_ROWS_PEOPLE_DAY, truncated=False):
            resp = client.get(_url("people-day"), headers=_bearer())
        assert "X-Truncated" not in resp.headers

    def test_r5_no_x_truncated_header_when_under_limit(self, client, auth_mock):
        _setup_auth(auth_mock)
        with _mock_raw_events(_ROWS_ALERTS, truncated=False):
            resp = client.get(_url("alerts"), headers=_bearer())
        assert "X-Truncated" not in resp.headers

    def test_r4_limit_above_max_clamped_to_max(self, client, auth_mock):
        """Caller cannot exceed MAX_LIMIT even by passing an explicit limit param."""
        _setup_auth(auth_mock)
        mock_client = MagicMock()
        mock_client.run_raw_events.return_value = ([], False)
        with patch("routes.v1.report.BqClient", return_value=mock_client):
            client.get(_url("people-day", qs=f"{_DATE_RANGE}&limit=99999"), headers=_bearer())
        _, kwargs = mock_client.run_raw_events.call_args
        assert kwargs.get("limit") == MAX_LIMIT

    def test_r5_limit_above_max_clamped_to_max(self, client, auth_mock):
        _setup_auth(auth_mock)
        mock_client = MagicMock()
        mock_client.run_raw_events.return_value = ([], False)
        with patch("routes.v1.report.BqClient", return_value=mock_client):
            client.get(_url("alerts", qs=f"{_DATE_RANGE}&limit=99999"), headers=_bearer())
        _, kwargs = mock_client.run_raw_events.call_args
        assert kwargs.get("limit") == MAX_LIMIT

    def test_r4_default_limit_when_not_supplied(self, client, auth_mock):
        _setup_auth(auth_mock)
        mock_client = MagicMock()
        mock_client.run_raw_events.return_value = ([], False)
        with patch("routes.v1.report.BqClient", return_value=mock_client):
            client.get(_url("people-day"), headers=_bearer())
        _, kwargs = mock_client.run_raw_events.call_args
        assert kwargs.get("limit") == DEFAULT_LIMIT

    def test_r5_default_limit_when_not_supplied(self, client, auth_mock):
        _setup_auth(auth_mock)
        mock_client = MagicMock()
        mock_client.run_raw_events.return_value = ([], False)
        with patch("routes.v1.report.BqClient", return_value=mock_client):
            client.get(_url("alerts"), headers=_bearer())
        _, kwargs = mock_client.run_raw_events.call_args
        assert kwargs.get("limit") == DEFAULT_LIMIT

    def test_r1_r3_aggregate_endpoints_have_no_x_truncated(self, client, auth_mock):
        """Aggregate endpoints (R1–R3) must never set X-Truncated."""
        _setup_auth(auth_mock)
        for slug in ("occupancy/area", "occupancy/floor", "utilisation/building"):
            with _mock_aggregate():
                resp = client.get(_url(slug), headers=_bearer())
            assert "X-Truncated" not in resp.headers, (
                f"X-Truncated unexpectedly set on aggregate endpoint {slug}"
            )
