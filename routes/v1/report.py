"""routes/v1/report.py — BigQuery reporting endpoints for ft-api.

URL pattern:  GET /api/v1/customers/<id>/reporting/<slug>

All routes are protected by :func:`auth.tenant.require_tenant` (applied once
at the Blueprint level) and cost-guard middleware via
:func:`report.bq_client.parse_and_clamp_dates`.

Slug → report mapping (MVP R1–R5)
----------------------------------
| Slug                  | Report                            | Type      |
|-----------------------|-----------------------------------|-----------|
| occupancy/area        | Per-area occupancy time-series    | aggregate |
| occupancy/floor       | Per-floor occupancy time-series   | aggregate |
| utilisation/building  | Building utilisation %            | aggregate |
| people-day            | People-day counts (R4)            | raw-event |
| alerts                | Geofence alert history (R5)       | raw-event |

R4 and R5 (raw-event) enforce ``LIMIT @limit`` and return ``X-Truncated: true``
when the row count hits the cap.

Common query parameters (all endpoints)
----------------------------------------
- ``from``    (required) ISO 8601 date — start of reporting window
- ``to``      (required) ISO 8601 date — end of window
- ``siteId``  (optional) filter to a single site
- ``limit``   (optional, R4/R5 only) row cap; default 1 000, max 5 000
"""

from __future__ import annotations

from flask import Blueprint, g, jsonify, make_response, request

from auth.tenant import require_tenant
from report.bq_client import (
    DEFAULT_LIMIT,
    MAX_LIMIT,
    BqClient,
    DateWindow,
    bq_table,
    build_report_params,
    build_site_filter,
    clamp_limit,
    parse_and_clamp_dates,
)

# ---------------------------------------------------------------------------
# Blueprint registration
# ---------------------------------------------------------------------------

report_bp = Blueprint(
    "report",
    __name__,
    url_prefix="/customers/<id>/reporting",
)

# Apply tenant isolation before every request in this blueprint.
# require_tenant() verifies the JWT, checks the URL <id> matches the token
# customerId claim, and injects g.bq_customer_id.
report_bp.before_request(require_tenant())


# ---------------------------------------------------------------------------
# R1 — Per-area occupancy time-series
# ---------------------------------------------------------------------------

_SQL_OCCUPANCY_AREA = """\
SELECT
  area_id,
  TIMESTAMP_TRUNC(event_ts, HOUR)       AS hour,
  COUNT(DISTINCT tag_id)                AS tagCount
FROM {table}
WHERE
  DATE(event_ts) BETWEEN @fromDate AND @toDate
  AND customer_id = @customerId
  AND area_id IS NOT NULL
  {site_filter}
GROUP BY area_id, hour
ORDER BY hour, area_id
"""


@report_bp.get("/occupancy/area")
def occupancy_area(id: str):  # noqa: A002
    """R1 — Per-area occupancy time-series (tag count per hour per area)."""
    window = parse_and_clamp_dates(request.args.get("from"), request.args.get("to"))
    site_sql, site_params = build_site_filter(request.args.get("siteId"))
    sql = _SQL_OCCUPANCY_AREA.format(
        table=bq_table("location_events"),
        site_filter=site_sql,
    )
    params = build_report_params(g.bq_customer_id, window) + site_params
    rows = BqClient().run_report(sql, params)
    return _make_report_response(rows, window, customer_id=g.bq_customer_id, report_type="occupancy/area")


# ---------------------------------------------------------------------------
# R2 — Per-floor occupancy time-series
# ---------------------------------------------------------------------------

_SQL_OCCUPANCY_FLOOR = """\
SELECT
  floor,
  TIMESTAMP_TRUNC(event_ts, HOUR)       AS hour,
  COUNT(DISTINCT tag_id)                AS tagCount
FROM {table}
WHERE
  DATE(event_ts) BETWEEN @fromDate AND @toDate
  AND customer_id = @customerId
  AND floor IS NOT NULL
  {site_filter}
GROUP BY floor, hour
ORDER BY hour, floor
"""


@report_bp.get("/occupancy/floor")
def occupancy_floor(id: str):  # noqa: A002
    """R2 — Per-floor occupancy time-series (tag count per hour per floor)."""
    window = parse_and_clamp_dates(request.args.get("from"), request.args.get("to"))
    site_sql, site_params = build_site_filter(request.args.get("siteId"))
    sql = _SQL_OCCUPANCY_FLOOR.format(
        table=bq_table("location_events"),
        site_filter=site_sql,
    )
    params = build_report_params(g.bq_customer_id, window) + site_params
    rows = BqClient().run_report(sql, params)
    return _make_report_response(rows, window, customer_id=g.bq_customer_id, report_type="occupancy/floor")


# ---------------------------------------------------------------------------
# R3 — Building utilisation %
# ---------------------------------------------------------------------------

_SQL_UTILISATION_BUILDING = """\
WITH hourly AS (
  SELECT
    DATE(event_ts)                                        AS day,
    COUNT(DISTINCT TIMESTAMP_TRUNC(event_ts, HOUR))       AS occupied_hours
  FROM {table}
  WHERE
    DATE(event_ts) BETWEEN @fromDate AND @toDate
    AND customer_id = @customerId
    {site_filter}
  GROUP BY day
)
SELECT
  day,
  occupied_hours,
  24                                                AS total_hours,
  ROUND(occupied_hours / 24.0 * 100, 2)            AS utilisation_pct
FROM hourly
ORDER BY day
"""


@report_bp.get("/utilisation/building")
def utilisation_building(id: str):  # noqa: A002
    """R3 — Building utilisation % (occupied hours / total hours per day)."""
    window = parse_and_clamp_dates(request.args.get("from"), request.args.get("to"))
    site_sql, site_params = build_site_filter(request.args.get("siteId"))
    sql = _SQL_UTILISATION_BUILDING.format(
        table=bq_table("location_events"),
        site_filter=site_sql,
    )
    params = build_report_params(g.bq_customer_id, window) + site_params
    rows = BqClient().run_report(sql, params)
    return _make_report_response(rows, window, customer_id=g.bq_customer_id, report_type="utilisation/building")


# ---------------------------------------------------------------------------
# R4 — People-day report (raw events, LIMIT enforced)
# ---------------------------------------------------------------------------

_SQL_PEOPLE_DAY = """\
SELECT
  tag_id,
  DATE(event_ts)                                            AS day,
  MIN(event_ts)                                             AS first_seen,
  MAX(event_ts)                                             AS last_seen,
  TIMESTAMP_DIFF(MAX(event_ts), MIN(event_ts), MINUTE)      AS duration_min
FROM {table}
WHERE
  DATE(event_ts) BETWEEN @fromDate AND @toDate
  AND customer_id = @customerId
  {site_filter}
GROUP BY tag_id, day
ORDER BY day, tag_id
LIMIT @limit
"""


@report_bp.get("/people-day")
def people_day(id: str):  # noqa: A002
    """R4 — People-day counts (raw events, LIMIT enforced)."""
    window = parse_and_clamp_dates(request.args.get("from"), request.args.get("to"))
    limit, _ = clamp_limit(_parse_limit(request.args.get("limit")))
    site_sql, site_params = build_site_filter(request.args.get("siteId"))
    sql = _SQL_PEOPLE_DAY.format(
        table=bq_table("location_events"),
        site_filter=site_sql,
    )
    params = build_report_params(g.bq_customer_id, window) + site_params
    rows, truncated = BqClient().run_raw_events(sql, params, limit=limit)
    return _make_report_response(
        rows, window,
        customer_id=g.bq_customer_id,
        report_type="people-day",
        truncated=truncated,
    )


# ---------------------------------------------------------------------------
# R5 — Geofence alert history (raw events, LIMIT enforced)
# ---------------------------------------------------------------------------

# Note: geofence_events has no site_id column — siteId filter is not applied.
_SQL_ALERTS = """\
SELECT
  rule_id,
  tag_id,
  event_type,
  triggered_at
FROM {table}
WHERE
  DATE(triggered_at) BETWEEN @fromDate AND @toDate
  AND customer_id = @customerId
ORDER BY triggered_at DESC
LIMIT @limit
"""


@report_bp.get("/alerts")
def alerts(id: str):  # noqa: A002
    """R5 — Geofence alert history (raw events, LIMIT enforced)."""
    window = parse_and_clamp_dates(request.args.get("from"), request.args.get("to"))
    limit, _ = clamp_limit(_parse_limit(request.args.get("limit")))
    sql = _SQL_ALERTS.format(table=bq_table("geofence_events"))
    params = build_report_params(g.bq_customer_id, window)
    rows, truncated = BqClient().run_raw_events(sql, params, limit=limit)
    return _make_report_response(
        rows, window,
        customer_id=g.bq_customer_id,
        report_type="alerts",
        truncated=truncated,
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_report_response(
    rows: list[dict],
    window: DateWindow,
    *,
    customer_id: str,
    report_type: str,
    truncated: bool = False,
) -> object:
    """Build the standard JSON response envelope for all report endpoints.

    Args:
        rows:         Query result rows.
        window:       Validated (possibly clamped) date window.
        customer_id:  Authenticated tenant ID (from ``g.bq_customer_id``).
        report_type:  Human-readable report slug (e.g. ``"occupancy/area"``).
        truncated:    Whether the result was capped by a LIMIT clause.

    Returns:
        Flask response with JSON body and optional ``X-Truncated`` header.
    """
    resp = make_response(
        jsonify(
            {
                "customerId": customer_id,
                "reportType": report_type,
                "from": window.from_date.isoformat(),
                "to": window.to_date.isoformat(),
                "clamped": window.clamped,
                "count": len(rows),
                "rows": rows,
            }
        )
    )
    if truncated:
        resp.headers["X-Truncated"] = "true"
    return resp


def _parse_limit(raw: str | None) -> int | None:
    """Parse the ``limit`` query param; return ``None`` if absent or non-integer."""
    if raw is None:
        return None
    try:
        return int(raw)
    except ValueError:
        return None
