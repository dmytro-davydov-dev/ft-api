"""routes/v1/report.py — BigQuery reporting endpoints for ft-api.

URL pattern:  GET /api/v1/customers/<id>/reporting/<slug>

All routes are protected by :func:`auth.tenant.require_tenant` (applied once
at the Blueprint level) and cost-guard middleware (:func:`report.bq_client.parse_and_clamp_dates`).

Slug → report mapping (MVP R1–R5)
----------------------------------
| Slug                  | Report                            | Type      |
|-----------------------|-----------------------------------|-----------|
| occupancy-area        | Per-area occupancy time-series    | aggregate |
| occupancy-floor       | Per-floor occupancy time-series   | aggregate |
| utilisation-building  | Building utilisation %            | aggregate |
| people-day            | People-day counts (R4)            | raw-event |
| alerts                | Geofence alert history (R5)       | raw-event |

R4 and R5 (raw-event) enforce ``LIMIT @limit`` and return ``X-Truncated: true``
when the row count hits the cap.  The full SQL bodies will be added in FLO-31;
stubs are in place so cost-guard integration can be verified independently.
"""

from __future__ import annotations

from flask import Blueprint, g, jsonify, make_response, request

from auth.tenant import require_tenant
from report.bq_client import (
    DEFAULT_LIMIT,
    MAX_LIMIT,
    BqClient,
    build_report_params,
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
# Aggregate reports (R1–R3) — no row limit
# ---------------------------------------------------------------------------

# SQL bodies are stubs; full queries delivered in FLO-31.

_SQL_OCCUPANCY_AREA = """\
-- R1: per-area occupancy time-series
-- TODO(FLO-31): implement full SQL
SELECT CAST(@fromDate AS DATE) AS placeholder
WHERE 1=0
"""

_SQL_OCCUPANCY_FLOOR = """\
-- R2: per-floor occupancy time-series
-- TODO(FLO-31): implement full SQL
SELECT CAST(@fromDate AS DATE) AS placeholder
WHERE 1=0
"""

_SQL_UTILISATION_BUILDING = """\
-- R3: building utilisation %
-- TODO(FLO-31): implement full SQL
SELECT CAST(@fromDate AS DATE) AS placeholder
WHERE 1=0
"""


@report_bp.get("/occupancy-area")
def occupancy_area(id: str):  # noqa: A002 — id is the URL param
    """R1 — Per-area occupancy time-series."""
    window = parse_and_clamp_dates(request.args.get("from"), request.args.get("to"))
    params = build_report_params(g.bq_customer_id, window)
    rows = BqClient().run_report(_SQL_OCCUPANCY_AREA, params)
    return _make_report_response(rows, window)


@report_bp.get("/occupancy-floor")
def occupancy_floor(id: str):  # noqa: A002
    """R2 — Per-floor occupancy time-series."""
    window = parse_and_clamp_dates(request.args.get("from"), request.args.get("to"))
    params = build_report_params(g.bq_customer_id, window)
    rows = BqClient().run_report(_SQL_OCCUPANCY_FLOOR, params)
    return _make_report_response(rows, window)


@report_bp.get("/utilisation-building")
def utilisation_building(id: str):  # noqa: A002
    """R3 — Building utilisation %."""
    window = parse_and_clamp_dates(request.args.get("from"), request.args.get("to"))
    params = build_report_params(g.bq_customer_id, window)
    rows = BqClient().run_report(_SQL_UTILISATION_BUILDING, params)
    return _make_report_response(rows, window)


# ---------------------------------------------------------------------------
# Raw-event reports (R4, R5) — LIMIT @limit enforced
# ---------------------------------------------------------------------------

# NOTE: SQL must end with `LIMIT @limit` — BqClient.run_raw_events injects
# the @limit INT64 parameter automatically.

_SQL_PEOPLE_DAY = """\
-- R4: people-day counts (raw events)
-- TODO(FLO-31): implement full SQL
SELECT CAST(@fromDate AS DATE) AS placeholder
WHERE 1=0
LIMIT @limit
"""

_SQL_ALERTS = """\
-- R5: geofence alert history (raw events)
-- TODO(FLO-31): implement full SQL
SELECT CAST(@fromDate AS DATE) AS placeholder
WHERE 1=0
LIMIT @limit
"""


@report_bp.get("/people-day")
def people_day(id: str):  # noqa: A002
    """R4 — People-day counts (raw events, LIMIT enforced)."""
    window = parse_and_clamp_dates(request.args.get("from"), request.args.get("to"))
    limit, _ = clamp_limit(_parse_limit(request.args.get("limit")))
    params = build_report_params(g.bq_customer_id, window)
    rows, truncated = BqClient().run_raw_events(_SQL_PEOPLE_DAY, params, limit=limit)
    return _make_report_response(rows, window, truncated=truncated)


@report_bp.get("/alerts")
def alerts(id: str):  # noqa: A002
    """R5 — Geofence alert history (raw events, LIMIT enforced)."""
    window = parse_and_clamp_dates(request.args.get("from"), request.args.get("to"))
    limit, _ = clamp_limit(_parse_limit(request.args.get("limit")))
    params = build_report_params(g.bq_customer_id, window)
    rows, truncated = BqClient().run_raw_events(_SQL_ALERTS, params, limit=limit)
    return _make_report_response(rows, window, truncated=truncated)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_report_response(
    rows: list[dict],
    window,
    *,
    truncated: bool = False,
) -> object:
    """Build the JSON response, reflecting the (possibly clamped) date window."""
    resp = make_response(
        jsonify(
            {
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
    """Parse the ``limit`` query param; return ``None`` if absent or invalid."""
    if raw is None:
        return None
    try:
        return int(raw)
    except ValueError:
        return None
