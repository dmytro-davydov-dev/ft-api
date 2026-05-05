"""GET /api/v1/customers/<id>/events — raw location-event stream.

Returns recent BLE tag detection events from the ``location_events`` BigQuery
table, scoped to the authenticated tenant, ordered newest-first.

This endpoint powers the Events Stream page in ft-web-app.  It is a raw-event
query (same cost-guard pattern as R4/R5 in report.py):

* ``from`` / ``to`` are required date parameters (partition pruning).
* ``limit`` is optional; default 1 000, max 5 000.
* ``siteId`` is optional; filters to a single site.
* ``X-Truncated: true`` is set on the response when the row cap is hit.

URL: GET /api/v1/customers/<id>/events

Query parameters
----------------
from     : YYYY-MM-DD  inclusive start date (required)
to       : YYYY-MM-DD  inclusive end date   (required)
siteId   : string      optional site filter
limit    : integer     row cap (default 1 000, max 5 000)
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
# Blueprint
# ---------------------------------------------------------------------------

events_bp = Blueprint(
    "events",
    __name__,
    url_prefix="/customers/<id>",
)

events_bp.before_request(require_tenant())

# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------

_SQL_EVENTS = """\
SELECT
  event_ts,
  tag_id,
  gateway_id,
  area_id,
  zone_id,
  floor,
  site_id,
  rssi,
  battery_pct
FROM {table}
WHERE
  DATE(event_ts) BETWEEN @fromDate AND @toDate
  AND customer_id = @customerId
  {site_filter}
ORDER BY event_ts DESC
LIMIT @limit
"""


# ---------------------------------------------------------------------------
# Route
# ---------------------------------------------------------------------------


@events_bp.get("/events")
def list_events(id: str):  # noqa: A002
    """Return recent location events for the authenticated tenant."""
    window = parse_and_clamp_dates(request.args.get("from"), request.args.get("to"))
    limit, _ = clamp_limit(_parse_limit(request.args.get("limit")))
    site_sql, site_params = build_site_filter(request.args.get("siteId"))

    sql = _SQL_EVENTS.format(
        table=bq_table("location_events"),
        site_filter=site_sql,
    )
    params = build_report_params(g.bq_customer_id, window) + site_params
    rows, truncated = BqClient().run_raw_events(sql, params, limit=limit)

    resp = make_response(
        jsonify(
            {
                "customerId": g.bq_customer_id,
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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_limit(raw: str | None) -> int | None:
    if raw is None:
        return None
    try:
        return int(raw)
    except ValueError:
        return None
