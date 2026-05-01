"""report/bq_client.py — Cost-guard BigQuery client for ft-api.

Enforces partition pruning and cost controls on every BigQuery query.
Applied at the router level via :func:`parse_and_clamp_dates` and
:class:`BqClient` — cannot be bypassed.

Cost-guard rules
----------------

+-------------------------------------+-----------------------------------------+------------------------------------------+
| Rule                                | Enforcement                             | Consequence                              |
+=====================================+=========================================+==========================================+
| Every query must filter on ``ts``   | ``parse_and_clamp_dates`` raises 400    | Endpoint rejects before BQ call          |
| (partition key)                     | when ``from``/``to`` are absent         |                                          |
+-------------------------------------+-----------------------------------------+------------------------------------------+
| Maximum date range: 90 days         | Range silently clamped to               | ``200`` with clamped range               |
|                                     | ``MAX_RANGE_DAYS``                      | reflected in response                    |
+-------------------------------------+-----------------------------------------+------------------------------------------+
| ``LIMIT`` on raw-event queries      | ``LIMIT @limit`` injected as BQ         | ``X-Truncated: true`` header when        |
| (R4, R5)                            | INT64 parameter; default 1 000,         | ``len(rows) >= limit``                   |
|                                     | max 5 000                               |                                          |
+-------------------------------------+-----------------------------------------+------------------------------------------+
| ``customerId`` always parameterised | ``build_report_params`` returns a       | SQL injection prevented; BQ caches       |
|                                     | ``ScalarQueryParameter`` — never        | query plan                               |
|                                     | string interpolation                    |                                          |
+-------------------------------------+-----------------------------------------+------------------------------------------+

Usage (route handler sketch)
-----------------------------

::

    from flask import g, jsonify, make_response, request
    from report.bq_client import BqClient, parse_and_clamp_dates, build_report_params

    # Inside a route protected by require_tenant():
    window = parse_and_clamp_dates(request.args.get("from"), request.args.get("to"))
    params = build_report_params(g.bq_customer_id, window)
    client = BqClient()
    rows, truncated = client.run_raw_events(_MY_SQL, params, limit=window.limit)

    resp = make_response(jsonify({
        "from": window.from_date.isoformat(),
        "to":   window.to_date.isoformat(),
        "rows": rows,
    }))
    if truncated:
        resp.headers["X-Truncated"] = "true"
    return resp

See also
--------
- ``auth/tenant.py`` — :func:`require_tenant` must be applied *before* cost-guard
  so unauthenticated requests are rejected before any BigQuery quota is consumed.
- ``wiki/features/analytics.md`` — report endpoint inventory and schema.
"""

from __future__ import annotations

import os
from datetime import date, timedelta
from typing import TYPE_CHECKING, NamedTuple

from flask import abort

if TYPE_CHECKING:  # pragma: no cover
    from google.cloud import bigquery

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MAX_RANGE_DAYS: int = 90
"""Maximum allowed date range for any BigQuery report query."""

DEFAULT_LIMIT: int = 1_000
"""Default row cap for raw-event queries (R4, R5)."""

MAX_LIMIT: int = 5_000
"""Hard upper bound on the row cap for raw-event queries."""


# ---------------------------------------------------------------------------
# Value objects
# ---------------------------------------------------------------------------


class DateWindow(NamedTuple):
    """Validated, possibly-clamped date range for a report request.

    Attributes:
        from_date: Inclusive start date (after validation and clamping).
        to_date:   Inclusive end date (after validation and clamping).
        clamped:   ``True`` when the original range exceeded :data:`MAX_RANGE_DAYS`
                   and was silently shortened.
    """

    from_date: date
    to_date: date
    clamped: bool


# ---------------------------------------------------------------------------
# Cost-guard helpers (pure functions — no Flask or BQ dependency)
# ---------------------------------------------------------------------------


def parse_and_clamp_dates(
    from_str: str | None,
    to_str: str | None,
) -> DateWindow:
    """Parse ``from``/``to`` query parameters and enforce the 90-day max range.

    Called by every reporting route *before* a BigQuery query is issued.

    Args:
        from_str: Value of the ``from`` query parameter (``YYYY-MM-DD``).
        to_str:   Value of the ``to`` query parameter (``YYYY-MM-DD``).

    Returns:
        A :class:`DateWindow` with validated and (if necessary) clamped dates.

    Raises:
        ``werkzeug.exceptions.BadRequest`` (HTTP 400) when either parameter
        is absent or contains an invalid date string.
        ``werkzeug.exceptions.BadRequest`` (HTTP 400) when ``from`` is after
        ``to``.
    """
    if not from_str or not to_str:
        abort(
            400,
            description=(
                "Missing required query parameters: 'from' and 'to' (YYYY-MM-DD). "
                "All report queries must include a date range to enable partition pruning."
            ),
        )

    try:
        from_date = date.fromisoformat(from_str)
    except ValueError:
        abort(400, description=f"Invalid 'from' date '{from_str}' — use YYYY-MM-DD")

    try:
        to_date = date.fromisoformat(to_str)
    except ValueError:
        abort(400, description=f"Invalid 'to' date '{to_str}' — use YYYY-MM-DD")

    if from_date > to_date:
        abort(400, description="'from' must be on or before 'to'")

    clamped = False
    if (to_date - from_date).days > MAX_RANGE_DAYS:
        to_date = from_date + timedelta(days=MAX_RANGE_DAYS)
        clamped = True

    return DateWindow(from_date=from_date, to_date=to_date, clamped=clamped)


def clamp_limit(requested: int | None) -> tuple[int, bool]:
    """Return ``(effective_limit, was_clamped)``.

    Ensures the limit stays within ``[1, MAX_LIMIT]``.  If *requested* is
    ``None``, :data:`DEFAULT_LIMIT` is used without marking as clamped.

    Args:
        requested: Caller-supplied row limit, or ``None`` for the default.

    Returns:
        A 2-tuple ``(limit, was_clamped)`` where *was_clamped* is ``True``
        only when a positive *requested* value exceeded :data:`MAX_LIMIT`.
    """
    if requested is None:
        return DEFAULT_LIMIT, False
    effective = max(1, min(int(requested), MAX_LIMIT))
    return effective, int(requested) > MAX_LIMIT


# ---------------------------------------------------------------------------
# BigQuery parameter helpers
# ---------------------------------------------------------------------------


def build_report_params(
    customer_id: str,
    window: DateWindow,
) -> list["bigquery.ScalarQueryParameter"]:
    """Build the standard set of BigQuery query parameters for a report.

    Returns a list containing three parameterised values:

    * ``@customerId`` — tenant isolation (STRING)
    * ``@fromDate``   — inclusive start of the date window (DATE)
    * ``@toDate``     — inclusive end of the date window (DATE)

    **Never** interpolate *customer_id* into a SQL string — always pass it
    through this function so BQ can cache the query plan and SQL injection is
    structurally impossible.

    Args:
        customer_id: The authenticated tenant's ID (from ``g.bq_customer_id``).
        window:      Validated date window from :func:`parse_and_clamp_dates`.

    Returns:
        List of :class:`google.cloud.bigquery.ScalarQueryParameter` instances.
    """
    from google.cloud import bigquery  # noqa: PLC0415 — lazy import keeps tests fast

    return [
        bigquery.ScalarQueryParameter("customerId", "STRING", customer_id),
        bigquery.ScalarQueryParameter("fromDate", "DATE", window.from_date.isoformat()),
        bigquery.ScalarQueryParameter("toDate", "DATE", window.to_date.isoformat()),
    ]


def bq_table(table_name: str) -> str:
    """Return a backtick-quoted BigQuery table reference.

    Uses the ``BQ_DATASET`` environment variable (default: ``flowterra_dev``).
    The BigQuery Python client resolves the GCP project from Application Default
    Credentials, so no project prefix is required here.

    Args:
        table_name: Unqualified table name (e.g. ``"location_events"``).

    Returns:
        String of the form `` `dataset.table_name` `` ready to embed in SQL.
    """
    dataset = os.environ.get("BQ_DATASET", "flowterra_dev")
    return f"`{dataset}.{table_name}`"


def build_site_filter(
    site_id: str | None,
) -> tuple[str, list["bigquery.ScalarQueryParameter"]]:
    """Return an optional ``AND siteId = @siteId`` WHERE fragment and its parameter.

    Args:
        site_id: Caller-supplied site ID from the ``siteId`` query parameter,
                 or ``None`` if the parameter was absent.

    Returns:
        A 2-tuple ``(sql_fragment, params)`` where *sql_fragment* is either an
        empty string (no filter) or ``"AND siteId = @siteId"``, and *params* is
        either an empty list or a one-element list containing a
        :class:`google.cloud.bigquery.ScalarQueryParameter`.
    """
    if site_id is None:
        return "", []
    from google.cloud import bigquery  # noqa: PLC0415

    return (
        "AND siteId = @siteId",
        [bigquery.ScalarQueryParameter("siteId", "STRING", site_id)],
    )


def build_raw_event_params(
    customer_id: str,
    window: DateWindow,
    limit: int,
) -> list["bigquery.ScalarQueryParameter"]:
    """Extend :func:`build_report_params` with a ``@limit`` INT64 parameter.

    Used exclusively by raw-event queries (R4, R5) where a ``LIMIT @limit``
    clause must appear in the SQL.

    Args:
        customer_id: The authenticated tenant's ID.
        window:      Validated date window.
        limit:       Effective row cap (already clamped via :func:`clamp_limit`).

    Returns:
        List of four :class:`google.cloud.bigquery.ScalarQueryParameter` instances
        (``@customerId``, ``@fromDate``, ``@toDate``, ``@limit``).
    """
    from google.cloud import bigquery  # noqa: PLC0415

    base = build_report_params(customer_id, window)
    base.append(bigquery.ScalarQueryParameter("limit", "INT64", limit))
    return base


# ---------------------------------------------------------------------------
# BigQuery client
# ---------------------------------------------------------------------------


class BqClient:
    """Thin BigQuery client that enforces cost-guard rules on every query.

    Instantiate once per request (or inject via dependency) — the underlying
    :class:`google.cloud.bigquery.Client` is cheap to construct because Cloud
    Run provides Application Default Credentials automatically.

    Args:
        project: GCP project ID.  ``None`` resolves from Application Default
                 Credentials (correct on Cloud Run).
    """

    def __init__(self, project: str | None = None) -> None:
        from google.cloud import bigquery  # noqa: PLC0415

        self._bq = bigquery
        self._client = bigquery.Client(project=project)

    # ------------------------------------------------------------------
    # Aggregate queries (R1–R3) — no row-limit required
    # ------------------------------------------------------------------

    def run_report(
        self,
        sql: str,
        params: list["bigquery.ScalarQueryParameter"],
    ) -> list[dict]:
        """Execute a parameterised aggregate report query.

        The SQL **must** include ``WHERE ts BETWEEN @fromDate AND @toDate``
        (or equivalent) so BigQuery can prune partitions.  The caller is
        responsible for supplying the correct parameters via
        :func:`build_report_params`.

        Args:
            sql:    Parameterised SQL string using ``@name`` placeholders.
            params: Query parameters from :func:`build_report_params`.

        Returns:
            List of row dicts (keys match BigQuery column names).
        """
        job_config = self._bq.QueryJobConfig(query_parameters=params)
        rows = self._client.query(sql, job_config=job_config).result()
        return [dict(row) for row in rows]

    # ------------------------------------------------------------------
    # Raw-event queries (R4, R5) — LIMIT @limit enforced
    # ------------------------------------------------------------------

    def run_raw_events(
        self,
        sql: str,
        params: list["bigquery.ScalarQueryParameter"],
        *,
        limit: int = DEFAULT_LIMIT,
    ) -> tuple[list[dict], bool]:
        """Execute a raw-event query with a hard row cap.

        The *sql* string **must** end with ``LIMIT @limit``; the corresponding
        INT64 ``@limit`` parameter is injected automatically by this method —
        do *not* add it to *params* manually (use :func:`build_report_params`,
        not :func:`build_raw_event_params`, as input here).

        Args:
            sql:    Parameterised SQL string that ends with ``LIMIT @limit``.
            params: Base parameters from :func:`build_report_params`.
            limit:  Effective row cap (default :data:`DEFAULT_LIMIT`, max
                    :data:`MAX_LIMIT`).  Values outside ``[1, MAX_LIMIT]`` are
                    clamped automatically.

        Returns:
            ``(rows, truncated)`` — *rows* is a list of row dicts; *truncated*
            is ``True`` when ``len(rows) >= limit``, indicating the result was
            capped and the caller should set ``X-Truncated: true`` on the HTTP
            response.
        """
        from google.cloud import bigquery  # noqa: PLC0415

        effective_limit, _ = clamp_limit(limit)

        # Append the @limit parameter so SQL `LIMIT @limit` resolves correctly.
        full_params = list(params) + [
            bigquery.ScalarQueryParameter("limit", "INT64", effective_limit),
        ]

        job_config = self._bq.QueryJobConfig(query_parameters=full_params)
        rows = [dict(row) for row in self._client.query(sql, job_config=job_config).result()]
        truncated = len(rows) >= effective_limit
        return rows, truncated
