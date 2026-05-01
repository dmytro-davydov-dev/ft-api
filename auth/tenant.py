"""Tenant isolation middleware for ft-api report routes.

Applied at the Blueprint level via before_request — enforces tenant
boundaries on every route within the blueprint without per-endpoint
decoration.

Behaviour (executed in order on every request):
  1. Verify the Firebase JWT (same logic as require_auth).
  2. Assert URL ``{id}`` == token ``customerId`` claim → 403 on mismatch.
  3. Inject ``g.bq_customer_id`` for use as a parameterised BigQuery
     query argument (never interpolate into SQL strings).

Client-supplied ``customerId`` query/body parameters are ignored entirely;
only the JWT claim is authoritative.

BigQuery usage pattern in route handlers::

    from google.cloud import bigquery
    from flask import g

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("customerId", "STRING", g.bq_customer_id),
        ]
    )
    query = "SELECT ... FROM `table` WHERE customerId = @customerId AND ..."

Integration::

    report_bp = Blueprint("report", __name__, url_prefix="/api/v1/customers/<id>/reports")
    report_bp.before_request(require_tenant())

Works in conjunction with the cost-guard middleware (D7) — apply
require_tenant *first* so unauthenticated/mismatched requests are
rejected before any BigQuery quota is consumed.
"""

from firebase_admin import auth
from flask import abort, g, request


def require_tenant(customer_id_param: str = "id"):
    """Return a ``before_request`` hook that enforces tenant isolation.

    Args:
        customer_id_param: Name of the URL path variable that carries the
            customer ID. Defaults to ``"id"`` (e.g. ``/customers/<id>/...``).

    Returns:
        Callable suitable for ``Blueprint.before_request(require_tenant())``.
    """

    def _hook() -> None:
        # ------------------------------------------------------------------
        # Step 1: Verify Firebase JWT.
        # ------------------------------------------------------------------
        token = _extract_bearer_token()
        if not token:
            abort(401, description="Missing Authorization header")

        try:
            decoded = auth.verify_id_token(token)
        except auth.ExpiredIdTokenError:
            abort(401, description="Token expired")
        except auth.InvalidIdTokenError:
            abort(401, description="Invalid token")
        except Exception:  # noqa: BLE001
            abort(401, description="Token verification failed")

        customer_id = decoded.get("customerId")
        if not customer_id:
            abort(
                403,
                description="Missing customerId claim — user not provisioned",
            )

        g.uid = decoded["uid"]
        g.customer_id = customer_id

        # ------------------------------------------------------------------
        # Step 2: Assert URL {id} matches the token's customerId claim.
        # If the route has no URL segment matching customer_id_param we skip
        # the check (supports nested resources without a top-level id).
        # ------------------------------------------------------------------
        url_id = (request.view_args or {}).get(customer_id_param)
        if url_id is not None and url_id != customer_id:
            abort(
                403,
                description=(
                    "Tenant mismatch: URL id does not match token customerId"
                ),
            )

        # ------------------------------------------------------------------
        # Step 3: Inject parameterised BigQuery argument.
        # Route handlers MUST use g.bq_customer_id as a query parameter —
        # never interpolate it into a SQL string.
        # ------------------------------------------------------------------
        g.bq_customer_id = customer_id

    return _hook


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _extract_bearer_token() -> str | None:
    """Extract the raw JWT from the ``Authorization: Bearer <token>`` header."""
    header = request.headers.get("Authorization", "")
    return header[7:] if header.startswith("Bearer ") else None
