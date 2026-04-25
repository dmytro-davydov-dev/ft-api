"""GET /api/v1/dashboard — tenant dashboard data stub.

Returns an empty payload scoped to the authenticated customerId.
Full implementation (occupancy metrics, alerts) is Phase 3.
"""
from flask import Blueprint, g, jsonify

from auth.middleware import require_auth

dashboard_bp = Blueprint("dashboard", __name__)


@dashboard_bp.get("/dashboard")
@require_auth
def get_dashboard():
    """Return dashboard data for the authenticated tenant. Stub — expand in Phase 3."""
    return jsonify(
        {
            "customerId": g.customer_id,
            "occupancy": [],   # TODO(Phase 3): live occupancy from Firestore
            "alerts": [],      # TODO(Phase 3): geofence alert feed
            "utilisation": {}, # TODO(Phase 3): BigQuery utilisation aggregates
        }
    )
