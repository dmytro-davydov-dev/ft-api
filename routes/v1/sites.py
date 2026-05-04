"""GET /api/v1/customers/<customer_id>/sites — site + floor config.

Phase 4: returns static config seeded from the pilot site JSON.
Phase 6+: replace with Firestore reads so operators can manage sites via UI.
"""
from flask import Blueprint, g, jsonify

from auth.middleware import require_auth

sites_bp = Blueprint("sites", __name__)

# ---------------------------------------------------------------------------
# Pilot site config (mirrors ft-sim/config/sample_site.json).
# One record per tenant for MVP; multi-site support lands in Phase 6.
# ---------------------------------------------------------------------------
_PILOT_SITE = {
    "id": "site-hq-pilot",
    "name": "HQ Pilot Office",
    "description": "2-floor office (50×40 m per floor = 2 000 m²/floor). 7 zones.",
    "floorplan": {
        "width_m": 50,
        "height_m": 40,
        "floors": 2,
        "floor_area_m2": 2000,
    },
    "floors": [
        {
            "floor": 1,
            "label": "Floor 1",
            "gateway_count": 20,
            "zones": [
                {"id": "zone-reception",  "label": "Reception",          "area_m2": 400},
                {"id": "zone-open-plan",  "label": "Open Plan",           "area_m2": 625},
                {"id": "zone-meeting-a",  "label": "Meeting Room A",      "area_m2": 144},
                {"id": "zone-meeting-b",  "label": "Meeting Room B",      "area_m2": 144},
                {"id": "zone-kitchen",    "label": "Kitchen & Break Area", "area_m2": 600},
            ],
        },
        {
            "floor": 2,
            "label": "Floor 2",
            "gateway_count": 10,
            "zones": [
                {"id": "zone-floor2-open",       "label": "Floor 2 Open Plan", "area_m2": 1400},
                {"id": "zone-floor2-boardroom",  "label": "Boardroom",          "area_m2": 600},
            ],
        },
    ],
}


@sites_bp.get("/customers/<customer_id>/sites")
@require_auth
def list_sites(customer_id: str):
    """Return all sites for a tenant. MVP: one static pilot site."""
    if customer_id != g.customer_id:
        return jsonify({"error": "Forbidden"}), 403

    return jsonify({"customerId": customer_id, "sites": [_PILOT_SITE]})
