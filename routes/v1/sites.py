"""GET /api/v1/customers/<customer_id>/sites — site + floor config.

Phase 4: returns static config seeded from the pilot site JSON,
         merged with any mutable fields (sitePhotos) stored in Firestore.
Phase 6+: replace with full Firestore reads so operators can manage sites via UI.
"""
import logging

from firebase_admin import firestore as fs
from flask import Blueprint, g, jsonify

from auth.middleware import require_auth

logger = logging.getLogger(__name__)

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


def _merge_firestore_fields(customer_id: str, site: dict) -> dict:
    """Overlay mutable fields from Firestore onto the static site config.

    Reads customers/{customerId}/sites/{siteId}. If the document doesn't exist
    (backend-seeded site never written to Firestore yet) the static config is
    returned unchanged.
    """
    try:
        doc = fs.client().document(
            f"customers/{customer_id}/sites/{site['id']}"
        ).get()
        if doc.exists:
            data = doc.to_dict() or {}
            if "sitePhotos" in data:
                site = {**site, "sitePhotos": data["sitePhotos"]}
    except Exception:  # noqa: BLE001
        logger.warning("Firestore read failed for site %s — returning static config", site["id"])
    return site


@sites_bp.get("/customers/<customer_id>/sites")
@require_auth
def list_sites(customer_id: str):
    """Return all sites for a tenant. MVP: one static pilot site."""
    if customer_id != g.customer_id:
        return jsonify({"error": "Forbidden"}), 403

    site = _merge_firestore_fields(customer_id, _PILOT_SITE)
    return jsonify({"customerId": customer_id, "sites": [site]})
