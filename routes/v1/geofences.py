"""GET /api/v1/customers/<customer_id>/geofences — geofence configuration.

Phase 4: returns static pilot geofences (mirrors the Firestore schema).
Phase 5+: replace with Firestore reads once _check_geofence in ingest-fn is
          wired; operators will manage rules via the UI CRUD endpoints.

Firestore path: customers/{customerId}/geofences/{geofenceId}

Schema (per geofence):
  {
    "id":                STRING,
    "name":              STRING,
    "areaIds":           [STRING],          -- zone IDs the fence covers
    "rules":             [                  -- trigger rules
      { "trigger": "enter"|"exit",
        "roles":   [STRING],               -- asset roles that fire the rule
        "notify":  [STRING] }              -- FCM tokens / channels
    ],
    "capacityThreshold": INT | null         -- optional; alerts when exceeded
  }
"""
from flask import Blueprint, g, jsonify

from auth.middleware import require_auth

geofences_bp = Blueprint("geofences", __name__)

# ---------------------------------------------------------------------------
# Pilot geofence config (mirrors wiki/features/geofencing.md examples).
# One site's worth of zones mapped from _PILOT_SITE in sites.py.
# ---------------------------------------------------------------------------
_PILOT_GEOFENCES = [
    {
        "id": "fence-restricted-server",
        "name": "Server Room (Restricted)",
        "areaIds": ["zone-floor2-boardroom"],
        "rules": [
            {
                "trigger": "enter",
                "roles": ["visitor", "contractor"],
                "notify": ["fcm:facility-manager"],
            }
        ],
        "capacityThreshold": None,
    },
    {
        "id": "fence-reception-entry",
        "name": "Reception Entry Zone",
        "areaIds": ["zone-reception"],
        "rules": [
            {
                "trigger": "enter",
                "roles": ["visitor"],
                "notify": ["fcm:receptionist"],
            },
            {
                "trigger": "exit",
                "roles": ["visitor"],
                "notify": ["fcm:receptionist"],
            },
        ],
        "capacityThreshold": None,
    },
    {
        "id": "fence-open-plan-capacity",
        "name": "Open Plan — Capacity Watch",
        "areaIds": ["zone-open-plan", "zone-floor2-open"],
        "rules": [],
        "capacityThreshold": 50,
    },
    {
        "id": "fence-kitchen-break",
        "name": "Kitchen & Break Area",
        "areaIds": ["zone-kitchen"],
        "rules": [
            {
                "trigger": "enter",
                "roles": ["employee"],
                "notify": [],
            }
        ],
        "capacityThreshold": 30,
    },
]


@geofences_bp.get("/customers/<customer_id>/geofences")
@require_auth
def list_geofences(customer_id: str):
    """Return all geofence configs for a tenant. MVP: static pilot data."""
    if customer_id != g.customer_id:
        return jsonify({"error": "Forbidden"}), 403

    return jsonify({
        "customerId": customer_id,
        "geofences":  _PILOT_GEOFENCES,
    })
