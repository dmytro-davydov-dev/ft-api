"""GET /api/v1/customers/<customer_id>/tags — tag / device registry.

Phase 4: returns static pilot tag roster derived from ft-sim tag IDs.
Phase 5+: replace with Firestore reads once the ingest-fn writes tag state
          (last-seen, zone, battery) to customers/{customerId}/tags/{tagId}.

Tag shape (per device):
  {
    "id":         STRING,       -- matches tagId in BQ tag_events
    "label":      STRING,       -- human-readable name
    "type":       "badge",      -- badge | asset (extend in Phase 5)
    "batteryPct": INT,          -- 0–100; null if unknown
    "lastSeen":   ISO-8601 | null,
    "zoneId":     STRING | null,
    "floor":      INT | null,
    "status":     "active" | "inactive" | "low_battery"
  }
"""
import datetime

from flask import Blueprint, g, jsonify

from auth.middleware import require_auth

tags_bp = Blueprint("tags", __name__)

# ---------------------------------------------------------------------------
# Pilot tag roster  (tag-0001 … tag-0010 matching ft-sim default config).
# batteryPct, lastSeen, zoneId, floor are representative static values;
# Phase 5 will replace with live reads from Firestore.
# ---------------------------------------------------------------------------

_BASE_TS = datetime.datetime(2026, 5, 7, 8, 0, 0, tzinfo=datetime.timezone.utc)


def _iso(delta_minutes: int) -> str:
    return (_BASE_TS + datetime.timedelta(minutes=delta_minutes)).isoformat()


_PILOT_TAGS = [
    {
        "id":         "tag-0001",
        "label":      "Employee Badge 1",
        "type":       "badge",
        "batteryPct": 87,
        "lastSeen":   _iso(30),
        "zoneId":     "zone-reception",
        "floor":      1,
        "status":     "active",
    },
    {
        "id":         "tag-0002",
        "label":      "Employee Badge 2",
        "type":       "badge",
        "batteryPct": 72,
        "lastSeen":   _iso(25),
        "zoneId":     "zone-open-plan",
        "floor":      1,
        "status":     "active",
    },
    {
        "id":         "tag-0003",
        "label":      "Employee Badge 3",
        "type":       "badge",
        "batteryPct": 14,
        "lastSeen":   _iso(10),
        "zoneId":     "zone-meeting-a",
        "floor":      1,
        "status":     "low_battery",
    },
    {
        "id":         "tag-0004",
        "label":      "Employee Badge 4",
        "type":       "badge",
        "batteryPct": 95,
        "lastSeen":   _iso(45),
        "zoneId":     "zone-floor2-open",
        "floor":      2,
        "status":     "active",
    },
    {
        "id":         "tag-0005",
        "label":      "Employee Badge 5",
        "type":       "badge",
        "batteryPct": 61,
        "lastSeen":   _iso(5),
        "zoneId":     "zone-kitchen",
        "floor":      1,
        "status":     "active",
    },
    {
        "id":         "tag-0006",
        "label":      "Contractor Badge 1",
        "type":       "badge",
        "batteryPct": 43,
        "lastSeen":   _iso(60),
        "zoneId":     "zone-floor2-boardroom",
        "floor":      2,
        "status":     "active",
    },
    {
        "id":         "tag-0007",
        "label":      "Visitor Badge 1",
        "type":       "badge",
        "batteryPct": 58,
        "lastSeen":   _iso(90),
        "zoneId":     "zone-reception",
        "floor":      1,
        "status":     "active",
    },
    {
        "id":         "tag-0008",
        "label":      "Employee Badge 6",
        "type":       "badge",
        "batteryPct": 11,
        "lastSeen":   _iso(15),
        "zoneId":     "zone-open-plan",
        "floor":      1,
        "status":     "low_battery",
    },
    {
        "id":         "tag-0009",
        "label":      "Employee Badge 7",
        "type":       "badge",
        "batteryPct": 80,
        "lastSeen":   None,
        "zoneId":     None,
        "floor":      None,
        "status":     "inactive",
    },
    {
        "id":         "tag-0010",
        "label":      "Asset Tracker 1",
        "type":       "asset",
        "batteryPct": 99,
        "lastSeen":   _iso(120),
        "zoneId":     "zone-meeting-b",
        "floor":      1,
        "status":     "active",
    },
]


@tags_bp.get("/customers/<customer_id>/tags")
@require_auth
def list_tags(customer_id: str):
    """Return all tags / devices for a tenant. MVP: static pilot roster."""
    if customer_id != g.customer_id:
        return jsonify({"error": "Forbidden"}), 403

    return jsonify({
        "customerId": customer_id,
        "tags":       _PILOT_TAGS,
    })
