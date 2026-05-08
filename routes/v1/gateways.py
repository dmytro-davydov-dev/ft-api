"""GET /api/v1/customers/<customer_id>/gateways — BLE gateway registry.

Phase 4: returns static pilot roster matching the 30-gateway HQ Pilot site
         (20 on floor 1, 10 on floor 2 per sites.py / sample_site.json).
Phase 5+: replace with Firestore reads once ingest-fn writes gateway heartbeat
          state to customers/{customerId}/gateways/{gatewayId}.

Gateway shape (per device):
  {
    "id":            STRING,                   -- stable gateway ID
    "label":         STRING,                   -- human-readable name
    "model":         STRING,                   -- hardware model
    "siteId":        STRING,
    "floor":         INT,
    "zoneId":        STRING | null,
    "ipAddress":     STRING | null,            -- LAN IP (internal)
    "status":        "online" | "offline" | "degraded",
    "lastHeartbeat": ISO-8601 | null,
    "tagCount":      INT                       -- tags currently visible
  }
"""
import datetime

from flask import Blueprint, g, jsonify

from auth.middleware import require_auth

gateways_bp = Blueprint("gateways", __name__)

# ---------------------------------------------------------------------------
# Pilot gateway roster.
# 30 Minew G1 units placed in the HQ Pilot site:
#   Floor 1 (20 gw) — reception ×4, open-plan ×6, meeting-a ×1, meeting-b ×1,
#                      kitchen ×6, corridor ×2
#   Floor 2 (10 gw) — floor2-open ×7, floor2-boardroom ×3
#
# Most are "online"; 2 are "offline"; 1 is "degraded" — realistic for a
# 30-node deployment.  tagCount reflects a mid-morning snapshot.
# ---------------------------------------------------------------------------

_BASE_TS = datetime.datetime(2026, 5, 8, 9, 0, 0, tzinfo=datetime.timezone.utc)


def _iso(delta_seconds: int) -> str:
    return (_BASE_TS + datetime.timedelta(seconds=delta_seconds)).isoformat()


_PILOT_GATEWAYS = [
    # ── Floor 1 · Reception (4) ──────────────────────────────────────────────
    {
        "id":            "gw-f1-01",
        "label":         "F1 · Reception NW",
        "model":         "Minew G1",
        "siteId":        "site-hq-pilot",
        "floor":         1,
        "zoneId":        "zone-reception",
        "ipAddress":     "10.0.1.11",
        "status":        "online",
        "lastHeartbeat": _iso(-30),
        "tagCount":      3,
    },
    {
        "id":            "gw-f1-02",
        "label":         "F1 · Reception NE",
        "model":         "Minew G1",
        "siteId":        "site-hq-pilot",
        "floor":         1,
        "zoneId":        "zone-reception",
        "ipAddress":     "10.0.1.12",
        "status":        "online",
        "lastHeartbeat": _iso(-45),
        "tagCount":      2,
    },
    {
        "id":            "gw-f1-03",
        "label":         "F1 · Reception SW",
        "model":         "Minew G1",
        "siteId":        "site-hq-pilot",
        "floor":         1,
        "zoneId":        "zone-reception",
        "ipAddress":     "10.0.1.13",
        "status":        "online",
        "lastHeartbeat": _iso(-20),
        "tagCount":      1,
    },
    {
        "id":            "gw-f1-04",
        "label":         "F1 · Reception SE",
        "model":         "Minew G1",
        "siteId":        "site-hq-pilot",
        "floor":         1,
        "zoneId":        "zone-reception",
        "ipAddress":     "10.0.1.14",
        "status":        "degraded",
        "lastHeartbeat": _iso(-120),
        "tagCount":      1,
    },
    # ── Floor 1 · Open Plan (6) ───────────────────────────────────────────────
    {
        "id":            "gw-f1-05",
        "label":         "F1 · Open Plan A",
        "model":         "Minew G1",
        "siteId":        "site-hq-pilot",
        "floor":         1,
        "zoneId":        "zone-open-plan",
        "ipAddress":     "10.0.1.15",
        "status":        "online",
        "lastHeartbeat": _iso(-15),
        "tagCount":      4,
    },
    {
        "id":            "gw-f1-06",
        "label":         "F1 · Open Plan B",
        "model":         "Minew G1",
        "siteId":        "site-hq-pilot",
        "floor":         1,
        "zoneId":        "zone-open-plan",
        "ipAddress":     "10.0.1.16",
        "status":        "online",
        "lastHeartbeat": _iso(-10),
        "tagCount":      3,
    },
    {
        "id":            "gw-f1-07",
        "label":         "F1 · Open Plan C",
        "model":         "Minew G1",
        "siteId":        "site-hq-pilot",
        "floor":         1,
        "zoneId":        "zone-open-plan",
        "ipAddress":     "10.0.1.17",
        "status":        "online",
        "lastHeartbeat": _iso(-8),
        "tagCount":      2,
    },
    {
        "id":            "gw-f1-08",
        "label":         "F1 · Open Plan D",
        "model":         "Minew G1",
        "siteId":        "site-hq-pilot",
        "floor":         1,
        "zoneId":        "zone-open-plan",
        "ipAddress":     "10.0.1.18",
        "status":        "online",
        "lastHeartbeat": _iso(-22),
        "tagCount":      3,
    },
    {
        "id":            "gw-f1-09",
        "label":         "F1 · Open Plan E",
        "model":         "Minew G1",
        "siteId":        "site-hq-pilot",
        "floor":         1,
        "zoneId":        "zone-open-plan",
        "ipAddress":     "10.0.1.19",
        "status":        "online",
        "lastHeartbeat": _iso(-18),
        "tagCount":      1,
    },
    {
        "id":            "gw-f1-10",
        "label":         "F1 · Open Plan F",
        "model":         "Minew G1",
        "siteId":        "site-hq-pilot",
        "floor":         1,
        "zoneId":        "zone-open-plan",
        "ipAddress":     "10.0.1.20",
        "status":        "offline",
        "lastHeartbeat": _iso(-3600),
        "tagCount":      0,
    },
    # ── Floor 1 · Meeting Room A (1) ──────────────────────────────────────────
    {
        "id":            "gw-f1-11",
        "label":         "F1 · Meeting A",
        "model":         "Minew G1",
        "siteId":        "site-hq-pilot",
        "floor":         1,
        "zoneId":        "zone-meeting-a",
        "ipAddress":     "10.0.1.21",
        "status":        "online",
        "lastHeartbeat": _iso(-12),
        "tagCount":      1,
    },
    # ── Floor 1 · Meeting Room B (1) ──────────────────────────────────────────
    {
        "id":            "gw-f1-12",
        "label":         "F1 · Meeting B",
        "model":         "Minew G1",
        "siteId":        "site-hq-pilot",
        "floor":         1,
        "zoneId":        "zone-meeting-b",
        "ipAddress":     "10.0.1.22",
        "status":        "online",
        "lastHeartbeat": _iso(-40),
        "tagCount":      0,
    },
    # ── Floor 1 · Kitchen (6) ─────────────────────────────────────────────────
    {
        "id":            "gw-f1-13",
        "label":         "F1 · Kitchen A",
        "model":         "Minew G1",
        "siteId":        "site-hq-pilot",
        "floor":         1,
        "zoneId":        "zone-kitchen",
        "ipAddress":     "10.0.1.23",
        "status":        "online",
        "lastHeartbeat": _iso(-5),
        "tagCount":      2,
    },
    {
        "id":            "gw-f1-14",
        "label":         "F1 · Kitchen B",
        "model":         "Minew G1",
        "siteId":        "site-hq-pilot",
        "floor":         1,
        "zoneId":        "zone-kitchen",
        "ipAddress":     "10.0.1.24",
        "status":        "online",
        "lastHeartbeat": _iso(-6),
        "tagCount":      1,
    },
    {
        "id":            "gw-f1-15",
        "label":         "F1 · Kitchen C",
        "model":         "Minew G1",
        "siteId":        "site-hq-pilot",
        "floor":         1,
        "zoneId":        "zone-kitchen",
        "ipAddress":     "10.0.1.25",
        "status":        "online",
        "lastHeartbeat": _iso(-9),
        "tagCount":      2,
    },
    {
        "id":            "gw-f1-16",
        "label":         "F1 · Kitchen D",
        "model":         "Minew G1",
        "siteId":        "site-hq-pilot",
        "floor":         1,
        "zoneId":        "zone-kitchen",
        "ipAddress":     "10.0.1.26",
        "status":        "online",
        "lastHeartbeat": _iso(-14),
        "tagCount":      0,
    },
    {
        "id":            "gw-f1-17",
        "label":         "F1 · Kitchen E",
        "model":         "Minew G1",
        "siteId":        "site-hq-pilot",
        "floor":         1,
        "zoneId":        "zone-kitchen",
        "ipAddress":     "10.0.1.27",
        "status":        "online",
        "lastHeartbeat": _iso(-25),
        "tagCount":      1,
    },
    {
        "id":            "gw-f1-18",
        "label":         "F1 · Kitchen F",
        "model":         "Minew G1",
        "siteId":        "site-hq-pilot",
        "floor":         1,
        "zoneId":        "zone-kitchen",
        "ipAddress":     "10.0.1.28",
        "status":        "online",
        "lastHeartbeat": _iso(-30),
        "tagCount":      1,
    },
    # ── Floor 1 · Corridor (2) ────────────────────────────────────────────────
    {
        "id":            "gw-f1-19",
        "label":         "F1 · Corridor North",
        "model":         "Minew G1",
        "siteId":        "site-hq-pilot",
        "floor":         1,
        "zoneId":        None,
        "ipAddress":     "10.0.1.29",
        "status":        "online",
        "lastHeartbeat": _iso(-35),
        "tagCount":      0,
    },
    {
        "id":            "gw-f1-20",
        "label":         "F1 · Corridor South",
        "model":         "Minew G1",
        "siteId":        "site-hq-pilot",
        "floor":         1,
        "zoneId":        None,
        "ipAddress":     "10.0.1.30",
        "status":        "offline",
        "lastHeartbeat": _iso(-7200),
        "tagCount":      0,
    },
    # ── Floor 2 · Open Plan (7) ───────────────────────────────────────────────
    {
        "id":            "gw-f2-01",
        "label":         "F2 · Open Plan A",
        "model":         "Minew G1",
        "siteId":        "site-hq-pilot",
        "floor":         2,
        "zoneId":        "zone-floor2-open",
        "ipAddress":     "10.0.2.11",
        "status":        "online",
        "lastHeartbeat": _iso(-10),
        "tagCount":      2,
    },
    {
        "id":            "gw-f2-02",
        "label":         "F2 · Open Plan B",
        "model":         "Minew G1",
        "siteId":        "site-hq-pilot",
        "floor":         2,
        "zoneId":        "zone-floor2-open",
        "ipAddress":     "10.0.2.12",
        "status":        "online",
        "lastHeartbeat": _iso(-15),
        "tagCount":      1,
    },
    {
        "id":            "gw-f2-03",
        "label":         "F2 · Open Plan C",
        "model":         "Minew G1",
        "siteId":        "site-hq-pilot",
        "floor":         2,
        "zoneId":        "zone-floor2-open",
        "ipAddress":     "10.0.2.13",
        "status":        "online",
        "lastHeartbeat": _iso(-20),
        "tagCount":      2,
    },
    {
        "id":            "gw-f2-04",
        "label":         "F2 · Open Plan D",
        "model":         "Minew G1",
        "siteId":        "site-hq-pilot",
        "floor":         2,
        "zoneId":        "zone-floor2-open",
        "ipAddress":     "10.0.2.14",
        "status":        "online",
        "lastHeartbeat": _iso(-8),
        "tagCount":      1,
    },
    {
        "id":            "gw-f2-05",
        "label":         "F2 · Open Plan E",
        "model":         "Minew G1",
        "siteId":        "site-hq-pilot",
        "floor":         2,
        "zoneId":        "zone-floor2-open",
        "ipAddress":     "10.0.2.15",
        "status":        "online",
        "lastHeartbeat": _iso(-12),
        "tagCount":      0,
    },
    {
        "id":            "gw-f2-06",
        "label":         "F2 · Open Plan F",
        "model":         "Minew G1",
        "siteId":        "site-hq-pilot",
        "floor":         2,
        "zoneId":        "zone-floor2-open",
        "ipAddress":     "10.0.2.16",
        "status":        "online",
        "lastHeartbeat": _iso(-28),
        "tagCount":      1,
    },
    {
        "id":            "gw-f2-07",
        "label":         "F2 · Open Plan G",
        "model":         "Minew G1",
        "siteId":        "site-hq-pilot",
        "floor":         2,
        "zoneId":        "zone-floor2-open",
        "ipAddress":     "10.0.2.17",
        "status":        "online",
        "lastHeartbeat": _iso(-33),
        "tagCount":      1,
    },
    # ── Floor 2 · Boardroom (3) ───────────────────────────────────────────────
    {
        "id":            "gw-f2-08",
        "label":         "F2 · Boardroom A",
        "model":         "Minew G1",
        "siteId":        "site-hq-pilot",
        "floor":         2,
        "zoneId":        "zone-floor2-boardroom",
        "ipAddress":     "10.0.2.18",
        "status":        "online",
        "lastHeartbeat": _iso(-5),
        "tagCount":      1,
    },
    {
        "id":            "gw-f2-09",
        "label":         "F2 · Boardroom B",
        "model":         "Minew G1",
        "siteId":        "site-hq-pilot",
        "floor":         2,
        "zoneId":        "zone-floor2-boardroom",
        "ipAddress":     "10.0.2.19",
        "status":        "online",
        "lastHeartbeat": _iso(-7),
        "tagCount":      1,
    },
    {
        "id":            "gw-f2-10",
        "label":         "F2 · Boardroom C",
        "model":         "Minew G1",
        "siteId":        "site-hq-pilot",
        "floor":         2,
        "zoneId":        "zone-floor2-boardroom",
        "ipAddress":     "10.0.2.20",
        "status":        "online",
        "lastHeartbeat": _iso(-11),
        "tagCount":      1,
    },
]


@gateways_bp.get("/customers/<customer_id>/gateways")
@require_auth
def list_gateways(customer_id: str):
    """Return all gateways for a tenant. MVP: static pilot roster."""
    if customer_id != g.customer_id:
        return jsonify({"error": "Forbidden"}), 403

    return jsonify({
        "customerId": customer_id,
        "gateways":   _PILOT_GATEWAYS,
    })
