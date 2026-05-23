"""api/drone/sites.py — drone site management endpoints.

Routes (Blueprint prefix /drone/sites, registered at /api/v1/drone/sites):
  POST   /api/v1/drone/sites          — create a new site
  GET    /api/v1/drone/sites          — list all sites for tenant
  DELETE /api/v1/drone/sites/<id>     — delete site (409 if captures exist)

customerId is always from the Firebase JWT (g.customer_id), never the body.
"""
from __future__ import annotations

from flask import Blueprint, abort, g, jsonify, request

from api.db.supabase_client import get_supabase_client
from auth.middleware import require_auth

drone_sites_bp = Blueprint("drone_sites", __name__, url_prefix="/drone/sites")


@drone_sites_bp.post("")
@require_auth
def create_site():
    """POST /api/v1/drone/sites — create a named site for the tenant."""
    body = request.get_json(silent=True) or {}
    name = body.get("name", "").strip()
    if not name:
        abort(422, description="'name' is required")

    db = get_supabase_client()
    result = (
        db.table("sites")
        .insert({"customer_id": g.customer_id, "name": name})
        .execute()
    )
    row = result.data[0]
    captures_count = _capture_count(db, row["id"])
    return jsonify({
        "site_id": row["id"],
        "name": row["name"],
        "capture_count": captures_count,
        "created_at": row["created_at"],
    }), 201


@drone_sites_bp.get("")
@require_auth
def list_sites():
    """GET /api/v1/drone/sites — list all sites for the tenant."""
    db = get_supabase_client()
    result = (
        db.table("sites")
        .select("*")
        .eq("customer_id", g.customer_id)
        .execute()
    )
    sites = []
    for row in result.data:
        captures = (
            db.table("captures")
            .select("id, captured_at")
            .eq("site_id", row["id"])
            .order("captured_at", desc=True)
            .execute()
        )
        last_capture_at = captures.data[0].get("captured_at") if captures.data else None
        sites.append({
            "site_id": row["id"],
            "name": row["name"],
            "capture_count": len(captures.data),
            "last_capture_at": last_capture_at,
        })
    return jsonify({"sites": sites}), 200


@drone_sites_bp.delete("/<site_id>")
@require_auth
def delete_site(site_id: str):
    """DELETE /api/v1/drone/sites/<site_id> — delete site (409 if captures exist)."""
    db = get_supabase_client()
    site = _get_site_or_404(db, site_id, g.customer_id)

    count = _capture_count(db, site["id"])
    if count > 0:
        abort(409, description="Cannot delete site with existing captures")

    db.table("sites").delete().eq("id", site["id"]).execute()
    return jsonify({"deleted": True}), 200


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _get_site_or_404(db, site_id: str, customer_id: str) -> dict:
    result = (
        db.table("sites")
        .select("*")
        .eq("id", site_id)
        .eq("customer_id", customer_id)
        .execute()
    )
    if not result.data:
        abort(404, description="Site not found")
    return result.data[0]


def _capture_count(db, site_id: str) -> int:
    result = db.table("captures").select("id").eq("site_id", site_id).execute()
    return len(result.data)
