"""api/drone/captures.py — drone capture lifecycle endpoints.

Routes (registered at /api/v1/drone/sites/<siteId>/captures):
  POST /api/v1/drone/sites/<siteId>/captures                       — create capture + upload URLs
  POST /api/v1/drone/sites/<siteId>/captures/<captureId>/process   — trigger ODM processing
  GET  /api/v1/drone/sites/<siteId>/captures/<captureId>           — status + tile URL
"""
from __future__ import annotations

from flask import Blueprint, abort, g, jsonify, request

from api.db.supabase_client import get_supabase_client
from api.drone import nodeodm_client, storage
from auth.middleware import require_auth

drone_captures_bp = Blueprint(
    "drone_captures", __name__, url_prefix="/drone/sites/<site_id>/captures"
)

_MAX_PHOTO_COUNT = 500
_DEFAULT_ODM_OPTIONS = {"feature_quality": "medium", "pc_quality": "medium", "mesh": False}


@drone_captures_bp.post("")
@require_auth
def create_capture(site_id: str):
    """POST /api/v1/drone/sites/<site_id>/captures — create capture record + signed upload URLs."""
    db = get_supabase_client()
    _get_site_or_404(db, site_id, g.customer_id)

    body = request.get_json(silent=True) or {}
    captured_at = body.get("captured_at")
    photo_count = body.get("photo_count")
    filenames = body.get("filenames", [])

    errors = _validate_capture_body(captured_at, photo_count, filenames)
    if errors:
        abort(422, description="; ".join(errors))

    result = (
        db.table("captures")
        .insert({
            "site_id": site_id,
            "customer_id": g.customer_id,
            "captured_at": captured_at,
            "photo_count": photo_count,
            "status": "pending",
            "metadata": _DEFAULT_ODM_OPTIONS.copy(),
        })
        .execute()
    )
    row = result.data[0]
    capture_id = row["id"]

    upload_urls = storage.generate_upload_urls(capture_id, filenames)

    return jsonify({
        "capture_id": capture_id,
        "status": "pending",
        "upload_urls": upload_urls,
    }), 201


@drone_captures_bp.post("/<capture_id>/process")
@require_auth
def process_capture(site_id: str, capture_id: str):
    """POST /api/v1/drone/sites/<site_id>/captures/<capture_id>/process — trigger ODM."""
    db = get_supabase_client()
    _get_site_or_404(db, site_id, g.customer_id)
    capture = _get_capture_or_404(db, capture_id, site_id)

    if capture["status"] not in ("pending", "uploading"):
        abort(409, description=f"Capture status '{capture['status']}' cannot be processed")

    options = capture.get("metadata") or _DEFAULT_ODM_OPTIONS
    bucket = __import__("os").environ.get("GCS_DRONE_BUCKET", "flowterra-drone-dev")
    photo_urls = [
        f"https://storage.googleapis.com/{bucket}/captures/{capture_id}/photos/{fn}"
        for fn in _list_photo_filenames(capture_id)
    ]

    try:
        odm_task_id = nodeodm_client.create_task(capture_id, photo_urls, options)
    except nodeodm_client.NodeODMError as exc:
        db.table("captures").update({
            "status": "error",
            "metadata": {**(capture.get("metadata") or {}), "error": "nodeodm_unreachable"},
        }).eq("id", capture_id).execute()
        abort(503, description="NodeODM service unavailable")

    db.table("captures").update({
        "status": "processing",
        "odm_task_id": odm_task_id,
    }).eq("id", capture_id).execute()

    return jsonify({
        "capture_id": capture_id,
        "status": "processing",
        "odm_task_id": odm_task_id,
    }), 202


@drone_captures_bp.get("/<capture_id>")
@require_auth
def get_capture(site_id: str, capture_id: str):
    """GET /api/v1/drone/sites/<site_id>/captures/<capture_id> — status + tile URL."""
    db = get_supabase_client()
    _get_site_or_404(db, site_id, g.customer_id)
    capture = _get_capture_or_404(db, capture_id, site_id)

    tiles = None
    if capture["status"] == "ready":
        tiles = storage.tiles_url(capture_id)

    return jsonify({
        "capture_id": capture["id"],
        "site_id": capture["site_id"],
        "captured_at": capture["captured_at"],
        "status": capture["status"],
        "photo_count": capture["photo_count"],
        "tiles_url": tiles,
        "metadata": capture.get("metadata") or {},
    }), 200


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _validate_capture_body(captured_at, photo_count, filenames) -> list[str]:
    errors = []
    if not captured_at:
        errors.append("'captured_at' is required")
    if photo_count is None:
        errors.append("'photo_count' is required")
    elif not isinstance(photo_count, int) or photo_count < 1:
        errors.append("'photo_count' must be a positive integer")
    elif photo_count > _MAX_PHOTO_COUNT:
        errors.append(f"'photo_count' exceeds maximum of {_MAX_PHOTO_COUNT}")
    if not filenames:
        errors.append("'filenames' is required")
    elif photo_count and len(filenames) != photo_count:
        errors.append("'filenames' length must equal 'photo_count'")
    return errors


def _get_site_or_404(db, site_id: str, customer_id: str) -> dict:
    result = (
        db.table("sites")
        .select("id")
        .eq("id", site_id)
        .eq("customer_id", customer_id)
        .execute()
    )
    if not result.data:
        abort(404, description="Site not found")
    return result.data[0]


def _get_capture_or_404(db, capture_id: str, site_id: str) -> dict:
    result = (
        db.table("captures")
        .select("*")
        .eq("id", capture_id)
        .eq("site_id", site_id)
        .execute()
    )
    if not result.data:
        abort(404, description="Capture not found")
    return result.data[0]


def _list_photo_filenames(capture_id: str) -> list[str]:
    """Stub: in production, list blobs under captures/{id}/photos/ in GCS."""
    return []
