"""GET /api/v1/me — authenticated user profile stub.

Returns the authenticated user's uid and customerId claim.
Full implementation (Firestore user document fetch) is Phase 3.
"""
from flask import Blueprint, g, jsonify

from auth.middleware import require_auth

me_bp = Blueprint("me", __name__)


@me_bp.get("/me")
@require_auth
def get_me():
    """Return current user identity. Stub — expand in Phase 3."""
    return jsonify(
        {
            "uid": g.uid,
            "customerId": g.customer_id,
            # TODO(Phase 3): fetch full user profile from Firestore
            #   customers/{customerId}/users/{uid}
        }
    )
