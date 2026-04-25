"""Firebase JWT authentication middleware for ft-api.

All protected routes must be decorated with @require_auth.
The decorator verifies the Firebase ID token, extracts the
`customerId` custom claim, and injects both into flask.g.

See: wiki/infrastructure/firebase-auth.md for the full spec.
"""
import functools

from firebase_admin import auth
from flask import abort, g, request


def require_auth(f):
    """Route decorator: verify Firebase JWT and inject g.uid / g.customer_id."""

    @functools.wraps(f)
    def decorated(*args, **kwargs):
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

        # Route handlers read g.uid and g.customer_id for all tenant-scoped ops.
        g.uid = decoded["uid"]
        g.customer_id = customer_id
        return f(*args, **kwargs)

    return decorated


def _extract_bearer_token() -> str | None:
    header = request.headers.get("Authorization", "")
    return header[7:] if header.startswith("Bearer ") else None
