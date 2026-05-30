"""Supabase PostgreSQL client for ft-api drone module.

Thin wrapper around the supabase-py library. Use get_supabase_client()
to obtain a client instance — this function is mockable in tests.

The client is cached at module level (one instance per process) to avoid
re-establishing a connection on every request.
"""
import logging
import os

from flask import abort
from supabase import Client, create_client

logger = logging.getLogger(__name__)

_client: Client | None = None


def get_supabase_client() -> Client:
    """Return a cached Supabase client.

    Aborts with 503 when:
    - env vars are missing (misconfiguration)
    - Supabase host is unreachable (ConnectError / DNS failure)
    """
    global _client
    if _client is None:
        url = os.environ.get("SUPABASE_URL", "")
        key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")
        if not url or not key:
            abort(
                503,
                description=(
                    "Supabase is not configured. "
                    "Set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY in Cloud Run."
                ),
            )
        try:
            _client = create_client(url, key)
        except Exception as exc:
            logger.error("Supabase client init failed (%s): %s", type(exc).__name__, exc)
            abort(503, description=f"Supabase unreachable: {type(exc).__name__}")
    return _client
