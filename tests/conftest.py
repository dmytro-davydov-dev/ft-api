"""Pytest fixtures for ft-api tests.

DISABLE_ODM_POLLER is set before any import so the background APScheduler
thread is never started during the test suite.
"""
import os
os.environ.setdefault("DISABLE_ODM_POLLER", "1")
os.environ.setdefault("GCS_DRONE_BUCKET", "test-drone-bucket")
os.environ.setdefault("SUPABASE_URL", "http://localhost:54321")
os.environ.setdefault("SUPABASE_SERVICE_ROLE_KEY", "test-key")

import sys
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Real exception stubs — MagicMock instances can't be used in except clauses.
# ---------------------------------------------------------------------------

class _ExpiredIdTokenError(Exception):
    """Stub for firebase_admin.auth.ExpiredIdTokenError."""


class _InvalidIdTokenError(Exception):
    """Stub for firebase_admin.auth.InvalidIdTokenError."""


# ---------------------------------------------------------------------------
# Build a firebase_admin mock whose .auth sub-module exposes the stubs.
# Must happen before any app-level import.
# ---------------------------------------------------------------------------

_auth_mock = MagicMock()
_auth_mock.ExpiredIdTokenError = _ExpiredIdTokenError
_auth_mock.InvalidIdTokenError = _InvalidIdTokenError

_firebase_mock = MagicMock()
_firebase_mock.auth = _auth_mock
_firebase_mock._apps = {}  # satisfy the guard in create_app

sys.modules["firebase_admin"] = _firebase_mock
sys.modules["firebase_admin.auth"] = _auth_mock


# ---------------------------------------------------------------------------
# google-cloud-bigquery stub — prevents import errors when BqClient helpers
# (build_report_params, build_site_filter, etc.) do their lazy
# `from google.cloud import bigquery` inside route handlers.
#
# test_bq_client.py installs a richer stub with a real ScalarQueryParameter
# class for direct bq_client unit tests; this session-level stub satisfies
# the route-level tests that patch BqClient entirely.
# ---------------------------------------------------------------------------

def _make_bq_stub() -> MagicMock:
    stub = MagicMock()

    class _Param:
        def __init__(self, name: str, kind: str, value):
            self.name = name
            self.kind = kind
            self.value = value

    stub.ScalarQueryParameter = _Param
    stub.QueryJobConfig = MagicMock(side_effect=lambda **kw: MagicMock(**kw))
    return stub


_bq_stub = _make_bq_stub()
sys.modules.setdefault("google", MagicMock())
sys.modules.setdefault("google.cloud", MagicMock())
sys.modules["google.cloud.bigquery"] = _bq_stub
sys.modules["google.cloud.storage"] = MagicMock()

# supabase stub — prevents import errors; tests patch get_supabase_client directly
sys.modules.setdefault("supabase", MagicMock())
sys.modules.setdefault("supabase.client", MagicMock())

# apscheduler stub
sys.modules.setdefault("apscheduler", MagicMock())
sys.modules.setdefault("apscheduler.schedulers", MagicMock())
sys.modules.setdefault("apscheduler.schedulers.background", MagicMock())

# requests stub
sys.modules.setdefault("requests", MagicMock())


@pytest.fixture()
def app():
    """Return a configured Flask test app with Firebase Admin mocked."""
    # Force module reload so the patched sys.modules are picked up cleanly.
    import importlib

    for mod in list(sys.modules.keys()):
        if mod.startswith("routes") or mod.startswith("report") or mod.startswith("api") or mod == "app":
            del sys.modules[mod]

    import app as app_module  # noqa: PLC0415

    flask_app = app_module.create_app()
    flask_app.config["TESTING"] = True
    yield flask_app


@pytest.fixture()
def client(app):
    """Return a Flask test client."""
    return app.test_client()


@pytest.fixture()
def auth_mock():
    """Return the patched firebase_admin.auth module for per-test configuration."""
    return _auth_mock
