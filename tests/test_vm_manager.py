"""Unit tests for api/drone/vm_manager.py.

Coverage target: ≥ 85%
All GCP Compute Engine, Supabase, and requests calls are mocked.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
import requests


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_compute(vm_status: str = "RUNNING"):
    compute = MagicMock()
    instances = MagicMock()
    compute.instances.return_value = instances

    get_req = MagicMock()
    instances.get.return_value = get_req
    get_req.execute.return_value = {"status": vm_status}

    start_req = MagicMock()
    instances.start.return_value = start_req
    start_req.execute.return_value = {}

    stop_req = MagicMock()
    instances.stop.return_value = stop_req
    stop_req.execute.return_value = {}

    return compute, instances


def _make_db(active_count: int = 0):
    db = MagicMock()
    tbl = MagicMock()
    db.table.return_value = tbl
    sel = MagicMock()
    tbl.select.return_value = sel
    in_q = MagicMock()
    sel.in_.return_value = in_q
    in_q.execute.return_value = MagicMock(data=[{"id": f"cap-{i}"} for i in range(active_count)])
    return db


# ---------------------------------------------------------------------------
# ensure_vm_running tests
# ---------------------------------------------------------------------------

class TestEnsureVmRunning:
    def test_starts_stopped_vm_and_waits(self):
        compute, instances = _make_compute(vm_status="TERMINATED")

        with patch("api.drone.vm_manager._compute", return_value=compute), \
             patch("api.drone.vm_manager._wait_for_nodeodm_ready") as mock_wait:
            from api.drone import vm_manager  # noqa: PLC0415
            vm_manager.ensure_vm_running(project="proj", zone="zone", instance="inst")

        instances.start.assert_called_once_with(project="proj", zone="zone", instance="inst")
        mock_wait.assert_called_once()

    def test_skips_start_when_already_running(self):
        compute, instances = _make_compute(vm_status="RUNNING")

        with patch("api.drone.vm_manager._compute", return_value=compute), \
             patch("api.drone.vm_manager._wait_for_nodeodm_ready") as mock_wait:
            from api.drone import vm_manager  # noqa: PLC0415
            vm_manager.ensure_vm_running(project="proj", zone="zone", instance="inst")

        instances.start.assert_not_called()
        mock_wait.assert_called_once()

    def test_skips_start_when_staging(self):
        compute, instances = _make_compute(vm_status="STAGING")

        with patch("api.drone.vm_manager._compute", return_value=compute), \
             patch("api.drone.vm_manager._wait_for_nodeodm_ready"):
            from api.drone import vm_manager  # noqa: PLC0415
            vm_manager.ensure_vm_running(project="proj", zone="zone", instance="inst")

        instances.start.assert_not_called()


# ---------------------------------------------------------------------------
# shutdown_vm_if_idle tests
# Note: get_supabase_client is lazily imported inside the function, so we
#       patch it at the source module rather than on vm_manager.
# ---------------------------------------------------------------------------

class TestShutdownVmIfIdle:
    def test_stops_vm_when_no_active_captures(self):
        compute, instances = _make_compute(vm_status="RUNNING")
        db = _make_db(active_count=0)

        with patch("api.drone.vm_manager._compute", return_value=compute), \
             patch("api.db.supabase_client.get_supabase_client", return_value=db):
            from api.drone import vm_manager  # noqa: PLC0415
            vm_manager.shutdown_vm_if_idle(project="proj", zone="zone", instance="inst")

        instances.stop.assert_called_once_with(project="proj", zone="zone", instance="inst")

    def test_keeps_vm_running_when_active_captures_present(self):
        compute, instances = _make_compute(vm_status="RUNNING")
        db = _make_db(active_count=2)

        with patch("api.drone.vm_manager._compute", return_value=compute), \
             patch("api.db.supabase_client.get_supabase_client", return_value=db):
            from api.drone import vm_manager  # noqa: PLC0415
            vm_manager.shutdown_vm_if_idle(project="proj", zone="zone", instance="inst")

        instances.stop.assert_not_called()

    def test_does_not_stop_already_terminated_vm(self):
        compute, instances = _make_compute(vm_status="TERMINATED")
        db = _make_db(active_count=0)

        with patch("api.drone.vm_manager._compute", return_value=compute), \
             patch("api.db.supabase_client.get_supabase_client", return_value=db):
            from api.drone import vm_manager  # noqa: PLC0415
            vm_manager.shutdown_vm_if_idle(project="proj", zone="zone", instance="inst")

        instances.stop.assert_not_called()


# ---------------------------------------------------------------------------
# _wait_for_nodeodm_ready tests
# ---------------------------------------------------------------------------

class TestWaitForNodeodmReady:
    def test_returns_when_nodeodm_responds_200(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 200

        with patch("api.drone.vm_manager.requests.get", return_value=mock_resp):
            from api.drone import vm_manager  # noqa: PLC0415
            vm_manager._wait_for_nodeodm_ready(timeout=10)  # should not raise

    def test_raises_on_timeout(self, monkeypatch):
        from api.drone import vm_manager  # noqa: PLC0415

        monkeypatch.setattr(
            "api.drone.vm_manager.requests.get",
            lambda *a, **kw: (_ for _ in ()).throw(requests.exceptions.ConnectionError("refused")),
        )
        monkeypatch.setattr("api.drone.vm_manager.time.sleep", lambda _: None)

        tick = {"t": 0.0}

        def fake_monotonic():
            tick["t"] += 10
            return tick["t"]

        monkeypatch.setattr("api.drone.vm_manager.time.monotonic", fake_monotonic)

        with pytest.raises(RuntimeError, match="NodeODM did not become ready"):
            vm_manager._wait_for_nodeodm_ready(timeout=5)

    def test_retries_until_ready(self):
        call_count = {"n": 0}

        def mock_get(url, timeout=5):
            call_count["n"] += 1
            resp = MagicMock()
            resp.status_code = 200 if call_count["n"] >= 3 else 503
            return resp

        with patch("api.drone.vm_manager.requests.get", side_effect=mock_get), \
             patch("api.drone.vm_manager.time.sleep"):
            from api.drone import vm_manager  # noqa: PLC0415
            vm_manager._wait_for_nodeodm_ready(timeout=60)

        assert call_count["n"] >= 3
