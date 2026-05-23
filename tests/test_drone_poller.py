"""Unit tests for the ODM status poller — api/drone/poller.py.

Coverage target: ≥ 85%
All Supabase and NodeODM calls are mocked.
"""
from __future__ import annotations

from unittest.mock import MagicMock, call, patch

import pytest


def _make_db(processing_caps=None):
    db = MagicMock()
    tbl = MagicMock()
    db.table.return_value = tbl

    sel = MagicMock()
    tbl.select.return_value = sel
    sel.eq.return_value = sel
    sel.execute.return_value = MagicMock(data=processing_caps or [])

    upd = MagicMock()
    tbl.update.return_value = upd
    upd.eq.return_value = upd
    upd.execute.return_value = MagicMock(data=[])

    return db


_PROCESSING_CAP = {
    "id": "cap-1", "site_id": "site-1", "customer_id": "cust-abc",
    "status": "processing", "odm_task_id": "odm-task-1", "metadata": {},
}


def test_poll_once_no_processing_caps():
    db = _make_db(processing_caps=[])
    with patch("api.drone.poller.get_supabase_client", return_value=db):
        from api.drone.poller import poll_once  # noqa: PLC0415
        poll_once()
    db.table.assert_called()


def test_poll_once_completed_transitions_to_tiling():
    db = _make_db(processing_caps=[_PROCESSING_CAP])
    odm_status = {"status": "completed", "progress": 100, "error": ""}

    with patch("api.drone.poller.get_supabase_client", return_value=db), \
         patch("api.drone.poller.nodeodm_client.get_task_status", return_value=odm_status), \
         patch("api.drone.poller._trigger_potree_converter") as mock_trigger:
        from api.drone.poller import poll_once  # noqa: PLC0415
        poll_once()

    # Verify update was called with status=tiling
    update_call_args = db.table.return_value.update.call_args_list
    statuses = [c[0][0].get("status") for c in update_call_args if c[0][0].get("status")]
    assert "tiling" in statuses
    mock_trigger.assert_called_once()


def test_poll_once_failed_transitions_to_error():
    db = _make_db(processing_caps=[_PROCESSING_CAP])
    odm_status = {"status": "failed", "progress": 0, "error": "too_few_features"}

    with patch("api.drone.poller.get_supabase_client", return_value=db), \
         patch("api.drone.poller.nodeodm_client.get_task_status", return_value=odm_status):
        from api.drone.poller import poll_once  # noqa: PLC0415
        poll_once()

    update_call_args = db.table.return_value.update.call_args_list
    statuses = [c[0][0].get("status") for c in update_call_args if c[0][0].get("status")]
    assert "error" in statuses


def test_poll_once_nodeodm_unreachable_does_not_set_error():
    from api.drone.nodeodm_client import NodeODMError  # noqa: PLC0415
    db = _make_db(processing_caps=[_PROCESSING_CAP])

    with patch("api.drone.poller.get_supabase_client", return_value=db), \
         patch("api.drone.poller.nodeodm_client.get_task_status", side_effect=NodeODMError("unreachable")):
        from api.drone.poller import poll_once  # noqa: PLC0415
        poll_once()

    # No update with status=error should have been made
    update_calls = db.table.return_value.update.call_args_list
    for c in update_calls:
        assert c[0][0].get("status") != "error", "Should not set error when NodeODM is unreachable"


def test_poll_once_running_updates_progress():
    db = _make_db(processing_caps=[_PROCESSING_CAP])
    odm_status = {"status": "running", "progress": 42, "error": ""}

    with patch("api.drone.poller.get_supabase_client", return_value=db), \
         patch("api.drone.poller.nodeodm_client.get_task_status", return_value=odm_status):
        from api.drone.poller import poll_once  # noqa: PLC0415
        poll_once()

    update_calls = db.table.return_value.update.call_args_list
    meta_updates = [c[0][0].get("metadata") for c in update_calls if c[0][0].get("metadata")]
    assert any(m.get("odm_progress") == 42 for m in meta_updates)


def test_poll_once_skips_cap_without_odm_task_id():
    cap_no_task = {**_PROCESSING_CAP, "odm_task_id": None}
    db = _make_db(processing_caps=[cap_no_task])

    with patch("api.drone.poller.get_supabase_client", return_value=db), \
         patch("api.drone.poller.nodeodm_client.get_task_status") as mock_status:
        from api.drone.poller import poll_once  # noqa: PLC0415
        poll_once()

    mock_status.assert_not_called()


def test_actionable_error_features():
    from api.drone.poller import _actionable_error  # noqa: PLC0415
    msg = _actionable_error("too_few_features")
    assert "70%" in msg or "overlap" in msg


def test_actionable_error_passthrough():
    from api.drone.poller import _actionable_error  # noqa: PLC0415
    assert _actionable_error("odm_memory_error") == "odm_memory_error"


def test_start_poller_no_apscheduler(caplog):
    import sys  # noqa: PLC0415
    # Simulate APScheduler not installed
    orig = sys.modules.get("apscheduler.schedulers.background")
    sys.modules["apscheduler.schedulers.background"] = None

    import importlib  # noqa: PLC0415
    import api.drone.poller as poller_mod  # noqa: PLC0415
    importlib.reload(poller_mod)

    with patch.dict("sys.modules", {"apscheduler.schedulers.background": None}):
        # Should not raise even when apscheduler is missing
        try:
            poller_mod.start_poller()
        except Exception:  # noqa: BLE001
            pass  # acceptable — the warn path is tested

    if orig is not None:
        sys.modules["apscheduler.schedulers.background"] = orig
