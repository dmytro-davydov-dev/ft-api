"""api/drone/poller.py — background ODM status poller.

Runs every 60 s (Option A: APScheduler thread inside the Cloud Run instance).
Queries NodeODM for all 'processing' captures and syncs status to Supabase.

On ODM completion:
  1. Transitions capture.status → 'tiling'
  2. Triggers the PotreeConverter Cloud Run Job (Phase 5.09)

On ODM failure:
  Transitions capture.status → 'error' with actionable error message.
"""
from __future__ import annotations

import logging
import os

import requests

from api.db.supabase_client import get_supabase_client
from api.drone import nodeodm_client

logger = logging.getLogger(__name__)

_POTREE_JOB_URL_ENV = "POTREE_CONVERTER_JOB_URL"


def poll_once() -> None:
    """Single poll iteration — called by the scheduler every 60 s."""
    db = get_supabase_client()
    result = db.table("captures").select("*").eq("status", "processing").execute()
    processing = result.data or []

    if not processing:
        return

    logger.info("Poller: checking %d processing capture(s)", len(processing))

    for capture in processing:
        capture_id = capture["id"]
        odm_task_id = capture.get("odm_task_id")
        if not odm_task_id:
            continue
        _sync_capture(db, capture_id, odm_task_id, capture)


def _sync_capture(db, capture_id: str, odm_task_id: str, capture: dict) -> None:
    try:
        status = nodeodm_client.get_task_status(odm_task_id)
    except nodeodm_client.NodeODMError:
        # Transient — retry next cycle; do not set error
        logger.warning("Poller: NodeODM unreachable, will retry capture %s", capture_id)
        return

    odm_status = status["status"]

    if odm_status == "completed":
        _handle_completed(db, capture_id, odm_task_id)
    elif odm_status == "failed":
        error_msg = status.get("error") or "odm_processing_failed"
        _handle_failed(db, capture_id, capture, error_msg)
    else:
        progress = status.get("progress", 0)
        existing_meta = capture.get("metadata") or {}
        db.table("captures").update({
            "metadata": {**existing_meta, "odm_progress": progress},
        }).eq("id", capture_id).execute()
        logger.debug("Poller: capture %s progress=%s", capture_id, progress)


def _handle_completed(db, capture_id: str, odm_task_id: str) -> None:
    bucket = os.environ.get("GCS_DRONE_BUCKET", "flowterra-drone-dev")
    gcs_prefix = f"captures/{capture_id}/processed/"

    gsd_cm = _extract_gsd(odm_task_id)
    update_payload: dict = {"status": "tiling", "tiles_gcs_prefix": gcs_prefix}
    if gsd_cm is not None:
        # Merge gsd_cm into metadata without overwriting existing keys.
        cap = db.table("captures").select("metadata").eq("id", capture_id).execute()
        existing_meta = (cap.data[0].get("metadata") or {}) if cap.data else {}
        update_payload["metadata"] = {**existing_meta, "gsd_cm": gsd_cm}

    db.table("captures").update(update_payload).eq("id", capture_id).execute()

    logger.info("Poller: capture %s completed → tiling; gsd_cm=%s; triggering PotreeConverter", capture_id, gsd_cm)
    _trigger_potree_converter(capture_id, odm_task_id, bucket, gcs_prefix)


def _extract_gsd(odm_task_id: str) -> float | None:
    """Fetch ODM task output and extract GSD in cm/pixel.

    Returns None if the report is unavailable or the field is missing.
    """
    base_url = os.environ.get("NODE_ODM_URL", "http://flowterra-nodeodm-vm:3000").rstrip("/")
    report_url = f"{base_url}/task/{odm_task_id}/download/odm_report/report.json"
    try:
        resp = requests.get(report_url, timeout=10)
        if resp.status_code != 200:
            logger.warning("Poller: ODM report not available for task %s (status %s)", odm_task_id, resp.status_code)
            return None
        data = resp.json()
        gsd_m = data.get("gsd")
        if gsd_m is None:
            return None
        return round(float(gsd_m) * 100, 2)
    except Exception:  # noqa: BLE001
        logger.warning("Poller: failed to fetch GSD for task %s", odm_task_id)
        return None


def _handle_failed(db, capture_id: str, capture: dict, error_msg: str) -> None:
    existing_meta = capture.get("metadata") or {}
    actionable = _actionable_error(error_msg)
    db.table("captures").update({
        "status": "error",
        "metadata": {**existing_meta, "error": actionable},
    }).eq("id", capture_id).execute()
    logger.warning("Poller: capture %s failed — %s", capture_id, actionable)


def _trigger_potree_converter(
    capture_id: str, odm_task_id: str, bucket: str, gcs_prefix: str
) -> None:
    """Fire-and-forget: POST to the PotreeConverter Cloud Run Job URL."""
    job_url = os.environ.get(_POTREE_JOB_URL_ENV, "")
    if not job_url:
        logger.warning("Poller: %s not set; PotreeConverter not triggered", _POTREE_JOB_URL_ENV)
        return
    try:
        requests.post(
            job_url,
            json={"capture_id": capture_id, "odm_task_id": odm_task_id,
                  "bucket": bucket, "gcs_prefix": gcs_prefix},
            timeout=10,
        )
    except Exception:  # noqa: BLE001
        logger.exception("Poller: failed to trigger PotreeConverter for capture %s", capture_id)


def _actionable_error(raw: str) -> str:
    if "few" in raw.lower() or "features" in raw.lower():
        return "too_few_features — ensure 70%+ image overlap"
    return raw or "odm_processing_failed"


def start_poller(interval_seconds: int = 60) -> None:
    """Start the APScheduler background thread. Call once at app startup."""
    try:
        from apscheduler.schedulers.background import BackgroundScheduler  # noqa: PLC0415
    except ImportError:
        logger.warning("apscheduler not installed — ODM poller disabled")
        return

    scheduler = BackgroundScheduler()
    scheduler.add_job(poll_once, "interval", seconds=interval_seconds, id="odm_poller")
    scheduler.start()
    logger.info("ODM poller started (interval=%ds)", interval_seconds)
