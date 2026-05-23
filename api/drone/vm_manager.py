"""api/drone/vm_manager.py — GPU VM lifecycle management.

Starts and stops the flowterra-nodeodm-vm Spot VM on demand to keep
idle GPU cost at $0/hr.  Called by the ODM trigger endpoint (before
posting a task) and by the poller (when no active captures remain).
"""
from __future__ import annotations

import logging
import os
import time

import requests

logger = logging.getLogger(__name__)

_PROJECT_ENV   = "GCP_PROJECT"
_ZONE_ENV      = "GCP_ZONE"
_INSTANCE_ENV  = "NODEODM_INSTANCE"
_NODEODM_URL_ENV = "NODE_ODM_URL"

_DEFAULT_INSTANCE = "flowterra-nodeodm-vm"
_DEFAULT_ZONE     = "europe-west1-b"
_NODEODM_READY_TIMEOUT = 60
_IDLE_GRACE_SECONDS    = 300  # 5 min grace before auto-stop


def _compute():
    """Return a googleapiclient compute resource (lazy import)."""
    from googleapiclient import discovery  # noqa: PLC0415
    return discovery.build("compute", "v1")


def _env(key: str, default: str = "") -> str:
    return os.environ.get(key, default)


def ensure_vm_running(
    project: str | None = None,
    zone: str | None = None,
    instance: str | None = None,
) -> None:
    """Start the NodeODM VM if it is stopped; wait for it to be ready.

    Args:
        project:  GCP project ID. Falls back to GCP_PROJECT env var.
        zone:     GCP zone.       Falls back to GCP_ZONE env var.
        instance: Instance name.  Falls back to NODEODM_INSTANCE env var.

    Raises:
        RuntimeError: If the VM does not become ready within the timeout.
    """
    project  = project  or _env(_PROJECT_ENV)
    zone     = zone     or _env(_ZONE_ENV, _DEFAULT_ZONE)
    instance = instance or _env(_INSTANCE_ENV, _DEFAULT_INSTANCE)

    compute = _compute()
    result = compute.instances().get(project=project, zone=zone, instance=instance).execute()
    vm_status = result.get("status", "")

    if vm_status not in ("RUNNING", "STAGING"):
        logger.info("vm_manager: starting instance %s (current status: %s)", instance, vm_status)
        compute.instances().start(project=project, zone=zone, instance=instance).execute()
    else:
        logger.debug("vm_manager: instance %s already %s", instance, vm_status)

    _wait_for_nodeodm_ready(timeout=_NODEODM_READY_TIMEOUT)


def shutdown_vm_if_idle(
    project: str | None = None,
    zone: str | None = None,
    instance: str | None = None,
) -> None:
    """Stop the NodeODM VM when no captures are actively processing.

    Checks for any captures in 'processing' or 'tiling' state; stops the
    VM only when none are found.  A 5-minute grace period is enforced by
    the caller (the poller) — this function checks state and stops.

    Args:
        project:  GCP project ID.
        zone:     GCP zone.
        instance: Instance name.
    """
    from api.db.supabase_client import get_supabase_client  # noqa: PLC0415

    project  = project  or _env(_PROJECT_ENV)
    zone     = zone     or _env(_ZONE_ENV, _DEFAULT_ZONE)
    instance = instance or _env(_INSTANCE_ENV, _DEFAULT_INSTANCE)

    db = get_supabase_client()
    active = (
        db.table("captures")
        .select("id")
        .in_("status", ["processing", "tiling"])
        .execute()
    )

    if active.data:
        logger.debug(
            "vm_manager: %d active capture(s) — VM kept running", len(active.data)
        )
        return

    compute = _compute()
    result = compute.instances().get(project=project, zone=zone, instance=instance).execute()
    vm_status = result.get("status", "")

    if vm_status == "RUNNING":
        logger.info("vm_manager: no active captures — stopping instance %s", instance)
        compute.instances().stop(project=project, zone=zone, instance=instance).execute()
    else:
        logger.debug("vm_manager: instance %s already %s; nothing to do", instance, vm_status)


def _wait_for_nodeodm_ready(timeout: int = _NODEODM_READY_TIMEOUT) -> None:
    """Poll NodeODM /info until it returns HTTP 200 or timeout is reached."""
    base_url = _env(_NODEODM_URL_ENV, "http://flowterra-nodeodm-vm:3000").rstrip("/")
    info_url = f"{base_url}/info"
    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:
        try:
            resp = requests.get(info_url, timeout=5)
            if resp.status_code == 200:
                logger.info("vm_manager: NodeODM ready at %s", info_url)
                return
        except requests.exceptions.ConnectionError:
            pass
        time.sleep(5)

    raise RuntimeError(
        f"NodeODM did not become ready within {timeout}s at {info_url}"
    )
