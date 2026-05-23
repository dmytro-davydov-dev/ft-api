"""NodeODM REST client for ft-api.

NodeODM endpoint is read from NODE_ODM_URL env var (default: http://flowterra-nodeodm-vm:3000).
Auth token from NODE_ODM_TOKEN env var.
"""
import os

import requests

_DEFAULT_ODM_URL = "http://flowterra-nodeodm-vm:3000"
_TIMEOUT = 30


def _base_url() -> str:
    return os.environ.get("NODE_ODM_URL", _DEFAULT_ODM_URL).rstrip("/")


def _headers() -> dict:
    token = os.environ.get("NODE_ODM_TOKEN", "")
    return {"Authorization": f"Bearer {token}"}


def create_task(capture_id: str, photo_urls: list[str], options: dict) -> str:
    """POST a new task to NodeODM and return the odm_task_id.

    Args:
        capture_id: Used for idempotency tagging (optional, informational).
        photo_urls: GCS URLs of uploaded photos.
        options:    ODM processing options dict from capture.metadata.

    Returns:
        odm_task_id (str)

    Raises:
        NodeODMError: If NodeODM is unreachable or returns a non-2xx status.
    """
    url = f"{_base_url()}/task/new/init"
    payload = {
        "options": [
            {"name": "feature-quality", "value": options.get("feature_quality", "medium")},
            {"name": "pc-quality", "value": options.get("pc_quality", "medium")},
            {"name": "mesh", "value": options.get("mesh", False)},
        ],
        "images": photo_urls,
    }
    try:
        resp = requests.post(url, json=payload, headers=_headers(), timeout=_TIMEOUT)
        resp.raise_for_status()
    except requests.exceptions.ConnectionError as exc:
        raise NodeODMError("nodeodm_unreachable") from exc
    except requests.exceptions.HTTPError as exc:
        raise NodeODMError(f"nodeodm_http_error: {exc}") from exc

    data = resp.json()
    task_id = data.get("uuid") or data.get("taskId") or data.get("id")
    if not task_id:
        raise NodeODMError("nodeodm_no_task_id_in_response")
    return str(task_id)


def get_task_status(odm_task_id: str) -> dict:
    """GET task status from NodeODM.

    Returns:
        Dict with at least "status" key: "queued" | "running" | "completed" | "failed"
        and optional "progress" (0-100) and "error" keys.

    Raises:
        NodeODMError: If NodeODM is unreachable.
    """
    url = f"{_base_url()}/task/{odm_task_id}"
    try:
        resp = requests.get(url, headers=_headers(), timeout=_TIMEOUT)
        resp.raise_for_status()
    except requests.exceptions.ConnectionError as exc:
        raise NodeODMError("nodeodm_unreachable") from exc
    except requests.exceptions.HTTPError as exc:
        raise NodeODMError(f"nodeodm_http_error: {exc}") from exc

    data = resp.json()
    status_code = data.get("status", {}).get("code", 0)
    # NodeODM status codes: 10=queued, 20=running, 30=failed, 40=completed
    _CODE_MAP = {10: "queued", 20: "running", 30: "failed", 40: "completed"}
    status = _CODE_MAP.get(status_code, "running")
    return {
        "status": status,
        "progress": data.get("processingTime", 0),
        "error": data.get("status", {}).get("referenceName", ""),
    }


def get_task_download_url(odm_task_id: str) -> str:
    """Return the NodeODM download URL for the LAS point cloud output."""
    return f"{_base_url()}/task/{odm_task_id}/download/odm_georeferenced_model.las"


class NodeODMError(Exception):
    """Raised when NodeODM is unreachable or returns an error."""
