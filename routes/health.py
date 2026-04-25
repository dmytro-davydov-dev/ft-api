"""Health check endpoint.

GET /health → {"status": "ok"}

Unauthenticated — used by Cloud Run liveness/readiness probes
and uptime monitors.
"""
from flask import Blueprint, jsonify

health_bp = Blueprint("health", __name__)


@health_bp.get("/health")
def health():
    return jsonify({"status": "ok"})
