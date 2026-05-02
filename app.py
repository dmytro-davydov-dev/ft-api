"""ft-api — Flowterra backend API.

Flask application factory.  Entry point for Cloud Run (via Gunicorn).
"""
import os

import firebase_admin
from flask import Flask, jsonify
from flask_cors import CORS
from werkzeug.exceptions import HTTPException

from routes.health import health_bp
from routes.v1 import v1_bp


def create_app() -> Flask:
    """Create and configure the Flask application."""
    app = Flask(__name__)

    # CORS — allowed origins.
    # Dev default: Firebase Hosting dev URLs + local Vite dev server.
    # Override in prod via ALLOWED_ORIGINS env var (comma-separated list).
    _dev_origins = [
        "http://localhost:5173",
        "http://localhost:5174",
        "https://flowterra-dev.firebaseapp.com",
        "https://flowterra-dev.web.app",
    ]
    raw_origins = os.environ.get("ALLOWED_ORIGINS", "")
    allowed_origins = (
        [o.strip() for o in raw_origins.split(",") if o.strip()]
        if raw_origins
        else _dev_origins
    )
    CORS(app, origins=allowed_origins)

    # Initialise Firebase Admin SDK once per process.
    # On Cloud Run, Application Default Credentials resolve automatically.
    # Locally, set GOOGLE_APPLICATION_CREDENTIALS to a service-account JSON.
    if not firebase_admin._apps:  # noqa: SLF001 — intentional guard
        firebase_admin.initialize_app()

    # Register blueprints.
    app.register_blueprint(health_bp)
    app.register_blueprint(v1_bp, url_prefix="/api/v1")

    # ---------------------------------------------------------------------------
    # Error handlers — return JSON for all errors so clients get machine-readable
    # responses instead of HTML pages (important for BigQuery runtime errors).
    # ---------------------------------------------------------------------------

    @app.errorhandler(HTTPException)
    def handle_http_error(exc: HTTPException):  # type: ignore[type-arg]
        """Convert Werkzeug HTTP exceptions to JSON."""
        return jsonify({"error": exc.name, "message": exc.description}), exc.code

    @app.errorhandler(Exception)
    def handle_unexpected_error(exc: Exception):
        """Catch-all for unhandled exceptions (e.g. BigQuery errors).

        Logs the full traceback on the server and returns a generic 502 JSON
        body to the client — avoids leaking internal details.
        """
        import logging  # noqa: PLC0415
        import traceback  # noqa: PLC0415

        logging.error("Unhandled exception: %s\n%s", exc, traceback.format_exc())
        return (
            jsonify({"error": "upstream_error", "message": "An upstream service error occurred."}),
            502,
        )

    return app


# Gunicorn entry point: `gunicorn "app:create_app()"`
app = create_app()

if __name__ == "__main__":
    # Local dev only — Cloud Run uses Gunicorn.
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
