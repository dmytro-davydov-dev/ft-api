"""API v1 blueprint — aggregates all /api/v1/* routes."""
from flask import Blueprint

from .dashboard import dashboard_bp
from .me import me_bp

v1_bp = Blueprint("v1", __name__)

v1_bp.register_blueprint(me_bp)
v1_bp.register_blueprint(dashboard_bp)
