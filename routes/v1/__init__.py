"""API v1 blueprint — aggregates all /api/v1/* routes."""
from flask import Blueprint

from .dashboard import dashboard_bp
from .events import events_bp
from .gateways import gateways_bp
from .geofences import geofences_bp
from .me import me_bp
from .report import report_bp
from .sites import sites_bp
from .people import people_bp
from .tags import tags_bp

v1_bp = Blueprint("v1", __name__)

v1_bp.register_blueprint(me_bp)
v1_bp.register_blueprint(dashboard_bp)
v1_bp.register_blueprint(events_bp)
v1_bp.register_blueprint(gateways_bp)
v1_bp.register_blueprint(geofences_bp)
v1_bp.register_blueprint(people_bp)
v1_bp.register_blueprint(report_bp)
v1_bp.register_blueprint(sites_bp)
v1_bp.register_blueprint(tags_bp)
