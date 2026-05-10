"""GET  /api/v1/customers/<customer_id>/people — personnel registry.
POST /api/v1/customers/<customer_id>/people — add a person record.

Phase 4: GET returns a static pilot roster of 10 personnel records.
         POST echoes back the submitted record with a generated id.
Phase 5+: replace with Firestore reads/writes from
          customers/{customerId}/people/{personId}.

Person shape:
  {
    "id":               STRING,
    "firstName":        STRING,
    "lastName":         STRING,
    "email":            STRING,
    "phone":            STRING,
    "company":          STRING,
    "role":             STRING,
    "nationality":      STRING,
    "tagId":            STRING | null,   -- assigned BLE badge id
    "pictureUrl":       STRING | null,   -- GCS public URL or null
    "supervisor":       STRING,
    "emergencyContact": STRING
  }
"""
from __future__ import annotations

import uuid

from flask import Blueprint, g, jsonify, request

from auth.middleware import require_auth

people_bp = Blueprint("people", __name__)

# ---------------------------------------------------------------------------
# Pilot personnel roster (10 records matching the HQ Pilot site).
# tagId values align with the pilot tag roster in tags.py (tag-0001…tag-0010).
# ---------------------------------------------------------------------------

_PILOT_PEOPLE: list[dict] = [
    {
        "id":               "person-0001",
        "firstName":        "Alice",
        "lastName":         "Nguyen",
        "email":            "alice.nguyen@example.com",
        "phone":            "+44 7700 900001",
        "company":          "Flowterra Demo Corp",
        "role":             "Software Engineer",
        "nationality":      "Vietnamese",
        "tagId":            "tag-0001",
        "pictureUrl":       None,
        "supervisor":       "Bob Chen",
        "emergencyContact": "+44 7700 900099",
    },
    {
        "id":               "person-0002",
        "firstName":        "Bob",
        "lastName":         "Chen",
        "email":            "bob.chen@example.com",
        "phone":            "+44 7700 900002",
        "company":          "Flowterra Demo Corp",
        "role":             "Engineering Manager",
        "nationality":      "Chinese",
        "tagId":            "tag-0002",
        "pictureUrl":       None,
        "supervisor":       "Clara Osei",
        "emergencyContact": "+44 7700 900098",
    },
    {
        "id":               "person-0003",
        "firstName":        "Clara",
        "lastName":         "Osei",
        "email":            "clara.osei@example.com",
        "phone":            "+44 7700 900003",
        "company":          "Flowterra Demo Corp",
        "role":             "VP of Engineering",
        "nationality":      "Ghanaian",
        "tagId":            "tag-0003",
        "pictureUrl":       None,
        "supervisor":       "",
        "emergencyContact": "+44 7700 900097",
    },
    {
        "id":               "person-0004",
        "firstName":        "David",
        "lastName":         "Kowalski",
        "email":            "david.kowalski@example.com",
        "phone":            "+44 7700 900004",
        "company":          "Flowterra Demo Corp",
        "role":             "Product Designer",
        "nationality":      "Polish",
        "tagId":            "tag-0004",
        "pictureUrl":       None,
        "supervisor":       "Clara Osei",
        "emergencyContact": "+44 7700 900096",
    },
    {
        "id":               "person-0005",
        "firstName":        "Emma",
        "lastName":         "Svensson",
        "email":            "emma.svensson@example.com",
        "phone":            "+44 7700 900005",
        "company":          "Flowterra Demo Corp",
        "role":             "Data Analyst",
        "nationality":      "Swedish",
        "tagId":            "tag-0005",
        "pictureUrl":       None,
        "supervisor":       "Bob Chen",
        "emergencyContact": "+44 7700 900095",
    },
    {
        "id":               "person-0006",
        "firstName":        "Femi",
        "lastName":         "Adeyemi",
        "email":            "femi.adeyemi@contractor.io",
        "phone":            "+44 7700 900006",
        "company":          "Contractor IO",
        "role":             "Facilities Contractor",
        "nationality":      "Nigerian",
        "tagId":            "tag-0006",
        "pictureUrl":       None,
        "supervisor":       "Clara Osei",
        "emergencyContact": "+44 7700 900094",
    },
    {
        "id":               "person-0007",
        "firstName":        "Greta",
        "lastName":         "Müller",
        "email":            "greta.muller@visitor.org",
        "phone":            "+44 7700 900007",
        "company":          "Visitor Org",
        "role":             "Visitor",
        "nationality":      "German",
        "tagId":            "tag-0007",
        "pictureUrl":       None,
        "supervisor":       "",
        "emergencyContact": "+44 7700 900093",
    },
    {
        "id":               "person-0008",
        "firstName":        "Hiroshi",
        "lastName":         "Tanaka",
        "email":            "hiroshi.tanaka@example.com",
        "phone":            "+44 7700 900008",
        "company":          "Flowterra Demo Corp",
        "role":             "Backend Engineer",
        "nationality":      "Japanese",
        "tagId":            "tag-0008",
        "pictureUrl":       None,
        "supervisor":       "Bob Chen",
        "emergencyContact": "+44 7700 900092",
    },
    {
        "id":               "person-0009",
        "firstName":        "Ifeoma",
        "lastName":         "Eze",
        "email":            "ifeoma.eze@example.com",
        "phone":            "+44 7700 900009",
        "company":          "Flowterra Demo Corp",
        "role":             "DevOps Engineer",
        "nationality":      "Nigerian",
        "tagId":            "tag-0009",
        "pictureUrl":       None,
        "supervisor":       "Clara Osei",
        "emergencyContact": "+44 7700 900091",
    },
    {
        "id":               "person-0010",
        "firstName":        "Jonas",
        "lastName":         "Eriksson",
        "email":            "jonas.eriksson@example.com",
        "phone":            "+44 7700 900010",
        "company":          "Flowterra Demo Corp",
        "role":             "Security Engineer",
        "nationality":      "Swedish",
        "tagId":            None,
        "pictureUrl":       None,
        "supervisor":       "Clara Osei",
        "emergencyContact": "+44 7700 900090",
    },
]

# In-memory store for people added via POST during this process lifetime.
# Phase 5: swap for Firestore writes.
_added_people: list[dict] = []


def _all_people() -> list[dict]:
    return _PILOT_PEOPLE + _added_people


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@people_bp.get("/customers/<customer_id>/people")
@require_auth
def list_people(customer_id: str):
    """Return all personnel records for a tenant. MVP: pilot roster."""
    if customer_id != g.customer_id:
        return jsonify({"error": "Forbidden"}), 403

    return jsonify({
        "customerId": customer_id,
        "people":     _all_people(),
    })


@people_bp.post("/customers/<customer_id>/people")
@require_auth
def create_person(customer_id: str):
    """Add a new person record. MVP: stored in-memory; Phase 5 → Firestore."""
    if customer_id != g.customer_id:
        return jsonify({"error": "Forbidden"}), 403

    body = request.get_json(silent=True) or {}

    required_fields = {
        "firstName", "lastName", "email", "phone",
        "company", "role", "nationality", "supervisor", "emergencyContact",
    }
    missing = required_fields - body.keys()
    if missing:
        return jsonify({"error": f"Missing fields: {sorted(missing)}"}), 400

    person: dict = {
        "id":               f"person-{uuid.uuid4().hex[:8]}",
        "firstName":        body["firstName"],
        "lastName":         body["lastName"],
        "email":            body["email"],
        "phone":            body["phone"],
        "company":          body["company"],
        "role":             body["role"],
        "nationality":      body["nationality"],
        "tagId":            body.get("tagId"),
        "pictureUrl":       body.get("pictureUrl"),
        "supervisor":       body["supervisor"],
        "emergencyContact": body["emergencyContact"],
    }
    _added_people.append(person)

    return jsonify(person), 201
