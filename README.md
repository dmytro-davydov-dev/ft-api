# ft-api

Primary REST API for the Flowterra platform. A single Flask service deployed on Google Cloud Run — the backend entry point for all client-initiated requests from ft-web-app.

Handles authentication, tenant isolation, and BigQuery-backed analytics reporting. All routes are scoped to a verified `customerId` tenant; cross-tenant data access is not possible by design.

## Stack

- **Language:** Python 3.11
- **Framework:** Flask
- **Deployment:** Cloud Run (scale-to-zero, `min=0 max=5`)
- **Auth:** Firebase Admin SDK (JWT verification)
- **Data:** Firestore (operational), BigQuery (analytics/reports)
- **Secrets:** GCP Secret Manager
- **CI/CD:** GitHub Actions → `gcloud run deploy`

## Structure

```
ft-api/
├── app.py                  # Flask application factory (Gunicorn entry point)
├── auth/
│   ├── middleware.py       # require_auth — JWT verification decorator
│   └── tenant.py          # require_tenant — blueprint-level tenant isolation hook
├── routes/
│   ├── health.py           # GET /health — unauthenticated liveness probe
│   └── v1/
│       ├── me.py           # GET /api/v1/me
│       ├── dashboard.py    # GET /api/v1/dashboard
│       └── report/         # (Phase 4) report endpoints — protected by require_tenant
├── analytics/
│   └── schema/             # BigQuery schema definitions
├── tests/
│   ├── conftest.py         # Firebase Admin mock fixtures
│   ├── test_middleware.py  # Tests for require_auth
│   └── test_tenant.py      # Tests for require_tenant (FLO-28)
├── documentation/          # Developer reference docs (see below)
├── requirements.txt
└── Dockerfile
```

## Local dev

```bash
python3.11 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
GOOGLE_APPLICATION_CREDENTIALS=path/to/sa.json flask --app app run --port 8080
```

## Tests

```bash
pytest
```

All tests mock the Firebase Admin SDK — no real GCP calls are made.

## Documentation

| Topic | Doc |
|---|---|
| Authentication & tenant isolation | [documentation/auth.md](documentation/auth.md) |
