# Authentication & Tenant Isolation

ft-api uses Firebase Auth JWTs for authentication. Two middleware components live in `auth/` — choose based on the route type.

---

## `auth/middleware.py` — `require_auth`

A per-endpoint decorator for general routes that do not serve BigQuery-backed data.

**What it does:**
- Extracts the `Authorization: Bearer <token>` header
- Verifies the Firebase JWT via the Admin SDK
- Rejects expired or invalid tokens with `401`
- Rejects tokens missing the `customerId` claim with `403`
- Injects `g.uid` and `g.customer_id` into the Flask request context

**Usage:**

```python
from auth.middleware import require_auth
from flask import g, jsonify

@blueprint.get("/me")
@require_auth
def get_me():
    return jsonify({"uid": g.uid, "customerId": g.customer_id})
```

---

## `auth/tenant.py` — `require_tenant()`

A Blueprint-level `before_request` hook for report routes (Phase 4+). Adds two guarantees on top of `require_auth`:

1. **URL–token match** — asserts the `<id>` path segment equals the token's `customerId` claim. A mismatch returns `403` before any query runs. This prevents a user from accessing another tenant's data by changing the URL.
2. **Parameterised BigQuery injection** — injects `g.bq_customer_id` for use as a named query parameter. Client-supplied `customerId` values (query string, request body) are ignored entirely.

Applied once at the blueprint level — no per-endpoint decoration needed:

```python
from auth.tenant import require_tenant
from flask import Blueprint

report_bp = Blueprint("report", __name__, url_prefix="/api/v1/customers/<id>/reports")
report_bp.before_request(require_tenant())  # all child routes are covered
```

**BigQuery query pattern in route handlers:**

```python
from google.cloud import bigquery
from flask import g

job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter("customerId", "STRING", g.bq_customer_id),
    ]
)
query = "SELECT ... FROM `table` WHERE customerId = @customerId AND ..."

# Never interpolate directly:
# f"WHERE customerId = '{g.customer_id}'"  ← FORBIDDEN (SQL injection + kills BQ plan caching)
```

---

## Which middleware to use

| Route type | Middleware |
|---|---|
| `/me`, `/dashboard`, any non-report route | `@require_auth` (per-endpoint decorator) |
| Report routes under `/customers/<id>/...` | `require_tenant()` (blueprint `before_request`) |

`require_tenant` is intentionally separate from `require_auth` to avoid adding BigQuery injection overhead to routes that don't need it.

---

## Error responses

| Condition | Status |
|---|---|
| Missing `Authorization` header | `401` |
| Expired token | `401` |
| Invalid / unverifiable token | `401` |
| Token valid but `customerId` claim absent | `403` |
| URL `<id>` does not match token `customerId` | `403` |

---

## Tests

- `tests/test_middleware.py` — covers `require_auth`
- `tests/test_tenant.py` — covers `require_tenant`, including both FLO-28 exit criteria (cross-tenant access blocked, URL mismatch rejected)
