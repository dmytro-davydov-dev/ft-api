#!/usr/bin/env python3
"""analytics/scripts/seed_bq.py — Idempotent BigQuery seeding script.

Loads the Phase 3 synthetic dataset (``ft-sim/data/synthetic_events.jsonl``)
into the Flowterra dev BigQuery dataset.  Safe to re-run — the target table
is truncated before each load.

Usage
-----
::

    python analytics/scripts/seed_bq.py \\
      --project   flowterra-dev \\
      --dataset   flowterra_dev \\
      --table     location_events \\
      --source    sim/data/synthetic_events.jsonl \\
      --tenant-id tenant-abc

    # Optional: override the default site-id
    python analytics/scripts/seed_bq.py ... --site-id building-hq

Idempotency
-----------
The script truncates the dev table (``WRITE_TRUNCATE``) before loading, so
repeated runs always result in a clean, up-to-date dataset.

Validation (run automatically after load)
-----------------------------------------
1. Row count after load == JSONL line count.
2. ``COUNT(*) WHERE customerId = <tenant-id>`` ≥ 10 000.
3. All required JSONL fields present before load (schema guard).
4. All event timestamps fall within the 09:00–12:00 UTC window.

Field mapping (JSONL → BigQuery ``location_events``)
-----------------------------------------------------
| JSONL field   | BQ field      | Notes                              |
|---------------|---------------|------------------------------------|
| ts (epoch ms) | ts TIMESTAMP  | Converted to UTC ISO-8601          |
| --tenant-id   | customerId    | CLI arg overrides file value       |
| --site-id     | siteId        | CLI arg; default "default-site"    |
| tagId         | tagId         |                                    |
| zoneId        | areaId        | Renamed                            |
| gatewayId     | gatewayId     |                                    |
| rssi          | rssi          |                                    |
| floor         | floor         |                                    |
| batteryPct    | batteryPct    |                                    |
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

# ---------------------------------------------------------------------------
# Schema constants derived from analytics/schema/bq_schema.sql
# ---------------------------------------------------------------------------

# Fields that MUST be present in every JSONL record (before transformation).
# siteId and customerId are supplied via CLI args, not the JSONL file.
_REQUIRED_JSONL_FIELDS: frozenset[str] = frozenset({"ts", "tagId", "gatewayId"})

# Simulated-day time window: 09:00 ≤ hour < 12:00 UTC (inclusive of 9, exclusive of 12+).
_SIM_WINDOW_START_HOUR: int = 9
_SIM_WINDOW_END_HOUR: int = 12  # exclusive upper bound (events at 11:59 are valid)

_MIN_ROW_COUNT: int = 10_000


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Idempotent BigQuery seeding script — Flowterra dev dataset",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--project", required=True, help="GCP project ID (e.g. flowterra-dev)")
    p.add_argument("--dataset", required=True, help="BQ dataset name (e.g. flowterra_dev)")
    p.add_argument("--table", required=True, help="BQ table name (e.g. location_events)")
    p.add_argument(
        "--source",
        required=True,
        type=Path,
        help="Path to synthetic_events.jsonl",
    )
    p.add_argument(
        "--tenant-id",
        required=True,
        help="customerId to inject — overrides value in JSONL",
    )
    p.add_argument(
        "--site-id",
        default="default-site",
        help="siteId to inject (not present in JSONL)",
    )
    return p


# ---------------------------------------------------------------------------
# JSONL helpers
# ---------------------------------------------------------------------------


def load_jsonl(path: Path) -> list[dict]:
    """Read all records from a JSONL file.

    Args:
        path: Absolute or relative path to the ``.jsonl`` file.

    Returns:
        List of parsed record dicts (one per non-empty line).

    Raises:
        FileNotFoundError: If *path* does not exist.
        ValueError: If any line contains invalid JSON.
    """
    if not path.exists():
        raise FileNotFoundError(f"Source file not found: {path}")

    records: list[dict] = []
    with path.open(encoding="utf-8") as fh:
        for lineno, line in enumerate(fh, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSON on line {lineno}: {exc}") from exc
    return records


# ---------------------------------------------------------------------------
# Schema validation
# ---------------------------------------------------------------------------


def validate_schema(records: list[dict]) -> None:
    """Assert that every record contains the required JSONL source fields.

    This is a pre-load guard — runs against the raw JSONL before any
    transformation so schema errors surface immediately rather than inside BQ.

    Args:
        records: Raw records as loaded by :func:`load_jsonl`.

    Raises:
        ValueError: If any record is missing a required field, with a message
            identifying the record index and missing fields.
    """
    for idx, record in enumerate(records):
        missing = _REQUIRED_JSONL_FIELDS - record.keys()
        if missing:
            raise ValueError(
                f"Record {idx} is missing required fields: {sorted(missing)!r}. "
                f"Record keys present: {sorted(record.keys())!r}"
            )


# ---------------------------------------------------------------------------
# Record transformation
# ---------------------------------------------------------------------------


def transform_record(
    record: dict,
    tenant_id: str,
    site_id: str,
) -> dict:
    """Map a raw JSONL record to the BigQuery ``location_events`` schema.

    Performs three key operations:

    1. Converts ``ts`` from epoch-milliseconds (int) to UTC ISO-8601 string
       (the format expected by BQ TIMESTAMP columns when loading JSON).
    2. Overrides ``customerId`` with the CLI ``--tenant-id`` argument.
    3. Injects ``siteId`` from the CLI ``--site-id`` argument (not in JSONL).
    4. Renames ``zoneId`` → ``areaId``.

    Args:
        record:    Raw record from :func:`load_jsonl`.
        tenant_id: Value for ``customerId`` (from ``--tenant-id``).
        site_id:   Value for ``siteId`` (from ``--site-id``).

    Returns:
        Dict ready to insert into BigQuery ``location_events``.
    """
    ts_dt = datetime.fromtimestamp(record["ts"] / 1000.0, tz=timezone.utc)
    return {
        "ts": ts_dt.isoformat(),
        "customerId": tenant_id,
        "siteId": site_id,
        "tagId": record["tagId"],
        "areaId": record.get("zoneId"),
        "gatewayId": record["gatewayId"],
        "rssi": record.get("rssi"),
        "floor": record.get("floor"),
        "batteryPct": record.get("batteryPct"),
    }


# ---------------------------------------------------------------------------
# Timestamp window validation
# ---------------------------------------------------------------------------


def validate_timestamp_window(records: list[dict]) -> None:
    """Assert that every raw record's ``ts`` falls in the 09:00–12:00 UTC window.

    Args:
        records: Raw records (epoch-ms timestamps).

    Raises:
        ValueError: If any record has a timestamp outside the expected window,
            with the offending record index and UTC datetime.
    """
    for idx, record in enumerate(records):
        ts_dt = datetime.fromtimestamp(record["ts"] / 1000.0, tz=timezone.utc)
        if not (_SIM_WINDOW_START_HOUR <= ts_dt.hour < _SIM_WINDOW_END_HOUR):
            raise ValueError(
                f"Record {idx} timestamp {ts_dt.isoformat()} is outside the "
                f"expected sim window (09:00–12:00 UTC). "
                f"Hour={ts_dt.hour}"
            )


# ---------------------------------------------------------------------------
# BigQuery operations
# ---------------------------------------------------------------------------


def _full_table_id(project: str, dataset: str, table: str) -> str:
    return f"{project}.{dataset}.{table}"


def seed_table(
    project: str,
    dataset: str,
    table: str,
    rows: list[dict],
    *,
    bq_client=None,
) -> int:
    """Truncate *table* and load *rows* in a single streaming-insert batch.

    Uses ``WRITE_TRUNCATE`` (load job) — not streaming inserts — so the
    operation is atomic and easy to re-run.

    Args:
        project:   GCP project ID.
        dataset:   BigQuery dataset name.
        table:     BigQuery table name.
        rows:      Transformed records ready for BQ.
        bq_client: Optional pre-constructed ``bigquery.Client`` (for tests).

    Returns:
        Number of rows loaded (from the load job result).
    """
    from google.cloud import bigquery  # noqa: PLC0415

    client = bq_client or bigquery.Client(project=project)
    table_id = _full_table_id(project, dataset, table)

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=False,
        schema=[
            bigquery.SchemaField("ts", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("customerId", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("siteId", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("tagId", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("areaId", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("gatewayId", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("rssi", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("floor", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("batteryPct", "INTEGER", mode="NULLABLE"),
        ],
    )

    # BQ load jobs accept an iterable of dicts for JSON source format.
    import io  # noqa: PLC0415

    jsonl_bytes = "\n".join(json.dumps(row) for row in rows).encode("utf-8")
    load_job = client.load_table_from_file(
        io.BytesIO(jsonl_bytes),
        table_id,
        job_config=job_config,
    )
    load_job.result()  # blocks until complete; raises on BQ errors

    destination = client.get_table(table_id)
    return destination.num_rows


# ---------------------------------------------------------------------------
# Post-load validation
# ---------------------------------------------------------------------------


def validate_row_count(
    project: str,
    dataset: str,
    table: str,
    expected: int,
    tenant_id: str,
    *,
    bq_client=None,
) -> None:
    """Run two COUNT assertions against the loaded table.

    1. Total row count == *expected* (JSONL line count).
    2. ``COUNT(*) WHERE customerId = <tenant_id>`` ≥ :data:`_MIN_ROW_COUNT`.

    Args:
        project:   GCP project ID.
        dataset:   BigQuery dataset name.
        table:     BigQuery table name.
        expected:  Expected total row count (== JSONL line count).
        tenant_id: The ``customerId`` value used during seeding.
        bq_client: Optional pre-constructed ``bigquery.Client`` (for tests).

    Raises:
        AssertionError: If either count assertion fails.
    """
    from google.cloud import bigquery  # noqa: PLC0415

    client = bq_client or bigquery.Client(project=project)
    table_id = _full_table_id(project, dataset, table)

    # 1. Total count
    total_result = list(
        client.query(f"SELECT COUNT(*) AS n FROM `{table_id}`").result()
    )
    actual_total = total_result[0]["n"]
    assert actual_total == expected, (
        f"Row count mismatch: expected {expected}, got {actual_total}"
    )

    # 2. Tenant count
    tenant_result = list(
        client.query(
            f"SELECT COUNT(*) AS n FROM `{table_id}` "
            f"WHERE customerId = @tenantId",
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("tenantId", "STRING", tenant_id)
                ]
            ),
        ).result()
    )
    tenant_count = tenant_result[0]["n"]
    assert tenant_count >= _MIN_ROW_COUNT, (
        f"Tenant row count too low: expected ≥ {_MIN_ROW_COUNT}, "
        f"got {tenant_count} for customerId={tenant_id!r}"
    )


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


def run(
    project: str,
    dataset: str,
    table: str,
    source: Path,
    tenant_id: str,
    site_id: str,
    *,
    bq_client=None,
) -> None:
    """Full seeding pipeline: load → validate → transform → seed → assert.

    Args:
        project:   GCP project ID.
        dataset:   BigQuery dataset name.
        table:     BigQuery table name.
        source:    Path to the ``.jsonl`` source file.
        tenant_id: ``customerId`` to inject.
        site_id:   ``siteId`` to inject.
        bq_client: Optional pre-constructed BQ client (for tests / DI).
    """
    print(f"[seed_bq] Source: {source}")
    print(f"[seed_bq] Target: {project}.{dataset}.{table}")
    print(f"[seed_bq] tenant-id: {tenant_id}  site-id: {site_id}")

    # Step 1: Load JSONL
    print("[seed_bq] Loading JSONL …")
    records = load_jsonl(source)
    jsonl_count = len(records)
    print(f"[seed_bq] Loaded {jsonl_count:,} records")

    # Step 2: Validate schema
    print("[seed_bq] Validating source schema …")
    validate_schema(records)

    # Step 3: Validate timestamp window
    print("[seed_bq] Validating timestamp window …")
    validate_timestamp_window(records)

    # Step 4: Transform
    print("[seed_bq] Transforming records …")
    rows = [transform_record(r, tenant_id, site_id) for r in records]

    # Step 5: Truncate + load
    print("[seed_bq] Seeding BigQuery (WRITE_TRUNCATE) …")
    loaded = seed_table(project, dataset, table, rows, bq_client=bq_client)
    print(f"[seed_bq] Loaded {loaded:,} rows")

    # Step 6: Post-load assertions
    print("[seed_bq] Validating post-load counts …")
    validate_row_count(
        project, dataset, table, jsonl_count, tenant_id, bq_client=bq_client
    )

    print("[seed_bq] ✓ All validations passed")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> None:
    args = _build_parser().parse_args(argv)
    try:
        run(
            project=args.project,
            dataset=args.dataset,
            table=args.table,
            source=args.source,
            tenant_id=args.tenant_id,
            site_id=args.site_id,
        )
    except (AssertionError, ValueError, FileNotFoundError) as exc:
        print(f"[seed_bq] ERROR: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
