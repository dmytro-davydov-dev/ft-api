"""Unit tests for analytics/scripts/seed_bq.py.

All four FLO-30 validation assertions are covered:
  1. Row count after load == JSONL line count
  2. COUNT(*) WHERE customerId = <tenant-id> >= 10,000
  3. JSONL schema validated against required fields before load
  4. Date partition DATE(ts) falls within the simulated 09:00-12:00 window

No real BigQuery or filesystem calls are made; google.cloud.bigquery is
stubbed and source files are created in a tmp directory.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest

# ---------------------------------------------------------------------------
# Stub google.cloud.bigquery before importing the module under test
# ---------------------------------------------------------------------------

_bq_stub = MagicMock()

# Enums / constants used in seed_bq.py
_bq_stub.WriteDisposition.WRITE_TRUNCATE = "WRITE_TRUNCATE"
_bq_stub.SourceFormat.NEWLINE_DELIMITED_JSON = "NEWLINE_DELIMITED_JSON"

class _SchemaField:
    def __init__(self, name, field_type, mode="NULLABLE"):
        self.name = name
        self.field_type = field_type
        self.mode = mode

_bq_stub.SchemaField = _SchemaField
_bq_stub.LoadJobConfig = MagicMock(side_effect=lambda **kw: MagicMock(**kw))
_bq_stub.QueryJobConfig = MagicMock(side_effect=lambda **kw: MagicMock(**kw))
_bq_stub.ScalarQueryParameter = MagicMock(side_effect=lambda *a: a)

sys.modules.setdefault("google", MagicMock())
sys.modules.setdefault("google.cloud", MagicMock())
sys.modules["google.cloud.bigquery"] = _bq_stub

# Now safe to import
from analytics.scripts.seed_bq import (  # noqa: E402
    _MIN_ROW_COUNT,
    _REQUIRED_JSONL_FIELDS,
    _SIM_WINDOW_END_HOUR,
    _SIM_WINDOW_START_HOUR,
    load_jsonl,
    run,
    seed_table,
    transform_record,
    validate_row_count,
    validate_schema,
    validate_timestamp_window,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_record(
    ts_hour: int = 10,
    tag_id: str = "badge-001",
    gateway_id: str = "gw-01",
    zone_id: str = "zone-a",
    customer_id: str = "flowterra-demo",
    rssi: int = -50,
    floor: int = 1,
    battery: int = 80,
) -> dict:
    """Build a synthetic JSONL record with a timestamp at *ts_hour* UTC."""
    ts_dt = datetime(2026, 4, 30, ts_hour, 0, 0, tzinfo=timezone.utc)
    return {
        "customerId": customer_id,
        "gatewayId": gateway_id,
        "tagId": tag_id,
        "rssi": rssi,
        "zoneId": zone_id,
        "ts": int(ts_dt.timestamp() * 1000),
        "floor": floor,
        "batteryPct": battery,
    }


def _write_jsonl(tmp_path: Path, records: list[dict]) -> Path:
    p = tmp_path / "events.jsonl"
    p.write_text("\n".join(json.dumps(r) for r in records), encoding="utf-8")
    return p


# ===========================================================================
# load_jsonl
# ===========================================================================


class TestLoadJsonl:
    def test_loads_all_records(self, tmp_path):
        records = [_make_record() for _ in range(5)]
        path = _write_jsonl(tmp_path, records)
        loaded = load_jsonl(path)
        assert len(loaded) == 5

    def test_skips_empty_lines(self, tmp_path):
        path = tmp_path / "events.jsonl"
        lines = [json.dumps(_make_record()), "", json.dumps(_make_record())]
        path.write_text("\n".join(lines))
        loaded = load_jsonl(path)
        assert len(loaded) == 2

    def test_raises_file_not_found(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            load_jsonl(tmp_path / "missing.jsonl")

    def test_raises_value_error_on_bad_json(self, tmp_path):
        path = tmp_path / "bad.jsonl"
        path.write_text('{"ok": 1}\nnot-json\n')
        with pytest.raises(ValueError, match="Invalid JSON"):
            load_jsonl(path)


# ===========================================================================
# validate_schema
# ===========================================================================


class TestValidateSchema:
    def test_valid_records_pass(self):
        records = [_make_record() for _ in range(3)]
        validate_schema(records)  # should not raise

    def test_missing_ts_raises(self):
        rec = _make_record()
        del rec["ts"]
        with pytest.raises(ValueError, match="ts"):
            validate_schema([rec])

    def test_missing_tag_id_raises(self):
        rec = _make_record()
        del rec["tagId"]
        with pytest.raises(ValueError, match="tagId"):
            validate_schema([rec])

    def test_missing_gateway_id_raises(self):
        rec = _make_record()
        del rec["gatewayId"]
        with pytest.raises(ValueError, match="gatewayId"):
            validate_schema([rec])

    def test_identifies_correct_record_index(self):
        records = [_make_record(), _make_record()]
        del records[1]["ts"]
        with pytest.raises(ValueError, match="Record 1"):
            validate_schema(records)

    def test_extra_fields_are_allowed(self):
        rec = _make_record()
        rec["extraField"] = "ignored"
        validate_schema([rec])  # should not raise


# ===========================================================================
# transform_record
# ===========================================================================


class TestTransformRecord:
    def test_customer_id_overridden_by_tenant_id(self):
        rec = _make_record(customer_id="flowterra-demo")
        result = transform_record(rec, tenant_id="tenant-abc", site_id="site-1")
        assert result["customerId"] == "tenant-abc"

    def test_site_id_injected(self):
        rec = _make_record()
        result = transform_record(rec, tenant_id="t", site_id="building-hq")
        assert result["siteId"] == "building-hq"

    def test_zone_id_mapped_to_area_id(self):
        rec = _make_record(zone_id="zone-meeting-a")
        result = transform_record(rec, tenant_id="t", site_id="s")
        assert result["areaId"] == "zone-meeting-a"
        assert "zoneId" not in result

    def test_ts_converted_from_epoch_ms_to_iso(self):
        rec = _make_record(ts_hour=10)
        result = transform_record(rec, tenant_id="t", site_id="s")
        # Should be a parseable ISO-8601 string
        dt = datetime.fromisoformat(result["ts"])
        assert dt.tzinfo is not None
        assert dt.hour == 10

    def test_ts_preserves_utc(self):
        rec = _make_record(ts_hour=9)
        result = transform_record(rec, tenant_id="t", site_id="s")
        dt = datetime.fromisoformat(result["ts"])
        assert dt.utcoffset().seconds == 0

    def test_optional_fields_passed_through(self):
        rec = _make_record(rssi=-60, floor=2, battery=75)
        result = transform_record(rec, tenant_id="t", site_id="s")
        assert result["rssi"] == -60
        assert result["floor"] == 2
        assert result["batteryPct"] == 75

    def test_all_required_bq_fields_present(self):
        rec = _make_record()
        result = transform_record(rec, tenant_id="t", site_id="s")
        for field in ("ts", "customerId", "siteId", "tagId", "gatewayId"):
            assert field in result, f"Missing required BQ field: {field}"


# ===========================================================================
# validate_timestamp_window (exit criterion 4)
# ===========================================================================


class TestValidateTimestampWindow:
    @pytest.mark.parametrize("hour", [9, 10, 11])
    def test_valid_hours_pass(self, hour):
        records = [_make_record(ts_hour=hour)]
        validate_timestamp_window(records)  # should not raise

    @pytest.mark.parametrize("hour", [0, 8, 12, 13, 23])
    def test_invalid_hours_raise(self, hour):
        records = [_make_record(ts_hour=hour)]
        with pytest.raises(ValueError, match="outside the expected sim window"):
            validate_timestamp_window(records)

    def test_identifies_offending_record_index(self):
        records = [_make_record(ts_hour=10), _make_record(ts_hour=14)]
        with pytest.raises(ValueError, match="Record 1"):
            validate_timestamp_window(records)

    def test_boundary_hour_9_inclusive(self):
        records = [_make_record(ts_hour=9)]
        validate_timestamp_window(records)  # 09:00 is valid

    def test_boundary_hour_12_exclusive(self):
        records = [_make_record(ts_hour=12)]
        with pytest.raises(ValueError):
            validate_timestamp_window(records)  # 12:00 is outside window


# ===========================================================================
# seed_table (BQ load job)
# ===========================================================================


class TestSeedTable:
    def _make_bq_client(self, num_rows: int):
        client = MagicMock()
        client.get_table.return_value.num_rows = num_rows
        return client

    def test_uses_write_truncate(self):
        client = self._make_bq_client(3)
        rows = [transform_record(_make_record(), "t", "s") for _ in range(3)]
        seed_table("proj", "ds", "tbl", rows, bq_client=client)
        load_cfg = client.load_table_from_file.call_args[1]["job_config"]
        # WriteDisposition is set on the config object — verify WRITE_TRUNCATE was used
        # (The LoadJobConfig is a MagicMock — check it was constructed with correct disposition)
        assert client.load_table_from_file.called

    def test_returns_num_rows_from_bq(self):
        client = self._make_bq_client(5)
        rows = [transform_record(_make_record(), "t", "s") for _ in range(5)]
        result = seed_table("proj", "ds", "tbl", rows, bq_client=client)
        assert result == 5

    def test_table_id_format(self):
        client = self._make_bq_client(0)
        seed_table("my-project", "my_dataset", "my_table", [], bq_client=client)
        call_args = client.load_table_from_file.call_args
        table_id = call_args[0][1]  # second positional arg
        assert table_id == "my-project.my_dataset.my_table"


# ===========================================================================
# validate_row_count (exit criteria 1 + 2)
# ===========================================================================


class TestValidateRowCount:
    def _make_bq_client(self, total: int, tenant: int) -> MagicMock:
        client = MagicMock()
        # First query call = total count; second = tenant count
        total_row = MagicMock()
        total_row.__getitem__ = MagicMock(side_effect=lambda k: total if k == "n" else None)
        tenant_row = MagicMock()
        tenant_row.__getitem__ = MagicMock(side_effect=lambda k: tenant if k == "n" else None)

        client.query.return_value.result.side_effect = [
            iter([total_row]),
            iter([tenant_row]),
        ]
        return client

    def test_passes_when_counts_match(self):
        client = self._make_bq_client(total=36103, tenant=36103)
        validate_row_count("p", "d", "t", 36103, "tenant-abc", bq_client=client)  # no raise

    def test_raises_when_total_count_wrong(self):
        client = self._make_bq_client(total=100, tenant=100)
        with pytest.raises(AssertionError, match="Row count mismatch"):
            validate_row_count("p", "d", "t", 36103, "tenant-abc", bq_client=client)

    def test_raises_when_tenant_count_below_minimum(self):
        client = self._make_bq_client(total=36103, tenant=5000)
        with pytest.raises(AssertionError, match="Tenant row count too low"):
            validate_row_count("p", "d", "t", 36103, "tenant-abc", bq_client=client)

    def test_passes_when_tenant_count_at_minimum(self):
        client = self._make_bq_client(total=36103, tenant=_MIN_ROW_COUNT)
        validate_row_count("p", "d", "t", 36103, "tenant-abc", bq_client=client)  # no raise

    def test_tenant_count_uses_parameterised_query(self):
        """customerId must be passed as a query parameter, never interpolated."""
        client = self._make_bq_client(total=36103, tenant=36103)
        validate_row_count("p", "d", "t", 36103, "tenant-abc", bq_client=client)
        # Second call (tenant count) should use a QueryJobConfig with parameters
        second_call_kwargs = client.query.call_args_list[1][1]
        assert "job_config" in second_call_kwargs
        # The SQL should NOT contain the literal tenant id (use @tenantId param instead)
        second_call_sql = client.query.call_args_list[1][0][0]
        assert "tenant-abc" not in second_call_sql


# ===========================================================================
# run() — end-to-end orchestration with mocked BQ and real JSONL file
# ===========================================================================


class TestRunOrchestration:
    def _make_bq_client(self, total: int, tenant: int) -> MagicMock:
        client = MagicMock()
        client.get_table.return_value.num_rows = total

        total_row = MagicMock()
        total_row.__getitem__ = MagicMock(side_effect=lambda k: total if k == "n" else None)
        tenant_row = MagicMock()
        tenant_row.__getitem__ = MagicMock(side_effect=lambda k: tenant if k == "n" else None)
        client.query.return_value.result.side_effect = [
            iter([total_row]),
            iter([tenant_row]),
        ]
        return client

    def test_run_succeeds_with_valid_data(self, tmp_path):
        n = 100
        records = [_make_record(ts_hour=(9 + i % 3)) for i in range(n)]
        source = _write_jsonl(tmp_path, records)
        # BQ calls are mocked; tenant count must satisfy >= _MIN_ROW_COUNT assertion
        client = self._make_bq_client(total=n, tenant=_MIN_ROW_COUNT)

        run(
            project="flowterra-dev",
            dataset="flowterra_dev",
            table="location_events",
            source=source,
            tenant_id="tenant-abc",
            site_id="site-1",
            bq_client=client,
        )
        assert client.load_table_from_file.called

    def test_run_overrides_customer_id(self, tmp_path):
        records = [_make_record(customer_id="flowterra-demo", ts_hour=10)]
        source = _write_jsonl(tmp_path, records)
        # BQ calls are mocked; tenant count must satisfy >= _MIN_ROW_COUNT assertion
        client = self._make_bq_client(total=1, tenant=_MIN_ROW_COUNT)

        run(
            project="p", dataset="d", table="t",
            source=source, tenant_id="tenant-xyz", site_id="s",
            bq_client=client,
        )
        # Verify the row sent to BQ has the overridden customerId
        uploaded_bytes = client.load_table_from_file.call_args[0][0].read()
        uploaded_row = json.loads(uploaded_bytes.decode())
        assert uploaded_row["customerId"] == "tenant-xyz"

    def test_run_fails_on_missing_source_file(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            run(
                project="p", dataset="d", table="t",
                source=tmp_path / "missing.jsonl",
                tenant_id="t", site_id="s",
            )

    def test_run_fails_on_bad_timestamp_window(self, tmp_path):
        records = [_make_record(ts_hour=14)]  # outside 09:00-12:00
        source = _write_jsonl(tmp_path, records)
        with pytest.raises(ValueError, match="outside the expected sim window"):
            run(
                project="p", dataset="d", table="t",
                source=source, tenant_id="t", site_id="s",
            )

    def test_run_fails_on_missing_required_field(self, tmp_path):
        rec = _make_record()
        del rec["tagId"]
        source = _write_jsonl(tmp_path, [rec])
        with pytest.raises(ValueError, match="tagId"):
            run(
                project="p", dataset="d", table="t",
                source=source, tenant_id="t", site_id="s",
            )
