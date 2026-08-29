from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from conftest import BASE_NS, write_configuration, write_facts

from adqat.config import load_config
from adqat.periods import Period
from adqat.pointblank import run_pointblank
from adqat.source import SourceError, select_period, validate_source


def test_selects_long_rows_preserves_nanoseconds_and_normalizes_nan(
    synthetic_project: tuple[Path, Path],
) -> None:
    run_path, source_path = synthetic_project
    before = source_path.stat()
    config = load_config(run_path).resolve_work_unit("demo_wxt_work_unit")
    selected = select_period(
        config,
        Period(datetime(2025, 1, 1, tzinfo=UTC), datetime(2025, 1, 2, tzinfo=UTC)),
    )
    assert selected.row_count == 5
    assert selected.data.get_column("time").dtype == pl.Datetime("ns", "UTC")
    assert selected.data.get_column("time").to_physical()[0] == BASE_NS + 1
    assert selected.data.filter(pl.col("time").dt.nanosecond() == 2)["observed_value"][0] is None
    assert set(selected.data["variable"]) == {"temperature", "wind_speed"}
    after = source_path.stat()
    assert (before.st_size, before.st_mtime_ns) == (after.st_size, after.st_mtime_ns)


def test_normalizes_configured_sentinel_as_missing_without_range_failures(tmp_path: Path) -> None:
    run_path = write_configuration(tmp_path)
    write_facts(tmp_path, [{"time": BASE_NS + 1, "value": -9999.9}])
    config = load_config(run_path).resolve_work_unit("demo_wxt_work_unit")
    selected = select_period(
        config,
        Period(datetime(2025, 1, 1, tzinfo=UTC), datetime(2025, 1, 2, tzinfo=UTC)),
    )
    assert selected.data["observed_value"].to_list() == [None]
    result = run_pointblank(selected.data, selected.key_schema, config, "sentinel-run")
    assert result.findings.select("check_id", "bit").rows() == [("temperature_missing", 0)]


@pytest.mark.parametrize("hive", [False, True])
def test_hive_and_non_hive_sources_are_equivalent(tmp_path: Path, hive: bool) -> None:
    run_path = write_configuration(tmp_path, hive=hive)
    write_facts(tmp_path, [{"time": BASE_NS + 7, "value": 10.0}], hive=hive)
    config = load_config(run_path).resolve_work_unit("demo_wxt_work_unit")
    validate_source(config)
    selected = select_period(
        config,
        Period(datetime(2025, 1, 1, tzinfo=UTC), datetime(2025, 1, 2, tzinfo=UTC)),
    )
    assert selected.data.select("variable", "observed_value").to_dicts() == [
        {"variable": "temperature", "observed_value": 10.0}
    ]


def test_empty_period_has_stable_schema(synthetic_project: tuple[Path, Path]) -> None:
    run_path, _ = synthetic_project
    config = load_config(run_path).resolve_work_unit("demo_wxt_work_unit")
    selected = select_period(
        config,
        Period(datetime(2025, 1, 2, tzinfo=UTC), datetime(2025, 1, 3, tzinfo=UTC)),
    )
    assert selected.row_count == 0
    assert selected.key_schema.field("time").type == pa.timestamp("ns", tz="UTC")
    assert selected.data.schema["observed_value"] == pl.Float64
    assert selected.data.schema["observed_value_string"] == pl.String


def test_rejects_missing_and_nonnumeric_columns(tmp_path: Path) -> None:
    run_path = write_configuration(tmp_path)
    write_facts(tmp_path, [{"value": 1.0}])
    loaded = load_config(run_path)
    config = loaded.resolve_work_unit("demo_wxt_work_unit")
    config.profile.variables["temperature"].column = "value_string"
    with pytest.raises(SourceError, match="must be numeric"):
        validate_source(config)


def test_union_by_name_supplies_missing_columns(tmp_path: Path) -> None:
    run_path = write_configuration(tmp_path)
    first_path = write_facts(tmp_path, [{"time": BASE_NS + 1, "value": 10.0}])
    full = pq.read_table(first_path)
    pq.write_table(full, first_path.with_name("part-001.parquet"))
    pq.write_table(full.drop(["value_float64"]), first_path)
    config = load_config(run_path).resolve_work_unit("demo_wxt_work_unit")
    selected = select_period(
        config,
        Period(datetime(2025, 1, 1, tzinfo=UTC), datetime(2025, 1, 2, tzinfo=UTC)),
    )
    assert selected.row_count == 2
    assert selected.data["observed_value"].null_count() == 1


def test_pointblank_normalizes_findings_and_summaries(
    synthetic_project: tuple[Path, Path],
) -> None:
    run_path, _ = synthetic_project
    config = load_config(run_path).resolve_work_unit("demo_wxt_work_unit")
    selected = select_period(
        config,
        Period(datetime(2025, 1, 1, tzinfo=UTC), datetime(2025, 1, 2, tzinfo=UTC)),
    )
    result = run_pointblank(selected.data, selected.key_schema, config, "run-one")
    pairs = result.findings.select("check_id", "bit").rows()
    assert pairs.count(("temperature_missing", 0)) == 1
    assert pairs.count(("temperature_instrument", 3)) == 2
    assert pairs.count(("temperature_physical", 2)) == 1
    assert result.findings.height == 4
    assert result.check_results.height == 5
    missing = result.check_results.filter(pl.col("check_id") == "temperature_missing").row(
        0, named=True
    )
    assert missing["units_tested"] == 4
    assert missing["units_failed"] == 1


def test_pointblank_empty_period_produces_zero_unit_results(
    synthetic_project: tuple[Path, Path],
) -> None:
    run_path, _ = synthetic_project
    config = load_config(run_path).resolve_work_unit("demo_wxt_work_unit")
    selected = select_period(
        config,
        Period(datetime(2025, 1, 2, tzinfo=UTC), datetime(2025, 1, 3, tzinfo=UTC)),
    )
    result = run_pointblank(selected.data, selected.key_schema, config, "run-empty")
    assert result.findings.is_empty()
    assert result.check_results["units_tested"].to_list() == [0, 0, 0, 0, 0]
