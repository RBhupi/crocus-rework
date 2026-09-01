from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import duckdb
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import yaml
from conftest import (
    BASE_NS,
    aggregate_quality_document,
    quality_document,
    run_document,
    write_facts,
)

from adqat.runner import run_new


def test_dense_minute_product_uses_valid_values_and_circular_wind(tmp_path: Path) -> None:
    rules = quality_document()
    variables = rules["profiles"]["demo_wxt"]["variables"]
    variables["temperature"]["aggregation"] = "mean"
    wind = variables.pop("wind_speed")
    wind["where"]["measurement"] = "wxt.wind.direction"
    wind["units"] = "degree"
    wind["aggregation"] = "circular_mean"
    wind["checks"][1]["args"] = {"left": 0, "right": 360}
    variables["wind_direction"] = wind
    variables["heater_status"] = {
        "column": "value_float64",
        "where": {
            "measurement": "wxt.heater.status",
            "field": "value",
            "value_type": "float64",
        },
        "units": "1",
        "aggregation": "mode",
        "checks": [
            {
                "id": "heater_status_missing",
                "method": "col_vals_not_null",
                "flag": "missing_value",
            }
        ],
    }
    variables["operating_state"] = {
        "column": "value_string",
        "data_type": "string",
        "where": {
            "measurement": "wxt.operating.state",
            "field": "value",
            "value_type": "string",
        },
        "units": "1",
        "aggregation": "mode",
        "checks": [
            {
                "id": "operating_state_missing",
                "method": "col_vals_not_null",
                "flag": "missing_value",
            }
        ],
    }
    rules["profiles"]["demo_aqt"]["variables"]["co"]["aggregation"] = "mean"
    (tmp_path / "quality_rules.yaml").write_text(
        yaml.safe_dump(rules, sort_keys=False), encoding="utf-8"
    )
    aggregate_rules = aggregate_quality_document()
    aggregate_rules["profiles"]["demo_wxt"]["variables"] = {
        name: {"coverage": {"minimum_valid_count": 1}} for name in variables
    }
    (tmp_path / "aggregate_quality_rules.yaml").write_text(
        yaml.safe_dump(aggregate_rules, sort_keys=False), encoding="utf-8"
    )
    run = run_document(
        str(tmp_path / "facts" / "*.parquet"),
        str(tmp_path / "results"),
        start="2025-01-01T00:00:00Z",
        end="2025-01-01T00:03:00Z",
        period="all",
    )
    run["processing"]["aggregation"] = {"period": 1, "units": "minutes"}
    run["quality"]["aggregate_rules"] = "aggregate_quality_rules.yaml"
    run_path = tmp_path / "processing_run.yaml"
    run_path.write_text(yaml.safe_dump(run, sort_keys=False), encoding="utf-8")
    write_facts(
        tmp_path,
        [
            {"time": BASE_NS + 1_000_000_000, "value": 10.0},
            {"time": BASE_NS + 2_000_000_000, "value": 20.0},
            {"time": BASE_NS + 3_000_000_000, "value": 100.0},
            {
                "time": BASE_NS + 4_000_000_000,
                "measurement": "wxt.wind.direction",
                "value": 359.0,
            },
            {
                "time": BASE_NS + 5_000_000_000,
                "measurement": "wxt.wind.direction",
                "value": 1.0,
            },
            {"time": BASE_NS + 6_000_000_000, "measurement": "wxt.heater.status", "value": 2.0},
            {"time": BASE_NS + 7_000_000_000, "measurement": "wxt.heater.status", "value": 3.0},
            {"time": BASE_NS + 8_000_000_000, "measurement": "wxt.heater.status", "value": 2.0},
            {
                "time": BASE_NS + 9_000_000_000,
                "measurement": "wxt.operating.state",
                "value_type": "string",
                "value": "ready",
            },
            {
                "time": BASE_NS + 10_000_000_000,
                "measurement": "wxt.operating.state",
                "value_type": "string",
                "value": "warming",
            },
            {
                "time": BASE_NS + 11_000_000_000,
                "measurement": "wxt.operating.state",
                "value_type": "string",
                "value": "ready",
            },
            {"time": BASE_NS + 121_000_000_000, "value": 30.0},
        ],
    )

    summary = run_new(run_path, "demo_wxt_work_unit", "minute-run")
    period = next((summary.run_dir / "work_units" / "demo_wxt_work_unit").iterdir())
    table = pq.read_table(period / "aggregate_data.parquet")
    frame = pl.from_arrow(table)
    assert isinstance(frame, pl.DataFrame)
    assert frame.height == 12
    assert summary.aggregate_rows == 12

    first_temperature = frame.filter(
        (pl.col("time") == datetime(2025, 1, 1, tzinfo=UTC)) & (pl.col("variable") == "temperature")
    ).row(0, named=True)
    assert first_temperature["total_count"] == 3
    assert first_temperature["valid_count"] == 2
    assert first_temperature["value_float64"] == 15.0
    assert first_temperature["qc_bits"] == 0

    first_wind = frame.filter(
        (pl.col("time") == datetime(2025, 1, 1, tzinfo=UTC))
        & (pl.col("variable") == "wind_direction")
    ).row(0, named=True)
    assert min(abs(first_wind["value_float64"]), abs(first_wind["value_float64"] - 360)) < 1e-9
    assert min(abs(first_wind["mean"]), abs(first_wind["mean"] - 360)) < 1e-9
    assert first_wind["median"] is None
    assert first_wind["iqr"] is None

    assert (
        frame.filter(
            (pl.col("time") == datetime(2025, 1, 1, tzinfo=UTC))
            & (pl.col("variable") == "heater_status")
        )["value_float64"].item()
        == 2.0
    )
    assert (
        frame.filter(
            (pl.col("time") == datetime(2025, 1, 1, tzinfo=UTC))
            & (pl.col("variable") == "operating_state")
        )["value_string"].item()
        == "ready"
    )

    missing = frame.filter(pl.col("time") == datetime(2025, 1, 1, 0, 1, tzinfo=UTC))
    assert missing["total_count"].to_list() == [0, 0, 0, 0]
    assert missing["aggregate_valid"].to_list() == [False, False, False, False]
    assert missing["qc_bits"].to_list() == [1] * 4
    assert table.schema.field("qc_bits").type == pa.uint8()
    success = yaml.safe_load((period / "success.json").read_text(encoding="utf-8"))
    assert success["aggregate_rows"] == 12
    assert success["missing_aggregate_rows"] == 7


def test_minute_product_requires_an_aggregation_for_every_variable(tmp_path: Path) -> None:
    rules = quality_document()
    rules["profiles"]["demo_wxt"]["variables"]["temperature"]["aggregation"] = "mean"
    rules["profiles"]["demo_aqt"]["variables"]["co"]["aggregation"] = "mean"
    (tmp_path / "quality_rules.yaml").write_text(
        yaml.safe_dump(rules, sort_keys=False), encoding="utf-8"
    )
    aggregate_rules = aggregate_quality_document()
    (tmp_path / "aggregate_quality_rules.yaml").write_text(
        yaml.safe_dump(aggregate_rules, sort_keys=False), encoding="utf-8"
    )
    run = run_document(str(tmp_path / "facts" / "*.parquet"), str(tmp_path / "results"))
    run["processing"]["aggregation"] = {"period": 1, "units": "minutes"}
    run["quality"]["aggregate_rules"] = "aggregate_quality_rules.yaml"
    run_path = tmp_path / "processing_run.yaml"
    run_path.write_text(yaml.safe_dump(run, sort_keys=False), encoding="utf-8")

    from adqat.config import ConfigError, load_config

    try:
        load_config(run_path)
    except ConfigError as error:
        assert "lack an aggregation method" in str(error)
    else:  # pragma: no cover
        raise AssertionError("expected missing aggregation method to be rejected")


def test_minute_qc_uses_fixed_uint8_bits_and_never_propagates_raw_bits(
    tmp_path: Path,
) -> None:
    rules = quality_document()
    for profile in rules["profiles"].values():
        for variable in profile["variables"].values():
            variable["aggregation"] = "mean"
    (tmp_path / "quality_rules.yaml").write_text(
        yaml.safe_dump(rules, sort_keys=False), encoding="utf-8"
    )
    aggregate = aggregate_quality_document()
    aggregate["profiles"]["demo_wxt"]["variables"]["temperature"] = {
        "coverage": {"minimum_valid_count": 3, "minimum_valid_fraction": 0.8},
        "variability": {"maximum_standard_deviation": 1.0},
        "stuck": {"maximum_standard_deviation": 0.0, "minimum_valid_count": 3},
        "physical_range": {"left": 0, "right": 7},
        "instrument_range": {"left": 2, "right": 6},
    }
    (tmp_path / "aggregate_quality_rules.yaml").write_text(
        yaml.safe_dump(aggregate, sort_keys=False), encoding="utf-8"
    )
    run = run_document(
        str(tmp_path / "facts" / "*.parquet"),
        str(tmp_path / "results"),
        end="2025-01-01T00:04:00Z",
        period="all",
    )
    run["processing"]["aggregation"] = {"period": 1, "units": "minutes"}
    run["quality"]["aggregate_rules"] = "aggregate_quality_rules.yaml"
    run_path = tmp_path / "processing_run.yaml"
    run_path.write_text(yaml.safe_dump(run, sort_keys=False), encoding="utf-8")
    second = 1_000_000_000
    write_facts(
        tmp_path,
        [
            *[{"time": BASE_NS + offset * second, "value": 1.0} for offset in (1, 2, 3)],
            *[
                {"time": BASE_NS + (60 + offset) * second, "value": value}
                for offset, value in zip((1, 2, 3), (4.0, 8.0, 12.0), strict=True)
            ],
            *[{"time": BASE_NS + (120 + offset) * second, "value": 5.0} for offset in (1, 2)],
            {"time": BASE_NS + 123 * second, "value": 100.0},
            *[{"time": BASE_NS + (180 + offset) * second, "value": -1.0} for offset in (1, 2, 3)],
        ],
    )

    summary = run_new(run_path, "demo_wxt_work_unit", "minute-eight-bit")
    period = next((summary.run_dir / "work_units" / "demo_wxt_work_unit").iterdir())
    minute = pl.read_parquet(period / "aggregate_data.parquet").filter(
        pl.col("variable") == "temperature"
    )
    assert minute.schema["qc_bits"] == pl.UInt8
    assert minute["qc_bits"].to_list() == [36, 82, 1, 44]
    assert minute["total_count"].to_list() == [3, 3, 3, 3]
    assert minute["valid_count"].to_list() == [3, 3, 2, 3]
    assert minute["aggregate_valid"].to_list() == [True, True, False, True]
    assert minute["value_float64"].to_list() == [1.0, 8.0, None, -1.0]
    assert all((value & 128) == 0 for value in minute["qc_bits"])


def test_daily_minute_files_query_as_one_logical_table(tmp_path: Path) -> None:
    rules = quality_document()
    for profile in rules["profiles"].values():
        for variable in profile["variables"].values():
            variable["aggregation"] = "mean"
    (tmp_path / "quality_rules.yaml").write_text(
        yaml.safe_dump(rules, sort_keys=False), encoding="utf-8"
    )
    (tmp_path / "aggregate_quality_rules.yaml").write_text(
        yaml.safe_dump(aggregate_quality_document(), sort_keys=False), encoding="utf-8"
    )
    run = run_document(
        str(tmp_path / "facts" / "*.parquet"),
        str(tmp_path / "results"),
        start="2025-01-01T00:00:00Z",
        end="2025-01-03T00:00:00Z",
        period="1d",
    )
    run["processing"]["aggregation"] = {"period": 1, "units": "minutes"}
    run["quality"]["aggregate_rules"] = "aggregate_quality_rules.yaml"
    run_path = tmp_path / "processing_run.yaml"
    run_path.write_text(yaml.safe_dump(run, sort_keys=False), encoding="utf-8")
    write_facts(
        tmp_path,
        [
            {"time": BASE_NS + 1, "value": 10.0},
            {"time": BASE_NS + 86_400_000_000_001, "value": 20.0},
        ],
    )

    summary = run_new(run_path, "demo_wxt_work_unit", "minute-days")
    pattern = str(summary.run_dir / "work_units" / "*" / "*" / "aggregate_data.parquet")
    row = (
        duckdb.connect()
        .execute(
            """
        SELECT count(*), count(DISTINCT time), count(DISTINCT variable)
        FROM read_parquet(?, union_by_name = true)
        """,
            [pattern],
        )
        .fetchone()
    )
    assert row == (2 * 1_440 * 2, 2 * 1_440, 2)


def test_ten_second_aggregation_uses_configured_fixed_duration(tmp_path: Path) -> None:
    rules = quality_document()
    for profile in rules["profiles"].values():
        for variable in profile["variables"].values():
            variable["aggregation"] = "mean"
    (tmp_path / "quality_rules.yaml").write_text(
        yaml.safe_dump(rules, sort_keys=False), encoding="utf-8"
    )
    (tmp_path / "aggregate_quality_rules.yaml").write_text(
        yaml.safe_dump(aggregate_quality_document(), sort_keys=False), encoding="utf-8"
    )
    run = run_document(
        str(tmp_path / "facts" / "*.parquet"),
        str(tmp_path / "results"),
        end="2025-01-01T00:01:00Z",
        period="all",
    )
    run["processing"]["aggregation"] = {"period": 10, "units": "seconds"}
    run["quality"]["aggregate_rules"] = "aggregate_quality_rules.yaml"
    run_path = tmp_path / "processing_run.yaml"
    run_path.write_text(yaml.safe_dump(run, sort_keys=False), encoding="utf-8")
    write_facts(
        tmp_path,
        [
            {"time": BASE_NS + 1_000_000_000, "value": 10.0},
            {"time": BASE_NS + 9_000_000_000, "value": 20.0},
            {"time": BASE_NS + 11_000_000_000, "value": 30.0},
        ],
    )

    summary = run_new(run_path, "demo_wxt_work_unit", "ten-second-run")
    period = next((summary.run_dir / "work_units" / "demo_wxt_work_unit").iterdir())
    temperature = pl.read_parquet(period / "aggregate_data.parquet").filter(
        pl.col("variable") == "temperature"
    )
    assert summary.aggregate_rows == 12
    assert temperature.height == 6
    assert temperature["aggregation_period"].unique().item() == 10
    assert temperature["aggregation_period_units"].unique().item() == "seconds"
    assert temperature["aggregation_period_seconds"].unique().item() == 10
    assert temperature["total_count"].to_list() == [2, 1, 0, 0, 0, 0]
    assert temperature["value_float64"].to_list() == [15.0, 30.0, None, None, None, None]
    assert temperature["qc_bits"].to_list() == [0, 0, 1, 1, 1, 1]
    assert temperature["time"].diff().drop_nulls().dt.total_seconds().unique().item() == 10
