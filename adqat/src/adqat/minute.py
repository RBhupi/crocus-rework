from __future__ import annotations

import math
from datetime import UTC, datetime, timedelta
from typing import Any

import polars as pl
import pyarrow as pa

from adqat.config import ResolvedConfig
from adqat.periods import Period
from adqat.pointblank import EngineResult


def minute_data_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("time", pa.timestamp("ns", tz="UTC"), nullable=False),
            pa.field("sensor", pa.string(), nullable=False),
            pa.field("vsn", pa.string(), nullable=False),
            pa.field("instrument_id", pa.string(), nullable=False),
            pa.field("variable", pa.string(), nullable=False),
            pa.field("units", pa.string()),
            pa.field("aggregation_method", pa.string(), nullable=False),
            pa.field("value_float64", pa.float64()),
            pa.field("value_string", pa.string()),
            pa.field("total_count", pa.uint32(), nullable=False),
            pa.field("valid_count", pa.uint32(), nullable=False),
            pa.field("invalid_count", pa.uint32(), nullable=False),
            pa.field("missing_value_count", pa.uint32(), nullable=False),
            pa.field("physical_range_count", pa.uint32(), nullable=False),
            pa.field("instrument_range_count", pa.uint32(), nullable=False),
            pa.field("valid_fraction", pa.float64()),
            pa.field("observed_rate_hz", pa.float64(), nullable=False),
            pa.field("maximum_gap_seconds", pa.float64()),
            pa.field("mean", pa.float64()),
            pa.field("median", pa.float64()),
            pa.field("standard_deviation", pa.float64()),
            pa.field("minimum", pa.float64()),
            pa.field("maximum", pa.float64()),
            pa.field("q25", pa.float64()),
            pa.field("q75", pa.float64()),
            pa.field("iqr", pa.float64()),
            pa.field("circular_resultant_length", pa.float64()),
            pa.field("aggregate_valid", pa.bool_(), nullable=False),
            pa.field("qc_bits", pa.uint64(), nullable=False),
            pa.field("run_id", pa.string(), nullable=False),
            pa.field("work_unit_id", pa.string(), nullable=False),
            pa.field("config_hash", pa.string(), nullable=False),
        ]
    )


def aggregate_one_minute(
    data: pl.DataFrame,
    engine_result: EngineResult,
    config: ResolvedConfig,
    period: Period,
    run_id: str,
) -> pl.DataFrame:
    """Create one dense row per minute and configured variable.

    Raw observations are never resampled. Raw checks determine which values can
    contribute to the representative one-minute value and descriptive statistics.
    A completely absent variable/minute receives the configured missing-sample bit.
    """

    time_name = config.run.source.time.column
    key_names = config.run.source.observation_keys
    raw = _attach_raw_qc(data, engine_result.findings, key_names)
    metadata = _variable_metadata(config)
    if raw.height:
        raw = (
            raw.join(metadata, on="variable", how="left", validate="m:1")
            .with_columns(
                pl.col(time_name).alias("_observation_time"),
                pl.col(time_name).dt.truncate("1m").alias("time"),
                (pl.col("raw_qc_bits") == 0).alias("_valid"),
            )
            .sort("_observation_time")
        )
        aggregated = _aggregate_observed(raw, config, "_observation_time")
    else:
        aggregated = _empty_aggregates()

    grid = _minute_grid(period, metadata)
    identity = config.work_unit.filters
    missing_sample_bit = config.rules.flags["missing_sample"].bit
    missing_sample_mask = 1 << missing_sample_bit
    result = (
        grid.join(aggregated, on=["time", "variable"], how="left", validate="1:1")
        .with_columns(
            pl.lit(str(identity["sensor"]), dtype=pl.String).alias("sensor"),
            pl.lit(str(identity["vsn"]), dtype=pl.String).alias("vsn"),
            pl.lit(str(identity["instrument_id"]), dtype=pl.String).alias("instrument_id"),
            pl.col("total_count").fill_null(0).cast(pl.UInt32),
            pl.col("valid_count").fill_null(0).cast(pl.UInt32),
            pl.col("missing_value_count").fill_null(0).cast(pl.UInt32),
            pl.col("physical_range_count").fill_null(0).cast(pl.UInt32),
            pl.col("instrument_range_count").fill_null(0).cast(pl.UInt32),
            pl.col("observed_rate_hz").fill_null(0.0),
            pl.when(pl.col("total_count").fill_null(0) == 0)
            .then(pl.lit(missing_sample_mask, dtype=pl.UInt64))
            .otherwise(pl.col("qc_bits").fill_null(0).cast(pl.UInt64))
            .alias("qc_bits"),
        )
        .with_columns(
            (pl.col("total_count") - pl.col("valid_count")).alias("invalid_count"),
            pl.when(pl.col("total_count") > 0)
            .then(pl.col("valid_count") / pl.col("total_count"))
            .otherwise(pl.lit(None, dtype=pl.Float64))
            .alias("valid_fraction"),
            (pl.col("valid_count") > 0).alias("aggregate_valid"),
            pl.lit(run_id, dtype=pl.String).alias("run_id"),
            pl.lit(config.work_unit.id, dtype=pl.String).alias("work_unit_id"),
            pl.lit(config.config_hash, dtype=pl.String).alias("config_hash"),
        )
        .select(minute_data_schema().names)
        .cast(_polars_schema(), strict=True)
        .sort("time", "variable")
    )
    _validate_minute_data(result, grid.height)
    return result


def _attach_raw_qc(
    data: pl.DataFrame,
    findings: pl.DataFrame,
    key_names: list[str],
) -> pl.DataFrame:
    identity = [*key_names, "variable"]
    if findings.is_empty():
        return data.with_columns(pl.lit(0, dtype=pl.UInt64).alias("raw_qc_bits"))
    flags = (
        findings.select(*identity, "bit")
        .unique()
        .with_columns(
            pl.lit(2, dtype=pl.UInt64)
            .pow(pl.col("bit"))
            .cast(pl.UInt64)
            .alias("_mask")
        )
        .group_by(*identity)
        .agg(pl.col("_mask").sum().cast(pl.UInt64).alias("raw_qc_bits"))
    )
    return data.join(flags, on=identity, how="left", validate="m:1").with_columns(
        pl.col("raw_qc_bits").fill_null(0).cast(pl.UInt64)
    )


def _aggregate_observed(
    raw: pl.DataFrame,
    config: ResolvedConfig,
    time_name: str,
) -> pl.DataFrame:
    missing_mask = 1 << config.rules.flags["missing_value"].bit
    physical_mask = 1 << config.rules.flags["physical_range"].bit
    instrument_mask = 1 << config.rules.flags["instrument_range"].bit
    radians = pl.col("observed_value") * math.pi / 180.0
    prepared = raw.with_columns(
        pl.when(pl.col("_valid") & (pl.col("aggregation_method") == "circular_mean"))
        .then(radians.sin())
        .otherwise(None)
        .alias("_sin"),
        pl.when(pl.col("_valid") & (pl.col("aggregation_method") == "circular_mean"))
        .then(radians.cos())
        .otherwise(None)
        .alias("_cos"),
    )
    numeric = pl.col("observed_value").filter(pl.col("_valid"))
    string = pl.col("observed_value_string").filter(pl.col("_valid"))
    grouped = prepared.group_by(
        "time", "variable", "aggregation_method", maintain_order=True
    ).agg(
        pl.len().cast(pl.UInt32).alias("total_count"),
        pl.col("_valid").sum().cast(pl.UInt32).alias("valid_count"),
        ((pl.col("raw_qc_bits") & missing_mask) != 0)
        .sum()
        .cast(pl.UInt32)
        .alias("missing_value_count"),
        ((pl.col("raw_qc_bits") & physical_mask) != 0)
        .sum()
        .cast(pl.UInt32)
        .alias("physical_range_count"),
        ((pl.col("raw_qc_bits") & instrument_mask) != 0)
        .sum()
        .cast(pl.UInt32)
        .alias("instrument_range_count"),
        (pl.len() / 60.0).alias("observed_rate_hz"),
        (pl.col(time_name).diff().dt.total_nanoseconds().max() / 1_000_000_000.0).alias(
            "maximum_gap_seconds"
        ),
        numeric.mean().alias("mean"),
        numeric.median().alias("median"),
        numeric.std(ddof=0).alias("standard_deviation"),
        numeric.min().alias("minimum"),
        numeric.max().alias("maximum"),
        numeric.quantile(0.25, interpolation="linear").alias("q25"),
        numeric.quantile(0.75, interpolation="linear").alias("q75"),
        numeric.last().alias("_last_numeric"),
        numeric.mode().sort().first().alias("_mode_numeric"),
        string.last().alias("_last_string"),
        string.mode().sort().first().alias("_mode_string"),
        pl.col("_sin").mean().alias("_sin_mean"),
        pl.col("_cos").mean().alias("_cos_mean"),
    )
    minute_bits = _minute_bits(prepared)
    grouped = grouped.join(minute_bits, on=["time", "variable"], how="left", validate="1:1")
    circular_degrees = (
        pl.arctan2(pl.col("_sin_mean"), pl.col("_cos_mean")) * 180.0 / math.pi + 360.0
    ) % 360.0
    resultant = (pl.col("_sin_mean").pow(2) + pl.col("_cos_mean").pow(2)).sqrt()
    is_circular = pl.col("aggregation_method") == "circular_mean"
    circular_standard_deviation = (
        -2.0 * resultant.clip(1e-15, 1.0).log()
    ).sqrt() * 180.0 / math.pi
    return (
        grouped.with_columns(
            pl.when(is_circular)
            .then(circular_degrees)
            .otherwise(pl.col("mean"))
            .alias("mean"),
            pl.when(is_circular)
            .then(circular_standard_deviation)
            .otherwise(pl.col("standard_deviation"))
            .alias("standard_deviation"),
            *[
                pl.when(is_circular)
                .then(pl.lit(None, dtype=pl.Float64))
                .otherwise(pl.col(name))
                .alias(name)
                for name in ("median", "minimum", "maximum", "q25", "q75")
            ],
        )
        .with_columns(
            (pl.col("q75") - pl.col("q25")).alias("iqr"),
            pl.when(is_circular)
            .then(resultant)
            .otherwise(None)
            .alias("circular_resultant_length"),
            pl.when(pl.col("aggregation_method") == "mean")
            .then(pl.col("mean"))
            .when(is_circular)
            .then(pl.col("mean"))
            .when(pl.col("aggregation_method") == "mode")
            .then(pl.col("_mode_numeric"))
            .when(pl.col("aggregation_method") == "last")
            .then(pl.col("_last_numeric"))
            .otherwise(None)
            .alias("value_float64"),
            pl.when(pl.col("aggregation_method") == "mode")
            .then(pl.col("_mode_string"))
            .when(pl.col("aggregation_method") == "last")
            .then(pl.col("_last_string"))
            .otherwise(None)
            .alias("value_string"),
        )
        .drop(
            "_last_numeric",
            "_mode_numeric",
            "_last_string",
            "_mode_string",
            "_sin_mean",
            "_cos_mean",
        )
    )


def _minute_bits(raw: pl.DataFrame) -> pl.DataFrame:
    flagged = raw.filter(pl.col("raw_qc_bits") != 0)
    if flagged.is_empty():
        return raw.select("time", "variable").unique().with_columns(
            pl.lit(0, dtype=pl.UInt64).alias("qc_bits")
        )
    exploded = (
        flagged.select("time", "variable", "raw_qc_bits")
        .unique()
        .group_by("time", "variable")
        .agg(pl.col("raw_qc_bits").unique().alias("_masks"))
        .with_columns(
            pl.col("_masks")
            .map_elements(_bitwise_or, return_dtype=pl.UInt64)
            .alias("qc_bits")
        )
        .drop("_masks")
    )
    all_groups = raw.select("time", "variable").unique()
    return all_groups.join(exploded, on=["time", "variable"], how="left").with_columns(
        pl.col("qc_bits").fill_null(0).cast(pl.UInt64)
    )


def _bitwise_or(values: Any) -> int:
    result = 0
    for value in values:
        result |= int(value)
    return result


def _variable_metadata(config: ResolvedConfig) -> pl.DataFrame:
    rows = [
        {
            "variable": name,
            "units": variable.units,
            "aggregation_method": variable.aggregation,
        }
        for name, variable in config.profile.variables.items()
    ]
    return pl.DataFrame(
        rows,
        schema={
            "variable": pl.String,
            "units": pl.String,
            "aggregation_method": pl.String,
        },
    )


def _minute_grid(period: Period, metadata: pl.DataFrame) -> pl.DataFrame:
    start = period.start.astimezone(UTC).replace(second=0, microsecond=0)
    minutes: list[datetime] = []
    value = start
    while value < period.end:
        minutes.append(value)
        value += timedelta(minutes=1)
    minute_frame = pl.DataFrame(
        {"time": minutes}, schema={"time": pl.Datetime("ns", "UTC")}
    )
    return minute_frame.join(metadata, how="cross")


def _empty_aggregates() -> pl.DataFrame:
    schema = _polars_schema()
    return pl.DataFrame(
        schema={
            name: dtype
            for name, dtype in schema.items()
            if name
            not in {
                "sensor",
                "vsn",
                "instrument_id",
                "units",
                "aggregation_method",
                "invalid_count",
                "valid_fraction",
                "aggregate_valid",
                "run_id",
                "work_unit_id",
                "config_hash",
            }
        }
    )


def _polars_schema() -> pl.Schema:
    return pl.Schema(
        {
            "time": pl.Datetime("ns", "UTC"),
            "sensor": pl.String,
            "vsn": pl.String,
            "instrument_id": pl.String,
            "variable": pl.String,
            "units": pl.String,
            "aggregation_method": pl.String,
            "value_float64": pl.Float64,
            "value_string": pl.String,
            "total_count": pl.UInt32,
            "valid_count": pl.UInt32,
            "invalid_count": pl.UInt32,
            "missing_value_count": pl.UInt32,
            "physical_range_count": pl.UInt32,
            "instrument_range_count": pl.UInt32,
            "valid_fraction": pl.Float64,
            "observed_rate_hz": pl.Float64,
            "maximum_gap_seconds": pl.Float64,
            "mean": pl.Float64,
            "median": pl.Float64,
            "standard_deviation": pl.Float64,
            "minimum": pl.Float64,
            "maximum": pl.Float64,
            "q25": pl.Float64,
            "q75": pl.Float64,
            "iqr": pl.Float64,
            "circular_resultant_length": pl.Float64,
            "aggregate_valid": pl.Boolean,
            "qc_bits": pl.UInt64,
            "run_id": pl.String,
            "work_unit_id": pl.String,
            "config_hash": pl.String,
        }
    )


def _validate_minute_data(frame: pl.DataFrame, expected_rows: int) -> None:
    if frame.height != expected_rows:
        raise ValueError(
            f"1-minute product row count mismatch: expected {expected_rows}, got {frame.height}"
        )
    if frame.select(pl.struct("time", "variable").n_unique()).item() != frame.height:
        raise ValueError("1-minute product must be unique by time and variable")
    invalid = frame.filter(
        (pl.col("total_count") < pl.col("valid_count"))
        | (pl.col("aggregate_valid") != (pl.col("valid_count") > 0))
    )
    if invalid.height:
        raise ValueError("1-minute product contains inconsistent counts or validity")
