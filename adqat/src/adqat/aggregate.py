from __future__ import annotations

import math
from datetime import UTC, datetime, timedelta
from typing import Any

import polars as pl
import pyarrow as pa

from adqat.config import ResolvedConfig
from adqat.periods import Period
from adqat.pointblank import EngineResult


def aggregate_data_schema() -> pa.Schema:
    fields: list[pa.Field[Any]] = [
        pa.field("time", pa.timestamp("ns", tz="UTC"), nullable=False),
        pa.field("sensor", pa.string(), nullable=False),
        pa.field("vsn", pa.string(), nullable=False),
        pa.field("instrument_id", pa.string(), nullable=False),
        pa.field("variable", pa.string(), nullable=False),
        pa.field("units", pa.string()),
        pa.field("aggregation_method", pa.string(), nullable=False),
        pa.field("aggregation_period", pa.uint32(), nullable=False),
        pa.field("aggregation_period_units", pa.string(), nullable=False),
        pa.field("aggregation_period_seconds", pa.uint32(), nullable=False),
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
        pa.field("qc_bits", pa.uint8(), nullable=False),
        pa.field("run_id", pa.string(), nullable=False),
        pa.field("work_unit_id", pa.string(), nullable=False),
        pa.field("config_hash", pa.string(), nullable=False),
    ]
    return pa.schema(fields)


def aggregate_fixed_period(
    data: pl.DataFrame,
    engine_result: EngineResult,
    config: ResolvedConfig,
    period: Period,
    run_id: str,
) -> pl.DataFrame:
    """Create one dense row per configured fixed interval and variable.

    Raw observations are never resampled. Raw checks determine which values can
    contribute to the representative fixed-period value and descriptive statistics.
    Raw failure bits are not copied into this product. They only exclude source
    observations from aggregation. The independent aggregate rule set produces
    one unsigned eight-bit mask for each configured variable/interval.
    """

    time_name = config.run.source.time.column
    interval_seconds = config.run.processing.aggregation_seconds
    truncate_duration = config.run.processing.aggregation_polars_duration
    if interval_seconds is None or truncate_duration is None:
        raise ValueError("fixed-period aggregation is not configured")
    aggregation = config.run.processing.aggregation
    if aggregation is None:  # pragma: no cover - equivalent guard above
        raise ValueError("fixed-period aggregation is not configured")
    key_names = config.run.source.observation_keys
    raw = _attach_raw_qc(data, engine_result.findings, key_names)
    metadata = _variable_metadata(config)
    if raw.height:
        raw = (
            raw.join(metadata, on="variable", how="left", validate="m:1")
            .with_columns(
                pl.col(time_name).alias("_observation_time"),
                pl.col(time_name).dt.truncate(truncate_duration).alias("time"),
                (pl.col("raw_qc_bits") == 0).alias("_valid"),
            )
            .sort("_observation_time")
        )
        aggregated = _aggregate_observed(raw, config, "_observation_time", interval_seconds)
    else:
        aggregated = _empty_aggregates()

    grid = _aggregate_grid(period, metadata, interval_seconds)
    identity = config.work_unit.filters
    result = (
        grid.join(aggregated, on=["time", "variable"], how="left", validate="1:1")
        .with_columns(
            pl.lit(str(identity["sensor"]), dtype=pl.String).alias("sensor"),
            pl.lit(str(identity["vsn"]), dtype=pl.String).alias("vsn"),
            pl.lit(str(identity["instrument_id"]), dtype=pl.String).alias("instrument_id"),
            pl.lit(aggregation.period, dtype=pl.UInt32).alias("aggregation_period"),
            pl.lit(aggregation.units, dtype=pl.String).alias("aggregation_period_units"),
            pl.lit(interval_seconds, dtype=pl.UInt32).alias("aggregation_period_seconds"),
            pl.col("total_count").fill_null(0).cast(pl.UInt32),
            pl.col("valid_count").fill_null(0).cast(pl.UInt32),
            pl.col("missing_value_count").fill_null(0).cast(pl.UInt32),
            pl.col("physical_range_count").fill_null(0).cast(pl.UInt32),
            pl.col("instrument_range_count").fill_null(0).cast(pl.UInt32),
            pl.col("observed_rate_hz").fill_null(0.0),
        )
        .with_columns(
            (pl.col("total_count") - pl.col("valid_count")).alias("invalid_count"),
            pl.when(pl.col("total_count") > 0)
            .then(pl.col("valid_count") / pl.col("total_count"))
            .otherwise(pl.lit(None, dtype=pl.Float64))
            .alias("valid_fraction"),
        )
        .pipe(_apply_aggregate_qc)
        .with_columns(
            pl.lit(run_id, dtype=pl.String).alias("run_id"),
            pl.lit(config.work_unit.id, dtype=pl.String).alias("work_unit_id"),
            pl.lit(config.config_hash, dtype=pl.String).alias("config_hash"),
        )
        .select(aggregate_data_schema().names)
        .cast(_polars_schema(), strict=True)
        .sort("time", "variable")
    )
    _validate_aggregate_data(result, grid.height)
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
        .with_columns(pl.lit(2, dtype=pl.UInt64).pow(pl.col("bit")).cast(pl.UInt64).alias("_mask"))
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
    interval_seconds: int,
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
    grouped = prepared.group_by("time", "variable", "aggregation_method", maintain_order=True).agg(
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
        (pl.len() / float(interval_seconds)).alias("observed_rate_hz"),
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
    circular_degrees = (
        pl.arctan2(pl.col("_sin_mean"), pl.col("_cos_mean")) * 180.0 / math.pi + 360.0
    ) % 360.0
    resultant = (pl.col("_sin_mean").pow(2) + pl.col("_cos_mean").pow(2)).sqrt()
    is_circular = pl.col("aggregation_method") == "circular_mean"
    circular_standard_deviation = (-2.0 * resultant.clip(1e-15, 1.0).log()).sqrt() * 180.0 / math.pi
    return (
        grouped.with_columns(
            pl.when(is_circular).then(circular_degrees).otherwise(pl.col("mean")).alias("mean"),
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
            pl.when(is_circular).then(resultant).otherwise(None).alias("circular_resultant_length"),
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


def _apply_aggregate_qc(frame: pl.DataFrame) -> pl.DataFrame:
    no_value = pl.col("value_float64").is_null() & pl.col("value_string").is_null()
    insufficient = (
        (pl.col("valid_count") < pl.col("_minimum_valid_count"))
        | (
            pl.col("_minimum_valid_fraction").is_not_null()
            & (pl.col("valid_fraction").fill_null(0.0) < pl.col("_minimum_valid_fraction"))
        )
        | no_value
    )
    usable = ~insufficient
    numeric_value = pl.col("value_float64").is_not_null()
    variability = (
        usable
        & pl.col("_maximum_standard_deviation").is_not_null()
        & pl.col("standard_deviation").is_not_null()
        & (pl.col("standard_deviation") > pl.col("_maximum_standard_deviation"))
    )
    stuck = (
        usable
        & pl.col("_stuck_maximum_standard_deviation").is_not_null()
        & (pl.col("valid_count") >= pl.col("_stuck_minimum_valid_count"))
        & pl.col("standard_deviation").is_not_null()
        & (pl.col("standard_deviation") <= pl.col("_stuck_maximum_standard_deviation"))
    )
    below_physical = usable & numeric_value & _below("_physical")
    above_physical = usable & numeric_value & _above("_physical")
    below_instrument = usable & numeric_value & _below("_instrument")
    above_instrument = usable & numeric_value & _above("_instrument")
    qc_bits = (
        insufficient.cast(pl.UInt8)
        + variability.cast(pl.UInt8) * 2
        + stuck.cast(pl.UInt8) * 4
        + below_physical.cast(pl.UInt8) * 8
        + above_physical.cast(pl.UInt8) * 16
        + below_instrument.cast(pl.UInt8) * 32
        + above_instrument.cast(pl.UInt8) * 64
    ).cast(pl.UInt8)
    return frame.with_columns(qc_bits.alias("qc_bits")).with_columns(
        ((pl.col("qc_bits") & 1) == 0).alias("aggregate_valid"),
        pl.when((pl.col("qc_bits") & 1) == 0)
        .then(pl.col("value_float64"))
        .otherwise(None)
        .alias("value_float64"),
        pl.when((pl.col("qc_bits") & 1) == 0)
        .then(pl.col("value_string"))
        .otherwise(None)
        .alias("value_string"),
    )


def _below(prefix: str) -> pl.Expr:
    configured = pl.col(f"{prefix}_left").is_not_null()
    return configured & pl.when(pl.col(f"{prefix}_left_inclusive")).then(
        pl.col("value_float64") < pl.col(f"{prefix}_left")
    ).otherwise(pl.col("value_float64") <= pl.col(f"{prefix}_left"))


def _above(prefix: str) -> pl.Expr:
    configured = pl.col(f"{prefix}_right").is_not_null()
    return configured & pl.when(pl.col(f"{prefix}_right_inclusive")).then(
        pl.col("value_float64") > pl.col(f"{prefix}_right")
    ).otherwise(pl.col("value_float64") >= pl.col(f"{prefix}_right"))


def _variable_metadata(config: ResolvedConfig) -> pl.DataFrame:
    if config.aggregate_profile is None:
        raise ValueError("fixed-period aggregation requires resolved aggregate quality rules")
    rows = [
        {
            "variable": name,
            "units": variable.units,
            "aggregation_method": variable.aggregation,
            "_minimum_valid_count": aggregate.coverage.minimum_valid_count,
            "_minimum_valid_fraction": aggregate.coverage.minimum_valid_fraction,
            "_maximum_standard_deviation": (
                aggregate.variability.maximum_standard_deviation
                if aggregate.variability is not None
                else None
            ),
            "_stuck_maximum_standard_deviation": (
                aggregate.stuck.maximum_standard_deviation if aggregate.stuck is not None else None
            ),
            "_stuck_minimum_valid_count": (
                aggregate.stuck.minimum_valid_count if aggregate.stuck is not None else None
            ),
            **_range_metadata("_physical", aggregate.physical_range),
            **_range_metadata("_instrument", aggregate.instrument_range),
        }
        for name, variable in config.profile.variables.items()
        for aggregate in [config.aggregate_profile.variables[name]]
    ]
    return pl.DataFrame(
        rows,
        schema={
            "variable": pl.String,
            "units": pl.String,
            "aggregation_method": pl.String,
            "_minimum_valid_count": pl.UInt32,
            "_minimum_valid_fraction": pl.Float64,
            "_maximum_standard_deviation": pl.Float64,
            "_stuck_maximum_standard_deviation": pl.Float64,
            "_stuck_minimum_valid_count": pl.UInt32,
            "_physical_left": pl.Float64,
            "_physical_right": pl.Float64,
            "_physical_left_inclusive": pl.Boolean,
            "_physical_right_inclusive": pl.Boolean,
            "_instrument_left": pl.Float64,
            "_instrument_right": pl.Float64,
            "_instrument_left_inclusive": pl.Boolean,
            "_instrument_right_inclusive": pl.Boolean,
        },
    )


def _range_metadata(prefix: str, definition: Any | None) -> dict[str, Any]:
    return {
        f"{prefix}_left": definition.left if definition is not None else None,
        f"{prefix}_right": definition.right if definition is not None else None,
        f"{prefix}_left_inclusive": (definition.inclusive[0] if definition is not None else True),
        f"{prefix}_right_inclusive": (definition.inclusive[1] if definition is not None else True),
    }


def _aggregate_grid(period: Period, metadata: pl.DataFrame, interval_seconds: int) -> pl.DataFrame:
    start = period.start.astimezone(UTC)
    intervals: list[datetime] = []
    value = start
    while value < period.end:
        intervals.append(value)
        value += timedelta(seconds=interval_seconds)
    interval_frame = pl.DataFrame({"time": intervals}, schema={"time": pl.Datetime("ns", "UTC")})
    return interval_frame.join(metadata, how="cross")


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
                "aggregation_period",
                "aggregation_period_units",
                "aggregation_period_seconds",
                "invalid_count",
                "valid_fraction",
                "aggregate_valid",
                "run_id",
                "work_unit_id",
                "config_hash",
                "qc_bits",
            }
        }
    )


def _polars_schema() -> pl.Schema:
    schema: dict[str, pl.DataType | type[pl.DataType]] = {
        "time": pl.Datetime("ns", "UTC"),
        "sensor": pl.String,
        "vsn": pl.String,
        "instrument_id": pl.String,
        "variable": pl.String,
        "units": pl.String,
        "aggregation_method": pl.String,
        "aggregation_period": pl.UInt32,
        "aggregation_period_units": pl.String,
        "aggregation_period_seconds": pl.UInt32,
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
        "qc_bits": pl.UInt8,
        "run_id": pl.String,
        "work_unit_id": pl.String,
        "config_hash": pl.String,
    }
    return pl.Schema(schema)


def _validate_aggregate_data(frame: pl.DataFrame, expected_rows: int) -> None:
    if frame.height != expected_rows:
        raise ValueError(
            f"aggregate product row count mismatch: expected {expected_rows}, got {frame.height}"
        )
    if frame.select(pl.struct("time", "variable").n_unique()).item() != frame.height:
        raise ValueError("aggregate product must be unique by time and variable")
    invalid = frame.filter(
        (pl.col("total_count") < pl.col("valid_count"))
        | (pl.col("qc_bits") >= 128)
        | (pl.col("aggregate_valid") != ((pl.col("qc_bits") & 1) == 0))
        | (
            ((pl.col("qc_bits") & 1) != 0)
            & (pl.col("value_float64").is_not_null() | pl.col("value_string").is_not_null())
        )
    )
    if invalid.height:
        raise ValueError("aggregate product contains inconsistent counts or validity")
