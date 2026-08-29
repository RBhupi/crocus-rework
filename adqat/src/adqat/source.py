from __future__ import annotations

import glob
from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import Any, cast

import polars as pl
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.types as pat

from adqat.config import ResolvedConfig, Scalar, VariableDefinition
from adqat.periods import Period


class SourceError(RuntimeError):
    """Raised when a configured source cannot be selected safely."""


@dataclass(frozen=True)
class SelectedPeriod:
    data: pl.DataFrame
    key_schema: pa.Schema
    source_files: tuple[Path, ...]

    @property
    def row_count(self) -> int:
        return self.data.height


def validate_source(config: ResolvedConfig) -> pa.Schema:
    dataset = _open_dataset(config)
    return _validate_dataset(dataset, config)


def _validate_dataset(dataset: ds.Dataset, config: ResolvedConfig) -> pa.Schema:
    schema = dataset.schema
    required = _required_columns(config)
    missing = sorted(required.difference(schema.names))
    if missing:
        raise SourceError(f"source is missing required columns: {', '.join(missing)}")
    for variable_name, variable in config.profile.variables.items():
        field = schema.field(variable.column)
        is_numeric = (
            pat.is_integer(field.type) or pat.is_floating(field.type) or pat.is_decimal(field.type)
        )
        if variable.data_type == "numeric" and not is_numeric:
            raise SourceError(
                f"variable {variable_name!r} column {variable.column!r} must be numeric, "
                f"not {field.type}"
            )
        is_string = pat.is_string(field.type) or pat.is_large_string(field.type)
        if variable.data_type == "string" and not is_string:
            raise SourceError(
                f"variable {variable_name!r} column {variable.column!r} must be a string, "
                f"not {field.type}"
            )
    time_field = schema.field(config.run.source.time.column)
    if not pat.is_timestamp(time_field.type):
        raise SourceError(
            f"time column {time_field.name!r} must be an Arrow timestamp, not {time_field.type}"
        )
    return schema


def select_period(config: ResolvedConfig, period: Period) -> SelectedPeriod:
    dataset = _open_dataset(config)
    _validate_dataset(dataset, config)
    filter_expression = _selection_filter(config, period)
    columns = sorted(_required_columns(config))
    try:
        table = dataset.to_table(
            columns=[*columns, "__filename"],
            filter=filter_expression,
        )
    except (pa.ArrowException, OSError, TypeError, ValueError) as error:
        raise SourceError(f"failed to select source period {period.id}: {error}") from error

    table = table.rename_columns([*columns, "_source_file"])
    key_schema = pa.schema(
        [table.schema.field(name) for name in config.run.source.observation_keys]
    )
    frame = cast(pl.DataFrame, pl.from_arrow(table))
    normalized = _normalize_variables(frame, config)
    source_files = tuple(
        Path(value)
        for value in normalized.get_column("_source_file").unique().sort().to_list()
        if value is not None
    )
    return SelectedPeriod(normalized, key_schema, source_files)


def _normalize_variables(frame: pl.DataFrame, config: ResolvedConfig) -> pl.DataFrame:
    keys = config.run.source.observation_keys
    schema = pl.Schema(
        [
            *((name, frame.schema[name]) for name in keys),
            ("variable", pl.String()),
            ("observed_value", pl.Float64()),
            ("observed_value_string", pl.String()),
            ("_source_file", pl.String()),
        ]
    )
    chunks: list[pl.DataFrame] = []
    for variable_name, variable in config.profile.variables.items():
        predicate = _polars_selector(variable)
        if variable.data_type == "numeric":
            values = pl.col(variable.column).cast(pl.Float64, strict=False)
            missing = values.is_nan()
            if variable.missing_values:
                missing = missing | values.is_in(variable.missing_values)
            chunks.append(
                frame.filter(predicate).select(
                    *keys,
                    pl.lit(variable_name, dtype=pl.String).alias("variable"),
                    pl.when(missing)
                    .then(pl.lit(None, dtype=pl.Float64))
                    .otherwise(values)
                    .alias("observed_value"),
                    pl.lit(None, dtype=pl.String).alias("observed_value_string"),
                    pl.col("_source_file").cast(pl.String),
                )
            )
        else:
            string_values = pl.col(variable.column).cast(pl.String, strict=False)
            string_missing = pl.lit(False)
            if variable.missing_strings:
                string_missing = string_values.is_in(variable.missing_strings)
            chunks.append(
                frame.filter(predicate).select(
                    *keys,
                    pl.lit(variable_name, dtype=pl.String).alias("variable"),
                    pl.lit(None, dtype=pl.Float64).alias("observed_value"),
                    pl.when(string_missing)
                    .then(pl.lit(None, dtype=pl.String))
                    .otherwise(string_values)
                    .alias("observed_value_string"),
                    pl.col("_source_file").cast(pl.String),
                )
            )
    if not chunks:
        return pl.DataFrame(schema=schema)
    return pl.concat(chunks, how="vertical_relaxed").select(schema.keys()).cast(schema)


def _polars_selector(variable: VariableDefinition) -> pl.Expr:
    expression: pl.Expr | None = None
    for column, value in variable.where.items():
        condition = pl.col(column) == pl.lit(value)
        expression = condition if expression is None else expression & condition
    if expression is None:
        raise SourceError("variable selector cannot be empty")
    return expression


def _selection_filter(config: ResolvedConfig, period: Period) -> ds.Expression:
    time_column = config.run.source.time.column
    expression = (ds.field(time_column) >= _arrow_time(period.start)) & (
        ds.field(time_column) < _arrow_time(period.end)
    )
    for column, value in config.work_unit.filters.items():
        expression &= ds.field(column) == _arrow_scalar(value)

    variable_expression: ds.Expression | None = None
    for variable in config.profile.variables.values():
        selector: ds.Expression | None = None
        for column, value in variable.where.items():
            condition = ds.field(column) == _arrow_scalar(value)
            selector = condition if selector is None else selector & condition
        if selector is not None:
            variable_expression = (
                selector if variable_expression is None else variable_expression | selector
            )
    if variable_expression is None:
        raise SourceError("profile contains no variable selectors")
    return expression & variable_expression


def _open_dataset(config: ResolvedConfig) -> ds.Dataset:
    return _dataset_for(
        config.source_path,
        config.run.source.options.hive_partitioning,
        config.run.source.options.union_by_name,
    )


@lru_cache(maxsize=16)
def _dataset_for(source_path: str, hive_partitioning: bool, union_by_name: bool) -> ds.Dataset:
    files = sorted(Path(path) for path in glob.glob(source_path, recursive=True))
    files = [path.resolve() for path in files if path.is_file() and path.suffix == ".parquet"]
    if not files:
        raise SourceError(f"source pattern matched no Parquet files: {source_path}")
    try:
        arguments: dict[str, Any] = {
            "source": [str(path) for path in files],
            "format": "parquet",
        }
        if hive_partitioning:
            arguments.update(
                {
                    "partitioning": "hive",
                    "partition_base_dir": str(_glob_base(source_path)),
                }
            )
        dataset = ds.dataset(**arguments)
        if union_by_name and len(files) > 1:
            schemas = [fragment.physical_schema for fragment in dataset.get_fragments()]
            if hive_partitioning:
                schemas.append(dataset.partitioning.schema)
            arguments["schema"] = pa.unify_schemas(schemas)
            dataset = ds.dataset(**arguments)
        return cast(ds.Dataset, dataset)
    except (pa.ArrowException, OSError, ValueError) as error:
        raise SourceError(f"cannot open Parquet source: {error}") from error


def _required_columns(config: ResolvedConfig) -> set[str]:
    columns = set(config.run.source.observation_keys)
    columns.update(config.work_unit.filters)
    for variable in config.profile.variables.values():
        columns.add(variable.column)
        columns.update(variable.where)
    return columns


def _glob_base(pattern: str) -> Path:
    special = min((pattern.find(char) for char in "*?[" if char in pattern), default=-1)
    prefix = pattern if special < 0 else pattern[:special]
    path = Path(prefix.rstrip("/"))
    return (path if path.is_dir() else path.parent).resolve()


def _arrow_time(value: datetime) -> pa.Scalar[Any]:
    return pa.scalar(value, type=pa.timestamp("ns", tz="UTC"))


def _arrow_scalar(value: Scalar) -> Any:
    return value
