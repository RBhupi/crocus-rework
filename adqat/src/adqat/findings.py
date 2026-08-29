from __future__ import annotations

from pathlib import Path
from typing import Any

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq


def findings_schema(key_schema: pa.Schema) -> pa.Schema:
    fields: list[pa.Field[Any]] = [key_schema.field(index) for index in range(len(key_schema))]
    fields.extend(
        [
            pa.field("variable", pa.string(), nullable=False),
            pa.field("check_id", pa.string(), nullable=False),
            pa.field("bit", pa.uint8(), nullable=False),
            pa.field("observed_value", pa.float64()),
            pa.field("observed_value_string", pa.string()),
            pa.field("score", pa.float64()),
            pa.field("run_id", pa.string(), nullable=False),
            pa.field("work_unit_id", pa.string(), nullable=False),
        ]
    )
    return pa.schema(fields)


def check_results_schema() -> pa.Schema:
    fields: list[pa.Field[Any]] = [
        pa.field("check_id", pa.string(), nullable=False),
        pa.field("variable", pa.string(), nullable=False),
        pa.field("flag_name", pa.string(), nullable=False),
        pa.field("bit", pa.uint8(), nullable=False),
        pa.field("engine", pa.string(), nullable=False),
        pa.field("processor", pa.string()),
        pa.field("units_tested", pa.int64(), nullable=False),
        pa.field("units_passed", pa.int64(), nullable=False),
        pa.field("units_failed", pa.int64(), nullable=False),
        pa.field("fraction_failed", pa.float64(), nullable=False),
        pa.field("warning", pa.bool_(), nullable=False),
        pa.field("error", pa.bool_(), nullable=False),
        pa.field("critical", pa.bool_(), nullable=False),
        pa.field("config_hash", pa.string(), nullable=False),
    ]
    return pa.schema(fields)


def qc_flags_schema(key_schema: pa.Schema) -> pa.Schema:
    fields: list[pa.Field[Any]] = [key_schema.field(index) for index in range(len(key_schema))]
    fields.extend(
        [
            pa.field("variable", pa.string(), nullable=False),
            pa.field("qc_bits", pa.uint64(), nullable=False),
            pa.field("run_id", pa.string(), nullable=False),
            pa.field("work_unit_id", pa.string(), nullable=False),
        ]
    )
    return pa.schema(fields)


def write_frame(frame: pl.DataFrame, schema: pa.Schema, path: Path) -> None:
    table = frame.to_arrow()
    if not isinstance(table, pa.Table):
        table = pa.Table.from_batches(list(table))
    table = table.select(schema.names).cast(schema, safe=True)
    pq.write_table(table, path, compression="zstd", version="2.6")
