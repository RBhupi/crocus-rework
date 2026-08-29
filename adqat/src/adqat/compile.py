from __future__ import annotations

import os
import uuid
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from adqat.findings import qc_flags_schema


class CompileError(RuntimeError):
    """Raised when sparse findings cannot be compiled."""


def compile_findings(
    findings_path: Path,
    output_path: Path,
    observation_keys: list[str],
    *,
    atomic: bool = False,
) -> None:
    try:
        findings = pq.read_table(findings_path)
        key_schema = pa.schema([findings.schema.field(name) for name in observation_keys])
        internal = _timestamps_to_integers(findings, observation_keys)
        connection = duckdb.connect()
        try:
            connection.register("findings", internal)
            keys_sql = ", ".join(_quote_identifier(name) for name in observation_keys)
            group_sql = ", ".join(
                [*(_quote_identifier(name) for name in observation_keys), '"variable"']
            )
            result = connection.execute(
                f"""
                SELECT {keys_sql}, "variable",
                       BIT_OR((1::UBIGINT) << CAST("bit" AS UTINYINT)) AS qc_bits,
                       MIN("run_id") AS run_id,
                       MIN("work_unit_id") AS work_unit_id
                FROM findings
                GROUP BY {group_sql}
                """
            ).to_arrow_table()
        finally:
            connection.close()
        result = _restore_timestamps(result, key_schema)
        result = result.select(qc_flags_schema(key_schema).names).cast(
            qc_flags_schema(key_schema), safe=True
        )
        target = output_path
        temporary = output_path.with_name(f".{output_path.name}.{uuid.uuid4().hex}.tmp")
        if atomic:
            target = temporary
        pq.write_table(result, target, compression="zstd", version="2.6")
        if atomic:
            os.replace(temporary, output_path)
    except Exception as error:
        raise CompileError(f"failed to compile {findings_path}: {error}") from error


def _timestamps_to_integers(table: pa.Table, keys: list[str]) -> pa.Table:
    arrays: list[Any] = []
    fields: list[pa.Field[Any]] = []
    for field, column in zip(table.schema, table.columns, strict=True):
        if field.name in keys and pa.types.is_timestamp(field.type):
            arrays.append(pc.cast(column, pa.int64()))
            fields.append(pa.field(field.name, pa.int64(), nullable=field.nullable))
        else:
            arrays.append(column)
            fields.append(field)
    return pa.Table.from_arrays(arrays, schema=pa.schema(fields))


def _restore_timestamps(table: pa.Table, key_schema: pa.Schema) -> pa.Table:
    key_types = {field.name: field.type for field in key_schema}
    arrays: list[Any] = []
    fields: list[pa.Field[Any]] = []
    for field, column in zip(table.schema, table.columns, strict=True):
        target_type = key_types.get(field.name)
        if target_type is not None and pa.types.is_timestamp(target_type):
            arrays.append(pc.cast(column, target_type))
            fields.append(pa.field(field.name, target_type, nullable=field.nullable))
        else:
            arrays.append(column)
            fields.append(field)
    return pa.Table.from_arrays(arrays, schema=pa.schema(fields))


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'
