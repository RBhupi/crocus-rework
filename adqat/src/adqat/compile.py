from __future__ import annotations

import os
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from adqat.findings import qc_flags_schema


class CompileError(RuntimeError):
    """Raised when sparse findings cannot be compiled."""


@dataclass(frozen=True)
class QCFlagContext:
    """Provenance copied from the snapshotted work-unit configuration."""

    sensor: str
    vsn: str
    instrument_id: str
    config_hash: str


def compile_findings(
    findings_path: Path,
    output_path: Path,
    observation_keys: list[str],
    context: QCFlagContext,
    *,
    atomic: bool = False,
) -> int:
    temporary: Path | None = None
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
            _validate_sparse_flags(connection, result, group_sql)
        finally:
            connection.close()
        result = _append_context(result, context)
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
        return result.num_rows
    except Exception as error:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
        raise CompileError(f"failed to compile {findings_path}: {error}") from error


def _append_context(table: pa.Table, context: QCFlagContext) -> pa.Table:
    for name in ("sensor", "vsn", "instrument_id", "config_hash"):
        value = getattr(context, name)
        if not value:
            raise ValueError(f"QC flag context {name!r} must be non-empty")
        if name in table.column_names:
            matches = pc.all(
                pc.equal(table[name], pa.scalar(value)), skip_nulls=False
            ).as_py()
            if table.num_rows > 0 and matches is not True:
                raise ValueError(
                    f"observation-key values for {name!r} do not match work-unit identity"
                )
            continue
        table = table.append_column(
            name,
            pa.array([value] * table.num_rows, type=pa.string()),
        )
    return table


def _validate_sparse_flags(
    connection: duckdb.DuckDBPyConnection,
    table: pa.Table,
    group_sql: str,
) -> None:
    connection.register("compiled_flags", table)
    zero_count = connection.execute(
        "SELECT count(*) FROM compiled_flags WHERE qc_bits = 0"
    ).fetchone()
    if zero_count is None or int(zero_count[0]) != 0:
        raise ValueError("compiled QC flags must contain only nonzero masks")
    duplicate_count = connection.execute(
        f"""
        SELECT count(*)
        FROM (
            SELECT {group_sql}, count(*) AS occurrences
            FROM compiled_flags
            GROUP BY {group_sql}
            HAVING count(*) > 1
        )
        """
    ).fetchone()
    if duplicate_count is None or int(duplicate_count[0]) != 0:
        raise ValueError("compiled QC flags must be unique by observation identity and variable")


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
