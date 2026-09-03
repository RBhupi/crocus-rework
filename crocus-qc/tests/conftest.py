"""Synthetic raw Parquet fixtures and the Stage 1 test harness.

Fixtures are built with pyarrow so their Arrow types match the production ingest schema
exactly (``crocus_raw.model.fact_schema``). No Pandas, Polars, or Pointblank anywhere in
the test path.

The seam under test is: **synthetic raw Parquet in -> 10-second Parquet product out**.
Tests assert on the product's columns, never on the generated SQL text, so the SQL
builder can be rewritten freely.
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from crocus_qc.config import TEN_SECONDS, VariableSpec, load_profile
from crocus_qc.reduce import build_stage1_sql, session_setup_sql

DAY = datetime(2025, 12, 15, tzinfo=timezone.utc)
SENSOR = "vaisala-wxt536"
VSN = "W08D"
INSTRUMENT = "wxt536-001"


def fact_schema() -> pa.Schema:
    """Mirror of ``crocus_raw.model.fact_schema()``."""
    return pa.schema(
        [
            pa.field("time", pa.timestamp("ns", tz="UTC"), nullable=False),
            pa.field("sensor", pa.string(), nullable=False),
            pa.field("vsn", pa.string(), nullable=False),
            pa.field("instrument_id", pa.string(), nullable=False),
            pa.field("measurement", pa.string(), nullable=False),
            pa.field("field", pa.string(), nullable=False),
            pa.field("series_id", pa.binary(16), nullable=False),
            pa.field("value_type", pa.string(), nullable=False),
            pa.field("value_float64", pa.float64()),
            pa.field("value_int64", pa.int64()),
            pa.field("value_uint64", pa.uint64()),
            pa.field("value_bool", pa.bool_()),
            pa.field("value_string", pa.string()),
        ]
    )


def _series_id(measurement: str) -> bytes:
    return hashlib.md5(measurement.encode()).digest()


class Obs:
    """One raw observation, at a real (possibly irregular) timestamp.

    ``offset`` is seconds from the start of the UTC day and may be fractional, so tests
    can reproduce genuinely irregular ~10 Hz sampling.
    """

    __slots__ = ("offset", "measurement", "value", "text")

    def __init__(
        self,
        offset: float,
        measurement: str,
        value: float | None = None,
        text: str | None = None,
    ) -> None:
        self.offset = offset
        self.measurement = measurement
        self.value = value
        self.text = text


def write_raw(
    root: Path,
    observations: Iterable[Obs],
    *,
    sensor: str = SENSOR,
    vsn: str = VSN,
    instrument: str = INSTRUMENT,
    day: datetime = DAY,
) -> Path:
    """Write observations into the production Hive layout under ``root``."""
    rows = sorted(observations, key=lambda o: (o.offset, o.measurement))
    times = [day + timedelta(seconds=o.offset) for o in rows]
    is_text = [o.text is not None for o in rows]

    table = pa.table(
        {
            "time": pa.array(times, pa.timestamp("ns", tz="UTC")),
            "sensor": pa.array([sensor] * len(rows), pa.string()),
            "vsn": pa.array([vsn] * len(rows), pa.string()),
            "instrument_id": pa.array([instrument] * len(rows), pa.string()),
            "measurement": pa.array([o.measurement for o in rows], pa.string()),
            "field": pa.array(["value"] * len(rows), pa.string()),
            "series_id": pa.array([_series_id(o.measurement) for o in rows], pa.binary(16)),
            "value_type": pa.array(
                ["string" if t else "float64" for t in is_text], pa.string()
            ),
            "value_float64": pa.array([o.value for o in rows], pa.float64()),
            "value_int64": pa.array([None] * len(rows), pa.int64()),
            "value_uint64": pa.array([None] * len(rows), pa.uint64()),
            "value_bool": pa.array([None] * len(rows), pa.bool_()),
            "value_string": pa.array([o.text for o in rows], pa.string()),
        },
        schema=fact_schema(),
    )

    # Mirrors the production tree exactly, including the ``facts/`` level that sits
    # between the dataset version root and the Hive keys.
    partition = (
        root
        / "facts"
        / f"sensor={sensor}"
        / f"vsn={vsn}"
        / f"instrument={instrument}"
        / f"date={day:%Y-%m-%d}"
    )
    partition.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, partition / "part-0.parquet")
    return root


def run_stage1(
    tmp_path: Path,
    observations: Iterable[Obs],
    variables: Sequence[VariableSpec],
    *,
    day: datetime = DAY,
    sensor: str = SENSOR,
    vsn: str = VSN,
) -> list[dict[str, Any]]:
    """Write fixtures, run the real Stage 1 statement, read the product back.

    Returns every row of the dense grid as a dict, so a test can index by bucket and
    assert on named columns.
    """
    dataset = tmp_path / "raw"
    write_raw(dataset, observations, sensor=sensor, vsn=vsn, day=day)
    output = tmp_path / "10sec.parquet"

    sql = build_stage1_sql(
        dataset_root=str(dataset),
        sensor=sensor,
        vsn=vsn,
        day=day.date(),
        variables=tuple(variables),
        period=TEN_SECONDS,
        output_path=str(output),
    )
    with duckdb.connect() as conn:
        conn.execute(session_setup_sql(threads=2, memory_limit="1GB", temp_dir=str(tmp_path)))
        conn.execute(sql)
        result = conn.execute(
            f"SELECT * FROM read_parquet('{output}') ORDER BY time"
        )
        names = [d[0] for d in result.description]
        return [dict(zip(names, row)) for row in result.fetchall()]


def bucket(rows: Sequence[dict[str, Any]], offset_seconds: int) -> dict[str, Any]:
    """The dense-grid row whose bucket starts ``offset_seconds`` into the day."""
    return rows[offset_seconds // TEN_SECONDS.seconds]


@pytest.fixture
def wxt_profile():
    return load_profile("wxt536")


def subset(profile, names: Sequence[str]) -> tuple[VariableSpec, ...]:
    """Narrow a profile's variables, so a test's SQL only covers what it asserts on."""
    return tuple(profile.variable(name) for name in names)
