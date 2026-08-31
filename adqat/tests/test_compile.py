from __future__ import annotations

from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from conftest import BASE_NS

from adqat.compile import CompileError, QCFlagContext, compile_findings
from adqat.findings import findings_schema, write_frame

CONTEXT = QCFlagContext(
    sensor="vaisala-wxt536",
    vsn="W08E",
    instrument_id="W08E--demo",
    config_hash="a" * 64,
)


def test_compiles_bits_idempotently_and_preserves_nanoseconds(tmp_path: Path) -> None:
    key_schema = pa.schema(
        [
            pa.field("time", pa.timestamp("ns", tz="UTC"), nullable=False),
            pa.field("series_id", pa.binary(16), nullable=False),
        ]
    )
    frame = pl.DataFrame(
        {
            "time": pl.Series([BASE_NS + 1] * 3, dtype=pl.Datetime("ns", "UTC")),
            "series_id": [b"x" * 16] * 3,
            "variable": ["temperature"] * 3,
            "check_id": ["a", "b", "b"],
            "bit": pl.Series([0, 63, 63], dtype=pl.UInt8),
            "observed_value": [100.0, 100.0, 100.0],
            "observed_value_string": [None, None, None],
            "score": [None, None, None],
            "run_id": ["run"] * 3,
            "work_unit_id": ["work"] * 3,
        }
    )
    findings_path = tmp_path / "findings.parquet"
    flags_path = tmp_path / "qc_flags.parquet"
    write_frame(frame, findings_schema(key_schema), findings_path)
    flagged = compile_findings(
        findings_path, flags_path, ["time", "series_id"], CONTEXT
    )
    flags = pq.read_table(flags_path)
    assert flagged == 1
    assert flags.num_rows == 1
    assert flags["time"].cast(pa.int64())[0].as_py() == BASE_NS + 1
    assert flags["qc_bits"][0].as_py() == (1 | (1 << 63))
    assert flags.column_names == [
        "time",
        "series_id",
        "sensor",
        "vsn",
        "instrument_id",
        "variable",
        "qc_bits",
        "run_id",
        "work_unit_id",
        "config_hash",
    ]
    assert flags["sensor"][0].as_py() == CONTEXT.sensor
    assert flags["vsn"][0].as_py() == CONTEXT.vsn
    assert flags["instrument_id"][0].as_py() == CONTEXT.instrument_id
    assert flags["run_id"][0].as_py() == "run"
    assert flags["work_unit_id"][0].as_py() == "work"
    assert flags["config_hash"][0].as_py() == CONTEXT.config_hash


def test_compiles_empty_findings_with_stable_schema(tmp_path: Path) -> None:
    key_schema = pa.schema([pa.field("time", pa.timestamp("ns", tz="UTC"))])
    empty = pl.DataFrame(
        schema={
            "time": pl.Datetime("ns", "UTC"),
            "variable": pl.String,
            "check_id": pl.String,
            "bit": pl.UInt8,
            "observed_value": pl.Float64,
            "observed_value_string": pl.String,
            "score": pl.Float64,
            "run_id": pl.String,
            "work_unit_id": pl.String,
        }
    )
    findings_path = tmp_path / "findings.parquet"
    flags_path = tmp_path / "qc_flags.parquet"
    write_frame(empty, findings_schema(key_schema), findings_path)
    flagged = compile_findings(findings_path, flags_path, ["time"], CONTEXT)
    flags = pq.read_table(flags_path)
    assert flagged == 0
    assert flags.num_rows == 0
    assert flags.schema.field("qc_bits").type == pa.uint64()
    assert flags.schema.field("config_hash").type == pa.string()


def test_identity_observation_key_is_not_duplicated_and_must_match(tmp_path: Path) -> None:
    key_schema = pa.schema(
        [
            pa.field("time", pa.timestamp("ns", tz="UTC"), nullable=False),
            pa.field("sensor", pa.string(), nullable=False),
        ]
    )
    frame = pl.DataFrame(
        {
            "time": pl.Series([BASE_NS + 1], dtype=pl.Datetime("ns", "UTC")),
            "sensor": [CONTEXT.sensor],
            "variable": ["temperature"],
            "check_id": ["missing"],
            "bit": pl.Series([0], dtype=pl.UInt8),
            "observed_value": [None],
            "observed_value_string": [None],
            "score": [None],
            "run_id": ["run"],
            "work_unit_id": ["work"],
        }
    )
    findings_path = tmp_path / "findings.parquet"
    flags_path = tmp_path / "qc_flags.parquet"
    write_frame(frame, findings_schema(key_schema), findings_path)

    compile_findings(findings_path, flags_path, ["time", "sensor"], CONTEXT)
    assert pq.read_table(flags_path).column_names.count("sensor") == 1

    mismatched = QCFlagContext("other-sensor", "W08E", "W08E--demo", "a" * 64)
    with pytest.raises(CompileError, match="do not match"):
        compile_findings(findings_path, flags_path, ["time", "sensor"], mismatched)
