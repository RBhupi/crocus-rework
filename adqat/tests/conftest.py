from __future__ import annotations

from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml

BASE_NS = 1_735_689_600_000_000_000  # 2025-01-01T00:00:00Z


def quality_document() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "flags": {
            "missing_value": {"bit": 0, "description": "Missing value."},
            "missing_sample": {"bit": 1, "description": "Missing sample."},
            "physical_range": {"bit": 2, "description": "Physical range."},
            "instrument_range": {"bit": 3, "description": "Instrument range."},
        },
        "profiles": {
            "demo_wxt": {
                "sampling": {"expected_frequency_hz": 10},
                "variables": {
                    "temperature": {
                        "column": "value_float64",
                        "where": {
                            "measurement": "wxt.env.temp",
                            "field": "value",
                            "value_type": "float64",
                        },
                        "units": "degree_Celsius",
                        "missing_values": [-9999.9],
                        "checks": [
                            {
                                "id": "temperature_missing",
                                "method": "col_vals_not_null",
                                "flag": "missing_value",
                            },
                            {
                                "id": "temperature_physical",
                                "method": "col_vals_between",
                                "flag": "physical_range",
                                "args": {"left": -80, "right": 70},
                            },
                            {
                                "id": "temperature_instrument",
                                "method": "col_vals_between",
                                "flag": "instrument_range",
                                "args": {"left": -50, "right": 60},
                            },
                        ],
                    },
                    "wind_speed": {
                        "column": "value_float64",
                        "where": {
                            "measurement": "wxt.wind.speed",
                            "field": "value",
                            "value_type": "float64",
                        },
                        "units": "m s-1",
                        "checks": [
                            {
                                "id": "wind_missing",
                                "method": "col_vals_not_null",
                                "flag": "missing_value",
                            },
                            {
                                "id": "wind_physical",
                                "method": "col_vals_between",
                                "flag": "physical_range",
                                "args": {"left": 0, "right": 100},
                            },
                        ],
                    },
                },
            },
            "demo_aqt": {
                "variables": {
                    "co": {
                        "column": "value_float64",
                        "where": {
                            "measurement": "aqt.gas.co",
                            "field": "value",
                            "value_type": "float64",
                        },
                        "units": "ppm",
                        "checks": [
                            {
                                "id": "co_missing",
                                "method": "col_vals_not_null",
                                "flag": "missing_value",
                            },
                            {
                                "id": "co_instrument",
                                "method": "col_vals_between",
                                "flag": "instrument_range",
                                "args": {"left": 0, "right": 100},
                            },
                        ],
                    }
                }
            },
        },
        "pipelines": {"basic_qc": {"stages": [{"id": "basic", "engine": "pointblank"}]}},
    }


def aggregate_quality_document() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "metadata": {
            "status": "demo",
            "description": "Synthetic aggregate quality rules for tests.",
        },
        "flags": {
            "insufficient_coverage": {"bit": 0, "description": "Insufficient coverage."},
            "excessive_variability": {"bit": 1, "description": "Excessive variability."},
            "stuck_value": {"bit": 2, "description": "Stuck value."},
            "below_physical_minimum": {"bit": 3, "description": "Below physical minimum."},
            "above_physical_maximum": {"bit": 4, "description": "Above physical maximum."},
            "below_instrument_minimum": {
                "bit": 5,
                "description": "Below instrument minimum.",
            },
            "above_instrument_maximum": {
                "bit": 6,
                "description": "Above instrument maximum.",
            },
            "reserved": {"bit": 7, "description": "Reserved and always zero."},
        },
        "profiles": {
            "demo_wxt": {
                "variables": {
                    "temperature": {
                        "coverage": {"minimum_valid_count": 1},
                        "physical_range": {"left": -80, "right": 70},
                        "instrument_range": {"left": -50, "right": 60},
                    },
                    "wind_speed": {
                        "coverage": {"minimum_valid_count": 1},
                        "physical_range": {"left": 0, "right": 100},
                    },
                }
            },
            "demo_aqt": {
                "variables": {
                    "co": {
                        "coverage": {"minimum_valid_count": 1},
                        "instrument_range": {"left": 0, "right": 100},
                    }
                }
            },
        },
    }


def run_document(
    source_path: str,
    output_root: str,
    *,
    hive: bool = False,
    start: str = "2025-01-01T00:00:00Z",
    end: str = "2025-01-02T00:00:00Z",
    period: str = "1d",
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "source": {
            "type": "parquet",
            "path": source_path,
            "options": {"hive_partitioning": hive, "union_by_name": True},
            "time": {"column": "time", "timezone": "UTC"},
            "observation_keys": ["time", "series_id"],
        },
        "quality": {"rules": "quality_rules.yaml", "pipeline": "basic_qc"},
        "selection": {"start": start, "end": end},
        "processing": {"period": period},
        "work_units": [
            {
                "id": "demo_wxt_work_unit",
                "profile": "demo_wxt",
                "filters": {
                    "sensor": "vaisala-wxt536",
                    "vsn": "W08E",
                    "instrument_id": "W08E--demo",
                },
            },
            {
                "id": "demo_aqt_work_unit",
                "profile": "demo_aqt",
                "filters": {
                    "sensor": "vaisala-aqt530",
                    "vsn": "W08E",
                    "instrument_id": "W08E--aqt-demo",
                },
            },
        ],
        "output": {"root": output_root},
    }


def write_configuration(
    root: Path,
    *,
    hive: bool = False,
    start: str = "2025-01-01T00:00:00Z",
    end: str = "2025-01-02T00:00:00Z",
    period: str = "1d",
) -> Path:
    facts = root / "facts"
    source = str(facts / "**" / "*.parquet") if hive else str(facts / "*.parquet")
    rules_path = root / "quality_rules.yaml"
    aggregate_rules_path = root / "aggregate_quality_rules.yaml"
    run_path = root / "processing_run.yaml"
    rules_path.write_text(yaml.safe_dump(quality_document(), sort_keys=False), encoding="utf-8")
    aggregate_rules_path.write_text(
        yaml.safe_dump(aggregate_quality_document(), sort_keys=False), encoding="utf-8"
    )
    run_path.write_text(
        yaml.safe_dump(
            run_document(
                source, str(root / "results"), hive=hive, start=start, end=end, period=period
            ),
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return run_path


def write_facts(root: Path, rows: list[dict[str, Any]], *, hive: bool = False) -> Path:
    if hive:
        directory = (
            root
            / "facts"
            / "sensor=vaisala-wxt536"
            / "vsn=W08E"
            / "instrument=W08E--demo"
            / "date=2025-01-01"
        )
    else:
        directory = root / "facts"
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / "part-000.parquet"
    schema = pa.schema(
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
    columns = {field.name: [] for field in schema}
    for index, row in enumerate(rows):
        columns["time"].append(row.get("time", BASE_NS + index))
        columns["sensor"].append(row.get("sensor", "vaisala-wxt536"))
        columns["vsn"].append(row.get("vsn", "W08E"))
        columns["instrument_id"].append(row.get("instrument_id", "W08E--demo"))
        columns["measurement"].append(row.get("measurement", "wxt.env.temp"))
        columns["field"].append(row.get("field", "value"))
        columns["series_id"].append(row.get("series_id", index.to_bytes(16, "big")))
        value_type = row.get("value_type", "float64")
        columns["value_type"].append(value_type)
        columns["value_float64"].append(row.get("value") if value_type == "float64" else None)
        columns["value_int64"].append(None)
        columns["value_uint64"].append(None)
        columns["value_bool"].append(None)
        columns["value_string"].append(row.get("value") if value_type == "string" else None)
    table = pa.Table.from_pydict(columns, schema=schema)
    pq.write_table(table, path, compression="zstd", version="2.6")
    return path


@pytest.fixture
def synthetic_project(tmp_path: Path) -> tuple[Path, Path]:
    run_path = write_configuration(tmp_path)
    facts_path = write_facts(
        tmp_path,
        [
            {"time": BASE_NS + 1, "value": 10.0},
            {"time": BASE_NS + 2, "value": float("nan")},
            {"time": BASE_NS + 3, "value": 65.0},
            {"time": BASE_NS + 4, "value": 100.0},
            {"time": BASE_NS + 5, "measurement": "wxt.wind.speed", "value": 5.0},
            {"time": BASE_NS + 6, "measurement": "unconfigured", "value": 999.0},
        ],
    )
    return run_path, facts_path
