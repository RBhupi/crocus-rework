from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Mapping

import pyarrow as pa


ValueType = Literal["float64", "int64", "uint64", "bool", "string"]
ScalarValue = float | int | bool | str

PROMOTED_TAGS = (
    "node",
    "vsn",
    "host",
    "plugin",
    "task",
    "job",
    "sensor",
    "site",
    "zone",
    "units",
    "missing",
)


@dataclass(frozen=True)
class ParsedValue:
    value_type: ValueType
    value: ScalarValue


@dataclass(frozen=True)
class InfluxPoint:
    time_ns: int
    measurement: str
    field: str
    parsed_value: ParsedValue
    tags: Mapping[str, str]


@dataclass(frozen=True)
class OutputPoint:
    point: InfluxPoint
    instrument_id: str


def parquet_schema(metadata: Mapping[bytes, bytes] | None = None) -> pa.Schema:
    fields = [
        pa.field("time", pa.timestamp("ns", tz="UTC"), nullable=False),
        pa.field("instrument_id", pa.string(), nullable=False),
        pa.field("measurement", pa.string(), nullable=False),
        pa.field("field", pa.string(), nullable=False),
        pa.field("value_type", pa.string(), nullable=False),
        pa.field("value_float64", pa.float64()),
        pa.field("value_int64", pa.int64()),
        pa.field("value_uint64", pa.uint64()),
        pa.field("value_bool", pa.bool_()),
        pa.field("value_string", pa.string()),
        pa.field("tags", pa.map_(pa.string(), pa.string()), nullable=False),
    ]
    fields.extend(pa.field(name, pa.string()) for name in PROMOTED_TAGS)
    return pa.schema(fields, metadata=metadata)
