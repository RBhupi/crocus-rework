from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Mapping

import pyarrow as pa


ValueType = Literal["float64", "int64", "uint64", "bool", "string"]
ScalarValue = float | int | bool | str


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


@dataclass(frozen=True)
class SeriesMetadata:
    series_id: bytes
    measurement: str
    tags: Mapping[str, str]
    vsn: str | None
    sensor: str | None
    instrument_id: str | None
    identity_source: str | None


def fact_schema(metadata: Mapping[bytes, bytes] | None = None) -> pa.Schema:
    fields = [
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
    return pa.schema(fields, metadata=metadata)


def series_schema(metadata: Mapping[bytes, bytes] | None = None) -> pa.Schema:
    return pa.schema(
        [
            pa.field("series_id", pa.binary(16), nullable=False),
            pa.field("sensor", pa.string(), nullable=False),
            pa.field("vsn", pa.string(), nullable=False),
            pa.field("instrument_id", pa.string(), nullable=False),
            pa.field("measurement", pa.string(), nullable=False),
            pa.field("identity_source", pa.string(), nullable=False),
            pa.field("tags", pa.map_(pa.string(), pa.string()), nullable=False),
            pa.field("fields", pa.list_(pa.string()), nullable=False),
            pa.field("value_types", pa.list_(pa.string()), nullable=False),
            pa.field("minimum_time", pa.timestamp("ns", tz="UTC"), nullable=False),
            pa.field("maximum_time", pa.timestamp("ns", tz="UTC"), nullable=False),
        ],
        metadata=metadata,
    )


parquet_schema = fact_schema
