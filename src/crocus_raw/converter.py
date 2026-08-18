from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from collections.abc import Collection
from typing import BinaryIO

from crocus_raw.instruments import InstrumentResolver
from crocus_raw.line_protocol import iter_logical_records, parse_record
from crocus_raw.model import OutputPoint
from crocus_raw.writer import HourlyDatasetWriter


@dataclass
class ConversionSummary:
    logical_records: int = 0
    parsed_point_rows: int = 0
    output_rows: int = 0
    upper_boundary_rows: int = 0
    filtered_instrument_rows: int = 0
    measurements: Counter[str] = field(default_factory=Counter)
    value_types: Counter[str] = field(default_factory=Counter)


def convert_stream(
    stream: BinaryIO,
    conversion_date: date,
    resolver: InstrumentResolver,
    writer: HourlyDatasetWriter,
    allowed_instrument_ids: Collection[str] | None = None,
    finalize_writer: bool = True,
) -> dict[str, object]:
    start_ns = _date_start_ns(conversion_date)
    end_ns = _date_start_ns(conversion_date + timedelta(days=1))
    summary = ConversionSummary()

    for record_number, record in enumerate(iter_logical_records(stream), start=1):
        summary.logical_records += 1
        for point in parse_record(record, record_number):
            summary.parsed_point_rows += 1
            if point.time_ns < start_ns:
                raise ValueError(
                    f"point precedes requested day: {point.measurement} {point.time_ns} < {start_ns}"
                )
            if point.time_ns >= end_ns:
                summary.upper_boundary_rows += 1
                continue
            instrument_id = resolver.resolve(point)
            if allowed_instrument_ids is not None and instrument_id not in allowed_instrument_ids:
                summary.filtered_instrument_rows += 1
                continue
            writer.append(OutputPoint(point=point, instrument_id=instrument_id))
            summary.output_rows += 1
            summary.measurements[point.measurement] += 1
            summary.value_types[point.parsed_value.value_type] += 1

    conversion_document = {
        "date": conversion_date.isoformat(),
        "logical_records": summary.logical_records,
        "parsed_point_rows": summary.parsed_point_rows,
        "output_rows": summary.output_rows,
        "upper_boundary_rows": summary.upper_boundary_rows,
        "filtered_instrument_rows": summary.filtered_instrument_rows,
        "measurements": dict(sorted(summary.measurements.items())),
        "value_types": dict(sorted(summary.value_types.items())),
    }
    if not finalize_writer:
        return conversion_document
    run_manifest = writer.finalize(conversion_document)
    return {**conversion_document, "run": run_manifest}


def _date_start_ns(value: date) -> int:
    moment = datetime(value.year, value.month, value.day, tzinfo=UTC)
    return int(moment.timestamp()) * 1_000_000_000
