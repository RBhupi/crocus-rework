from __future__ import annotations

import gc
from collections import Counter
from collections.abc import Collection
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import BinaryIO

from crocus_raw.decoder import CachedLineProtocolDecoder
from crocus_raw.instruments import InstrumentResolver
from crocus_raw.selection import Selection
from crocus_raw.writer import DailyDatasetWriter


DAY_NS = 86_400_000_000_000


@dataclass
class ConversionSummary:
    logical_records: int = 0
    parsed_point_rows: int = 0
    output_rows: int = 0
    quarantined_rows: int = 0
    upper_boundary_rows: int = 0
    filtered_selection_rows: int = 0
    filtered_instrument_rows: int = 0
    measurements: Counter[str] = field(default_factory=Counter)
    value_types: Counter[str] = field(default_factory=Counter)


def convert_stream(
    stream: BinaryIO,
    conversion_date: date,
    resolver: InstrumentResolver,
    writer: DailyDatasetWriter,
    selection: Selection | None = None,
    allowed_instrument_ids: Collection[str] | None = None,
    finalize_writer: bool = True,
) -> dict[str, object]:
    start_time = datetime(conversion_date.year, conversion_date.month, conversion_date.day, tzinfo=UTC)
    end_time = start_time + timedelta(days=1)
    result = convert_range_stream(
        stream,
        start_time,
        end_time,
        resolver,
        writer,
        selection=selection,
        allowed_instrument_ids=allowed_instrument_ids,
        finalize_writer=finalize_writer,
    )
    document = {
        key: value
        for key, value in result.items()
        if key not in {"start_time", "end_time", "dates"}
    }
    document["date"] = conversion_date.isoformat()
    return document


def convert_range_stream(
    stream: BinaryIO,
    start_time: datetime,
    end_time: datetime,
    resolver: InstrumentResolver,
    writer: DailyDatasetWriter,
    selection: Selection | None = None,
    allowed_instrument_ids: Collection[str] | None = None,
    finalize_writer: bool = True,
) -> dict[str, object]:
    if start_time.tzinfo is None or end_time.tzinfo is None:
        raise ValueError("range timestamps must be timezone-aware")
    start_ns = _datetime_ns(start_time)
    end_ns = _datetime_ns(end_time)
    if end_ns <= start_ns:
        raise ValueError("range end must be after range start")
    summary = ConversionSummary()
    daily: dict[str, ConversionSummary] = {}
    date_cache: dict[int, str] = {}
    daily_cache: dict[int, ConversionSummary] = {}
    selection_cache: dict[tuple[bytes, str], tuple[bool, bool]] = {}
    decoder = CachedLineProtocolDecoder(resolver)
    gc_was_enabled = gc.isenabled()
    if gc_was_enabled:
        gc.disable()
    try:
        for record in decoder.records(stream):
            summary.logical_records += 1
            series = record.series.metadata
            point_day = record.time_ns // DAY_NS
            date_text = date_cache.get(point_day)
            if date_text is None:
                date_text = datetime.fromtimestamp(
                    record.time_ns // 1_000_000_000, tz=UTC
                ).date().isoformat()
                date_cache[point_day] = date_text
            day_summary = daily_cache.get(point_day)
            if day_summary is None:
                day_summary = daily.setdefault(date_text, ConversionSummary())
                daily_cache[point_day] = day_summary
            day_summary.logical_records += 1
            for decoded_field in record.fields:
                summary.parsed_point_rows += 1
                day_summary.parsed_point_rows += 1
                if record.time_ns < start_ns:
                    raise ValueError(
                        f"point precedes requested range: {series.measurement} "
                        f"{record.time_ns} < {start_ns}"
                    )
                if record.time_ns >= end_ns:
                    summary.upper_boundary_rows += 1
                    day_summary.upper_boundary_rows += 1
                    continue

                selection_key = (series.series_id, decoded_field.name)
                selection_decision = selection_cache.get(selection_key)
                if selection_decision is None:
                    matches = selection is None or selection.matches_parts(
                        series.measurement,
                        decoded_field.name,
                        record.series.selection_tags,
                    )
                    candidate = selection is None or selection.matches_measurement_field(
                        series.measurement, decoded_field.name
                    )
                    selection_decision = (matches, candidate)
                    selection_cache[selection_key] = selection_decision
                matches, candidate = selection_decision
                missing_identity = series.vsn is None or series.sensor is None
                candidate_missing_identity = (
                    missing_identity
                    and candidate
                )
                if not matches and not candidate_missing_identity:
                    summary.filtered_selection_rows += 1
                    day_summary.filtered_selection_rows += 1
                    continue
                if (
                    not missing_identity
                    and allowed_instrument_ids is not None
                    and series.instrument_id not in allowed_instrument_ids
                ):
                    summary.filtered_instrument_rows += 1
                    day_summary.filtered_instrument_rows += 1
                    continue

                accepted = writer.append_value(
                    record.time_ns,
                    date_text,
                    series,
                    decoded_field.name,
                    decoded_field.value_type,
                    decoded_field.value,
                )
                if not accepted:
                    summary.quarantined_rows += 1
                    day_summary.quarantined_rows += 1
                    continue
                summary.output_rows += 1
                day_summary.output_rows += 1
                summary.measurements[series.measurement] += 1
                day_summary.measurements[series.measurement] += 1
                summary.value_types[decoded_field.value_type] += 1
                day_summary.value_types[decoded_field.value_type] += 1
    finally:
        if gc_was_enabled:
            gc.enable()

    conversion_document: dict[str, object] = {
        "start_time": start_time.astimezone(UTC).isoformat(),
        "end_time": end_time.astimezone(UTC).isoformat(),
        "series_cache_entries": len(decoder.series_cache),
        "dates": {
            point_date: {"date": point_date, **_summary_document(day_summary)}
            for point_date, day_summary in sorted(daily.items())
        },
        **_summary_document(summary),
    }
    if not finalize_writer:
        return conversion_document
    run_manifest = writer.finalize(conversion_document)
    return {**conversion_document, "run": run_manifest}


def _summary_document(summary: ConversionSummary) -> dict[str, object]:
    return {
        "logical_records": summary.logical_records,
        "parsed_point_rows": summary.parsed_point_rows,
        "output_rows": summary.output_rows,
        "quarantined_rows": summary.quarantined_rows,
        "upper_boundary_rows": summary.upper_boundary_rows,
        "filtered_selection_rows": summary.filtered_selection_rows,
        "filtered_instrument_rows": summary.filtered_instrument_rows,
        "measurements": dict(sorted(summary.measurements.items())),
        "value_types": dict(sorted(summary.value_types.items())),
    }


def _datetime_ns(value: datetime) -> int:
    utc_value = value.astimezone(UTC)
    return int(utc_value.timestamp()) * 1_000_000_000 + utc_value.microsecond * 1_000
