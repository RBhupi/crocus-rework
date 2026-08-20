from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import BinaryIO, Iterator

from crocus_raw.instruments import InstrumentResolver
from crocus_raw.line_protocol import LineProtocolError, parse_series_key, unescape_identifier
from crocus_raw.model import ScalarValue, SeriesMetadata, ValueType


@dataclass(frozen=True)
class DecodedSeries:
    metadata: SeriesMetadata
    selection_tags: dict[str, str]


@dataclass(frozen=True)
class DecodedField:
    name: str
    value_type: ValueType
    value: ScalarValue


@dataclass(frozen=True)
class DecodedRecord:
    series: DecodedSeries
    time_ns: int
    fields: tuple[DecodedField, ...]


class CachedLineProtocolDecoder:
    def __init__(self, resolver: InstrumentResolver):
        self.resolver = resolver
        self.series_cache: dict[bytes, DecodedSeries] = {}
        self.field_cache: dict[bytes, str] = {}

    def records(self, stream: BinaryIO) -> Iterator[DecodedRecord]:
        for record_number, record in enumerate(iter_buffered_records(stream), start=1):
            yield self.decode(record, record_number)

    def decode(self, record: bytes, record_number: int | None = None) -> DecodedRecord:
        label = f"record {record_number}" if record_number is not None else "record"
        series_end = _find_series_end(record)
        if series_end < 1:
            raise LineProtocolError(f"{label}: missing field set")
        raw_series = record[:series_end]
        series = self.series_cache.get(raw_series)
        if series is None:
            series = self._decode_series(raw_series, label)
            self.series_cache[raw_series] = series

        fields_and_time = record[series_end + 1 :]
        timestamp_separator = fields_and_time.rfind(b" ")
        if timestamp_separator < 1:
            raise LineProtocolError(f"{label}: missing nanosecond timestamp")
        try:
            time_ns = int(fields_and_time[timestamp_separator + 1 :])
        except ValueError as error:
            raise LineProtocolError(f"{label}: invalid timestamp") from error
        fields = _decode_fields(
            fields_and_time[:timestamp_separator], label, self.field_cache
        )
        return DecodedRecord(series=series, time_ns=time_ns, fields=fields)

    def _decode_series(self, raw_series: bytes, label: str) -> DecodedSeries:
        try:
            series_text = raw_series.decode("utf-8")
        except UnicodeDecodeError as error:
            raise LineProtocolError(f"{label}: invalid UTF-8 series") from error
        measurement, raw_tags = parse_series_key(series_text, label)
        retained_tags = {key: value for key, value in raw_tags.items() if key != "node"}
        vsn = retained_tags.get("vsn") or None
        sensor = retained_tags.get("sensor") or None
        identity = None
        if vsn is not None and sensor is not None:
            identity = self.resolver.resolve_tags(measurement, retained_tags)
        canonical = json.dumps(
            {"measurement": measurement, "tags": dict(sorted(retained_tags.items()))},
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        metadata = SeriesMetadata(
            series_id=hashlib.sha256(canonical).digest()[:16],
            measurement=measurement,
            tags=retained_tags,
            vsn=vsn,
            sensor=sensor,
            instrument_id=identity.instrument_id if identity else None,
            identity_source=identity.identity_source if identity else None,
        )
        return DecodedSeries(metadata=metadata, selection_tags=raw_tags)


def iter_buffered_records(stream: BinaryIO) -> Iterator[bytes]:
    pending = bytearray()
    in_string = False
    escaped = False
    for line in stream:
        continuation = bool(pending)
        line = line.removesuffix(b"\n")
        if line.endswith(b"\r"):
            line = line[:-1]
        if pending:
            pending.extend(b"\n")
            pending.extend(line)
        elif b'"' not in line:
            if line and not line.startswith(b"#"):
                yield line
            continue
        else:
            pending.extend(line)

        in_field_section = continuation
        for byte in line:
            if escaped:
                escaped = False
                continue
            if byte == 92:
                escaped = True
                continue
            if byte == 32 and not in_field_section:
                in_field_section = True
                continue
            if byte == 34 and in_field_section:
                in_string = not in_string
        if not in_string:
            if pending and not pending.startswith(b"#"):
                yield bytes(pending)
            pending.clear()
            escaped = False
    if in_string:
        raise LineProtocolError("unterminated quoted string at end of input")
    if pending and not pending.startswith(b"#"):
        yield bytes(pending)


def _find_series_end(record: bytes) -> int:
    position = record.find(b" ")
    while position >= 0:
        backslashes = 0
        cursor = position - 1
        while cursor >= 0 and record[cursor] == 92:
            backslashes += 1
            cursor -= 1
        if backslashes % 2 == 0:
            return position
        position = record.find(b" ", position + 1)
    return -1


def _decode_fields(
    field_text: bytes, label: str, field_cache: dict[bytes, str]
) -> tuple[DecodedField, ...]:
    raw_fields = _split_fields(field_text)
    fields: list[DecodedField] = []
    for raw_field in raw_fields:
        separator = _find_unescaped_equals(raw_field)
        if separator < 1:
            raise LineProtocolError(f"{label}: invalid field")
        raw_name = raw_field[:separator]
        name = field_cache.get(raw_name)
        if name is None:
            try:
                name = unescape_identifier(raw_name.decode("utf-8"))
            except UnicodeDecodeError as error:
                raise LineProtocolError(f"{label}: invalid UTF-8 field") from error
            field_cache[raw_name] = name
        value_type, value = _decode_value(raw_field[separator + 1 :], label)
        fields.append(DecodedField(name=name, value_type=value_type, value=value))
    if not fields:
        raise LineProtocolError(f"{label}: empty field set")
    return tuple(fields)


def _decode_value(raw: bytes, label: str) -> tuple[ValueType, ScalarValue]:
    if len(raw) >= 2 and raw[:1] == b'"' and raw[-1:] == b'"':
        try:
            text = raw[1:-1].decode("utf-8")
        except UnicodeDecodeError as error:
            raise LineProtocolError(f"{label}: invalid UTF-8 string") from error
        return "string", _unescape_string(text)
    if raw == b"true":
        return "bool", True
    if raw == b"false":
        return "bool", False
    if raw.endswith(b"i"):
        try:
            value = int(raw[:-1])
        except ValueError as error:
            raise LineProtocolError(f"{label}: invalid signed integer") from error
        if not -(2**63) <= value < 2**63:
            raise LineProtocolError(f"{label}: signed integer out of range")
        return "int64", value
    if raw.endswith(b"u"):
        try:
            value = int(raw[:-1])
        except ValueError as error:
            raise LineProtocolError(f"{label}: invalid unsigned integer") from error
        if not 0 <= value < 2**64:
            raise LineProtocolError(f"{label}: unsigned integer out of range")
        return "uint64", value
    try:
        return "float64", float(raw)
    except ValueError as error:
        raise LineProtocolError(f"{label}: invalid field value") from error


def _split_fields(raw: bytes) -> tuple[bytes, ...]:
    if b"," not in raw:
        return (raw,)
    parts: list[bytes] = []
    start = 0
    escaped = False
    in_string = False
    for index, byte in enumerate(raw):
        if escaped:
            escaped = False
        elif byte == 92:
            escaped = True
        elif byte == 34:
            in_string = not in_string
        elif byte == 44 and not in_string:
            parts.append(raw[start:index])
            start = index + 1
    if in_string:
        raise LineProtocolError("unterminated quoted string")
    parts.append(raw[start:])
    return tuple(parts)


def _find_unescaped_equals(raw: bytes) -> int:
    position = raw.find(b"=")
    while position >= 0:
        if position == 0 or raw[position - 1] != 92:
            return position
        position = raw.find(b"=", position + 1)
    return -1


def _unescape_string(text: str) -> str:
    result: list[str] = []
    escaped = False
    for character in text:
        if escaped:
            if character in {'"', "\\"}:
                result.append(character)
            else:
                result.extend(("\\", character))
            escaped = False
        elif character == "\\":
            escaped = True
        else:
            result.append(character)
    if escaped:
        result.append("\\")
    return "".join(result)
