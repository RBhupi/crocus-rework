from __future__ import annotations

from collections.abc import Iterator
from typing import BinaryIO

from crocus_raw.model import InfluxPoint, ParsedValue


class LineProtocolError(ValueError):
    pass


def iter_logical_records(stream: BinaryIO, chunk_size: int = 1024 * 1024) -> Iterator[bytes]:
    record = bytearray()
    in_field_section = False
    in_string = False
    escaped = False

    while chunk := stream.read(chunk_size):
        for byte in chunk:
            if byte == 10 and not in_string:
                if record and not record.startswith(b"#"):
                    yield bytes(record)
                record.clear()
                in_field_section = False
                escaped = False
                continue

            record.append(byte)

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

    if in_string:
        raise LineProtocolError("unterminated quoted string at end of input")
    if record and not record.startswith(b"#"):
        yield bytes(record)


def parse_record(record: bytes, record_number: int | None = None) -> list[InfluxPoint]:
    label = f"record {record_number}" if record_number is not None else "record"
    try:
        text = record.decode("utf-8")
    except UnicodeDecodeError as error:
        raise LineProtocolError(f"{label}: invalid UTF-8") from error

    series_end = _find_unescaped(text, " ")
    if series_end < 1:
        raise LineProtocolError(f"{label}: missing field set")

    series_text = text[:series_end]
    fields_and_time = text[series_end + 1 :]
    timestamp_separator = _find_last_unescaped_space(fields_and_time)
    if timestamp_separator < 1:
        raise LineProtocolError(f"{label}: missing nanosecond timestamp")

    field_text = fields_and_time[:timestamp_separator]
    timestamp_text = fields_and_time[timestamp_separator + 1 :]
    try:
        time_ns = int(timestamp_text)
    except ValueError as error:
        raise LineProtocolError(f"{label}: invalid timestamp {timestamp_text!r}") from error

    measurement, tags = parse_series_key(series_text, label)

    points: list[InfluxPoint] = []
    for one_field in _split_unescaped(field_text, ",", honor_quotes=True):
        separator = _find_unescaped(one_field, "=", honor_quotes=True)
        if separator < 1:
            raise LineProtocolError(f"{label}: invalid field {one_field!r}")
        field = _unescape_identifier(one_field[:separator])
        parsed_value = _parse_value(one_field[separator + 1 :], label)
        points.append(
            InfluxPoint(
                time_ns=time_ns,
                measurement=measurement,
                field=field,
                parsed_value=parsed_value,
                tags=tags,
            )
        )

    if not points:
        raise LineProtocolError(f"{label}: empty field set")
    return points


def parse_series_key(series_text: str, label: str = "series key") -> tuple[str, dict[str, str]]:
    series_parts = _split_unescaped(series_text, ",")
    measurement = _unescape_identifier(series_parts[0])
    if not measurement:
        raise LineProtocolError(f"{label}: empty measurement")

    tags: dict[str, str] = {}
    for tag_text in series_parts[1:]:
        separator = _find_unescaped(tag_text, "=")
        if separator < 1:
            raise LineProtocolError(f"{label}: invalid tag {tag_text!r}")
        key = _unescape_identifier(tag_text[:separator])
        value = _unescape_identifier(tag_text[separator + 1 :])
        if key in tags:
            raise LineProtocolError(f"{label}: duplicate tag {key!r}")
        tags[key] = value
    return measurement, tags


def unescape_identifier(text: str) -> str:
    return _unescape_identifier(text)


def _parse_value(text: str, label: str) -> ParsedValue:
    if len(text) >= 2 and text[0] == '"' and text[-1] == '"':
        return ParsedValue("string", _unescape_string(text[1:-1]))
    if text == "true":
        return ParsedValue("bool", True)
    if text == "false":
        return ParsedValue("bool", False)
    if text.endswith("i"):
        try:
            value = int(text[:-1])
        except ValueError as error:
            raise LineProtocolError(f"{label}: invalid signed integer {text!r}") from error
        if not -(2**63) <= value < 2**63:
            raise LineProtocolError(f"{label}: signed integer out of range {text!r}")
        return ParsedValue("int64", value)
    if text.endswith("u"):
        try:
            value = int(text[:-1])
        except ValueError as error:
            raise LineProtocolError(f"{label}: invalid unsigned integer {text!r}") from error
        if not 0 <= value < 2**64:
            raise LineProtocolError(f"{label}: unsigned integer out of range {text!r}")
        return ParsedValue("uint64", value)
    try:
        return ParsedValue("float64", float(text))
    except ValueError as error:
        raise LineProtocolError(f"{label}: invalid field value {text!r}") from error


def _split_unescaped(text: str, delimiter: str, honor_quotes: bool = False) -> list[str]:
    parts: list[str] = []
    start = 0
    escaped = False
    in_string = False
    for index, character in enumerate(text):
        if escaped:
            escaped = False
            continue
        if character == "\\":
            escaped = True
            continue
        if honor_quotes and character == '"':
            in_string = not in_string
            continue
        if character == delimiter and not in_string:
            parts.append(text[start:index])
            start = index + 1
    if in_string:
        raise LineProtocolError("unterminated quoted string")
    parts.append(text[start:])
    return parts


def _find_unescaped(text: str, delimiter: str, honor_quotes: bool = False) -> int:
    escaped = False
    in_string = False
    for index, character in enumerate(text):
        if escaped:
            escaped = False
            continue
        if character == "\\":
            escaped = True
            continue
        if honor_quotes and character == '"':
            in_string = not in_string
            continue
        if character == delimiter and not in_string:
            return index
    return -1


def _find_last_unescaped_space(text: str) -> int:
    escaped = False
    in_string = False
    last = -1
    for index, character in enumerate(text):
        if escaped:
            escaped = False
            continue
        if character == "\\":
            escaped = True
            continue
        if character == '"':
            in_string = not in_string
            continue
        if character == " " and not in_string:
            last = index
    return last


def _unescape_identifier(text: str) -> str:
    result: list[str] = []
    escaped = False
    for character in text:
        if escaped:
            result.append(character)
            escaped = False
        elif character == "\\":
            escaped = True
        else:
            result.append(character)
    if escaped:
        result.append("\\")
    return "".join(result)


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
