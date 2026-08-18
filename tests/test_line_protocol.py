from io import BytesIO

import pytest

from crocus_raw.line_protocol import LineProtocolError, iter_logical_records, parse_record


def test_parser_preserves_types_escapes_fields_and_multiline_strings():
    payload = (
        b"weather\\ station,node=n1,sensor=wxt\\ 536 "
        b"temperature=2.5,count=-3i,total=4u,ready=true,message=\"first line\nsecond \\\"line\\\"\" 123\n"
    )

    records = list(iter_logical_records(BytesIO(payload), chunk_size=7))
    points = parse_record(records[0], 1)

    assert len(records) == 1
    assert [point.field for point in points] == ["temperature", "count", "total", "ready", "message"]
    assert [point.parsed_value.value_type for point in points] == [
        "float64",
        "int64",
        "uint64",
        "bool",
        "string",
    ]
    assert points[-1].parsed_value.value == 'first line\nsecond "line"'
    assert points[0].measurement == "weather station"
    assert points[0].tags == {"node": "n1", "sensor": "wxt 536"}
    assert points[0].time_ns == 123


def test_parser_fails_on_unterminated_string():
    with pytest.raises(LineProtocolError, match="unterminated"):
        list(iter_logical_records(BytesIO(b'm value="broken\n')))


def test_parser_accepts_reference_wxt_record():
    record = (
        b"wxt.env.temp,host=000048b02d35a87e.ws-nxcore,missing=-9999.9,"
        b"node=000048b02d35a87e,plugin=registry.sagecontinuum.org/jrobrien/"
        b"waggle-wxt536:0.24.11.14,sensor=vaisala-wxt536,task=waggle-wxt536,"
        b"units=degree\\ Celsius,vsn=W08E,zone=core value=2.2 1735689600028420265"
    )

    [point] = parse_record(record)

    assert point.measurement == "wxt.env.temp"
    assert point.field == "value"
    assert point.parsed_value.value == 2.2
    assert point.tags["sensor"] == "vaisala-wxt536"
    assert point.tags["units"] == "degree Celsius"
    assert point.tags["missing"] == "-9999.9"
    assert point.time_ns == 1735689600028420265


@pytest.mark.parametrize("value", [f"{2**63}i", f"{2**64}u"])
def test_parser_rejects_integer_overflow(value):
    with pytest.raises(LineProtocolError, match="out of range"):
        parse_record(f"m value={value} 1".encode())
