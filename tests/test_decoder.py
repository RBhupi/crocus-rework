from io import BytesIO

import pytest

from crocus_raw.decoder import CachedLineProtocolDecoder, iter_buffered_records
from crocus_raw.instruments import InstrumentResolver
from crocus_raw.line_protocol import LineProtocolError, parse_record


@pytest.mark.parametrize(
    "record",
    [
        rb"weather\ station,node=42,sensor=vaisala\ wxt,vsn=W08E,zone=core quality=1i,value=2.5,enabled=true,count=3u 1735689600000000001",
        b'logs,node=42,sensor=test,vsn=W001 message="first\\\" line\nsecond, line" 1735689600000000002',
        rb"escaped,node=42,sensor=test,vsn=W001 field\ key=9i 1735689600000000003",
    ],
)
def test_cached_decoder_matches_reference_parser(record):
    decoded = CachedLineProtocolDecoder(InstrumentResolver()).decode(record)
    reference = parse_record(record)

    assert decoded.time_ns == reference[0].time_ns
    assert decoded.series.metadata.measurement == reference[0].measurement
    assert decoded.series.metadata.tags == {
        key: value for key, value in reference[0].tags.items() if key != "node"
    }
    assert [
        (field.name, field.value_type, field.value) for field in decoded.fields
    ] == [
        (point.field, point.parsed_value.value_type, point.parsed_value.value)
        for point in reference
    ]


def test_buffered_records_preserve_multiline_strings_and_comments():
    stream = BytesIO(
        b"# ignored\n"
        b'logs,sensor=test,vsn=W001 value="first\nsecond" 1735689600000000001\n'
    )

    [record] = list(iter_buffered_records(stream))

    assert b'"first\nsecond"' in record
    assert parse_record(record)[0].parsed_value.value == "first\nsecond"


def test_series_id_is_deterministic_and_excludes_node_and_field():
    decoder = CachedLineProtocolDecoder(InstrumentResolver())
    first = decoder.decode(
        b"wxt.env.temp,node=1,sensor=vaisala-wxt536,vsn=W08E value=1 1735689600000000001"
    )
    second = decoder.decode(
        b"wxt.env.temp,node=2,sensor=vaisala-wxt536,vsn=W08E quality=2i 1735689600000000002"
    )

    assert first.series.metadata.series_id == second.series.metadata.series_id
    assert "node" not in first.series.metadata.tags


def test_buffered_records_reject_unterminated_multiline_string():
    with pytest.raises(LineProtocolError, match="unterminated"):
        list(iter_buffered_records(BytesIO(b'logs,sensor=test value="open\n')))
