from datetime import date
from io import BytesIO
import json

import pyarrow.dataset as ds
import pyarrow.parquet as pq

from crocus_raw.converter import convert_stream
from crocus_raw.instruments import InstrumentResolver
from crocus_raw.selection import Selection
from crocus_raw.writer import HourlyDatasetWriter, WriterConfig


DAY_START = 1_735_689_600_000_000_000
NEXT_DAY = 1_735_776_000_000_000_000


def _writer(tmp_path, run_id, resolver, on_existing="error"):
    return HourlyDatasetWriter(
        WriterConfig(
            output_root=tmp_path,
            run_id=run_id,
            source_snapshot="snapshot-1",
            bucket="waggle",
            registry_fingerprint=resolver.fingerprint,
            selection_fingerprint="selection-1",
            rows_per_file=2,
            max_buffer_rows=3,
            on_existing=on_existing,
        )
    )


def test_conversion_routes_daily_partitions_preserves_schema_and_skips_compatible_partitions(tmp_path):
    payload = (
        f'wxt.env.temp,missing=-9999.9,node=n1,sensor=wxt,vsn=W001,zone=core value=2.5 {DAY_START + 1}\n'
        f'wxt.env.humidity,node=n1,sensor=wxt,vsn=W001,zone=core value=80 {DAY_START + 2}\n'
        f'sys.net.up,node=n2,sensor=system,vsn=W002 value=1 {DAY_START + 3_600_000_000_000 - 1}\n'
        f'sys.net.up,node=n2,sensor=system,vsn=W002 value=1 {DAY_START + 3_600_000_000_000}\n'
        f'log,node=n1,sensor=wxt,vsn=W001,zone=core value="first\nsecond" {DAY_START + 3_600_000_000_001}\n'
        f'wxt.env.temp,node=n1,sensor=wxt,vsn=W001,zone=core value=99 {NEXT_DAY}\n'
    ).encode()
    resolver = InstrumentResolver()

    summary = convert_stream(BytesIO(payload), date(2025, 1, 1), resolver, _writer(tmp_path, "run-1", resolver))

    assert summary["output_rows"] == 5
    assert summary["parsed_point_rows"] == 6
    assert summary["upper_boundary_rows"] == 1
    assert summary["run"]["conversion"]["upper_boundary_rows"] == 1
    parquet_files = sorted(
        tmp_path.glob("facts/sensor=*/vsn=*/instrument=*/date=2025-01-01/*.parquet")
    )
    assert len(parquet_files) == 3
    table = ds.dataset([str(path) for path in parquet_files], format="parquet").to_table()
    assert table.num_rows == 5
    assert table.schema.field("time").type.unit == "ns"
    assert "node" not in table.schema.names
    assert sorted(table.column("value_type").to_pylist()) == [
        "float64",
        "float64",
        "float64",
        "float64",
        "string",
    ]
    assert "first\nsecond" in table.column("value_string").to_pylist()

    manifests = list(
        tmp_path.glob("facts/sensor=*/vsn=*/instrument=*/date=2025-01-01/_manifest.json")
    )
    assert len(manifests) == 2
    assert all(json.loads(path.read_text())["status"] == "complete" for path in manifests)

    rerun = convert_stream(
        BytesIO(payload),
        date(2025, 1, 1),
        resolver,
        _writer(tmp_path, "run-2", resolver, on_existing="skip"),
    )
    assert rerun["run"]["completed_partition_count"] == 0
    assert rerun["run"]["skipped_existing_rows"] == 5
    assert len(
        list(tmp_path.glob("facts/sensor=*/vsn=*/instrument=*/date=2025-01-01/*.parquet"))
    ) == 3


def test_conversion_url_encodes_vsn_partition(tmp_path):
    payload = (
        f'wxt.env.temp,node=n1,sensor=wxt,vsn=W01/West,zone=core value=2.5 {DAY_START + 1}\n'
    ).encode()
    resolver = InstrumentResolver()

    convert_stream(
        BytesIO(payload),
        date(2025, 1, 1),
        resolver,
        _writer(tmp_path, "vsn-run", resolver),
    )

    assert list(
        tmp_path.glob("facts/sensor=wxt/vsn=W01%2FWest/instrument=*/date=2025-01-01/*.parquet")
    )


def test_conversion_quarantines_missing_vsn_without_storing_node(tmp_path):
    payload = (
        f'wxt.env.temp,node=numeric-node,sensor=wxt value=2.5 {DAY_START + 1}\n'
    ).encode()
    resolver = InstrumentResolver()

    summary = convert_stream(
        BytesIO(payload),
        date(2025, 1, 1),
        resolver,
        _writer(tmp_path, "quarantine-run", resolver),
    )

    assert summary["output_rows"] == 0
    assert summary["quarantined_rows"] == 1
    [path] = list(tmp_path.glob("_quarantine/reason=missing-vsn/date=2025-01-01/run=*/part-*.parquet"))
    table = ds.dataset(path, format="parquet").to_table()
    assert "node" not in table.schema.names
    assert "node" not in dict(table.column("tags")[0].as_py())


def test_conversion_combines_sensor_types_with_shared_measurement(tmp_path):
    payload = (
        f'shared.env.temp,node=1,sensor=vaisala-wxt536,vsn=W08E value=2.5 {DAY_START + 1}\n'
        f'shared.env.temp,node=2,sensor=vaisala-aqt560,vsn=W08E value=3.5 {DAY_START + 2}\n'
        f'shared.env.temp,node=3,sensor=other,vsn=W08E value=4.5 {DAY_START + 3}\n'
    ).encode()
    selection = Selection.from_document(
        {
            "selection_version": 1,
            "selectors": [
                {
                    "measurement": "shared.env.temp",
                    "tags": {"sensor": ["vaisala-wxt536"]},
                },
                {
                    "measurement": "shared.env.temp",
                    "tags": {"sensor": ["vaisala-aqt560"]},
                },
            ],
        }
    )
    resolver = InstrumentResolver()

    summary = convert_stream(
        BytesIO(payload),
        date(2025, 1, 1),
        resolver,
        _writer(tmp_path, "combined-run", resolver),
        selection=selection,
    )

    assert summary["output_rows"] == 2
    assert summary["filtered_selection_rows"] == 1
    assert len(list(tmp_path.glob("facts/sensor=vaisala-wxt536/**/*.parquet"))) == 1
    assert len(list(tmp_path.glob("facts/sensor=vaisala-aqt560/**/*.parquet"))) == 1
    [series_path] = list(tmp_path.glob("_series/run=*/part-*.parquet"))
    series_table = pq.read_table(series_path)
    assert series_table.num_rows == 2
    assert all("node" not in dict(tags) for tags in series_table["tags"].to_pylist())
