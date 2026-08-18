from datetime import date
from io import BytesIO
import json

import pyarrow.dataset as ds

from crocus_raw.converter import convert_stream
from crocus_raw.instruments import InstrumentResolver
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
            rows_per_file=2,
            max_buffer_rows=3,
            on_existing=on_existing,
        )
    )


def test_conversion_routes_hours_preserves_schema_and_skips_compatible_partitions(tmp_path):
    payload = (
        f'wxt.env.temp,missing=-9999.9,node=n1,sensor=wxt,zone=core value=2.5 {DAY_START + 1}\n'
        f'wxt.env.humidity,node=n1,sensor=wxt,zone=core value=80 {DAY_START + 2}\n'
        f'sys.net.up,node=n2 value=1 {DAY_START + 3_600_000_000_000 - 1}\n'
        f'sys.net.up,node=n2 value=1 {DAY_START + 3_600_000_000_000}\n'
        f'log,node=n1,sensor=wxt,zone=core value="first\nsecond" {DAY_START + 3_600_000_000_001}\n'
        f'wxt.env.temp,node=n1,sensor=wxt,zone=core value=99 {NEXT_DAY}\n'
    ).encode()
    resolver = InstrumentResolver()

    summary = convert_stream(BytesIO(payload), date(2025, 1, 1), resolver, _writer(tmp_path, "run-1", resolver))

    assert summary["output_rows"] == 5
    assert summary["parsed_point_rows"] == 6
    assert summary["upper_boundary_rows"] == 1
    assert summary["run"]["conversion"]["upper_boundary_rows"] == 1
    parquet_files = sorted(tmp_path.glob("schema_version=1/instrument=*/date=2025-01-01/hour=*/*.parquet"))
    assert len(parquet_files) == 4
    table = ds.dataset([str(path) for path in parquet_files], format="parquet").to_table()
    assert table.num_rows == 5
    assert table.schema.field("time").type.unit == "ns"
    assert "-9999.9" in table.column("missing").to_pylist()
    assert sorted(table.column("value_type").to_pylist()) == [
        "float64",
        "float64",
        "float64",
        "float64",
        "string",
    ]
    assert "first\nsecond" in table.column("value_string").to_pylist()

    manifests = list(tmp_path.glob("schema_version=1/instrument=*/date=2025-01-01/hour=*/_manifest.json"))
    assert len(manifests) == 4
    assert all(json.loads(path.read_text())["status"] == "complete" for path in manifests)

    rerun = convert_stream(
        BytesIO(payload),
        date(2025, 1, 1),
        resolver,
        _writer(tmp_path, "run-2", resolver, on_existing="skip"),
    )
    assert rerun["run"]["completed_partition_count"] == 0
    assert rerun["run"]["skipped_existing_rows"] == 5
    assert len(list(tmp_path.glob("schema_version=1/instrument=*/date=2025-01-01/hour=*/*.parquet"))) == 4
