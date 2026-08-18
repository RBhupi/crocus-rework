import tarfile
from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow.dataset as ds

from crocus_raw.backup import BackupShard
from crocus_raw.exporter import (
    ExportConfig,
    build_export_command,
    export_engine_range,
    stage_backup_shard,
)
from crocus_raw.instruments import InstrumentResolver
from crocus_raw.model import InfluxPoint, ParsedValue


DAY_START = 1_735_689_600_000_000_000
NEXT_DAY = 1_735_776_000_000_000_000


def _write_exporter(path: Path, fail: bool = False) -> Path:
    path.write_text(
        f"""#!/bin/sh
printf '%s\\n' 'wxt.env.temp,node=n1,sensor=vaisala-wxt536,zone=core value=2.5 {DAY_START + 1}'
printf '%s\\n' 'wxt.env.temp,node=n2,sensor=other,zone=core value=3.5 {DAY_START + 2}'
printf '%s\\n' 'wxt.env.temp,node=n1,sensor=vaisala-wxt536,zone=core value=9.5 {NEXT_DAY}'
exit {2 if fail else 0}
"""
    )
    path.chmod(0o755)
    return path


def _config(tmp_path: Path, influxd: Path) -> ExportConfig:
    resolver = InstrumentResolver()
    point = InfluxPoint(
        DAY_START,
        "wxt.env.temp",
        "value",
        ParsedValue("float64", 0.0),
        {"node": "n1", "sensor": "vaisala-wxt536", "zone": "core"},
    )
    return ExportConfig(
        influxd=influxd,
        influxd_version="2.7.11",
        bucket_id="b3a4e89ad74c5acc",
        bucket_name="waggle",
        output_dir=tmp_path / "output",
        source_snapshot="snapshot",
        measurements=("wxt.env.temp", "wxt.env.humidity"),
        allowed_instruments=frozenset({resolver.resolve(point)}),
        resolver=resolver,
        rows_per_file=2,
        max_buffer_rows=3,
    )


def test_export_command_repeats_measurements_without_compression(tmp_path):
    command = build_export_command(
        tmp_path / "influxd",
        "bucket",
        tmp_path / "engine",
        date(2025, 1, 1),
        ["wxt.env.temp", "wxt.env.humidity", "wxt.env.temp"],
    )

    assert command.count("--measurement") == 2
    assert command[command.index("--measurement") + 1] == "wxt.env.humidity"
    assert "--compress" not in command
    assert command[-2:] == ["--end", "2025-01-02T00:00:00Z"]


def test_backup_staging_preserves_engine_retention_policy_directory(tmp_path):
    source = tmp_path / "one.tsm"
    source.write_bytes(b"fixture")
    archive_path = tmp_path / "shard.tar.gz"
    with tarfile.open(archive_path, "w:gz") as archive:
        archive.add(source, arcname="bucket/autogen/10/one.tsm")
    shard = BackupShard(
        shard_id=10,
        archive=archive_path,
        start_time=datetime(2025, 1, 1, tzinfo=UTC),
        end_time=datetime(2025, 1, 8, tzinfo=UTC),
        compressed_size=archive_path.stat().st_size,
    )

    engine_dir = stage_backup_shard(shard, "bucket", tmp_path / "stage")

    assert (engine_dir / "data/bucket/autogen/10/one.tsm").read_bytes() == b"fixture"


def test_direct_export_filters_instruments_and_midnight(tmp_path):
    config = _config(tmp_path, _write_exporter(tmp_path / "influxd"))

    manifest = export_engine_range(
        tmp_path / "engine",
        date(2025, 1, 1),
        date(2025, 1, 2),
        config,
    )

    assert manifest["status"] == "complete"
    [day] = manifest["days"]
    assert day["parsed_point_rows"] == 3
    assert day["output_rows"] == 1
    assert day["filtered_instrument_rows"] == 1
    assert day["upper_boundary_rows"] == 1
    paths = list((tmp_path / "output").glob("schema_version=1/instrument=*/date=2025-01-01/hour=00/*.parquet"))
    table = ds.dataset([str(path) for path in paths], format="parquet").to_table()
    assert table.num_rows == 1
    assert table.schema.metadata[b"crocus.influxd_version"] == b"2.7.11"


def test_failed_export_does_not_publish_partitions(tmp_path):
    config = _config(tmp_path, _write_exporter(tmp_path / "influxd", fail=True))

    manifest = export_engine_range(
        tmp_path / "engine",
        date(2025, 1, 1),
        date(2025, 1, 2),
        config,
    )

    assert manifest["status"] == "incomplete"
    assert manifest["completed_days"] == 0
    assert manifest["errors"]
    assert not list((tmp_path / "output").glob("schema_version=1/instrument=*"))
