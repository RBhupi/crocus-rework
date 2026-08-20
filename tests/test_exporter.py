import json
import tarfile
from dataclasses import replace
from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow.dataset as ds
import pytest

from crocus_raw.backup import BackupBucket, BackupShard
from crocus_raw.exporter import (
    ExportConfig,
    build_export_command,
    export_backup_range,
    export_engine_range,
    stage_backup_shard,
)
from crocus_raw.instruments import InstrumentResolver
from crocus_raw.selection import Selection


DAY_START = 1_735_689_600_000_000_000
NEXT_DAY = 1_735_776_000_000_000_000
DAY_AFTER_NEXT = 1_735_862_400_000_000_000


def _write_exporter(path: Path, fail: bool = False) -> Path:
    path.write_text(
        f"""#!/bin/sh
printf '%s\\n' 'wxt.env.temp,node=n1,sensor=vaisala-wxt536,vsn=W001,zone=core quality=1i,value=2.5 {DAY_START + 1}'
printf '%s\\n' 'wxt.env.temp,node=n2,sensor=other,vsn=W002,zone=core value=3.5 {DAY_START + 2}'
printf '%s\\n' 'wxt.env.temp,node=n1,sensor=vaisala-wxt536,vsn=W001,zone=core value=9.5 {NEXT_DAY}'
exit {2 if fail else 0}
"""
    )
    path.chmod(0o755)
    return path


def _config(tmp_path: Path, influxd: Path) -> ExportConfig:
    resolver = InstrumentResolver()
    return ExportConfig(
        influxd=influxd,
        influxd_version="2.7.11",
        bucket_id="b3a4e89ad74c5acc",
        bucket_name="waggle",
        output_dir=tmp_path / "output",
        source_snapshot="snapshot",
        selection=Selection.from_document(
            {
                "selection_version": 1,
                "selectors": [
                    {
                        "measurement": "wxt.env.temp",
                        "fields": ["value"],
                        "tags": {"sensor": ["vaisala-*"], "vsn": ["W001"]},
                    },
                    {"measurement": "wxt.env.humidity"},
                ],
            }
        ),
        allowed_instruments=None,
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


def test_direct_export_filters_selection_and_midnight(tmp_path):
    config = _config(tmp_path, _write_exporter(tmp_path / "influxd"))
    legacy = tmp_path / "output/schema_version=1/sentinel"
    legacy.parent.mkdir(parents=True)
    legacy.write_text("legacy")

    manifest = export_engine_range(
        tmp_path / "engine",
        date(2025, 1, 1),
        date(2025, 1, 2),
        config,
    )

    assert manifest["status"] == "complete"
    [day] = manifest["days"]
    assert day["parsed_point_rows"] == 4
    assert day["output_rows"] == 1
    assert day["filtered_selection_rows"] == 2
    assert day["filtered_instrument_rows"] == 0
    assert day["upper_boundary_rows"] == 1
    paths = list(
        (tmp_path / "output").glob(
            "facts/sensor=vaisala-wxt536/vsn=W001/instrument=*/date=2025-01-01/*.parquet"
        )
    )
    table = ds.dataset([str(path) for path in paths], format="parquet").to_table()
    assert table.num_rows == 1
    assert table.schema.metadata[b"crocus.influxd_version"] == b"2.7.11"
    assert table.schema.metadata[b"crocus.registry_fingerprint"] == config.resolver.fingerprint.encode()
    assert table.schema.metadata[b"crocus.selection_fingerprint"] == config.selection.fingerprint.encode()
    assert legacy.read_text() == "legacy"
    assert (tmp_path / "output/_days/date=2025-01-01.json").is_file()
    assert (tmp_path / "output/_catalog/selected_sensors.csv").is_file()
    assert (tmp_path / "output/_catalog/selected_instruments.csv").is_file()
    assert (tmp_path / "output/_catalog/selected_variables.csv").is_file()
    assert (tmp_path / "output/_catalog/selected_series.csv").is_file()
    assert (tmp_path / "output/_catalog/metadata_conflicts.csv").is_file()
    assert not list((tmp_path / "output/_staging").glob("*"))
    assert "wxt.env.temp" in (
        tmp_path / "output/_catalog/selected_variables.csv"
    ).read_text()
    hive_table = ds.dataset(
        tmp_path / "output/facts",
        format="parquet",
        partitioning="hive",
        ignore_prefixes=[".", "_"],
    ).to_table(filter=ds.field("vsn") == "W001")
    assert hive_table.num_rows == 1


def test_failed_export_does_not_publish_partitions(tmp_path):
    config = replace(
        _config(tmp_path, _write_exporter(tmp_path / "influxd", fail=True)),
        rows_per_file=1,
    )

    manifest = export_engine_range(
        tmp_path / "engine",
        date(2025, 1, 1),
        date(2025, 1, 2),
        config,
    )

    assert manifest["status"] == "incomplete"
    assert manifest["completed_days"] == 0
    assert manifest["errors"]
    assert not list((tmp_path / "output/facts").glob("sensor=*"))
    assert not (tmp_path / "output/_days/date=2025-01-01.json").exists()
    assert not list((tmp_path / "output/_staging").glob("*"))


def test_completed_empty_day_is_not_exported_again(tmp_path):
    influxd = _write_exporter(tmp_path / "influxd")
    selection = Selection.from_document(
        {
            "selection_version": 1,
            "selectors": [
                {"measurement": "wxt.env.temp", "tags": {"site": ["DOES-NOT-EXIST"]}}
            ],
        }
    )
    config = replace(_config(tmp_path, influxd), selection=selection)

    first = export_engine_range(
        tmp_path / "engine",
        date(2025, 1, 1),
        date(2025, 1, 2),
        config,
    )
    influxd.write_text("#!/bin/sh\nexit 99\n")
    second = export_engine_range(
        tmp_path / "engine",
        date(2025, 1, 1),
        date(2025, 1, 2),
        config,
    )

    assert first["status"] == "complete"
    assert first["days"][0]["output_rows"] == 0
    assert second["status"] == "complete"
    assert second["days"][0]["resumed"] is True
    assert not second["errors"]


def test_existing_dataset_rejects_changed_selection(tmp_path):
    config = _config(tmp_path, _write_exporter(tmp_path / "influxd"))
    export_engine_range(
        tmp_path / "engine",
        date(2025, 1, 1),
        date(2025, 1, 2),
        config,
    )
    changed = replace(
        config,
        selection=Selection.from_document(
            {
                "selection_version": 1,
                "selectors": [{"measurement": "wxt.env.pressure"}],
            }
        ),
    )

    with pytest.raises(ValueError, match="existing dataset is incompatible"):
        export_engine_range(
            tmp_path / "engine",
            date(2025, 1, 2),
            date(2025, 1, 3),
            changed,
        )


def test_export_reports_metadata_conflicts_for_same_sensor_variable(tmp_path):
    influxd = tmp_path / "influxd"
    influxd.write_text(
        f"""#!/bin/sh
printf '%s\\n' 'wxt.env.temp,sensor=vaisala-wxt536,units=celsius,vsn=W001 value=2.5 {DAY_START + 1}'
printf '%s\\n' 'wxt.env.temp,sensor=vaisala-wxt536,units=fahrenheit,vsn=W002 value=3.5 {DAY_START + 2}'
"""
    )
    influxd.chmod(0o755)
    config = replace(
        _config(tmp_path, influxd),
        selection=Selection.from_document(
            {
                "selection_version": 1,
                "selectors": [
                    {
                        "measurement": "wxt.env.temp",
                        "tags": {"sensor": ["vaisala-wxt536"]},
                    }
                ],
            }
        ),
    )

    manifest = export_engine_range(
        tmp_path / "engine",
        date(2025, 1, 1),
        date(2025, 1, 2),
        config,
    )

    assert manifest["catalog"]["metadata_conflict_count"] == 1
    assert manifest["requires_review"] is True
    report = (tmp_path / "output/_catalog/metadata_conflicts.csv").read_text()
    assert "vaisala-wxt536::wxt.env.temp::value" in report
    assert "celsius" in report
    assert "fahrenheit" in report


def test_engine_mode_rejects_measurement_discovery(tmp_path):
    selection = Selection.from_document(
        {
            "selection_version": 2,
            "selectors": [
                {
                    "measurement_glob": "*",
                    "tags": {"sensor": ["vaisala-wxt536"]},
                }
            ],
        }
    )
    config = replace(
        _config(tmp_path, _write_exporter(tmp_path / "influxd")),
        selection=selection,
    )

    with pytest.raises(ValueError, match="require --backup-dir"):
        export_engine_range(
            tmp_path / "engine",
            date(2025, 1, 1),
            date(2025, 1, 2),
            config,
        )


def test_completed_backup_day_does_not_restage_archive(tmp_path):
    source = tmp_path / "one.tsm"
    source.write_bytes(b"fixture")
    archive_path = tmp_path / "shard.tar.gz"
    with tarfile.open(archive_path, "w:gz") as archive:
        archive.add(source, arcname="b3a4e89ad74c5acc/autogen/10/one.tsm")
    shard = BackupShard(
        shard_id=10,
        archive=archive_path,
        start_time=datetime(2025, 1, 1, tzinfo=UTC),
        end_time=datetime(2025, 1, 2, tzinfo=UTC),
        compressed_size=archive_path.stat().st_size,
    )
    backup = BackupBucket(
        bucket_id="b3a4e89ad74c5acc",
        bucket_name="waggle",
        manifest_path=tmp_path / "snapshot.manifest",
        manifest_sha256="fingerprint",
        snapshot="snapshot",
        shards=(shard,),
    )
    config = _config(tmp_path, _write_exporter(tmp_path / "influxd"))

    first = export_backup_range(
        backup,
        date(2025, 1, 1),
        date(2025, 1, 2),
        tmp_path / "work",
        config,
    )
    archive_path.unlink()
    second = export_backup_range(
        backup,
        date(2025, 1, 1),
        date(2025, 1, 2),
        tmp_path / "work",
        config,
    )

    assert first["status"] == "complete"
    assert second["status"] == "complete"
    assert second["days"][0]["resumed"] is True


def test_backup_discovers_sensor_measurements_and_exports_shard_once(tmp_path):
    command_log = tmp_path / "commands.log"
    influxd = tmp_path / "influxd"
    influxd.write_text(
        f"""#!/bin/sh
printf '%s\\n' "$*" >> '{command_log}'
if [ "$2" = "dump-tsm" ]; then
cat <<'EOF'
Index:
  Pos\tMin Time\tMax Time\tOfs\tSize\tKey\tField
  1\t2025-01-01T00:00:00Z\t2025-01-02T00:00:00Z\t5\t10\twxt.env.temp,node=n1,sensor=vaisala-wxt536,vsn=W001\tvalue
  2\t2025-01-01T00:00:00Z\t2025-01-02T00:00:00Z\t15\t10\twxt.env.humidity,node=n1,sensor=vaisala-wxt536,vsn=W001\tvalue
  3\t2025-01-01T00:00:00Z\t2025-01-02T00:00:00Z\t25\t10\twxt.env.pressure,node=n2,sensor=other,vsn=W002\tvalue
EOF
exit 0
fi
printf '%s\\n' 'wxt.env.temp,node=n1,sensor=vaisala-wxt536,vsn=W001 value=2.5 {DAY_START + 1}'
printf '%s\\n' 'wxt.env.humidity,node=n1,sensor=vaisala-wxt536,vsn=W001 value=80 {NEXT_DAY + 1}'
printf '%s\\n' 'wxt.env.temp,node=n2,sensor=other,vsn=W002 value=9.5 {NEXT_DAY + 2}'
printf '%s\\n' 'wxt.env.temp,node=n1,sensor=vaisala-wxt536,vsn=W001 value=99 {DAY_AFTER_NEXT}'
"""
    )
    influxd.chmod(0o755)
    source = tmp_path / "one.tsm"
    source.write_bytes(b"fixture")
    archive_path = tmp_path / "shard.tar.gz"
    with tarfile.open(archive_path, "w:gz") as archive:
        archive.add(source, arcname="b3a4e89ad74c5acc/autogen/10/one.tsm")
    shard = BackupShard(
        shard_id=10,
        archive=archive_path,
        start_time=datetime(2025, 1, 1, tzinfo=UTC),
        end_time=datetime(2025, 1, 3, tzinfo=UTC),
        compressed_size=archive_path.stat().st_size,
    )
    backup = BackupBucket(
        bucket_id="b3a4e89ad74c5acc",
        bucket_name="waggle",
        manifest_path=tmp_path / "snapshot.manifest",
        manifest_sha256="fingerprint",
        snapshot="snapshot",
        shards=(shard,),
    )
    selection = Selection.from_document(
        {
            "selection_version": 2,
            "selectors": [
                {
                    "measurement_glob": "wxt.*",
                    "tags": {"sensor": ["vaisala-wxt536"]},
                }
            ],
        }
    )
    config = replace(_config(tmp_path, influxd), selection=selection)

    first = export_backup_range(
        backup,
        date(2025, 1, 1),
        date(2025, 1, 3),
        tmp_path / "work",
        config,
    )

    assert first["status"] == "complete"
    assert [day["output_rows"] for day in first["days"]] == [1, 1]
    assert all(day["resumed"] is False for day in first["days"])
    commands = command_log.read_text().splitlines()
    export_commands = [command for command in commands if "inspect export-lp" in command]
    assert len(export_commands) == 1
    assert export_commands[0].count("--measurement") == 2
    assert "wxt.env.humidity" in export_commands[0]
    assert "wxt.env.temp" in export_commands[0]
    assert "wxt.env.pressure" not in export_commands[0]
    shard_markers = list((tmp_path / "output/_shards").glob("*.json"))
    assert len(shard_markers) == 1
    shard_document = json.loads(shard_markers[0].read_text())
    assert shard_document["measurement_count"] == 2
    assert shard_document["index_entries_scanned"] == 3
    assert list(
        (tmp_path / "output").glob(
            "facts/sensor=vaisala-wxt536/vsn=W001/instrument=*/date=2025-01-01/*.parquet"
        )
    )
    assert list(
        (tmp_path / "output").glob(
            "facts/sensor=vaisala-wxt536/vsn=W001/instrument=*/date=2025-01-02/*.parquet"
        )
    )

    archive_path.unlink()
    second = export_backup_range(
        backup,
        date(2025, 1, 1),
        date(2025, 1, 3),
        tmp_path / "work",
        config,
    )
    assert second["status"] == "complete"
    assert all(day["resumed"] is True for day in second["days"])
    assert command_log.read_text().splitlines() == commands
