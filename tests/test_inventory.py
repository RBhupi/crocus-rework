import csv
import json
import sqlite3
import tarfile
from pathlib import Path

from crocus_raw.backup import load_backup_bucket
from crocus_raw.instruments import InstrumentResolver
from crocus_raw.inventory import (
    InventoryConfig,
    inventory_backup,
    parse_dump_tsm_index,
)


def _write_executable(path: Path, content: str) -> Path:
    path.write_text(content)
    path.chmod(0o755)
    return path


def _make_backup(tmp_path: Path, corrupt: bool = False) -> tuple[Path, str]:
    backup_dir = tmp_path / "backup"
    backup_dir.mkdir()
    bucket_id = "b3a4e89ad74c5acc"
    archive_name = "snapshot.10.tar.gz"
    archive_path = backup_dir / archive_name
    if corrupt:
        archive_path.write_bytes(b"not a tar archive")
    else:
        source = tmp_path / "one.tsm"
        source.write_bytes(b"TSM fixture bytes")
        with tarfile.open(archive_path, "w:gz") as archive:
            archive.add(source, arcname=f"{bucket_id}/autogen/10/one.tsm")
    manifest = {
        "buckets": [
            {
                "bucketID": "other",
                "bucketName": "other",
                "retentionPolicies": [],
            },
            {
                "bucketID": bucket_id,
                "bucketName": "waggle",
                "retentionPolicies": [
                    {
                        "shardGroups": [
                            {
                                "startTime": "2025-01-01T00:00:00Z",
                                "endTime": "2025-01-08T00:00:00Z",
                                "shards": [
                                    {
                                        "id": 10,
                                        "fileName": archive_name,
                                        "size": archive_path.stat().st_size,
                                    }
                                ],
                            }
                        ]
                    }
                ],
            },
        ]
    }
    (backup_dir / "snapshot.manifest").write_text(json.dumps(manifest))
    return backup_dir, bucket_id


def _fake_influxd(tmp_path: Path) -> Path:
    return _write_executable(
        tmp_path / "influxd",
        """#!/bin/sh
if [ "$1" = "version" ]; then
  echo "InfluxDB v2.7.11"
  exit 0
fi
cat <<'EOF'
Index:
  Pos\tMin Time\tMax Time\tOfs\tSize\tKey\tField
  1\t2025-01-01T00:00:00.000000001Z\t2025-01-01T00:01:00.000000001Z\t5\t10\twxt.env.temp,missing=-9999.9,node=n1,sensor=vaisala-wxt536,units=degree\\ Celsius,zone=core\tvalue
  2\t2025-01-01T00:01:00.000000002Z\t2025-01-01T00:02:00.000000002Z\t15\t10\twxt.env.temp,missing=-9999.9,node=n1,sensor=vaisala-wxt536,units=degree\\ Celsius,zone=core\tvalue
  3\t2025-01-01T00:00:00Z\t2025-01-01T00:02:00Z\t25\t10\twxt.env.humidity,node=n1,sensor=vaisala-wxt536,units=percent,zone=core\tvalue
EOF
""",
    )


def test_manifest_selects_requested_bucket(tmp_path):
    backup_dir, bucket_id = _make_backup(tmp_path)

    bucket = load_backup_bucket(backup_dir, bucket_id)

    assert bucket.bucket_name == "waggle"
    assert bucket.snapshot == "snapshot"
    assert [shard.shard_id for shard in bucket.shards] == [10]


def test_dump_parser_handles_escapes_and_duplicate_blocks():
    lines = [
        "  1\t2025-01-01T00:00:00.1Z\t2025-01-01T00:00:01.000000002Z\t5\t10\t"
        "weather\\,station,node=n1,sensor=wxt\\ 536\tvalue\\ field\n"
    ]

    [entry] = list(parse_dump_tsm_index(lines))

    assert entry.measurement == "weather,station"
    assert entry.tags == {"node": "n1", "sensor": "wxt 536"}
    assert entry.field == "value field"
    assert entry.minimum_time_ns == 1_735_689_600_100_000_000
    assert entry.maximum_time_ns == 1_735_689_601_000_000_002


def test_dump_parser_rejects_malformed_index_row():
    lines = [
        "  1\t2025-01-01T00:00:00Z\tmissing columns\n",
    ]

    try:
        list(parse_dump_tsm_index(lines))
    except ValueError as error:
        assert "malformed" in str(error)
    else:
        raise AssertionError("malformed row was accepted")


def test_backup_inventory_writes_catalog_and_resumes(tmp_path):
    backup_dir, bucket_id = _make_backup(tmp_path)
    bucket = load_backup_bucket(backup_dir, bucket_id)
    influxd = _fake_influxd(tmp_path)
    output = tmp_path / "catalog"
    config = InventoryConfig(
        output_dir=output,
        work_dir=tmp_path / "work",
        bucket_id=bucket_id,
        bucket_name="waggle",
        influxd=influxd,
        influxd_version="2.7.11",
        source_snapshot=bucket.snapshot,
        source_fingerprint=bucket.manifest_sha256,
    )

    manifest = inventory_backup(bucket, config, InstrumentResolver())

    assert manifest["status"] == "complete"
    assert manifest["counts"] == {
        "instruments": 1,
        "variables": 2,
        "measurements": 2,
        "raw_series": 2,
    }
    assert (output / "wxt_measurements.txt").read_text().splitlines() == [
        "wxt.env.humidity",
        "wxt.env.temp",
    ]
    assert len((output / "wxt_instruments.txt").read_text().splitlines()) == 1
    with (output / "instrument_variables.csv").open() as stream:
        variables = list(csv.DictReader(stream))
    assert {row["measurement"] for row in variables} == {"wxt.env.temp", "wxt.env.humidity"}
    assert next(row for row in variables if row["measurement"] == "wxt.env.temp")["index_entries"] == "2"

    resumed = inventory_backup(
        bucket,
        InventoryConfig(**{**config.__dict__, "resume": True}),
        InstrumentResolver(),
    )
    assert resumed["counts"] == manifest["counts"]
    with sqlite3.connect(output / "inventory.sqlite") as connection:
        assert connection.execute("SELECT index_entries FROM shards").fetchone()[0] == 3


def test_corrupt_archive_is_reported_without_catalog_crash(tmp_path):
    backup_dir, bucket_id = _make_backup(tmp_path, corrupt=True)
    bucket = load_backup_bucket(backup_dir, bucket_id)
    output = tmp_path / "catalog"
    config = InventoryConfig(
        output_dir=output,
        work_dir=tmp_path / "work",
        bucket_id=bucket_id,
        bucket_name="waggle",
        influxd=_fake_influxd(tmp_path),
        influxd_version="2.7.11",
        source_snapshot=bucket.snapshot,
        source_fingerprint=bucket.manifest_sha256,
    )

    manifest = inventory_backup(bucket, config, InstrumentResolver())

    assert manifest["status"] == "incomplete"
    with (output / "inventory_errors.csv").open() as stream:
        errors = list(csv.DictReader(stream))
    assert len(errors) == 1
    assert errors[0]["source"] == "snapshot.10.tar.gz"
