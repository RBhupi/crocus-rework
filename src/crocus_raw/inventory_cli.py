from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

from crocus_raw.backup import load_backup_bucket
from crocus_raw.instruments import InstrumentResolver
from crocus_raw.inventory import InventoryConfig, inventory_backup, inventory_engine
from crocus_raw.runtime import validate_influxd


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="crocus-inventory",
        description="Build an instrument and variable catalog from TSM indexes without decoding values.",
    )
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--backup-dir", type=Path)
    source.add_argument("--engine-dir", type=Path)
    parser.add_argument("--bucket-id", required=True)
    parser.add_argument("--bucket-name", default="waggle")
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--work-dir", required=True, type=Path)
    parser.add_argument("--influxd", required=True, type=Path)
    parser.add_argument("--source-snapshot")
    parser.add_argument("--instrument-registry", type=Path)
    parser.add_argument("--resume", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    influxd_version = validate_influxd(arguments.influxd)
    resolver = (
        InstrumentResolver.from_json(arguments.instrument_registry)
        if arguments.instrument_registry
        else InstrumentResolver()
    )
    if arguments.backup_dir:
        backup = load_backup_bucket(arguments.backup_dir, arguments.bucket_id)
        config = InventoryConfig(
            output_dir=arguments.output,
            work_dir=arguments.work_dir,
            bucket_id=backup.bucket_id,
            bucket_name=backup.bucket_name,
            influxd=arguments.influxd,
            influxd_version=influxd_version,
            source_snapshot=backup.snapshot,
            source_fingerprint=backup.manifest_sha256,
            resume=arguments.resume,
        )
        manifest = inventory_backup(backup, config, resolver)
    else:
        if not arguments.source_snapshot:
            raise SystemExit("--source-snapshot is required with --engine-dir")
        fingerprint = hashlib.sha256(
            f"{arguments.engine_dir.resolve()}:{arguments.bucket_id}:{arguments.source_snapshot}".encode()
        ).hexdigest()
        config = InventoryConfig(
            output_dir=arguments.output,
            work_dir=arguments.work_dir,
            bucket_id=arguments.bucket_id,
            bucket_name=arguments.bucket_name,
            influxd=arguments.influxd,
            influxd_version=influxd_version,
            source_snapshot=arguments.source_snapshot,
            source_fingerprint=fingerprint,
            resume=arguments.resume,
        )
        manifest = inventory_engine(arguments.engine_dir, config, resolver)
    print(json.dumps(manifest, indent=2, sort_keys=True))
    return 0 if manifest["status"] == "complete" else 1


if __name__ == "__main__":
    raise SystemExit(main())
