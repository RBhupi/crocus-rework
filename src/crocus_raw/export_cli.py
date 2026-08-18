from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path

from crocus_raw.backup import load_backup_bucket
from crocus_raw.exporter import ExportConfig, export_backup_range, export_engine_range
from crocus_raw.instruments import InstrumentResolver
from crocus_raw.runtime import validate_influxd


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="crocus-export",
        description="Stream daily InfluxDB exports directly into instrument/hour Parquet.",
    )
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--backup-dir", type=Path)
    source.add_argument("--engine-dir", type=Path)
    parser.add_argument("--bucket-id", required=True)
    parser.add_argument("--bucket-name", default="waggle")
    parser.add_argument("--start-date", required=True, type=date.fromisoformat)
    parser.add_argument("--end-date", required=True, type=date.fromisoformat)
    parser.add_argument("--measurement-file", required=True, type=Path)
    parser.add_argument("--instrument-file", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--work-dir", type=Path)
    parser.add_argument("--influxd", required=True, type=Path)
    parser.add_argument("--source-snapshot")
    parser.add_argument("--instrument-registry", type=Path)
    parser.add_argument("--workers", type=int, choices=(1, 2), default=1)
    parser.add_argument("--rows-per-file", type=int, default=500_000)
    parser.add_argument("--max-buffer-rows", type=int, default=1_000_000)
    parser.add_argument("--on-existing", choices=("error", "skip"), default="skip")
    return parser


def main(argv: list[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    influxd_version = validate_influxd(arguments.influxd)
    measurements = tuple(sorted(_read_list(arguments.measurement_file)))
    instruments = frozenset(_read_list(arguments.instrument_file))
    if not measurements:
        raise SystemExit("measurement file is empty")
    if not instruments:
        raise SystemExit("instrument file is empty")
    resolver = (
        InstrumentResolver.from_json(arguments.instrument_registry)
        if arguments.instrument_registry
        else InstrumentResolver()
    )

    if arguments.backup_dir:
        if not arguments.work_dir:
            raise SystemExit("--work-dir is required with --backup-dir")
        backup = load_backup_bucket(arguments.backup_dir, arguments.bucket_id)
        source_snapshot = backup.snapshot
        bucket_name = backup.bucket_name
    else:
        if not arguments.source_snapshot:
            raise SystemExit("--source-snapshot is required with --engine-dir")
        source_snapshot = arguments.source_snapshot
        bucket_name = arguments.bucket_name

    config = ExportConfig(
        influxd=arguments.influxd,
        influxd_version=influxd_version,
        bucket_id=arguments.bucket_id,
        bucket_name=bucket_name,
        output_dir=arguments.output,
        source_snapshot=source_snapshot,
        measurements=measurements,
        allowed_instruments=instruments,
        resolver=resolver,
        workers=arguments.workers,
        rows_per_file=arguments.rows_per_file,
        max_buffer_rows=arguments.max_buffer_rows,
        on_existing=arguments.on_existing,
    )
    if arguments.backup_dir:
        manifest = export_backup_range(
            backup,
            arguments.start_date,
            arguments.end_date,
            arguments.work_dir,
            config,
        )
    else:
        manifest = export_engine_range(
            arguments.engine_dir,
            arguments.start_date,
            arguments.end_date,
            config,
        )
    print(json.dumps(manifest, indent=2, sort_keys=True))
    return 0 if manifest["status"] == "complete" else 1


def _read_list(path: Path) -> set[str]:
    return {
        line.strip()
        for line in path.read_text().splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    }


if __name__ == "__main__":
    raise SystemExit(main())
