from __future__ import annotations

import argparse
import gzip
import json
import sys
import uuid
from contextlib import nullcontext
from datetime import UTC, date, datetime
from pathlib import Path

from crocus_raw.converter import convert_stream
from crocus_raw.instruments import InstrumentResolver
from crocus_raw.writer import HourlyDatasetWriter, WriterConfig


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="crocus-raw",
        description="Stream one UTC day of Influx line protocol into instrument/hour Parquet datasets.",
    )
    parser.add_argument("--date", required=True, type=date.fromisoformat)
    parser.add_argument("--input", default="-", help="Line protocol path, .gz path, or - for stdin")
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--source-snapshot", required=True)
    parser.add_argument("--bucket", default="waggle")
    parser.add_argument("--instrument-registry", type=Path)
    parser.add_argument("--instrument-allowlist", type=Path)
    parser.add_argument("--influxd-version")
    parser.add_argument("--require-registry", action="store_true")
    parser.add_argument("--rows-per-file", type=int, default=500_000)
    parser.add_argument("--max-buffer-rows", type=int, default=1_000_000)
    parser.add_argument("--on-existing", choices=("error", "skip"), default="error")
    parser.add_argument("--run-id")
    return parser


def main(argv: list[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    resolver = (
        InstrumentResolver.from_json(arguments.instrument_registry, require_registry=arguments.require_registry)
        if arguments.instrument_registry
        else InstrumentResolver(require_registry=arguments.require_registry)
    )
    if arguments.require_registry and not arguments.instrument_registry:
        raise SystemExit("--require-registry requires --instrument-registry")

    run_id = arguments.run_id or _new_run_id()
    writer = HourlyDatasetWriter(
        WriterConfig(
            output_root=arguments.output,
            run_id=run_id,
            source_snapshot=arguments.source_snapshot,
            bucket=arguments.bucket,
            registry_fingerprint=resolver.fingerprint,
            influxd_version=arguments.influxd_version,
            rows_per_file=arguments.rows_per_file,
            max_buffer_rows=arguments.max_buffer_rows,
            on_existing=arguments.on_existing,
        )
    )

    with _open_input(arguments.input) as stream:
        allowed_instruments = (
            _read_allowlist(arguments.instrument_allowlist) if arguments.instrument_allowlist else None
        )
        summary = convert_stream(
            stream,
            arguments.date,
            resolver,
            writer,
            allowed_instrument_ids=allowed_instruments,
        )
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


def _open_input(path: str):
    if path == "-":
        return nullcontext(sys.stdin.buffer)
    if path.endswith(".gz"):
        return gzip.open(path, "rb")
    return Path(path).open("rb")


def _new_run_id() -> str:
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    return f"{timestamp}-{uuid.uuid4().hex[:12]}"


def _read_allowlist(path: Path) -> set[str]:
    return {
        line.strip()
        for line in path.read_text().splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    }
