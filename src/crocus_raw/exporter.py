from __future__ import annotations

import csv
import hashlib
import io
import json
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path, PurePosixPath
from typing import Iterable

import pyarrow.parquet as pq

from crocus_raw import __version__
from crocus_raw.backup import BackupBucket, BackupShard
from crocus_raw.converter import convert_range_stream, convert_stream
from crocus_raw.instruments import InstrumentResolver
from crocus_raw.inventory import inspect_tsm
from crocus_raw.runtime import describe_error, write_text_atomic
from crocus_raw.selection import Selection
from crocus_raw.writer import SCHEMA_VERSION, DailyDatasetWriter, WriterConfig


@dataclass(frozen=True)
class ExportConfig:
    influxd: Path
    influxd_version: str
    bucket_id: str
    bucket_name: str
    output_dir: Path
    source_snapshot: str
    selection: Selection
    allowed_instruments: frozenset[str] | None
    resolver: InstrumentResolver
    workers: int = 1
    rows_per_file: int = 500_000
    max_buffer_rows: int = 1_000_000
    on_existing: str = "skip"

    @property
    def measurements(self) -> tuple[str, ...]:
        return self.selection.measurements


def build_export_command(
    influxd: Path,
    bucket_id: str,
    engine_dir: Path,
    start_date: date,
    measurements: Iterable[str],
) -> list[str]:
    start_time = datetime(start_date.year, start_date.month, start_date.day, tzinfo=UTC)
    return build_export_range_command(
        influxd,
        bucket_id,
        engine_dir,
        start_time,
        start_time + timedelta(days=1),
        measurements,
    )


def build_export_range_command(
    influxd: Path,
    bucket_id: str,
    engine_dir: Path,
    start_time: datetime,
    end_time: datetime,
    measurements: Iterable[str],
) -> list[str]:
    command = [
        str(influxd),
        "inspect",
        "export-lp",
        "--bucket-id",
        bucket_id,
        "--engine-path",
        str(engine_dir),
    ]
    for measurement in sorted(set(measurements)):
        command.extend(("--measurement", measurement))
    command.extend(
        (
            "--output-path",
            "-",
            "--start",
            _format_influx_time(start_time),
            "--end",
            _format_influx_time(end_time),
        )
    )
    return command


def _format_influx_time(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def export_engine_range(
    engine_dir: Path,
    start_date: date,
    end_date: date,
    config: ExportConfig,
) -> dict[str, object]:
    if config.selection.requires_discovery:
        raise ValueError(
            "measurement_glob selections require --backup-dir; "
            "use exact measurements with --engine-dir"
        )
    dates = tuple(_dates(start_date, end_date))
    _prepare_dataset(config)
    completed, pending = _partition_completed_days(dates, config)
    run = _run_export_jobs(
        ((conversion_date, engine_dir) for conversion_date in pending),
        config,
        mode="engine",
        expected_days=len(pending),
        write_manifest=False,
    )
    return _write_export_manifest(
        config,
        "engine",
        len(dates),
        completed + run["days"],
        run["errors"],
    )


def export_backup_range(
    backup: BackupBucket,
    start_date: date,
    end_date: date,
    work_dir: Path,
    config: ExportConfig,
) -> dict[str, object]:
    requested_dates = tuple(_dates(start_date, end_date))
    range_start = datetime(start_date.year, start_date.month, start_date.day, tzinfo=UTC)
    range_end = datetime(end_date.year, end_date.month, end_date.day, tzinfo=UTC)
    _prepare_dataset(config)
    results: list[dict[str, object]] = []
    errors: list[dict[str, str]] = []
    work_dir.mkdir(parents=True, exist_ok=True)
    tasks: list[tuple[BackupShard, datetime, datetime]] = []
    covered_dates: set[date] = set()
    for shard in backup.shards:
        shard_start = max(range_start, shard.start_time.astimezone(UTC))
        shard_end = min(range_end, shard.end_time.astimezone(UTC))
        if shard_end <= shard_start:
            continue
        covered_dates.update(_datetime_dates(shard_start, shard_end))
        marker = _read_shard_marker(config, shard, shard_start, shard_end)
        if marker is not None:
            results.extend(_shard_marker_days(marker, resumed=True))
        else:
            tasks.append((shard, shard_start, shard_end))

    with ThreadPoolExecutor(max_workers=config.workers) as executor:
        futures = {
            executor.submit(
                _export_backup_shard,
                backup,
                shard,
                shard_start,
                shard_end,
                work_dir,
                config,
            ): shard
            for shard, shard_start, shard_end in tasks
        }
        for future in as_completed(futures):
            shard = futures[future]
            try:
                marker = future.result()
                results.extend(_shard_marker_days(marker, resumed=False))
                print(f"ok {shard.archive.name}", file=sys.stderr, flush=True)
            except Exception as error:
                errors.append({"source": shard.archive.name, "error": describe_error(error)})
                print(
                    f"error {shard.archive.name}: {describe_error(error)}",
                    file=sys.stderr,
                    flush=True,
                )

    missing_dates = sorted(set(requested_dates) - covered_dates)
    errors.extend(
        {"source": conversion_date.isoformat(), "error": "no backup shard covers this date"}
        for conversion_date in missing_dates
    )
    return _write_export_manifest(config, "backup", len(requested_dates), results, errors)


def _export_backup_shard(
    backup: BackupBucket,
    shard: BackupShard,
    start_time: datetime,
    end_time: datetime,
    work_dir: Path,
    config: ExportConfig,
) -> dict[str, object]:
    print(f"stage {shard.archive.name}", file=sys.stderr, flush=True)
    staging_root = Path(
        tempfile.mkdtemp(prefix=f"crocus-shard-{shard.shard_id}-", dir=work_dir)
    )
    started = time.monotonic()
    try:
        engine_dir = stage_backup_shard(shard, backup.bucket_id, staging_root)
        staged_at = time.monotonic()
        measurements, index_entries = discover_shard_measurements(engine_dir, config)
        discovered_at = time.monotonic()
        result = export_range(
            engine_dir,
            start_time,
            end_time,
            config,
            measurements,
            f"shard-{shard.shard_id}-{uuid.uuid4().hex[:10]}",
        )
        finished = time.monotonic()
        marker = {
            **_shard_identity(config, shard, start_time, end_time),
            "measurements": list(measurements),
            "measurement_count": len(measurements),
            "index_entries_scanned": index_entries,
            "conversion": result["conversion"],
            "days": result["days"],
            "run": result["run"],
            "timings_seconds": {
                "stage": staged_at - started,
                "discover": discovered_at - staged_at,
                "export_convert_write": finished - discovered_at,
                "total": finished - started,
            },
            "finished_at": datetime.now(UTC).isoformat(),
        }
        write_text_atomic(
            _shard_manifest_path(config, shard, start_time, end_time),
            json.dumps(marker, indent=2, sort_keys=True) + "\n",
        )
        return marker
    finally:
        shutil.rmtree(staging_root, ignore_errors=True)


def discover_shard_measurements(
    engine_dir: Path,
    config: ExportConfig,
) -> tuple[tuple[str, ...], int]:
    measurements = set(config.selection.measurements)
    if not config.selection.requires_discovery:
        return tuple(sorted(measurements)), 0
    index_entries = 0
    bucket_path = engine_dir / "data" / config.bucket_id
    for path in sorted(bucket_path.rglob("*.tsm")):
        for entry in inspect_tsm(config.influxd, path):
            index_entries += 1
            if config.selection.matches_parts(entry.measurement, entry.field, entry.tags):
                measurements.add(entry.measurement)
    return tuple(sorted(measurements)), index_entries


def stage_backup_shard(shard: BackupShard, bucket_id: str, staging_root: Path) -> Path:
    engine_dir = staging_root / "engine"
    file_count = 0
    with tarfile.open(shard.archive, mode="r|gz") as archive:
        for member in archive:
            member_path = PurePosixPath(member.name)
            if member_path.suffix != ".tsm":
                continue
            if not member.isfile() or ".." in member_path.parts or member_path.is_absolute():
                raise ValueError(f"unsafe TSM archive member: {member.name!r}")
            if not member_path.parts or member_path.parts[0] != bucket_id:
                continue
            source = archive.extractfile(member)
            if source is None:
                raise ValueError(f"could not read TSM member: {member.name!r}")
            target = engine_dir / "data" / Path(*member_path.parts)
            target.parent.mkdir(parents=True, exist_ok=True)
            if target.exists():
                raise ValueError(f"duplicate TSM filename in shard archive: {member_path.name}")
            with target.open("wb") as output:
                shutil.copyfileobj(source, output, length=1024 * 1024)
            file_count += 1
    if not file_count and shard.compressed_size > 32:
        raise ValueError(f"no TSM files found in {shard.archive}")
    return engine_dir


def export_day(
    engine_dir: Path,
    conversion_date: date,
    config: ExportConfig,
    orchestration_id: str,
) -> dict[str, object]:
    command = build_export_command(
        config.influxd,
        config.bucket_id,
        engine_dir,
        conversion_date,
        config.measurements,
    )
    run_id = f"{orchestration_id}-{conversion_date.isoformat()}-{uuid.uuid4().hex[:8]}"
    writer = DailyDatasetWriter(
        WriterConfig(
            output_root=config.output_dir,
            run_id=run_id,
            source_snapshot=config.source_snapshot,
            bucket=config.bucket_name,
            registry_fingerprint=config.resolver.fingerprint,
            selection_fingerprint=config.selection.fingerprint,
            influxd_version=config.influxd_version,
            rows_per_file=config.rows_per_file,
            max_buffer_rows=config.max_buffer_rows,
            on_existing=config.on_existing,
        )
    )
    with tempfile.TemporaryFile(mode="w+t") as error_stream:
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=error_stream,
        )
        assert process.stdout is not None
        try:
            conversion = convert_stream(
                process.stdout,
                conversion_date,
                config.resolver,
                writer,
                selection=config.selection,
                allowed_instrument_ids=config.allowed_instruments,
                finalize_writer=False,
            )
        except Exception:
            process.terminate()
            process.wait()
            writer.abort()
            raise
        finally:
            process.stdout.close()
        return_code = process.wait()
        if return_code:
            error_stream.seek(0)
            writer.abort()
            raise subprocess.CalledProcessError(
                return_code,
                command,
                stderr=error_stream.read(),
            )
    try:
        run_manifest = writer.finalize(conversion)
        day_manifest = _write_day_manifest(config, conversion_date, conversion, run_manifest)
    except Exception:
        writer.abort()
        raise
    return {
        **conversion,
        "run": run_manifest,
        "day_manifest": day_manifest,
        "command": command,
        "resumed": False,
    }


def export_range(
    engine_dir: Path,
    start_time: datetime,
    end_time: datetime,
    config: ExportConfig,
    measurements: tuple[str, ...],
    orchestration_id: str,
) -> dict[str, object]:
    command = build_export_range_command(
        config.influxd,
        config.bucket_id,
        engine_dir,
        start_time,
        end_time,
        measurements,
    )
    run_id = f"{orchestration_id}-{uuid.uuid4().hex[:8]}"
    writer = DailyDatasetWriter(
        WriterConfig(
            output_root=config.output_dir,
            run_id=run_id,
            source_snapshot=config.source_snapshot,
            bucket=config.bucket_name,
            registry_fingerprint=config.resolver.fingerprint,
            selection_fingerprint=config.selection.fingerprint,
            influxd_version=config.influxd_version,
            rows_per_file=config.rows_per_file,
            max_buffer_rows=config.max_buffer_rows,
            on_existing=config.on_existing,
        )
    )
    try:
        if measurements:
            conversion = _convert_export_process(
                command,
                start_time,
                end_time,
                config,
                writer,
            )
        else:
            conversion = convert_range_stream(
                io.BytesIO(),
                start_time,
                end_time,
                config.resolver,
                writer,
                selection=config.selection,
                allowed_instrument_ids=config.allowed_instruments,
                finalize_writer=False,
            )
        run_manifest = writer.finalize(conversion)
    except Exception:
        writer.abort()
        raise

    days: list[dict[str, object]] = []
    daily_summaries = conversion["dates"]
    for conversion_date in _datetime_dates(start_time, end_time):
        daily_conversion = daily_summaries.get(
            conversion_date.isoformat(),
            _empty_day_conversion(conversion_date),
        )
        day_manifest = _write_day_manifest(
            config,
            conversion_date,
            daily_conversion,
            run_manifest,
        )
        days.append(
            {
                **daily_conversion,
                "day_manifest": day_manifest,
                "resumed": False,
            }
        )
    return {
        "conversion": conversion,
        "days": days,
        "run": run_manifest,
        "command": command if measurements else None,
    }


def _convert_export_process(
    command: list[str],
    start_time: datetime,
    end_time: datetime,
    config: ExportConfig,
    writer: DailyDatasetWriter,
) -> dict[str, object]:
    with tempfile.TemporaryFile(mode="w+t") as error_stream:
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=error_stream)
        assert process.stdout is not None
        try:
            conversion = convert_range_stream(
                process.stdout,
                start_time,
                end_time,
                config.resolver,
                writer,
                selection=config.selection,
                allowed_instrument_ids=config.allowed_instruments,
                finalize_writer=False,
            )
        except Exception:
            process.terminate()
            process.wait()
            raise
        finally:
            process.stdout.close()
        return_code = process.wait()
        if return_code:
            error_stream.seek(0)
            raise subprocess.CalledProcessError(
                return_code,
                command,
                stderr=error_stream.read(),
            )
    return conversion


def _empty_day_conversion(conversion_date: date) -> dict[str, object]:
    return {
        "date": conversion_date.isoformat(),
        "logical_records": 0,
        "parsed_point_rows": 0,
        "output_rows": 0,
        "quarantined_rows": 0,
        "upper_boundary_rows": 0,
        "filtered_selection_rows": 0,
        "filtered_instrument_rows": 0,
        "measurements": {},
        "value_types": {},
    }


def _dataset_root(config: ExportConfig) -> Path:
    return config.output_dir


def _dataset_identity(config: ExportConfig) -> dict[str, object]:
    return {
        "status": "active",
        "storage_schema_version": SCHEMA_VERSION,
        "converter_version": __version__,
        "bucket_id": config.bucket_id,
        "bucket_name": config.bucket_name,
        "source_snapshot": config.source_snapshot,
        "influxd_version": config.influxd_version,
        "registry_fingerprint": config.resolver.fingerprint,
        "selection_fingerprint": config.selection.fingerprint,
    }


def _prepare_dataset(config: ExportConfig) -> None:
    root = _dataset_root(config)
    manifest_path = root / "_dataset.json"
    expected = _dataset_identity(config)
    if manifest_path.exists():
        document = json.loads(manifest_path.read_text())
        mismatches = {
            key: (document.get(key), value)
            for key, value in expected.items()
            if document.get(key) != value
        }
        if mismatches:
            raise ValueError(f"existing dataset is incompatible: {mismatches}")
    else:
        write_text_atomic(manifest_path, json.dumps(expected, indent=2, sort_keys=True) + "\n")

    selection_path = root / "_selection.json"
    selection_text = json.dumps(json.loads(config.selection.canonical_json), indent=2, sort_keys=True) + "\n"
    if selection_path.exists():
        if json.loads(selection_path.read_text()) != json.loads(selection_text):
            raise ValueError("existing dataset selection does not match selection fingerprint")
    else:
        write_text_atomic(selection_path, selection_text)


def _day_manifest_path(config: ExportConfig, conversion_date: date) -> Path:
    return _dataset_root(config) / "_days" / f"date={conversion_date.isoformat()}.json"


def _shard_manifest_path(
    config: ExportConfig,
    shard: BackupShard,
    start_time: datetime,
    end_time: datetime,
) -> Path:
    range_key = hashlib.sha256(
        f"{shard.archive.name}:{_format_influx_time(start_time)}:{_format_influx_time(end_time)}".encode()
    ).hexdigest()[:16]
    return _dataset_root(config) / "_shards" / f"shard={shard.shard_id}-{range_key}.json"


def _shard_identity(
    config: ExportConfig,
    shard: BackupShard,
    start_time: datetime,
    end_time: datetime,
) -> dict[str, object]:
    return {
        **_dataset_identity(config),
        "status": "complete",
        "source": shard.archive.name,
        "shard_id": shard.shard_id,
        "start_time": _format_influx_time(start_time),
        "end_time": _format_influx_time(end_time),
    }


def _read_shard_marker(
    config: ExportConfig,
    shard: BackupShard,
    start_time: datetime,
    end_time: datetime,
) -> dict[str, object] | None:
    path = _shard_manifest_path(config, shard, start_time, end_time)
    if not path.exists():
        return None
    document = json.loads(path.read_text())
    expected = _shard_identity(config, shard, start_time, end_time)
    mismatches = {
        key: (document.get(key), value)
        for key, value in expected.items()
        if document.get(key) != value
    }
    if mismatches:
        raise ValueError(f"existing shard manifest is incompatible: {path}: {mismatches}")
    return document


def _shard_marker_days(
    marker: dict[str, object], resumed: bool
) -> list[dict[str, object]]:
    return [{**day, "resumed": resumed} for day in marker.get("days", [])]


def _day_identity(config: ExportConfig, conversion_date: date) -> dict[str, object]:
    return {
        **_dataset_identity(config),
        "status": "complete",
        "date": conversion_date.isoformat(),
    }


def _partition_completed_days(
    dates: tuple[date, ...],
    config: ExportConfig,
) -> tuple[list[dict[str, object]], tuple[date, ...]]:
    completed: list[dict[str, object]] = []
    pending: list[date] = []
    for conversion_date in dates:
        path = _day_manifest_path(config, conversion_date)
        if not path.exists():
            pending.append(conversion_date)
            continue
        document = json.loads(path.read_text())
        expected = _day_identity(config, conversion_date)
        mismatches = {
            key: (document.get(key), value)
            for key, value in expected.items()
            if document.get(key) != value
        }
        if mismatches:
            raise ValueError(f"existing day manifest is incompatible: {path}: {mismatches}")
        completed.append(
            {
                **document.get("conversion", {}),
                "date": conversion_date.isoformat(),
                "day_manifest": document,
                "resumed": True,
            }
        )
    return completed, tuple(pending)


def _write_day_manifest(
    config: ExportConfig,
    conversion_date: date,
    conversion: dict[str, object],
    run_manifest: dict[str, object],
) -> dict[str, object]:
    completed_partitions = [
        str(path.parent.relative_to(_dataset_root(config)))
        for path in sorted(
            _dataset_root(config).glob(
                f"facts/sensor=*/vsn=*/instrument=*/date={conversion_date.isoformat()}/_manifest.json"
            )
        )
    ]
    document = {
        **_day_identity(config, conversion_date),
        "run_id": run_manifest["run_id"],
        "completed_partitions": completed_partitions,
        "completed_partition_count": len(completed_partitions),
        "conversion": conversion,
        "finished_at": datetime.now(UTC).isoformat(),
    }
    write_text_atomic(
        _day_manifest_path(config, conversion_date),
        json.dumps(document, indent=2, sort_keys=True) + "\n",
    )
    return document


def _run_export_jobs(
    jobs: Iterable[tuple[date, Path]],
    config: ExportConfig,
    mode: str,
    expected_days: int,
    write_manifest: bool = True,
) -> dict[str, object]:
    orchestration_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ") + f"-{uuid.uuid4().hex[:10]}"
    results: list[dict[str, object]] = []
    errors: list[dict[str, str]] = []
    with ThreadPoolExecutor(max_workers=config.workers) as executor:
        futures = {
            executor.submit(export_day, engine_dir, conversion_date, config, orchestration_id): conversion_date
            for conversion_date, engine_dir in jobs
        }
        for future in as_completed(futures):
            conversion_date = futures[future]
            try:
                result = future.result()
                results.append(result)
                print(f"ok {conversion_date.isoformat()}", file=sys.stderr, flush=True)
            except Exception as error:
                errors.append(
                    {"source": conversion_date.isoformat(), "error": describe_error(error)}
                )
                print(
                    f"error {conversion_date.isoformat()}: {describe_error(error)}",
                    file=sys.stderr,
                    flush=True,
                )
    if not write_manifest:
        return {"days": results, "errors": errors}
    return _write_export_manifest(config, mode, expected_days, results, errors)


def _write_export_manifest(
    config: ExportConfig,
    mode: str,
    expected_days: int,
    results: list[dict[str, object]],
    errors: list[dict[str, str]],
) -> dict[str, object]:
    results.sort(key=lambda result: str(result["date"]))
    errors.sort(key=lambda error: error["source"])
    status = "complete" if len(results) == expected_days and not errors else "incomplete"
    manifest = {
        "status": status,
        "export_version": 2,
        "storage_schema_version": SCHEMA_VERSION,
        "converter_version": __version__,
        "mode": mode,
        "bucket_id": config.bucket_id,
        "bucket_name": config.bucket_name,
        "source_snapshot": config.source_snapshot,
        "influxd_version": config.influxd_version,
        "registry_fingerprint": config.resolver.fingerprint,
        "selection_fingerprint": config.selection.fingerprint,
        "measurement_count": (
            None if config.selection.requires_discovery else len(config.measurements)
        ),
        "instrument_count": (
            len(config.allowed_instruments) if config.allowed_instruments is not None else None
        ),
        "expected_days": expected_days,
        "completed_days": len(results),
        "days": results,
        "errors": errors,
        "finished_at": datetime.now(UTC).isoformat(),
    }
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    path = config.output_dir / "export_runs" / f"{timestamp}-{uuid.uuid4().hex[:10]}.json"
    catalog_summary = _write_selected_catalogs(config)
    manifest["catalog"] = catalog_summary
    quarantined_rows = sum(int(day.get("quarantined_rows", 0)) for day in results)
    manifest["quarantined_rows"] = quarantined_rows
    manifest["requires_review"] = bool(
        catalog_summary["metadata_conflict_count"] or quarantined_rows
    )
    write_text_atomic(path, json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return manifest


def _write_selected_catalogs(config: ExportConfig) -> dict[str, object]:
    root = _dataset_root(config)
    sensors: dict[str, dict[str, object]] = {}
    instruments: dict[tuple[str, str, str], dict[str, object]] = {}
    variables: dict[tuple[str, str, str], int] = {}
    instrument_variables: dict[tuple[str, str, str, str, str], int] = {}

    for manifest_path in sorted(
        root.glob("facts/sensor=*/vsn=*/instrument=*/date=*/_manifest.json")
    ):
        document = json.loads(manifest_path.read_text())
        if document.get("status") != "complete":
            continue
        if document.get("selection_fingerprint") != config.selection.fingerprint:
            raise ValueError(f"partition selection fingerprint mismatch: {manifest_path}")
        sensor = str(document["sensor"])
        vsn = str(document["vsn"])
        instrument_id = str(document["instrument_id"])
        rows = int(document["row_count"])
        minimum = document.get("minimum_time_ns")
        maximum = document.get("maximum_time_ns")

        sensor_entry = sensors.setdefault(
            sensor,
            {
                "sensor": sensor,
                "instruments": set(),
                "vsns": set(),
                "row_count": 0,
                "minimum_time_ns": None,
                "maximum_time_ns": None,
            },
        )
        sensor_entry["instruments"].add(instrument_id)
        sensor_entry["vsns"].add(vsn)
        _observe_catalog_entry(sensor_entry, rows, minimum, maximum)

        instrument_key = (sensor, vsn, instrument_id)
        instrument_entry = instruments.setdefault(
            instrument_key,
            {
                "sensor": sensor,
                "vsn": vsn,
                "instrument_id": instrument_id,
                "row_count": 0,
                "minimum_time_ns": None,
                "maximum_time_ns": None,
            },
        )
        _observe_catalog_entry(instrument_entry, rows, minimum, maximum)

        for variable in document.get("variables", []):
            measurement = str(variable["measurement"])
            field_name = str(variable["field"])
            variable_key = (sensor, measurement, field_name)
            variables[variable_key] = variables.get(variable_key, 0) + int(variable["rows"])
            instrument_key = (sensor, vsn, instrument_id, measurement, field_name)
            instrument_variables[instrument_key] = (
                instrument_variables.get(instrument_key, 0) + int(variable["rows"])
            )

    series_rows: dict[bytes, dict[str, object]] = {}
    metadata_signatures: dict[tuple[str, str, str], set[tuple[object, ...]]] = {}
    for path in sorted(root.glob("_series/run=*/part-*.parquet")):
        table = pq.read_table(path)
        minimum_times = table.column("minimum_time").cast("int64").to_pylist()
        maximum_times = table.column("maximum_time").cast("int64").to_pylist()
        for row, minimum_time_ns, maximum_time_ns in zip(
            table.to_pylist(), minimum_times, maximum_times, strict=True
        ):
            series_id = bytes(row["series_id"])
            normalized = {
                "series_id": series_id.hex(),
                "sensor": row["sensor"],
                "vsn": row["vsn"],
                "instrument_id": row["instrument_id"],
                "measurement": row["measurement"],
                "identity_source": row["identity_source"],
                "tags": dict(row["tags"]),
                "fields": list(row["fields"]),
                "value_types": list(row["value_types"]),
                "minimum_time_ns": minimum_time_ns,
                "maximum_time_ns": maximum_time_ns,
            }
            existing = series_rows.get(series_id)
            if existing is not None:
                evolving_keys = {
                    "fields",
                    "value_types",
                    "minimum_time_ns",
                    "maximum_time_ns",
                }
                comparable = {
                    key: value for key, value in normalized.items() if key not in evolving_keys
                }
                existing_comparable = {
                    key: value for key, value in existing.items() if key not in evolving_keys
                }
                if existing_comparable != comparable:
                    raise ValueError(f"series metadata collision: {series_id.hex()}")
                existing["minimum_time_ns"] = min(existing["minimum_time_ns"], normalized["minimum_time_ns"])
                existing["maximum_time_ns"] = max(existing["maximum_time_ns"], normalized["maximum_time_ns"])
                existing["fields"] = sorted(set(existing["fields"]) | set(normalized["fields"]))
                existing["value_types"] = sorted(
                    set(existing["value_types"]) | set(normalized["value_types"])
                )
            else:
                series_rows[series_id] = normalized

    for normalized in series_rows.values():
        tags = normalized["tags"]
        for field_name in normalized["fields"]:
            key = (normalized["sensor"], normalized["measurement"], field_name)
            signature = (
                tags.get("units"),
                tags.get("missing"),
                tuple(normalized["value_types"]),
            )
            metadata_signatures.setdefault(key, set()).add(signature)

    conflicts = [
        {
            "variable_id": f"{sensor}::{measurement}::{field_name}",
            "sensor": sensor,
            "measurement": measurement,
            "field": field_name,
            "signature_count": len(signatures),
            "signatures": json.dumps(sorted(signatures, key=str), separators=(",", ":")),
        }
        for (sensor, measurement, field_name), signatures in sorted(metadata_signatures.items())
        if len(signatures) > 1
    ]

    _write_csv_atomic(
        root / "_catalog" / "selected_sensors.csv",
        ("sensor", "vsn_count", "instrument_count", "row_count", "minimum_time_ns", "maximum_time_ns"),
        (
            {
                "sensor": entry["sensor"],
                "vsn_count": len(entry["vsns"]),
                "instrument_count": len(entry["instruments"]),
                "row_count": entry["row_count"],
                "minimum_time_ns": entry["minimum_time_ns"],
                "maximum_time_ns": entry["maximum_time_ns"],
            }
            for _, entry in sorted(sensors.items())
        ),
    )
    _write_csv_atomic(
        root / "_catalog" / "selected_instruments.csv",
        ("sensor", "vsn", "instrument_id", "row_count", "minimum_time_ns", "maximum_time_ns"),
        (entry for _, entry in sorted(instruments.items())),
    )
    _write_csv_atomic(
        root / "_catalog" / "selected_variables.csv",
        ("variable_id", "sensor", "measurement", "field", "row_count"),
        (
            {
                "variable_id": f"{sensor}::{measurement}::{field_name}",
                "sensor": sensor,
                "measurement": measurement,
                "field": field_name,
                "row_count": rows,
            }
            for (sensor, measurement, field_name), rows in sorted(variables.items())
        ),
    )
    _write_csv_atomic(
        root / "_catalog" / "selected_instrument_variables.csv",
        ("instrument_variable_id", "sensor", "vsn", "instrument_id", "measurement", "field", "row_count"),
        (
            {
                "instrument_variable_id": f"{instrument_id}::{measurement}::{field_name}",
                "sensor": sensor,
                "vsn": vsn,
                "instrument_id": instrument_id,
                "measurement": measurement,
                "field": field_name,
                "row_count": rows,
            }
            for (sensor, vsn, instrument_id, measurement, field_name), rows in sorted(instrument_variables.items())
        ),
    )
    _write_csv_atomic(
        root / "_catalog" / "selected_series.csv",
        (
            "series_id",
            "sensor",
            "vsn",
            "instrument_id",
            "measurement",
            "identity_source",
            "tags_json",
            "fields_json",
            "value_types_json",
            "minimum_time_ns",
            "maximum_time_ns",
        ),
        (
            {
                **{
                    key: row[key]
                    for key in (
                        "series_id",
                        "sensor",
                        "vsn",
                        "instrument_id",
                        "measurement",
                        "identity_source",
                        "minimum_time_ns",
                        "maximum_time_ns",
                    )
                },
                "tags_json": json.dumps(row["tags"], separators=(",", ":"), sort_keys=True),
                "fields_json": json.dumps(row["fields"], separators=(",", ":")),
                "value_types_json": json.dumps(
                    row["value_types"], separators=(",", ":")
                ),
            }
            for _, row in sorted(series_rows.items())
        ),
    )
    _write_csv_atomic(
        root / "_catalog" / "metadata_conflicts.csv",
        ("variable_id", "sensor", "measurement", "field", "signature_count", "signatures"),
        conflicts,
    )
    return {
        "sensor_count": len(sensors),
        "instrument_count": len(instruments),
        "variable_count": len(variables),
        "instrument_variable_count": len(instrument_variables),
        "series_count": len(series_rows),
        "metadata_conflict_count": len(conflicts),
    }


def _observe_catalog_entry(
    entry: dict[str, object],
    rows: int,
    minimum: object,
    maximum: object,
) -> None:
    entry["row_count"] = int(entry["row_count"]) + rows
    if minimum is not None:
        current_minimum = entry["minimum_time_ns"]
        entry["minimum_time_ns"] = minimum if current_minimum is None else min(int(current_minimum), int(minimum))
    if maximum is not None:
        current_maximum = entry["maximum_time_ns"]
        entry["maximum_time_ns"] = maximum if current_maximum is None else max(int(current_maximum), int(maximum))


def _write_csv_atomic(
    path: Path,
    fieldnames: tuple[str, ...],
    rows: Iterable[dict[str, object]],
) -> None:
    stream = io.StringIO(newline="")
    writer = csv.DictWriter(stream, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)
    write_text_atomic(path, stream.getvalue())


def _dates(start_date: date, end_date: date) -> Iterable[date]:
    if end_date <= start_date:
        raise ValueError("end date must be after start date")
    current = start_date
    while current < end_date:
        yield current
        current += timedelta(days=1)


def _datetime_dates(start_time: datetime, end_time: datetime) -> tuple[date, ...]:
    start_date = start_time.astimezone(UTC).date()
    end_utc = end_time.astimezone(UTC)
    final_date = end_utc.date()
    if end_utc.time() == datetime.min.time():
        final_date -= timedelta(days=1)
    if final_date < start_date:
        return ()
    return tuple(_dates(start_date, final_date + timedelta(days=1)))
