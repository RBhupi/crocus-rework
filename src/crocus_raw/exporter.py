from __future__ import annotations

import json
import shutil
import subprocess
import sys
import tarfile
import tempfile
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path, PurePosixPath
from typing import Iterable

from crocus_raw import __version__
from crocus_raw.backup import BackupBucket, BackupShard
from crocus_raw.converter import convert_stream
from crocus_raw.instruments import InstrumentResolver
from crocus_raw.runtime import describe_error, write_text_atomic
from crocus_raw.writer import HourlyDatasetWriter, WriterConfig


@dataclass(frozen=True)
class ExportConfig:
    influxd: Path
    influxd_version: str
    bucket_id: str
    bucket_name: str
    output_dir: Path
    source_snapshot: str
    measurements: tuple[str, ...]
    allowed_instruments: frozenset[str]
    resolver: InstrumentResolver
    workers: int = 1
    rows_per_file: int = 500_000
    max_buffer_rows: int = 1_000_000
    on_existing: str = "skip"


def build_export_command(
    influxd: Path,
    bucket_id: str,
    engine_dir: Path,
    start_date: date,
    measurements: Iterable[str],
) -> list[str]:
    end_date = start_date + timedelta(days=1)
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
            f"{start_date.isoformat()}T00:00:00Z",
            "--end",
            f"{end_date.isoformat()}T00:00:00Z",
        )
    )
    return command


def export_engine_range(
    engine_dir: Path,
    start_date: date,
    end_date: date,
    config: ExportConfig,
) -> dict[str, object]:
    dates = tuple(_dates(start_date, end_date))
    return _run_export_jobs(
        ((conversion_date, engine_dir) for conversion_date in dates),
        config,
        mode="engine",
        expected_days=len(dates),
    )


def export_backup_range(
    backup: BackupBucket,
    start_date: date,
    end_date: date,
    work_dir: Path,
    config: ExportConfig,
) -> dict[str, object]:
    requested_dates = set(_dates(start_date, end_date))
    covered_dates: set[date] = set()
    results: list[dict[str, object]] = []
    errors: list[dict[str, str]] = []
    work_dir.mkdir(parents=True, exist_ok=True)

    for shard in backup.shards:
        shard_dates = sorted(
            conversion_date
            for conversion_date in requested_dates
            if shard.start_time.date() <= conversion_date < shard.end_time.date()
        )
        if not shard_dates:
            continue
        covered_dates.update(shard_dates)
        print(f"stage {shard.archive.name}", file=sys.stderr, flush=True)
        staging_root = Path(tempfile.mkdtemp(prefix=f"crocus-shard-{shard.shard_id}-", dir=work_dir))
        try:
            engine_dir = stage_backup_shard(shard, backup.bucket_id, staging_root)
            shard_result = _run_export_jobs(
                ((conversion_date, engine_dir) for conversion_date in shard_dates),
                config,
                mode="backup",
                expected_days=len(shard_dates),
                write_manifest=False,
            )
            results.extend(shard_result["days"])
            errors.extend(shard_result["errors"])
        except Exception as error:
            errors.append({"source": shard.archive.name, "error": describe_error(error)})
            print(
                f"error {shard.archive.name}: {describe_error(error)}",
                file=sys.stderr,
                flush=True,
            )
        finally:
            shutil.rmtree(staging_root, ignore_errors=True)

    missing_dates = sorted(requested_dates - covered_dates)
    errors.extend(
        {"source": conversion_date.isoformat(), "error": "no backup shard covers this date"}
        for conversion_date in missing_dates
    )
    return _write_export_manifest(config, "backup", len(requested_dates), results, errors)


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
    writer = HourlyDatasetWriter(
        WriterConfig(
            output_root=config.output_dir,
            run_id=run_id,
            source_snapshot=config.source_snapshot,
            bucket=config.bucket_name,
            registry_fingerprint=config.resolver.fingerprint,
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
    run_manifest = writer.finalize(conversion)
    return {**conversion, "run": run_manifest, "command": command}


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
        "export_version": 1,
        "converter_version": __version__,
        "mode": mode,
        "bucket_id": config.bucket_id,
        "bucket_name": config.bucket_name,
        "source_snapshot": config.source_snapshot,
        "influxd_version": config.influxd_version,
        "registry_fingerprint": config.resolver.fingerprint,
        "measurement_count": len(config.measurements),
        "instrument_count": len(config.allowed_instruments),
        "expected_days": expected_days,
        "completed_days": len(results),
        "days": results,
        "errors": errors,
        "finished_at": datetime.now(UTC).isoformat(),
    }
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    path = config.output_dir / "export_runs" / f"{timestamp}-{uuid.uuid4().hex[:10]}.json"
    write_text_atomic(path, json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return manifest


def _dates(start_date: date, end_date: date) -> Iterable[date]:
    if end_date <= start_date:
        raise ValueError("end date must be after start date")
    current = start_date
    while current < end_date:
        yield current
        current += timedelta(days=1)
