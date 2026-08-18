from __future__ import annotations

import hashlib
import json
import os
from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import quote

import pyarrow as pa
import pyarrow.parquet as pq

from crocus_raw import __version__
from crocus_raw.model import OutputPoint, PROMOTED_TAGS, parquet_schema


SCHEMA_VERSION = 1


@dataclass(frozen=True)
class WriterConfig:
    output_root: Path
    run_id: str
    source_snapshot: str
    bucket: str
    registry_fingerprint: str
    influxd_version: str | None = None
    rows_per_file: int = 500_000
    max_buffer_rows: int = 1_000_000
    on_existing: str = "error"


@dataclass
class PartitionState:
    instrument_id: str
    date: str
    hour: int
    relative_path: Path
    buffer: list[OutputPoint] = field(default_factory=list)
    files: list[dict[str, object]] = field(default_factory=list)
    row_count: int = 0
    minimum_time_ns: int | None = None
    maximum_time_ns: int | None = None
    measurements: Counter[str] = field(default_factory=Counter)
    fields: Counter[str] = field(default_factory=Counter)
    value_types: Counter[str] = field(default_factory=Counter)
    tag_keys: set[str] = field(default_factory=set)

    def observe(self, output_point: OutputPoint) -> None:
        point = output_point.point
        self.row_count += 1
        self.minimum_time_ns = (
            point.time_ns if self.minimum_time_ns is None else min(self.minimum_time_ns, point.time_ns)
        )
        self.maximum_time_ns = (
            point.time_ns if self.maximum_time_ns is None else max(self.maximum_time_ns, point.time_ns)
        )
        self.measurements[point.measurement] += 1
        self.fields[point.field] += 1
        self.value_types[point.parsed_value.value_type] += 1
        self.tag_keys.update(point.tags)


class HourlyDatasetWriter:
    def __init__(self, config: WriterConfig):
        if config.rows_per_file < 1 or config.max_buffer_rows < 1:
            raise ValueError("row limits must be positive")
        if config.on_existing not in {"error", "skip"}:
            raise ValueError("on_existing must be error or skip")
        self.config = config
        self.version_root = config.output_root / f"schema_version={SCHEMA_VERSION}"
        self.staging_root = self.version_root / "_staging" / config.run_id
        self.run_manifest_path = self.version_root / "_runs" / f"{config.run_id}.json"
        if self.staging_root.exists() or self.run_manifest_path.exists():
            raise FileExistsError(f"run ID already exists: {config.run_id}")
        self.states: dict[Path, PartitionState] = {}
        self.partition_decisions: dict[Path, str] = {}
        self.total_buffer_rows = 0
        self.skipped_existing_rows = 0
        self.schema = parquet_schema(
            {
                b"crocus.schema_version": str(SCHEMA_VERSION).encode(),
                b"crocus.converter_version": __version__.encode(),
                b"crocus.run_id": config.run_id.encode(),
                b"crocus.source_snapshot": config.source_snapshot.encode(),
                b"crocus.bucket": config.bucket.encode(),
                b"crocus.influxd_version": (config.influxd_version or "not-recorded").encode(),
            }
        )

    def append(self, output_point: OutputPoint) -> None:
        event_time = datetime.fromtimestamp(output_point.point.time_ns // 1_000_000_000, tz=UTC)
        relative_path = _partition_path(output_point.instrument_id, event_time)
        decision = self.partition_decisions.get(relative_path)
        if decision is None:
            decision = self._decide_partition(relative_path, output_point.instrument_id, event_time)
            self.partition_decisions[relative_path] = decision
        if decision == "skip":
            self.skipped_existing_rows += 1
            return

        state = self.states.get(relative_path)
        if state is None:
            state = PartitionState(
                instrument_id=output_point.instrument_id,
                date=event_time.date().isoformat(),
                hour=event_time.hour,
                relative_path=relative_path,
            )
            self.states[relative_path] = state
        state.buffer.append(output_point)
        state.observe(output_point)
        self.total_buffer_rows += 1

        if len(state.buffer) >= self.config.rows_per_file:
            self._flush(state)
        while self.total_buffer_rows >= self.config.max_buffer_rows:
            largest = max(self.states.values(), key=lambda candidate: len(candidate.buffer))
            if not largest.buffer:
                break
            self._flush(largest)

    def finalize(self, conversion_summary: dict[str, object] | None = None) -> dict[str, object]:
        for state in self.states.values():
            self._flush(state)
        completed: list[str] = []
        for relative_path in sorted(self.states, key=str):
            state = self.states[relative_path]
            staging_path = self.staging_root / relative_path
            _write_json_atomic(staging_path / "_manifest.json", self._partition_manifest(state))
            final_path = self.version_root / relative_path
            final_path.parent.mkdir(parents=True, exist_ok=True)
            if final_path.exists():
                raise FileExistsError(f"partition appeared during conversion: {final_path}")
            os.rename(staging_path, final_path)
            completed.append(str(relative_path))

        run_manifest = {
            "status": "complete",
            "schema_version": SCHEMA_VERSION,
            "converter_version": __version__,
            "run_id": self.config.run_id,
            "source_snapshot": self.config.source_snapshot,
            "bucket": self.config.bucket,
            "registry_fingerprint": self.config.registry_fingerprint,
            "influxd_version": self.config.influxd_version,
            "completed_partitions": completed,
            "completed_partition_count": len(completed),
            "skipped_existing_rows": self.skipped_existing_rows,
            "conversion": conversion_summary or {},
            "finished_at": datetime.now(UTC).isoformat(),
        }
        _write_json_atomic(self.run_manifest_path, run_manifest)
        return run_manifest

    def _decide_partition(self, relative_path: Path, instrument_id: str, event_time: datetime) -> str:
        final_path = self.version_root / relative_path
        if not final_path.exists():
            return "write"
        manifest_path = final_path / "_manifest.json"
        if not manifest_path.is_file():
            raise FileExistsError(f"existing partition is incomplete: {final_path}")
        manifest = json.loads(manifest_path.read_text())
        expected = {
            "status": "complete",
            "schema_version": SCHEMA_VERSION,
            "converter_version": __version__,
            "source_snapshot": self.config.source_snapshot,
            "bucket": self.config.bucket,
            "registry_fingerprint": self.config.registry_fingerprint,
            "influxd_version": self.config.influxd_version,
            "instrument_id": instrument_id,
            "date": event_time.date().isoformat(),
            "hour": event_time.hour,
        }
        mismatches = {key: (manifest.get(key), value) for key, value in expected.items() if manifest.get(key) != value}
        if mismatches:
            raise FileExistsError(f"existing partition is incompatible: {final_path}: {mismatches}")
        if self.config.on_existing == "error":
            raise FileExistsError(f"partition already complete: {final_path}")
        return "skip"

    def _flush(self, state: PartitionState) -> None:
        if not state.buffer:
            return
        staging_path = self.staging_root / state.relative_path
        staging_path.mkdir(parents=True, exist_ok=True)
        part_number = len(state.files)
        final_name = f"part-{part_number:05d}.parquet"
        temporary_path = staging_path / f".{final_name}.tmp"
        final_path = staging_path / final_name
        table = _points_to_table(state.buffer, self.schema)
        table = table.sort_by(
            [
                ("measurement", "ascending"),
                ("field", "ascending"),
                ("node", "ascending"),
                ("time", "ascending"),
            ]
        )
        pq.write_table(
            table,
            temporary_path,
            compression="zstd",
            compression_level=3,
            use_dictionary=True,
            write_statistics=True,
            version="2.6",
            row_group_size=min(len(table), 250_000),
        )
        os.replace(temporary_path, final_path)
        state.files.append(
            {
                "name": final_name,
                "rows": len(table),
                "bytes": final_path.stat().st_size,
                "sha256": _sha256(final_path),
            }
        )
        self.total_buffer_rows -= len(state.buffer)
        state.buffer.clear()

    def _partition_manifest(self, state: PartitionState) -> dict[str, object]:
        return {
            "status": "complete",
            "schema_version": SCHEMA_VERSION,
            "converter_version": __version__,
            "run_id": self.config.run_id,
            "source_snapshot": self.config.source_snapshot,
            "bucket": self.config.bucket,
            "registry_fingerprint": self.config.registry_fingerprint,
            "influxd_version": self.config.influxd_version,
            "instrument_id": state.instrument_id,
            "date": state.date,
            "hour": state.hour,
            "row_count": state.row_count,
            "minimum_time_ns": state.minimum_time_ns,
            "maximum_time_ns": state.maximum_time_ns,
            "measurements": dict(sorted(state.measurements.items())),
            "fields": dict(sorted(state.fields.items())),
            "value_types": dict(sorted(state.value_types.items())),
            "tag_keys": sorted(state.tag_keys),
            "files": state.files,
        }


def _partition_path(instrument_id: str, event_time: datetime) -> Path:
    encoded_id = quote(instrument_id, safe="-_.")
    return Path(
        f"instrument={encoded_id}",
        f"date={event_time.date().isoformat()}",
        f"hour={event_time.hour:02d}",
    )


def _points_to_table(points: list[OutputPoint], schema: pa.Schema) -> pa.Table:
    columns: dict[str, list[object]] = {field.name: [] for field in schema}
    for output_point in points:
        point = output_point.point
        value_type = point.parsed_value.value_type
        columns["time"].append(point.time_ns)
        columns["instrument_id"].append(output_point.instrument_id)
        columns["measurement"].append(point.measurement)
        columns["field"].append(point.field)
        columns["value_type"].append(value_type)
        for candidate in ("float64", "int64", "uint64", "bool", "string"):
            columns[f"value_{candidate}"].append(point.parsed_value.value if value_type == candidate else None)
        columns["tags"].append(sorted(point.tags.items()))
        for tag in PROMOTED_TAGS:
            columns[tag].append(point.tags.get(tag))
    arrays = [pa.array(columns[field.name], type=field.type) for field in schema]
    return pa.Table.from_arrays(arrays, schema=schema)


def _write_json_atomic(path: Path, document: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_name(f".{path.name}.tmp")
    temporary_path.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n")
    os.replace(temporary_path, path)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()
