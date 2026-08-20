from __future__ import annotations

import hashlib
import json
import os
import shutil
from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import quote

import pyarrow as pa
import pyarrow.parquet as pq

from crocus_raw import __version__
from crocus_raw.model import OutputPoint, ScalarValue, SeriesMetadata, ValueType, fact_schema, series_schema


STORAGE_SCHEMA_VERSION = 4
SCHEMA_VERSION = STORAGE_SCHEMA_VERSION
VALUE_TYPES = ("float64", "int64", "uint64", "bool", "string")


@dataclass(frozen=True)
class WriterConfig:
    output_root: Path
    run_id: str
    source_snapshot: str
    bucket: str
    registry_fingerprint: str
    selection_fingerprint: str
    influxd_version: str | None = None
    rows_per_file: int = 500_000
    max_buffer_rows: int = 1_000_000
    on_existing: str = "error"


@dataclass
class ColumnBuffer:
    times: list[int] = field(default_factory=list)
    measurements: list[str] = field(default_factory=list)
    fields: list[str] = field(default_factory=list)
    series_ids: list[bytes] = field(default_factory=list)
    value_types: list[ValueType] = field(default_factory=list)
    values: list[ScalarValue] = field(default_factory=list)

    def __len__(self) -> int:
        return len(self.times)

    def clear(self) -> None:
        self.times.clear()
        self.measurements.clear()
        self.fields.clear()
        self.series_ids.clear()
        self.value_types.clear()
        self.values.clear()


@dataclass
class PartitionState:
    sensor: str
    vsn: str
    instrument_id: str
    date: str
    relative_path: Path
    buffer: ColumnBuffer = field(default_factory=ColumnBuffer)
    files: list[dict[str, object]] = field(default_factory=list)
    row_count: int = 0
    minimum_time_ns: int | None = None
    maximum_time_ns: int | None = None
    measurements: Counter[str] = field(default_factory=Counter)
    fields: Counter[str] = field(default_factory=Counter)
    variables: Counter[tuple[str, str]] = field(default_factory=Counter)
    value_types: Counter[str] = field(default_factory=Counter)

    def observe(
        self,
        time_ns: int,
        measurement: str,
        field_name: str,
        series_id: bytes,
        value_type: ValueType,
        value: ScalarValue,
    ) -> None:
        self.buffer.times.append(time_ns)
        self.buffer.measurements.append(measurement)
        self.buffer.fields.append(field_name)
        self.buffer.series_ids.append(series_id)
        self.buffer.value_types.append(value_type)
        self.buffer.values.append(value)
        self.row_count += 1
        self.minimum_time_ns = time_ns if self.minimum_time_ns is None else min(self.minimum_time_ns, time_ns)
        self.maximum_time_ns = time_ns if self.maximum_time_ns is None else max(self.maximum_time_ns, time_ns)
        self.measurements[measurement] += 1
        self.fields[field_name] += 1
        self.variables[(measurement, field_name)] += 1
        self.value_types[value_type] += 1


@dataclass
class SeriesState:
    metadata: SeriesMetadata
    minimum_time_ns: int
    maximum_time_ns: int
    fields: set[str] = field(default_factory=set)
    value_types: set[str] = field(default_factory=set)

    def observe(self, time_ns: int, field_name: str, value_type: ValueType) -> None:
        self.minimum_time_ns = min(self.minimum_time_ns, time_ns)
        self.maximum_time_ns = max(self.maximum_time_ns, time_ns)
        self.fields.add(field_name)
        self.value_types.add(value_type)


@dataclass(frozen=True)
class QuarantineRecord:
    reason: str
    date: str
    time_ns: int
    measurement: str
    field: str
    series_id: bytes
    value_type: ValueType
    value: ScalarValue
    tags: tuple[tuple[str, str], ...]


@dataclass
class QuarantineState:
    reason: str
    date: str
    records: list[QuarantineRecord] = field(default_factory=list)
    files: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class RouteState:
    partition: PartitionState | None
    series: SeriesState


class DailyDatasetWriter:
    def __init__(self, config: WriterConfig):
        if config.rows_per_file < 1 or config.max_buffer_rows < 1:
            raise ValueError("row limits must be positive")
        if config.on_existing not in {"error", "skip"}:
            raise ValueError("on_existing must be error or skip")
        self.config = config
        self.dataset_root = config.output_root
        self.fact_root = self.dataset_root / "facts"
        self.staging_root = self.dataset_root / "_staging" / config.run_id
        self.run_manifest_path = self.dataset_root / "_runs" / f"{config.run_id}.json"
        if self.staging_root.exists() or self.run_manifest_path.exists():
            raise FileExistsError(f"run ID already exists: {config.run_id}")
        self.states: dict[Path, PartitionState] = {}
        self.routes: dict[tuple[bytes, str], RouteState] = {}
        self.series: dict[bytes, SeriesState] = {}
        self.quarantine_states: dict[tuple[str, str], QuarantineState] = {}
        self.quarantine_reasons: Counter[str] = Counter()
        self.total_buffer_rows = 0
        self.skipped_existing_rows = 0
        self.schema_metadata = {
            b"crocus.storage_schema_version": str(STORAGE_SCHEMA_VERSION).encode(),
            b"crocus.converter_version": __version__.encode(),
            b"crocus.run_id": config.run_id.encode(),
            b"crocus.source_snapshot": config.source_snapshot.encode(),
            b"crocus.bucket": config.bucket.encode(),
            b"crocus.registry_fingerprint": config.registry_fingerprint.encode(),
            b"crocus.selection_fingerprint": config.selection_fingerprint.encode(),
            b"crocus.influxd_version": (config.influxd_version or "not-recorded").encode(),
        }
        self.schema = fact_schema(self.schema_metadata)

    def append_value(
        self,
        time_ns: int,
        date_text: str,
        series: SeriesMetadata,
        field_name: str,
        value_type: ValueType,
        value: ScalarValue,
    ) -> bool:
        if series.vsn is None:
            self.quarantine_value(
                "missing-vsn", time_ns, date_text, series, field_name, value_type, value
            )
            return False
        if series.sensor is None:
            self.quarantine_value(
                "missing-sensor", time_ns, date_text, series, field_name, value_type, value
            )
            return False
        if series.instrument_id is None:
            raise ValueError("valid VSN/sensor series has no instrument ID")

        route_key = (series.series_id, date_text)
        route = self.routes.get(route_key)
        if route is None:
            relative_path = _partition_path(
                series.sensor, series.vsn, series.instrument_id, date_text
            )
            decision = self._decide_partition(
                relative_path, series.sensor, series.vsn, series.instrument_id, date_text
            )
            series_state = self._series_state(series, time_ns)
            if decision == "skip":
                route = RouteState(None, series_state)
            else:
                state = self.states.get(relative_path)
                if state is None:
                    state = PartitionState(
                        sensor=series.sensor,
                        vsn=series.vsn,
                        instrument_id=series.instrument_id,
                        date=date_text,
                        relative_path=relative_path,
                    )
                    self.states[relative_path] = state
                route = RouteState(state, series_state)
            self.routes[route_key] = route
        route.series.observe(time_ns, field_name, value_type)
        if route.partition is None:
            self.skipped_existing_rows += 1
            return True
        state = route.partition
        state.observe(
            time_ns,
            series.measurement,
            field_name,
            series.series_id,
            value_type,
            value,
        )
        self.total_buffer_rows += 1
        if len(state.buffer) >= self.config.rows_per_file:
            self._flush(state)
        while self.total_buffer_rows >= self.config.max_buffer_rows:
            largest = max(self.states.values(), key=lambda candidate: len(candidate.buffer))
            if not largest.buffer:
                break
            self._flush(largest)
        return True

    def append(self, output_point: OutputPoint) -> None:
        point = output_point.point
        retained_tags = {key: value for key, value in point.tags.items() if key != "node"}
        canonical = json.dumps(
            {"measurement": point.measurement, "tags": dict(sorted(retained_tags.items()))},
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        series = SeriesMetadata(
            series_id=hashlib.sha256(canonical).digest()[:16],
            measurement=point.measurement,
            tags=retained_tags,
            vsn=retained_tags.get("vsn"),
            sensor=retained_tags.get("sensor"),
            instrument_id=output_point.instrument_id,
            identity_source="legacy",
        )
        date_text = _date_from_ns(point.time_ns)
        self.append_value(
            point.time_ns,
            date_text,
            series,
            point.field,
            point.parsed_value.value_type,
            point.parsed_value.value,
        )

    def quarantine_value(
        self,
        reason: str,
        time_ns: int,
        date_text: str,
        series: SeriesMetadata,
        field_name: str,
        value_type: ValueType,
        value: ScalarValue,
    ) -> None:
        key = (reason, date_text)
        state = self.quarantine_states.get(key)
        if state is None:
            state = QuarantineState(reason, date_text)
            self.quarantine_states[key] = state
        state.records.append(
            QuarantineRecord(
                reason,
                date_text,
                time_ns,
                series.measurement,
                field_name,
                series.series_id,
                value_type,
                value,
                tuple(sorted(series.tags.items())),
            )
        )
        self.quarantine_reasons[reason] += 1
        if len(state.records) >= self.config.rows_per_file:
            self._flush_quarantine(state)

    def finalize(self, conversion_summary: dict[str, object] | None = None) -> dict[str, object]:
        for state in self.states.values():
            self._flush(state)
        completed: list[str] = []
        for relative_path in sorted(self.states, key=str):
            state = self.states[relative_path]
            staging_path = self.staging_root / "facts" / relative_path
            _write_json_atomic(staging_path / "_manifest.json", self._partition_manifest(state))
            final_path = self.fact_root / relative_path
            final_path.parent.mkdir(parents=True, exist_ok=True)
            if final_path.exists():
                raise FileExistsError(f"partition appeared during conversion: {final_path}")
            os.rename(staging_path, final_path)
            completed.append(str(Path("facts") / relative_path))

        series_fragment = self._write_series_fragment()
        quarantine_fragments = self._write_quarantine_fragments()
        shutil.rmtree(self.staging_root, ignore_errors=True)
        run_manifest = {
            "status": "complete",
            "storage_schema_version": STORAGE_SCHEMA_VERSION,
            "converter_version": __version__,
            "run_id": self.config.run_id,
            "source_snapshot": self.config.source_snapshot,
            "bucket": self.config.bucket,
            "registry_fingerprint": self.config.registry_fingerprint,
            "selection_fingerprint": self.config.selection_fingerprint,
            "influxd_version": self.config.influxd_version,
            "completed_partitions": completed,
            "completed_partition_count": len(completed),
            "series_fragment": series_fragment,
            "series_count": len(self.series),
            "quarantine_fragments": quarantine_fragments,
            "quarantined_rows": sum(self.quarantine_reasons.values()),
            "quarantine_reasons": dict(sorted(self.quarantine_reasons.items())),
            "requires_review": bool(self.quarantine_reasons),
            "skipped_existing_rows": self.skipped_existing_rows,
            "conversion": conversion_summary or {},
            "finished_at": datetime.now(UTC).isoformat(),
        }
        _write_json_atomic(self.run_manifest_path, run_manifest)
        return run_manifest

    def abort(self) -> None:
        shutil.rmtree(self.staging_root, ignore_errors=True)

    def _series_state(self, series: SeriesMetadata, time_ns: int) -> SeriesState:
        state = self.series.get(series.series_id)
        if state is None:
            state = SeriesState(series, time_ns, time_ns)
            self.series[series.series_id] = state
        elif state.metadata != series:
            raise ValueError(f"series ID collision: {series.series_id.hex()}")
        return state

    def _decide_partition(
        self,
        relative_path: Path,
        sensor: str,
        vsn: str,
        instrument_id: str,
        date_text: str,
    ) -> str:
        final_path = self.fact_root / relative_path
        if not final_path.exists():
            return "write"
        manifest_path = final_path / "_manifest.json"
        if not manifest_path.is_file():
            raise FileExistsError(f"existing partition is incomplete: {final_path}")
        manifest = json.loads(manifest_path.read_text())
        expected = {
            "status": "complete",
            "storage_schema_version": STORAGE_SCHEMA_VERSION,
            "converter_version": __version__,
            "source_snapshot": self.config.source_snapshot,
            "bucket": self.config.bucket,
            "registry_fingerprint": self.config.registry_fingerprint,
            "selection_fingerprint": self.config.selection_fingerprint,
            "influxd_version": self.config.influxd_version,
            "sensor": sensor,
            "vsn": vsn,
            "instrument_id": instrument_id,
            "date": date_text,
        }
        mismatches = {
            key: (manifest.get(key), value)
            for key, value in expected.items()
            if manifest.get(key) != value
        }
        if mismatches:
            raise FileExistsError(f"existing partition is incompatible: {final_path}: {mismatches}")
        if self.config.on_existing == "error":
            raise FileExistsError(f"partition already complete: {final_path}")
        return "skip"

    def _flush(self, state: PartitionState) -> None:
        if not state.buffer:
            return
        staging_path = self.staging_root / "facts" / state.relative_path
        staging_path.mkdir(parents=True, exist_ok=True)
        part_number = len(state.files)
        final_name = f"part-{part_number:05d}.parquet"
        temporary_path = staging_path / f".{final_name}.tmp"
        final_path = staging_path / final_name
        table = _buffer_to_table(state, self.schema)
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
            "storage_schema_version": STORAGE_SCHEMA_VERSION,
            "converter_version": __version__,
            "run_id": self.config.run_id,
            "source_snapshot": self.config.source_snapshot,
            "bucket": self.config.bucket,
            "registry_fingerprint": self.config.registry_fingerprint,
            "selection_fingerprint": self.config.selection_fingerprint,
            "influxd_version": self.config.influxd_version,
            "sensor": state.sensor,
            "vsn": state.vsn,
            "instrument_id": state.instrument_id,
            "date": state.date,
            "row_count": state.row_count,
            "minimum_time_ns": state.minimum_time_ns,
            "maximum_time_ns": state.maximum_time_ns,
            "measurements": dict(sorted(state.measurements.items())),
            "fields": dict(sorted(state.fields.items())),
            "variables": [
                {"measurement": measurement, "field": field_name, "rows": rows}
                for (measurement, field_name), rows in sorted(state.variables.items())
            ],
            "value_types": dict(sorted(state.value_types.items())),
            "files": state.files,
        }

    def _write_series_fragment(self) -> str | None:
        if not self.series:
            return None
        relative = Path("_series") / f"run={quote(self.config.run_id, safe='-_.')}" / "part-00000.parquet"
        final_path = self.dataset_root / relative
        temporary_path = self.staging_root / relative
        temporary_path.parent.mkdir(parents=True, exist_ok=True)
        states = [self.series[key] for key in sorted(self.series)]
        schema = series_schema(self.schema_metadata)
        table = pa.Table.from_arrays(
            [
                pa.array([state.metadata.series_id for state in states], type=pa.binary(16)),
                pa.array([state.metadata.sensor for state in states], type=pa.string()),
                pa.array([state.metadata.vsn for state in states], type=pa.string()),
                pa.array([state.metadata.instrument_id for state in states], type=pa.string()),
                pa.array([state.metadata.measurement for state in states], type=pa.string()),
                pa.array([state.metadata.identity_source for state in states], type=pa.string()),
                pa.array([sorted(state.metadata.tags.items()) for state in states], type=pa.map_(pa.string(), pa.string())),
                pa.array([sorted(state.fields) for state in states], type=pa.list_(pa.string())),
                pa.array([sorted(state.value_types) for state in states], type=pa.list_(pa.string())),
                pa.array([state.minimum_time_ns for state in states], type=pa.timestamp("ns", tz="UTC")),
                pa.array([state.maximum_time_ns for state in states], type=pa.timestamp("ns", tz="UTC")),
            ],
            schema=schema,
        )
        pq.write_table(table, temporary_path, compression="zstd", use_dictionary=True)
        final_path.parent.mkdir(parents=True, exist_ok=True)
        if final_path.exists():
            raise FileExistsError(f"series fragment already exists: {final_path}")
        os.rename(temporary_path, final_path)
        return str(relative)

    def _write_quarantine_fragments(self) -> list[str]:
        fragments: list[str] = []
        for _, state in sorted(self.quarantine_states.items()):
            self._flush_quarantine(state)
            relative_directory = (
                Path("_quarantine")
                / f"reason={quote(state.reason, safe='-_.')}"
                / f"date={state.date}"
                / f"run={quote(self.config.run_id, safe='-_.')}"
            )
            staging_directory = self.staging_root / relative_directory
            final_directory = self.dataset_root / relative_directory
            final_directory.parent.mkdir(parents=True, exist_ok=True)
            if final_directory.exists():
                raise FileExistsError(
                    f"quarantine run directory already exists: {final_directory}"
                )
            os.rename(staging_directory, final_directory)
            fragments.extend(str(relative_directory / name) for name in state.files)
        return fragments

    def _flush_quarantine(self, state: QuarantineState) -> None:
        if not state.records:
            return
        relative_directory = (
            Path("_quarantine")
            / f"reason={quote(state.reason, safe='-_.')}"
            / f"date={state.date}"
            / f"run={quote(self.config.run_id, safe='-_.')}"
        )
        part_name = f"part-{len(state.files):05d}.parquet"
        staging_path = self.staging_root / relative_directory / part_name
        temporary_path = staging_path.with_name(f".{part_name}.tmp")
        temporary_path.parent.mkdir(parents=True, exist_ok=True)
        table = _quarantine_table(state.records, self.schema_metadata)
        pq.write_table(table, temporary_path, compression="zstd", use_dictionary=True)
        os.replace(temporary_path, staging_path)
        state.files.append(part_name)
        state.records.clear()


HourlyDatasetWriter = DailyDatasetWriter


def _partition_path(sensor: str, vsn: str, instrument_id: str, date_text: str) -> Path:
    return Path(
        f"sensor={quote(sensor, safe='-_.')}",
        f"vsn={quote(vsn, safe='-_.')}",
        f"instrument={quote(instrument_id, safe='-_.')}",
        f"date={date_text}",
    )


def _buffer_to_table(state: PartitionState, schema: pa.Schema) -> pa.Table:
    buffer = state.buffer
    size = len(buffer)
    arrays: list[pa.Array] = [
        pa.array(buffer.times, type=pa.timestamp("ns", tz="UTC")),
        pa.repeat(state.sensor, size),
        pa.repeat(state.vsn, size),
        pa.repeat(state.instrument_id, size),
        pa.array(buffer.measurements, type=pa.string()),
        pa.array(buffer.fields, type=pa.string()),
        pa.array(buffer.series_ids, type=pa.binary(16)),
        pa.array(buffer.value_types, type=pa.string()),
    ]
    observed_types = set(buffer.value_types)
    arrow_types = {
        "float64": pa.float64(),
        "int64": pa.int64(),
        "uint64": pa.uint64(),
        "bool": pa.bool_(),
        "string": pa.string(),
    }
    for candidate in VALUE_TYPES:
        if candidate not in observed_types:
            arrays.append(pa.nulls(size, type=arrow_types[candidate]))
        elif len(observed_types) == 1:
            arrays.append(pa.array(buffer.values, type=arrow_types[candidate]))
        else:
            arrays.append(
                pa.array(
                    [
                        value if value_type == candidate else None
                        for value_type, value in zip(buffer.value_types, buffer.values, strict=True)
                    ],
                    type=arrow_types[candidate],
                )
            )
    return pa.Table.from_arrays(arrays, schema=schema)


def _quarantine_table(
    records: list[QuarantineRecord], metadata: dict[bytes, bytes]
) -> pa.Table:
    schema = pa.schema(
        [
            pa.field("time", pa.timestamp("ns", tz="UTC"), nullable=False),
            pa.field("measurement", pa.string(), nullable=False),
            pa.field("field", pa.string(), nullable=False),
            pa.field("series_id", pa.binary(16), nullable=False),
            pa.field("value_type", pa.string(), nullable=False),
            pa.field("value_float64", pa.float64()),
            pa.field("value_int64", pa.int64()),
            pa.field("value_uint64", pa.uint64()),
            pa.field("value_bool", pa.bool_()),
            pa.field("value_string", pa.string()),
            pa.field("tags", pa.map_(pa.string(), pa.string()), nullable=False),
        ],
        metadata=metadata,
    )
    arrays: list[pa.Array] = [
        pa.array([record.time_ns for record in records], type=pa.timestamp("ns", tz="UTC")),
        pa.array([record.measurement for record in records]),
        pa.array([record.field for record in records]),
        pa.array([record.series_id for record in records], type=pa.binary(16)),
        pa.array([record.value_type for record in records]),
    ]
    arrow_types = {
        "float64": pa.float64(),
        "int64": pa.int64(),
        "uint64": pa.uint64(),
        "bool": pa.bool_(),
        "string": pa.string(),
    }
    for candidate in VALUE_TYPES:
        arrays.append(
            pa.array(
                [record.value if record.value_type == candidate else None for record in records],
                type=arrow_types[candidate],
            )
        )
    arrays.append(pa.array([record.tags for record in records], type=pa.map_(pa.string(), pa.string())))
    return pa.Table.from_arrays(arrays, schema=schema)


def _date_from_ns(time_ns: int) -> str:
    return datetime.fromtimestamp(time_ns // 1_000_000_000, tz=UTC).date().isoformat()


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
