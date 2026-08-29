from __future__ import annotations

import hashlib
import importlib.metadata
import json
import os
import shutil
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from adqat import __version__
from adqat.compile import compile_findings
from adqat.config import (
    ConfigError,
    ResolvedConfig,
    dump_yaml,
    load_config,
    snapshot_documents,
    validate_run_id,
)
from adqat.findings import check_results_schema, findings_schema, write_frame
from adqat.netcdf import native_a1_filename, write_native_a1
from adqat.periods import Period
from adqat.pointblank import EngineResult
from adqat.source import SelectedPeriod


class StoreError(RuntimeError):
    """Raised when ADQAT-owned output cannot be persisted safely."""


@dataclass(frozen=True)
class ExistingRun:
    store: RunStore
    config: ResolvedConfig
    metadata: dict[str, Any]


class RunStore:
    def __init__(self, run_dir: Path, run_id: str, work_unit_id: str):
        self.run_dir = run_dir.resolve()
        self.run_id = run_id
        self.work_unit_id = work_unit_id
        self.staging_root = self.run_dir / ".staging"
        self.work_unit_root = self.run_dir / "work_units" / work_unit_id

    @classmethod
    def create(cls, config: ResolvedConfig, run_id: str) -> RunStore:
        validate_run_id(run_id)
        run_dir = (config.output_root / "runs" / run_id).resolve()
        _require_descendant(run_dir, config.output_root)
        if run_dir.exists():
            raise StoreError(f"run {run_id!r} already exists; use 'adqat resume {run_dir}'")
        run_dir.mkdir(parents=True)
        store = cls(run_dir, run_id, config.work_unit.id)
        store.staging_root.mkdir()
        store.work_unit_root.mkdir(parents=True)
        store._write_run_files(config)
        return store

    def completed(self, period: Period, config_hash: str) -> bool:
        marker = self.period_dir(period) / "success.json"
        if not marker.is_file():
            return False
        try:
            document = json.loads(marker.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return False
        return bool(
            document.get("status") == "success"
            and document.get("run_id") == self.run_id
            and document.get("work_unit_id") == self.work_unit_id
            and document.get("period_start") == period.start.isoformat()
            and document.get("period_end") == period.end.isoformat()
            and document.get("config_hash") == config_hash
        )

    def persist_period(
        self,
        period: Period,
        selected: SelectedPeriod,
        engine_result: EngineResult,
        config: ResolvedConfig,
    ) -> Path:
        final_dir = self.period_dir(period)
        _require_descendant(final_dir, self.run_dir)
        self._remove_incomplete_period(final_dir)
        self._remove_stale_staging(period)
        stage = self.staging_root / f"{period.id}-{uuid.uuid4().hex}"
        stage.mkdir(parents=False)
        try:
            findings_path = stage / "findings.parquet"
            write_frame(
                engine_result.findings,
                findings_schema(selected.key_schema),
                findings_path,
            )
            write_frame(
                engine_result.check_results,
                check_results_schema(),
                stage / "check_results.parquet",
            )
            compile_findings(
                findings_path,
                stage / "qc_flags.parquet",
                config.run.source.observation_keys,
            )
            fingerprint, file_count = input_fingerprint(selected.source_files)
            netcdf_name: str | None = None
            if config.run.output.netcdf is not None:
                netcdf_name = native_a1_filename(config, period)
                write_native_a1(
                    selected.data,
                    stage / "qc_flags.parquet",
                    stage / netcdf_name,
                    period,
                    config,
                    self.run_id,
                    fingerprint,
                    file_count,
                )
            success = {
                "status": "success",
                "run_id": self.run_id,
                "work_unit_id": self.work_unit_id,
                "period_start": period.start.isoformat(),
                "period_end": period.end.isoformat(),
                "rows_processed": selected.row_count,
                "findings": engine_result.findings.height,
                "input_fingerprint": fingerprint,
                "source_file_count": file_count,
                "netcdf_file": netcdf_name,
                "config_hash": config.config_hash,
                "completed_at": datetime.now(UTC).isoformat(),
            }
            _write_json_atomic(stage / "success.json", success)
            if final_dir.exists():
                raise StoreError(f"period output appeared concurrently: {final_dir}")
            os.rename(stage, final_dir)
        except Exception:
            # Keep staging evidence for diagnosis; resume removes it before recomputation.
            raise
        return final_dir

    def period_dir(self, period: Period) -> Path:
        return self.work_unit_root / period.id

    def list_period_dirs(self) -> list[Path]:
        if not self.work_unit_root.exists():
            return []
        return sorted(
            path
            for path in self.work_unit_root.iterdir()
            if path.is_dir() and (path / "findings.parquet").is_file()
        )

    def _write_run_files(self, config: ResolvedConfig) -> None:
        rules_document, run_document = snapshot_documents(config.loaded)
        _write_text_atomic(self.run_dir / "quality_rules.yaml", dump_yaml(rules_document))
        _write_text_atomic(self.run_dir / "processing_run.yaml", dump_yaml(run_document))
        metadata = {
            "run_id": self.run_id,
            "work_unit_id": self.work_unit_id,
            "config_hash": config.config_hash,
            "source": config.source_path,
            "pipeline": config.run.quality.pipeline,
            "selection_start": config.run.selection.start.isoformat(),
            "selection_end": config.run.selection.end.isoformat(),
            "processing_period": config.run.processing.period,
            "netcdf": (
                config.run.output.netcdf.model_dump(mode="json")
                if config.run.output.netcdf is not None
                else None
            ),
            "software_version": __version__,
            "dependencies": _dependency_versions(),
            "created_at": datetime.now(UTC).isoformat(),
        }
        _write_json_atomic(self.run_dir / "run.json", metadata)

    def _remove_incomplete_period(self, final_dir: Path) -> None:
        if not final_dir.exists():
            return
        _require_descendant(final_dir, self.run_dir)
        shutil.rmtree(final_dir)

    def _remove_stale_staging(self, period: Period) -> None:
        for path in self.staging_root.glob(f"{period.id}-*"):
            _require_descendant(path, self.run_dir)
            if path.is_dir():
                shutil.rmtree(path)


def open_existing_run(run_dir: str | Path) -> ExistingRun:
    directory = Path(run_dir).expanduser().resolve()
    try:
        metadata = json.loads((directory / "run.json").read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise StoreError(f"cannot load run metadata from {directory}: {error}") from error
    run_id = metadata.get("run_id")
    work_unit_id = metadata.get("work_unit_id")
    if not isinstance(run_id, str) or not isinstance(work_unit_id, str):
        raise StoreError("run.json is missing run_id or work_unit_id")
    try:
        loaded = load_config(directory / "processing_run.yaml")
        config = loaded.resolve_work_unit(work_unit_id)
    except ConfigError as error:
        raise StoreError(f"invalid snapshotted configuration: {error}") from error
    if config.config_hash != metadata.get("config_hash"):
        raise StoreError("snapshotted configuration hash does not match run.json")
    return ExistingRun(RunStore(directory, run_id, work_unit_id), config, metadata)


def input_fingerprint(paths: tuple[Path, ...]) -> tuple[str, int]:
    records: list[dict[str, Any]] = []
    for path in sorted(paths):
        stat = path.stat()
        records.append({"path": str(path), "size": stat.st_size, "mtime_ns": stat.st_mtime_ns})
    payload = json.dumps(records, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(payload).hexdigest(), len(records)


def _dependency_versions() -> dict[str, str]:
    versions: dict[str, str] = {}
    for distribution in (
        "duckdb",
        "netCDF4",
        "numpy",
        "pointblank",
        "polars",
        "pyarrow",
        "pydantic",
    ):
        try:
            versions[distribution] = importlib.metadata.version(distribution)
        except importlib.metadata.PackageNotFoundError:
            versions[distribution] = "not-installed"
    return versions


def _require_descendant(path: Path, parent: Path) -> None:
    resolved_path = path.resolve()
    resolved_parent = parent.resolve()
    try:
        resolved_path.relative_to(resolved_parent)
    except ValueError as error:
        raise StoreError(f"refusing to write outside {resolved_parent}: {resolved_path}") from error
    if resolved_path == resolved_parent:
        raise StoreError(f"refusing to treat output root as an artifact path: {resolved_path}")


def _write_json_atomic(path: Path, document: dict[str, Any]) -> None:
    _write_text_atomic(path, json.dumps(document, indent=2, sort_keys=True) + "\n")


def _write_text_atomic(path: Path, value: str) -> None:
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        temporary.write_text(value, encoding="utf-8")
        os.replace(temporary, path)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise
