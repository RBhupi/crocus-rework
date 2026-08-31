from __future__ import annotations

import secrets
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import pyarrow as pa

from adqat.compile import QCFlagContext, compile_findings
from adqat.config import LoadedConfig, ResolvedConfig, load_config
from adqat.minute import aggregate_one_minute
from adqat.periods import Period, iter_periods
from adqat.pointblank import EngineResult, run_pointblank
from adqat.source import select_period, validate_source
from adqat.store import RunStore, open_existing_run

EngineFunction = Callable[[pl.DataFrame, pa.Schema, ResolvedConfig, str], EngineResult]
ENGINE_REGISTRY: dict[str, EngineFunction] = {"pointblank": run_pointblank}


@dataclass
class RunSummary:
    run_dir: Path
    processed_periods: int = 0
    skipped_periods: int = 0
    empty_periods: int = 0
    findings: int = 0
    flagged_observations: int = 0
    minute_rows: int = 0
    missing_minute_rows: int = 0
    flagged_minute_rows: int = 0
    warnings: list[str] = field(default_factory=list)


def validate_configuration(path: str | Path) -> LoadedConfig:
    loaded = load_config(path)
    for work_unit in loaded.run.work_units:
        validate_source(loaded.resolve_work_unit(work_unit.id))
    return loaded


def run_new(
    path: str | Path,
    work_unit_id: str,
    run_id: str | None = None,
) -> RunSummary:
    loaded = load_config(path)
    config = loaded.resolve_work_unit(work_unit_id)
    validate_source(config)
    actual_run_id = run_id or generate_run_id()
    store = RunStore.create(config, actual_run_id)
    return _run_periods(store, config, validate_before_processing=False)


def resume_run(run_dir: str | Path) -> RunSummary:
    existing = open_existing_run(run_dir)
    return _run_periods(existing.store, existing.config, validate_before_processing=True)


def compile_run(run_dir: str | Path, period_id: str | None = None) -> int:
    existing = open_existing_run(run_dir)
    period_directories = existing.store.list_period_dirs()
    if period_id is not None:
        period_directories = [path for path in period_directories if path.name == period_id]
        if not period_directories:
            raise ValueError(f"unknown completed period {period_id!r}")
    for directory in period_directories:
        compile_findings(
            directory / "findings.parquet",
            directory / "qc_flags.parquet",
            existing.config.run.source.observation_keys,
            _flag_context(existing.config),
            atomic=True,
        )
    return len(period_directories)


def generate_run_id() -> str:
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    return f"{timestamp}-{secrets.token_hex(6)}"


def _run_periods(
    store: RunStore,
    config: ResolvedConfig,
    *,
    validate_before_processing: bool,
) -> RunSummary:
    summary = RunSummary(store.run_dir)
    source_validated = not validate_before_processing
    periods = iter_periods(
        config.run.selection.start,
        config.run.selection.end,
        config.run.processing.period,
    )
    for period in periods:
        if store.completed(period, config.config_hash):
            summary.skipped_periods += 1
            continue
        if not source_validated:
            validate_source(config)
            source_validated = True
        _run_period(store, config, period, summary)
    return summary


def _run_period(
    store: RunStore,
    config: ResolvedConfig,
    period: Period,
    summary: RunSummary,
) -> None:
    selected = select_period(config, period)
    if selected.row_count == 0:
        summary.empty_periods += 1
        summary.warnings.append(f"period {period.id} contained no matching observations")
    result = _run_pipeline(selected.data, selected.key_schema, config, store.run_id)
    minute_data = (
        aggregate_one_minute(selected.data, result, config, period, store.run_id)
        if config.run.processing.aggregation == "1minute"
        else None
    )
    _, flagged_observations = store.persist_period(
        period, selected, result, config, minute_data=minute_data
    )
    summary.processed_periods += 1
    summary.findings += result.findings.height
    summary.flagged_observations += flagged_observations
    if minute_data is not None:
        summary.minute_rows += minute_data.height
        summary.missing_minute_rows += minute_data.filter(
            pl.col("total_count") == 0
        ).height
        summary.flagged_minute_rows += minute_data.filter(pl.col("qc_bits") != 0).height


def _run_pipeline(
    data: pl.DataFrame,
    key_schema: pa.Schema,
    config: ResolvedConfig,
    run_id: str,
) -> EngineResult:
    stage = config.pipeline.stages[0]
    try:
        engine = ENGINE_REGISTRY[stage.engine]
    except KeyError as error:  # pragma: no cover - configuration currently constrains the value
        raise ValueError(f"unknown engine {stage.engine!r}") from error
    return engine(data, key_schema, config, run_id)


def _flag_context(config: ResolvedConfig) -> QCFlagContext:
    filters = config.work_unit.filters
    return QCFlagContext(
        sensor=str(filters["sensor"]),
        vsn=str(filters["vsn"]),
        instrument_id=str(filters["instrument_id"]),
        config_hash=config.config_hash,
    )
