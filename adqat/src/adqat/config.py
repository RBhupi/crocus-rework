from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import yaml
from pydantic import BaseModel, ConfigDict, Field, PositiveFloat, model_validator

SAFE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
GLOB_CHARS = re.compile(r"[*?\[]")
Scalar = str | int | float | bool
AggregationMethod = Literal["mean", "circular_mean", "mode", "last"]


class ConfigError(ValueError):
    """Raised when the combined ADQAT configuration is invalid."""


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class FlagDefinition(StrictModel):
    bit: int = Field(ge=0, le=63)
    description: str = Field(min_length=1)


class CheckDefinition(StrictModel):
    id: str = Field(min_length=1)
    method: Literal["col_vals_not_null", "col_vals_between"]
    flag: str = Field(min_length=1)
    args: dict[str, Any] = Field(default_factory=dict)
    thresholds: dict[Literal["warning", "error", "critical"], float] | None = None

    @model_validator(mode="after")
    def validate_method_arguments(self) -> CheckDefinition:
        if self.method == "col_vals_between":
            if "left" not in self.args or "right" not in self.args:
                raise ValueError("col_vals_between requires args.left and args.right")
            if self.args.get("na_pass") is False:
                raise ValueError("range checks must pass nulls; na_pass=false is not allowed")
        elif self.args:
            raise ValueError("col_vals_not_null does not accept args in ADQAT Version 1")
        if self.thresholds:
            for name, value in self.thresholds.items():
                if not 0 <= value <= 1:
                    raise ValueError(f"threshold {name} must be between 0 and 1")
        return self


class SamplingDefinition(StrictModel):
    expected_frequency_hz: PositiveFloat


class RulesMetadata(StrictModel):
    status: Literal["demo", "pilot", "approved"]
    description: str = Field(min_length=1)
    references: dict[str, str] = Field(default_factory=dict)


class VariableDefinition(StrictModel):
    column: str = Field(min_length=1)
    data_type: Literal["numeric", "string"] = "numeric"
    where: dict[str, Scalar]
    units: str | None = None
    missing_values: list[float] = Field(default_factory=list)
    missing_strings: list[str] = Field(default_factory=list)
    aggregation: AggregationMethod | None = None
    checks: list[CheckDefinition] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_long_selector(self) -> VariableDefinition:
        for key in ("measurement", "field"):
            if key not in self.where or not isinstance(self.where[key], str) or not self.where[key]:
                raise ValueError(f"variable selector requires a non-empty {key!r}")
        if any(not math.isfinite(value) for value in self.missing_values):
            raise ValueError("configured missing values must be finite; NaN is always normalized")
        if len(self.missing_values) != len(set(self.missing_values)):
            raise ValueError("configured missing values must be unique")
        if len(self.missing_strings) != len(set(self.missing_strings)):
            raise ValueError("configured missing strings must be unique")
        if self.data_type == "numeric" and self.missing_strings:
            raise ValueError("numeric variables cannot declare missing_strings")
        if self.data_type == "string":
            if self.missing_values:
                raise ValueError("string variables cannot declare numeric missing_values")
            if any(check.method != "col_vals_not_null" for check in self.checks):
                raise ValueError("string variables support only col_vals_not_null")
            if self.aggregation not in (None, "mode", "last"):
                raise ValueError("string variables support only mode or last aggregation")
        if self.aggregation == "circular_mean" and self.data_type != "numeric":
            raise ValueError("circular_mean aggregation requires a numeric variable")
        return self


class ProfileDefinition(StrictModel):
    sampling: SamplingDefinition | None = None
    variables: dict[str, VariableDefinition] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_profile(self) -> ProfileDefinition:
        selectors: set[tuple[str, str]] = set()
        check_ids: set[str] = set()
        for variable in self.variables.values():
            selector = (str(variable.where["measurement"]), str(variable.where["field"]))
            if selector in selectors:
                raise ValueError(f"duplicate measurement/field selector: {selector!r}")
            selectors.add(selector)
            for check in variable.checks:
                if check.id in check_ids:
                    raise ValueError(f"duplicate check ID: {check.id!r}")
                check_ids.add(check.id)
        return self


class StageDefinition(StrictModel):
    id: str = Field(min_length=1)
    engine: Literal["pointblank"]


class PipelineDefinition(StrictModel):
    stages: list[StageDefinition] = Field(min_length=1, max_length=1)

    @model_validator(mode="after")
    def validate_stage_ids(self) -> PipelineDefinition:
        ids = [stage.id for stage in self.stages]
        if len(ids) != len(set(ids)):
            raise ValueError("pipeline stage IDs must be unique")
        return self


class QualityRules(StrictModel):
    schema_version: Literal[1]
    metadata: RulesMetadata | None = None
    flags: dict[str, FlagDefinition] = Field(min_length=1)
    profiles: dict[str, ProfileDefinition] = Field(min_length=1)
    pipelines: dict[str, PipelineDefinition] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_rules(self) -> QualityRules:
        bits = [flag.bit for flag in self.flags.values()]
        if len(bits) != len(set(bits)):
            raise ValueError("flag bit numbers must be unique")
        for profile_name, profile in self.profiles.items():
            for variable in profile.variables.values():
                for check in variable.checks:
                    if check.flag not in self.flags:
                        raise ValueError(
                            f"profile {profile_name!r} check {check.id!r} references "
                            f"unknown flag {check.flag!r}"
                        )
        return self


class ParquetOptions(StrictModel):
    hive_partitioning: bool = False
    union_by_name: bool = True


class TimeDefinition(StrictModel):
    column: str = Field(min_length=1)
    timezone: str = Field(min_length=1)

    @model_validator(mode="after")
    def validate_timezone(self) -> TimeDefinition:
        try:
            ZoneInfo(self.timezone)
        except ZoneInfoNotFoundError as error:
            raise ValueError(f"unknown timezone {self.timezone!r}") from error
        return self


class SourceDefinition(StrictModel):
    type: Literal["parquet"]
    path: str = Field(min_length=1)
    options: ParquetOptions = Field(default_factory=ParquetOptions)
    time: TimeDefinition
    observation_keys: list[str] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_observation_keys(self) -> SourceDefinition:
        if len(self.observation_keys) != len(set(self.observation_keys)):
            raise ValueError("observation keys must be unique")
        if self.time.column not in self.observation_keys:
            raise ValueError("the configured time column must be an observation key")
        return self


class QualityReference(StrictModel):
    rules: str = Field(min_length=1)
    pipeline: str = Field(min_length=1)


class SelectionDefinition(StrictModel):
    start: datetime
    end: datetime

    @model_validator(mode="after")
    def validate_interval(self) -> SelectionDefinition:
        if self.start.tzinfo is None or self.end.tzinfo is None:
            raise ValueError("selection start and end must be timezone-aware")
        self.start = self.start.astimezone(UTC)
        self.end = self.end.astimezone(UTC)
        if self.start >= self.end:
            raise ValueError("selection start must be before end")
        return self


class ProcessingDefinition(StrictModel):
    period: Literal["1h", "1d", "1month", "1year", "all"]
    aggregation: Literal["1minute"] | None = None


class WorkUnitDefinition(StrictModel):
    id: str
    profile: str = Field(min_length=1)
    filters: dict[str, Scalar] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_id(self) -> WorkUnitDefinition:
        if not SAFE_ID.fullmatch(self.id):
            raise ValueError("work-unit ID must be path-safe and at most 128 characters")
        for name in ("sensor", "vsn", "instrument_id"):
            value = self.filters.get(name)
            if not isinstance(value, str) or not value:
                raise ValueError(
                    f"work unit requires a non-empty string filter for {name!r}"
                )
        return self


class NetCDFDefinition(StrictModel):
    enabled: Literal[True]
    site: str = Field(pattern=r"^[a-z]{3,8}$")
    instrument: str = Field(pattern=r"^[a-z0-9]{3,16}$")


class OutputDefinition(StrictModel):
    root: str = Field(min_length=1)
    netcdf: NetCDFDefinition | None = None


class ProcessingRun(StrictModel):
    schema_version: Literal[1]
    source: SourceDefinition
    quality: QualityReference
    selection: SelectionDefinition
    processing: ProcessingDefinition
    work_units: list[WorkUnitDefinition] = Field(min_length=1)
    output: OutputDefinition

    @model_validator(mode="after")
    def validate_work_units(self) -> ProcessingRun:
        ids = [work_unit.id for work_unit in self.work_units]
        if len(ids) != len(set(ids)):
            raise ValueError("work-unit IDs must be unique")
        if self.output.netcdf is not None:
            if self.processing.aggregation is not None:
                raise ValueError("NetCDF output is not supported with 1-minute aggregation")
            if self.processing.period != "1d":
                raise ValueError("NetCDF output requires processing.period '1d'")
            for boundary_name, boundary in (
                ("selection.start", self.selection.start),
                ("selection.end", self.selection.end),
            ):
                if (boundary.hour, boundary.minute, boundary.second, boundary.microsecond) != (
                    0,
                    0,
                    0,
                    0,
                ):
                    raise ValueError(f"NetCDF output requires {boundary_name} at UTC midnight")
        return self


@dataclass(frozen=True)
class LoadedConfig:
    run: ProcessingRun
    rules: QualityRules
    run_path: Path
    rules_path: Path
    source_path: str
    output_root: Path

    def resolve_work_unit(self, work_unit_id: str) -> ResolvedConfig:
        matches = [item for item in self.run.work_units if item.id == work_unit_id]
        if not matches:
            raise ConfigError(f"unknown work unit {work_unit_id!r}")
        work_unit = matches[0]
        profile = self.rules.profiles[work_unit.profile]
        pipeline = self.rules.pipelines[self.run.quality.pipeline]
        resolved = ResolvedConfig(self, work_unit, profile, pipeline, "")
        return ResolvedConfig(
            self,
            work_unit,
            profile,
            pipeline,
            _configuration_hash(resolved),
        )


@dataclass(frozen=True)
class ResolvedConfig:
    loaded: LoadedConfig
    work_unit: WorkUnitDefinition
    profile: ProfileDefinition
    pipeline: PipelineDefinition
    config_hash: str

    @property
    def run(self) -> ProcessingRun:
        return self.loaded.run

    @property
    def rules(self) -> QualityRules:
        return self.loaded.rules

    @property
    def source_path(self) -> str:
        return self.loaded.source_path

    @property
    def output_root(self) -> Path:
        return self.loaded.output_root


def load_config(path: str | Path) -> LoadedConfig:
    run_path = Path(path).expanduser().resolve()
    run_data = _read_yaml(run_path)
    try:
        run = ProcessingRun.model_validate(run_data)
    except Exception as error:
        raise ConfigError(f"invalid processing configuration: {error}") from error

    rules_path = _resolve_path(run_path.parent, run.quality.rules)
    try:
        rules = QualityRules.model_validate(_read_yaml(rules_path))
    except Exception as error:
        raise ConfigError(f"invalid quality rules: {error}") from error

    if run.quality.pipeline not in rules.pipelines:
        raise ConfigError(f"unknown pipeline {run.quality.pipeline!r}")
    for work_unit in run.work_units:
        if work_unit.profile not in rules.profiles:
            raise ConfigError(
                f"work unit {work_unit.id!r} references unknown profile {work_unit.profile!r}"
            )
        if run.processing.aggregation == "1minute":
            missing_aggregation = [
                name
                for name, variable in rules.profiles[work_unit.profile].variables.items()
                if variable.aggregation is None
            ]
            if missing_aggregation:
                raise ConfigError(
                    f"work unit {work_unit.id!r} uses 1-minute aggregation but variables "
                    f"lack an aggregation method: {', '.join(missing_aggregation)}"
                )

    source_path = _resolve_glob(run_path.parent, run.source.path)
    output_root = _resolve_path(run_path.parent, run.output.root)
    _validate_output_separation(source_path, output_root)
    return LoadedConfig(run, rules, run_path, rules_path, source_path, output_root)


def validate_run_id(run_id: str) -> str:
    if not SAFE_ID.fullmatch(run_id):
        raise ConfigError("run ID must be path-safe and at most 128 characters")
    return run_id


def snapshot_documents(loaded: LoadedConfig) -> tuple[dict[str, Any], dict[str, Any]]:
    rules_document = loaded.rules.model_dump(mode="json")
    run_document = loaded.run.model_dump(mode="json")
    run_document["source"]["path"] = loaded.source_path
    run_document["quality"]["rules"] = "quality_rules.yaml"
    run_document["output"]["root"] = str(loaded.output_root)
    return rules_document, run_document


def dump_yaml(document: dict[str, Any]) -> str:
    return str(yaml.safe_dump(document, sort_keys=False, allow_unicode=True))


def _configuration_hash(config: ResolvedConfig) -> str:
    document = {
        "quality_rules": config.rules.model_dump(mode="json"),
        "source": {
            **config.run.source.model_dump(mode="json"),
            "path": config.source_path,
        },
        "pipeline": config.run.quality.pipeline,
        "work_unit": config.work_unit.model_dump(mode="json"),
        "selection": config.run.selection.model_dump(mode="json"),
        "processing": config.run.processing.model_dump(mode="json"),
        "netcdf": (
            config.run.output.netcdf.model_dump(mode="json")
            if config.run.output.netcdf is not None
            else None
        ),
    }
    payload = json.dumps(document, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(payload).hexdigest()


def _read_yaml(path: Path) -> dict[str, Any]:
    try:
        document = yaml.safe_load(path.read_text(encoding="utf-8"))
    except OSError as error:
        raise ConfigError(f"cannot read {path}: {error}") from error
    except yaml.YAMLError as error:
        raise ConfigError(f"invalid YAML in {path}: {error}") from error
    if not isinstance(document, dict):
        raise ConfigError(f"{path} must contain a YAML mapping")
    return document


def _resolve_path(base: Path, value: str) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = base / path
    return path.resolve()


def _resolve_glob(base: Path, value: str) -> str:
    expanded = str(Path(value).expanduser())
    if Path(expanded).is_absolute():
        return expanded
    return str((base / expanded).resolve())


def _validate_output_separation(source_path: str, output_root: Path) -> None:
    match = GLOB_CHARS.search(source_path)
    prefix = source_path[: match.start()] if match else source_path
    source_base = Path(prefix.rstrip("/"))
    if match is None and not source_base.is_dir():
        source_base = source_base.parent
    source_base = source_base.resolve()
    if _contains(source_base, output_root) or _contains(output_root, source_base):
        raise ConfigError(f"output root {output_root} must not overlap source base {source_base}")


def _contains(parent: Path, child: Path) -> bool:
    try:
        child.relative_to(parent)
    except ValueError:
        return False
    return True
