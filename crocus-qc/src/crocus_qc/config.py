"""Configuration loading: frozen dataclasses over YAML. No Pydantic, no schema framework.

Two kinds of configuration are kept deliberately separate:

* **Scientific** (``SensorProfile``) -- stable instrument knowledge shipped with the
  package under ``profiles/``. For Stage 1 this is only: how to find a variable's rows
  in the long-format raw table, and how to reduce them.
* **Execution** (``PipelineConfig``) -- output location and DuckDB resource settings.

Stage 1 has no QA/QC, so there are no ranges, thresholds, or flag definitions here.
"""

from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass
from pathlib import Path

import yaml

PROFILE_DIR = Path(__file__).parent / "profiles"

#: Sentinel written by the raw ingest for "instrument reported no value".
#:
#: This is the *only* value-level normalization Stage 1 performs. It is not a QC check:
#: leaving -9999.9 in place would corrupt every mean, min, and standard deviation.
MISSING_SENTINEL = -9999.9

#: Tolerance for matching the float sentinel, which does not round-trip exactly.
SENTINEL_TOLERANCE = 1e-6

VALID_AGGREGATIONS = frozenset({"mean", "circular_mean", "mode", "last"})


@dataclass(frozen=True)
class VariableSpec:
    """One scientific variable within an instrument profile."""

    name: str
    measurement: str
    field: str
    value_type: str
    units: str
    aggregation: str
    data_type: str = "numeric"
    missing_strings: tuple[str, ...] = ()

    @property
    def is_string(self) -> bool:
        return self.data_type == "string"

    @property
    def has_spread_stats(self) -> bool:
        """Whether ``raw_min`` / ``raw_max`` / ``raw_std`` are scientifically meaningful.

        Only ``mean`` variables get all three. Circular variables get a circular
        ``raw_std`` but no min/max (ordering is undefined on a circle); ``mode`` and
        ``last`` variables get none, rather than carrying meaningless columns for the
        sake of a uniform schema.
        """
        return self.aggregation == "mean"


@dataclass(frozen=True)
class SensorProfile:
    sensor: str
    instrument_label: str
    variables: tuple[VariableSpec, ...]

    def variable(self, name: str) -> VariableSpec:
        for spec in self.variables:
            if spec.name == name:
                return spec
        raise KeyError(f"profile {self.sensor!r} has no variable {name!r}")


@dataclass(frozen=True)
class AggregationPeriod:
    """A DuckDB interval, written in native DuckDB syntax.

    ``raw`` goes straight into ``INTERVAL '...'``; ``label`` names the product; and
    ``seconds`` drives the dense grid step.
    """

    raw: str
    label: str
    seconds: int

    @property
    def rows_per_day(self) -> int:
        return 86_400 // self.seconds


#: Stage 1 is fixed at 10 seconds. Making this configurable is a one-line change, but
#: nothing currently requires it, and fixing it keeps the 8640-row invariant checkable.
TEN_SECONDS = AggregationPeriod(raw="10 seconds", label="10sec", seconds=10)


@dataclass(frozen=True)
class PipelineConfig:
    output_root: Path
    threads: int
    memory_limit: str
    temp_dir: str
    config_hash: str


def load_profile(sensor_or_path: str | Path) -> SensorProfile:
    """Load a profile by instrument label (``aqt530``) or by explicit path."""
    path = Path(sensor_or_path)
    if not path.suffix:
        path = PROFILE_DIR / f"{sensor_or_path}.yaml"
    if not path.is_file():
        available = sorted(p.stem for p in PROFILE_DIR.glob("*.yaml"))
        raise ValueError(f"no profile at {path}; bundled profiles: {available}")

    doc = yaml.safe_load(path.read_text())
    if not isinstance(doc, dict):
        raise ValueError(f"profile {path} is not a YAML mapping")

    raw_vars = doc.get("variables")
    if not isinstance(raw_vars, dict) or not raw_vars:
        raise ValueError(f"profile {path} defines no variables")

    specs: list[VariableSpec] = []
    for name, body in raw_vars.items():
        if not isinstance(body, dict):
            raise ValueError(f"profile {path}: variable {name!r} is not a mapping")
        aggregation = body.get("aggregation")
        if aggregation not in VALID_AGGREGATIONS:
            raise ValueError(
                f"profile {path}: variable {name!r} has aggregation {aggregation!r}; "
                f"expected one of {sorted(VALID_AGGREGATIONS)}"
            )
        data_type = body.get("data_type", "numeric")
        if data_type not in {"numeric", "string"}:
            raise ValueError(f"profile {path}: variable {name!r} has data_type {data_type!r}")
        if data_type == "string" and aggregation != "last":
            raise ValueError(
                f"profile {path}: string variable {name!r} only supports aggregation 'last'"
            )

        specs.append(
            VariableSpec(
                name=name,
                measurement=str(body["measurement"]),
                field=str(body.get("field", "value")),
                value_type=str(body.get("value_type", "float64")),
                units=str(body.get("units", "1")),
                aggregation=aggregation,
                data_type=data_type,
                missing_strings=tuple(body.get("missing_strings", ()) or ()),
            )
        )

    return SensorProfile(
        sensor=str(doc["sensor"]),
        instrument_label=str(doc["instrument_label"]),
        variables=tuple(specs),
    )


def resolve_threads(configured: int | None, env: dict[str, str] | None = None) -> int:
    env = os.environ if env is None else env
    if configured:
        return int(configured)
    slurm = env.get("SLURM_CPUS_PER_TASK")
    if slurm and slurm.isdigit() and int(slurm) > 0:
        return int(slurm)
    return os.cpu_count() or 1


def resolve_memory_limit(configured: str | None, env: dict[str, str] | None = None) -> str:
    """Resolve the DuckDB ``memory_limit``.

    SLURM reports ``SLURM_MEM_PER_NODE`` in megabytes. Only 80% is handed to DuckDB so
    the Python process has headroom inside the cgroup.
    """
    env = os.environ if env is None else env
    if configured:
        return str(configured)
    for key in ("SLURM_MEM_PER_NODE", "SLURM_MEM_PER_CPU"):
        raw = env.get(key, "")
        if raw.isdigit() and int(raw) > 0:
            total_mb = int(raw)
            if key == "SLURM_MEM_PER_CPU":
                total_mb *= resolve_threads(None, env)
            usable_gb = max(1, int(total_mb * 0.8) // 1024)
            return f"{usable_gb}GB"
    return "8GB"


def resolve_temp_dir(configured: str | None, env: dict[str, str] | None = None) -> str:
    env = os.environ if env is None else env
    return configured or env.get("TMPDIR") or env.get("SCRATCH") or "/tmp"


def load_config(path: str | Path, env: dict[str, str] | None = None) -> PipelineConfig:
    path = Path(path)
    text = path.read_text()
    doc = yaml.safe_load(text)
    if not isinstance(doc, dict):
        raise ValueError(f"config {path} is not a YAML mapping")

    output = doc.get("output") or {}
    if not isinstance(output, dict) or "root" not in output:
        raise ValueError(f"config {path}: output.root is required")

    execution = doc.get("execution") or {}
    if not isinstance(execution, dict):
        raise ValueError(f"config {path}: execution is not a mapping")

    return PipelineConfig(
        output_root=Path(output["root"]).expanduser(),
        threads=resolve_threads(execution.get("threads"), env),
        memory_limit=resolve_memory_limit(execution.get("memory_limit"), env),
        temp_dir=resolve_temp_dir(execution.get("temp_dir"), env),
        config_hash=hashlib.sha256(text.encode()).hexdigest()[:16],
    )
