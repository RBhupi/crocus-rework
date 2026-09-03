"""Work-unit orchestration: build SQL, run DuckDB, finalize atomically, record provenance.

One invocation processes one work unit (sensor x VSN x instrument x UTC day). There is
no internal concurrency across days or VSNs: DuckDB provides node-level parallelism and
SLURM provides cluster-level parallelism.

Python does no analytical work here. It never iterates raw rows, never computes a
statistic, and never materializes the raw dataset -- the whole reduction happens inside
one DuckDB statement that ends in ``COPY ... TO``.

Every phase is timed and the breakdown is written into ``_success.json``, so a slow day
on the cluster can be diagnosed from its output directory alone.
"""

from __future__ import annotations

import os
from datetime import date as Date
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

from . import __version__
from .config import TEN_SECONDS, AggregationPeriod, PipelineConfig, SensorProfile
from .provenance import SUCCESS_NAME, git_commit, read_provenance, write_json_atomic
from .reduce import FACTS_DIR, build_stage1_sql, raw_glob, session_setup_sql
from .timing import Stopwatch

PRODUCT_NAME = "10sec.parquet"
PROFILE_NAME = "_duckdb_profile.json"


def work_unit_dir(config: PipelineConfig, sensor: str, vsn: str, day: Date) -> Path:
    return config.output_root / sensor / vsn / f"{day:%Y-%m-%d}"


def _open_session(
    config: PipelineConfig, profile_output: Path | None = None
) -> duckdb.DuckDBPyConnection:
    """A DuckDB connection with this job's resource settings applied.

    The temp directory is created eagerly: DuckDB does not create it, and discovering
    that it is missing only when a large day spills would fail the job hours in.

    ``profile_output`` turns on DuckDB's own profiler, which breaks the statement down
    per operator -- scan vs. GROUP BY vs. join. The phase timings say *which phase* was
    slow; this says *which operator within the query* was slow.
    """
    Path(config.temp_dir).mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect()
    conn.execute(session_setup_sql(config.threads, config.memory_limit, config.temp_dir))
    if profile_output is not None:
        profile_output.parent.mkdir(parents=True, exist_ok=True)
        conn.execute("SET enable_profiling = 'json';")
        conn.execute(f"SET profiling_output = '{profile_output}';")
    return conn


def _assert_output_is_not_inside_dataset(output_root: Path, dataset_root: Path) -> None:
    """Refuse to write anywhere under the read-only production facts tree.

    The raw dataset is immutable input for every downstream product; a stray output
    root under it would corrupt the source for every other job on the cluster.
    """
    output = output_root.expanduser().resolve()
    dataset = dataset_root.expanduser().resolve()
    if output == dataset or dataset in output.parents:
        raise ValueError(
            f"refusing to run: output root {output} is inside the raw dataset "
            f"{dataset}; the raw dataset must never be written to"
        )


def run_work_unit(
    *,
    sensor: str,
    vsn: str,
    day: Date,
    dataset_root: Path,
    config: PipelineConfig,
    profile: SensorProfile,
    period: AggregationPeriod = TEN_SECONDS,
    force: bool = False,
    stopwatch: Stopwatch | None = None,
    sql_profile: bool = False,
) -> dict[str, Any]:
    """Reduce one sensor/VSN/day to a dense 10-second Parquet product.

    ``stopwatch`` may be supplied by the caller so phases it measured first (loading
    config and profile) appear in the same breakdown.
    """
    watch = stopwatch if stopwatch is not None else Stopwatch()
    _assert_output_is_not_inside_dataset(config.output_root, dataset_root)

    out_dir = work_unit_dir(config, sensor, vsn, day)
    success_path = out_dir / SUCCESS_NAME
    if success_path.exists() and not force:
        return read_provenance(success_path)

    out_dir.mkdir(parents=True, exist_ok=True)
    final_path = out_dir / PRODUCT_NAME
    tmp_path = out_dir / (PRODUCT_NAME + ".tmp")
    profile_path = out_dir / PROFILE_NAME if sql_profile else None

    with watch.phase("build_sql"):
        sql = build_stage1_sql(
            dataset_root=str(dataset_root),
            sensor=sensor,
            vsn=vsn,
            day=day,
            variables=profile.variables,
            period=period,
            output_path=str(tmp_path),
        )

    started = datetime.now(timezone.utc)
    with watch.phase("open_session"):
        conn = _open_session(config, profile_path)
    try:
        with watch.phase("execute_reduction"):
            conn.execute(sql)
        with watch.phase("read_duckdb_version"):
            duckdb_version = conn.execute("SELECT version()").fetchone()[0]
    finally:
        conn.close()

    # Publish only once the statement has completed: a killed job leaves a .tmp behind
    # and no _success.json, so the work unit is simply rerun.
    with watch.phase("publish"):
        os.replace(tmp_path, final_path)

    with watch.phase("verify_output"):
        with duckdb.connect() as conn:
            row_count = conn.execute(
                f"SELECT count(*) FROM read_parquet('{final_path}')"
            ).fetchone()[0]
    finished = datetime.now(timezone.utc)

    record = {
        "status": "success",
        "pipeline_version": __version__,
        "git_commit": git_commit(),
        "duckdb_version": duckdb_version,
        "config_hash": config.config_hash,
        "sensor": sensor,
        "vsn": vsn,
        "date": f"{day:%Y-%m-%d}",
        "aggregation_period": period.raw,
        "expected_rows": period.rows_per_day,
        "output_row_count": row_count,
        "variables": [spec.name for spec in profile.variables],
        "dataset_root": str(dataset_root),
        "input_glob": raw_glob(str(dataset_root), sensor, vsn, day),
        "output_path": str(final_path),
        "output_bytes": final_path.stat().st_size,
        "threads": config.threads,
        "memory_limit": config.memory_limit,
        "processing_start": started.isoformat().replace("+00:00", "Z"),
        "processing_end": finished.isoformat().replace("+00:00", "Z"),
        "elapsed_seconds": round((finished - started).total_seconds(), 3),
        "timings_seconds": watch.as_dict(),
    }
    if profile_path is not None:
        record["duckdb_profile_path"] = str(profile_path)

    with watch.phase("write_provenance"):
        write_json_atomic(record, success_path)
    # The provenance write is the last phase, so it cannot appear in the record it
    # writes; it is reported to the operator through the stopwatch instead.
    return record


def explain_work_unit(
    *,
    sensor: str,
    vsn: str,
    day: Date,
    dataset_root: Path,
    config: PipelineConfig,
    profile: SensorProfile,
    period: AggregationPeriod = TEN_SECONDS,
    analyze: bool = False,
) -> str:
    """Return the DuckDB query plan for a work unit, without publishing any product.

    ``analyze=True`` actually executes the statement (writing to a throwaway path in the
    temp directory) and returns the profiled plan with real timings and cardinalities.
    """
    Path(config.temp_dir).mkdir(parents=True, exist_ok=True)
    scratch = Path(config.temp_dir) / f"explain-{sensor}-{vsn}-{day:%Y%m%d}.parquet"
    sql = build_stage1_sql(
        dataset_root=str(dataset_root),
        sensor=sensor,
        vsn=vsn,
        day=day,
        variables=profile.variables,
        period=period,
        output_path=str(scratch),
    )
    keyword = "EXPLAIN ANALYZE" if analyze else "EXPLAIN"
    try:
        with _open_session(config) as conn:
            rows = conn.execute(f"{keyword} {sql}").fetchall()
    finally:
        scratch.unlink(missing_ok=True)
    return "\n".join(str(cell) for row in rows for cell in row)


def discover_work_units(
    dataset_root: Path,
    *,
    sensor: str | None = None,
    vsn: str | None = None,
    start: Date | None = None,
    end: Date | None = None,
) -> list[tuple[str, str, Date]]:
    """List the ``(sensor, vsn, day)`` work units present in the dataset.

    This reads directory names only -- no Parquet file is opened -- because its job is
    to build a SLURM array manifest, and on NFS listing four levels of Hive directories
    is cheap where opening files is not.

    ``instrument`` is collapsed away: it is part of the ingest layout, not of the work
    unit, and a work unit's glob already spans every instrument directory.
    """
    root = dataset_root.expanduser()
    units: set[tuple[str, str, Date]] = set()
    pattern = f"{FACTS_DIR}/sensor={sensor or '*'}/vsn={vsn or '*'}/instrument=*/date=*"
    for path in root.glob(pattern):
        if not path.is_dir():
            continue
        try:
            day = datetime.strptime(path.name.removeprefix("date="), "%Y-%m-%d").date()
        except ValueError:
            continue  # not a date-partition directory
        if (start and day < start) or (end and day > end):
            continue
        units.add(
            (
                path.parents[2].name.removeprefix("sensor="),
                path.parents[1].name.removeprefix("vsn="),
                day,
            )
        )
    return sorted(units)
