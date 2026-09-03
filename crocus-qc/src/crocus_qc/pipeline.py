"""Work-unit orchestration: build SQL, run DuckDB, finalize atomically, record provenance.

One invocation processes one VSN, walking its calendar a day at a time. The days are
sequential -- there is no internal concurrency across them, and none across VSNs either:
DuckDB provides node-level parallelism and SLURM provides cluster-level parallelism, one
job per station.

Python does no analytical work here. It never iterates raw rows, never computes a
statistic, and never materializes the raw dataset -- the whole reduction happens inside
one DuckDB statement that ends in ``COPY ... TO``.

Every phase is timed and the breakdown is written into ``_success.json``, so a slow day
on the cluster can be diagnosed from its output directory alone.
"""

from __future__ import annotations

import os
from collections.abc import Iterator, Sequence
from datetime import date as Date
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import duckdb

from . import __version__
from .config import SENSOR, TEN_SECONDS, AggregationPeriod, PipelineConfig, SensorProfile
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


def run_vsn(
    *,
    vsn: str,
    start: Date,
    end: Date,
    dataset_root: Path,
    config: PipelineConfig,
    profile: SensorProfile,
    period: AggregationPeriod = TEN_SECONDS,
    force: bool = False,
    sql_profile: bool = False,
) -> Iterator[tuple[dict[str, Any], Stopwatch]]:
    """Reduce every UTC day from ``start`` to ``end``, inclusive, for one VSN.

    A job is a VSN, not a day. A station carries on the order of 600 days, and a process
    per station-day would be that many interpreter and DuckDB startups for SLURM to
    schedule, queue, and log -- around a reduction that takes about a second. Walking the
    calendar inside one process amortises all of it, and SLURM still parallelises across
    stations, which is where the independence actually is.

    The calendar is walked rather than discovered. Listing the date partitions first
    would only tell us what reading them tells us anyway, at the cost of a second NFS
    traversal per job.

    Days are yielded lazily, one at a time, so a long job reports each day as it lands
    rather than at the end. Each day gets its own stopwatch: a shared one would fold
    yesterday's timings into today's provenance record. The stopwatch is yielded
    alongside the record because it outlives it -- the provenance write is still running
    when the record it writes is serialised, so that phase can only be reported here.
    """
    day = start
    while day <= end:
        watch = Stopwatch()
        record = run_work_unit(
            sensor=SENSOR,
            vsn=vsn,
            day=day,
            dataset_root=dataset_root,
            config=config,
            profile=profile,
            period=period,
            force=force,
            stopwatch=watch,
            sql_profile=sql_profile,
        )
        yield record, watch
        day += timedelta(days=1)


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


def work_unit_pattern(vsn: str | None = None) -> str:
    """The directory glob, relative to the dataset root, that work units live under.

    Exposed so that a search finding nothing can report what it looked for: the failure
    mode is a plausible-but-wrong ``--dataset`` or a mistyped VSN, and the pattern is the
    fastest way to see which level of the tree stopped matching. The sensor level is
    pinned to ``SENSOR``, so a dataset holding other instruments lists only the WXT536.
    """
    return f"{FACTS_DIR}/sensor={SENSOR}/vsn={vsn or '*'}/instrument=*/date=*"


def discover_work_units(
    dataset_root: Path,
    *,
    vsns: Sequence[str] | None = None,
    start: Date | None = None,
    end: Date | None = None,
) -> list[tuple[str, Date]]:
    """List the ``(vsn, day)`` work units present in the dataset.

    This reads directory names only -- no Parquet file is opened -- because its job is
    to build a job-array manifest, and on NFS listing four levels of Hive directories is
    cheap where opening files is not.

    ``instrument`` is collapsed away: it is part of the ingest layout, not of the work
    unit, and a work unit's glob already spans every instrument directory.

    Each named VSN is globbed separately, and **a named VSN that matches nothing raises**
    rather than being dropped. The VSN list drives the whole campaign: a mistyped VSN
    filtered out of a wide listing would just shorten the manifest, the run would
    complete with every unit succeeding, and an entire station would be missing from the
    output with nothing downstream able to tell. Silence is the dangerous answer here.

    Listing without naming any VSN is the exploratory case and returns whatever is there,
    including nothing.
    """
    root = dataset_root.expanduser()
    units: set[tuple[str, Date]] = set()
    missing: list[str] = []
    window = f" between {start} and {end}" if (start or end) else ""

    for vsn in vsns or [None]:
        found = _units_under(root, vsn, start, end)
        if vsn is not None and not found:
            missing.append(
                f"no work units for VSN {vsn!r}{window} under {root}/{work_unit_pattern(vsn)}"
            )
        units |= found

    if missing:
        raise LookupError("\n".join(missing))
    return sorted(units)


def _units_under(
    root: Path, vsn: str | None, start: Date | None, end: Date | None
) -> set[tuple[str, Date]]:
    units: set[tuple[str, Date]] = set()
    for path in root.glob(work_unit_pattern(vsn)):
        if not path.is_dir():
            continue
        try:
            day = datetime.strptime(path.name.removeprefix("date="), "%Y-%m-%d").date()
        except ValueError:
            continue  # not a date-partition directory
        if (start and day < start) or (end and day > end):
            continue
        units.add((path.parents[1].name.removeprefix("vsn="), day))
    return units
