"""Benchmark Stage 1 against a full-scale synthetic day.

Run this on the HPC node against the real dataset by passing ``--dataset``; without it,
the script synthesises a day at the production row count (WXT536 at ~10 Hz) so the plan
and the scaling curve can be inspected off-cluster.

    python benchmarks/stage1_benchmark.py                       # synthetic
    python benchmarks/stage1_benchmark.py --dataset /nfs/...    # real partitions

Everything measured here is a property of the generated SQL, so the synthetic run is a
faithful check of partition pruning, column projection, scan count, and thread scaling.
Absolute wall times are not: those must be re-measured on the cluster.
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import date as Date
from datetime import datetime
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from crocus_qc.config import TEN_SECONDS, load_profile  # noqa: E402
from crocus_qc.reduce import build_stage1_sql, raw_glob, session_setup_sql  # noqa: E402

DAY = Date(2025, 12, 15)

#: Row counts and sampling rates from the ADQAT pilot on W08D.
CASES = {
    "wxt536": {"sensor": "vaisala-wxt536", "vsn": "W08D", "hz": 10.0},
}


def synthesise(root: Path, profile, sensor: str, vsn: str, hz: float, threads: int) -> int:
    """Write one Hive partition of long-format facts, entirely inside DuckDB."""
    partition = (
        root / "facts" / f"sensor={sensor}" / f"vsn={vsn}"
        / f"instrument={profile.instrument_label}" / f"date={DAY:%Y-%m-%d}"
    )
    partition.mkdir(parents=True, exist_ok=True)
    target = partition / "part-0.parquet"
    if target.exists():
        with duckdb.connect() as conn:
            return conn.execute(f"SELECT count(*) FROM read_parquet('{target}')").fetchone()[0]

    ticks = int(86_400 * hz)
    step_ms = int(1000 / hz)
    specs = ", ".join(
        f"('{s.measurement}', '{s.field}', '{s.value_type}', {i})"
        for i, s in enumerate(profile.variables)
    )

    # Values are plausible but arbitrary: this benchmark measures the shape of the scan
    # and the GROUP BY, not scientific content. A sprinkling of sentinels and NULLs keeps
    # the CASE expression and the FILTER predicates on their real code paths.
    sql = f"""
    COPY (
        SELECT
            TIMESTAMPTZ '{DAY} 00:00:00+00' + INTERVAL (t.i * {step_ms}) MILLISECONDS AS time,
            '{sensor}'                      AS sensor,
            '{vsn}'                         AS vsn,
            '{profile.instrument_label}'    AS instrument_id,
            m.measurement                   AS measurement,
            m.field                         AS field,
            md5(m.measurement)[1:16]::BLOB  AS series_id,
            m.value_type                    AS value_type,
            CASE
                WHEN (t.i + m.idx) % 997 = 0  THEN -9999.9
                WHEN (t.i + m.idx) % 1499 = 0 THEN NULL
                ELSE 20.0 + m.idx + 5.0 * sin(t.i / 500.0)
            END::DOUBLE                     AS value_float64,
            NULL::BIGINT                    AS value_int64,
            NULL::UBIGINT                   AS value_uint64,
            NULL::BOOLEAN                   AS value_bool,
            NULL::VARCHAR                   AS value_string
        FROM range({ticks}) t(i)
        CROSS JOIN (VALUES {specs}) m(measurement, field, value_type, idx)
    ) TO '{target}' (FORMAT parquet, COMPRESSION zstd);
    """
    with duckdb.connect() as conn:
        conn.execute(f"SET threads = {threads};")
        conn.execute("SET TimeZone = 'UTC';")
        conn.execute(sql)
        return conn.execute(f"SELECT count(*) FROM read_parquet('{target}')").fetchone()[0]


def stage1_sql(dataset: Path, profile, sensor: str, vsn: str, output: Path) -> str:
    return build_stage1_sql(
        dataset_root=str(dataset),
        sensor=sensor,
        vsn=vsn,
        day=DAY,
        variables=profile.variables,
        period=TEN_SECONDS,
        output_path=str(output),
    )


def run_once(sql: str, threads: int, memory_limit: str, temp_dir: Path) -> float:
    temp_dir.mkdir(parents=True, exist_ok=True)
    with duckdb.connect() as conn:
        conn.execute(session_setup_sql(threads, memory_limit, str(temp_dir)))
        start = time.perf_counter()
        conn.execute(sql)
        return time.perf_counter() - start


def plan(sql: str, threads: int, memory_limit: str, temp_dir: Path, analyze: bool) -> str:
    temp_dir.mkdir(parents=True, exist_ok=True)
    keyword = "EXPLAIN ANALYZE" if analyze else "EXPLAIN"
    with duckdb.connect() as conn:
        conn.execute(session_setup_sql(threads, memory_limit, str(temp_dir)))
        rows = conn.execute(f"{keyword} {sql}").fetchall()
    return "\n".join(str(cell) for row in rows for cell in row)


def report(label: str, text: str) -> None:
    print(f"\n{'=' * 78}\n{label}\n{'=' * 78}")
    print(text)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", type=Path, help="real dataset root; synthesised if absent")
    parser.add_argument("--work-dir", type=Path, default=Path("bench-work"))
    parser.add_argument("--memory-limit", default="8GB")
    parser.add_argument("--threads", type=int, nargs="+", default=[1, 2, 4, 8])
    args = parser.parse_args()

    work = args.work_dir.expanduser().resolve()
    temp_dir = work / "scratch"
    max_threads = max(args.threads)

    print(f"duckdb {duckdb.__version__}   started {datetime.now():%Y-%m-%d %H:%M:%S}")

    for label, case in CASES.items():
        profile = load_profile(label)
        sensor, vsn = case["sensor"], case["vsn"]

        if args.dataset:
            dataset, raw_rows = args.dataset, None
        else:
            dataset = work / "raw"
            build_start = time.perf_counter()
            raw_rows = synthesise(dataset, profile, sensor, vsn, case["hz"], max_threads)
            print(
                f"\n[{label}] synthesised {raw_rows:,} raw rows "
                f"in {time.perf_counter() - build_start:.1f}s"
            )

        output = work / f"{label}-10sec.parquet"
        sql = stage1_sql(dataset, profile, sensor, vsn, output)

        report(f"{label}: input", f"{raw_glob(str(dataset), sensor, vsn, DAY)}\n"
               f"variables: {len(profile.variables)}"
               + (f"\nraw rows:  {raw_rows:,}" if raw_rows else ""))
        report(f"{label}: EXPLAIN", plan(sql, max_threads, args.memory_limit, temp_dir, False))
        report(
            f"{label}: EXPLAIN ANALYZE ({max_threads} threads)",
            plan(sql, max_threads, args.memory_limit, temp_dir, True),
        )

        scaling = []
        for threads in args.threads:
            elapsed = run_once(sql, threads, args.memory_limit, temp_dir)
            scaling.append((threads, elapsed))
        baseline = scaling[0][1]
        report(
            f"{label}: thread scaling at memory_limit={args.memory_limit}",
            "\n".join(
                f"  {t:>3} threads  {e:7.2f}s  speedup x{baseline / e:.2f}" for t, e in scaling
            ),
        )

        tight = run_once(sql, max_threads, "256MB", temp_dir)
        spill = sum(p.stat().st_size for p in temp_dir.rglob("*") if p.is_file())
        report(
            f"{label}: constrained memory",
            f"  memory_limit=256MB  {tight:.2f}s  (completed; spill residue {spill:,} bytes)",
        )

        with duckdb.connect() as conn:
            rows = conn.execute(f"SELECT count(*) FROM read_parquet('{output}')").fetchone()[0]
        report(
            f"{label}: output",
            f"  path      {output}\n"
            f"  rows      {rows:,} (expected {TEN_SECONDS.rows_per_day:,})\n"
            f"  bytes     {output.stat().st_size:,}\n"
            f"  columns   {len(profile.variables)} variables + time",
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
