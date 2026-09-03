# crocus-qc

Reduces CROCUS **Vaisala WXT536** high-frequency observations to a **10-second
statistical product**.

One invocation processes one **station**, walking that VSN's UTC days in order. SLURM
parallelises independent stations; the package contains no distributed computing.

Only the WXT536. The AQT530 samples once per 20 seconds, so a 10-second average of it is
the raw observation on a half-empty grid, and no bucket could ever carry a spread
statistic — averaging is a WXT536 operation. There is no `--sensor` flag.

```
raw Parquet (immutable, Hive-partitioned)
  → DuckDB: one scan, one GROUP BY into 10-second UTC buckets   → 10sec.parquet
  → _success.json (written last; the idempotency gate)
```

## Scope

This is **Stage 1 only, and Stage 1 performs no QA/QC.** There are no thresholds, no
flags, no bitmask, no coverage ratios, and no neighbouring-bucket arithmetic. The only
value-level preprocessing is normalising the known missing sentinel (`-9999.9`) to NULL,
which is required for the statistics to be correct at all.

Everything downstream — quality flags, NetCDF, coarser aggregation periods — is
deliberately deferred until this product is scientifically verified and benchmarked.

## The product

One row per 10-second bucket. A complete UTC day is exactly **8640 rows**, in ascending
time order, whether or not the instrument reported. Per variable:

| column | present for | meaning |
|---|---|---|
| `{v}` | all | the bucket's aggregate value, NULL if no observations |
| `{v}_n_samples` | all | observations that contributed; `0` for an empty bucket |
| `{v}_raw_min` | `mean` | smallest contributing observation |
| `{v}_raw_max` | `mean` | largest contributing observation |
| `{v}_raw_std` | `mean`, `circular_mean` | population standard deviation (`STDDEV_POP`, ddof=0), NULL below two samples |

Statistics that are not scientifically meaningful for a variable are **absent**, not
present-and-null: `wind_direction` has no min/max (ordering is undefined on a circle),
and `mode` / `last` variables carry no spread statistics at all.

Aggregation is per variable, from its profile:

| method | used by | expression |
|---|---|---|
| `mean` | ordinary numerics | `AVG` |
| `circular_mean` | `wind_direction` | `ATAN2` of mean sine and mean cosine, normalised to `[0, 360)` |
| `mode` | `heater_status` | `MODE` |
| `last` | accumulators, housekeeping | `MAX_BY(value, time)` — latest by real timestamp |

Empty buckets are explicit: value NULL, `n_samples` 0, spread NULL. A bucket holding a
single observation reports its value, min, and max but a NULL `raw_std` — one sample
measures no spread, and `STDDEV_POP` over one row returns 0.0, which reads as "perfectly
stable" instead. Nothing is interpolated and no timestamp is invented. Irregular raw sampling is preserved — each
observation simply belongs to the bucket its timestamp falls in.

## Design constraints

The raw dataset can contain tens of billions of rows, so:

- **DuckDB does all analytical computation.** Python loads profiles, builds a SQL string,
  invokes DuckDB, and renames files. It never iterates a raw row or computes a statistic.
- **The raw dataset is never loaded into Python memory.** The generated statement ends in
  `COPY ... TO`, so no rows return to the client.
- **One scan per work unit.** Every variable and every statistic is computed in the same
  `GROUP BY`, via `FILTER` clauses — not one query per statistic.
- Only the required Hive partition is named, which is the strongest available pruning.
- No Pandas, Polars, Pointblank, Ibis, Dask, Spark, or Ray. `pyarrow` is a test-only
  dependency, used to build fixtures with the exact ingest Arrow types.

`SET TimeZone = 'UTC'` is applied to every session. Without it, `TIMESTAMPTZ` renders in
the compute node's local timezone and the same input yields different output on different
HPC nodes.

## Install

```bash
pip install -e '.[dev]'
```

## Use

One job is one station. `run` walks that VSN's calendar itself, skipping days with no
raw partitions; omit `--start`/`--end` and it uses the station's own first and last day.

```bash
crocus-qc run --vsn W08D --dataset /nfs/gce/projects/crocus-server-admins/data-rework/crocus-rework-output/wxt-aqt-production-v5 --config pipeline.yaml
```

```bash
crocus-qc run --vsn W08D --start 2025-12-15 --end 2025-12-16 --dataset /path/to/dataset --config pipeline.yaml
```

```bash
crocus-qc explain --analyze --vsn W08D --date 2025-12-15 --dataset /path/to/dataset --config pipeline.yaml
```

```bash
crocus-qc discover --dataset /path/to/dataset --vsn W08D W08E --start 2025-12-15 --end 2025-12-16
```

```bash
crocus-qc profiles
```

`pipeline.yaml`:

```yaml
output:
  root: /path/to/products      # must never be under the raw dataset
execution:
  threads: 8                   # defaults to SLURM_CPUS_PER_TASK, then os.cpu_count()
  memory_limit: 24GB           # defaults to 80% of SLURM_MEM_PER_NODE
  temp_dir: /scratch/job       # defaults to TMPDIR, then SCRATCH, then /tmp
```

Omitted `execution` keys are resolved from the SLURM environment, so the same config file
works unchanged across job sizes.

## Where the time goes

Every phase of a run is timed. `run` prints the breakdown to **stderr** and records it in
`_success.json` under `timings_seconds`, so a slow day on the cluster can be diagnosed
from its output directory alone:

```
  load_config             0.001s    0.1%
  load_profile            0.003s    0.5%
  build_sql               0.000s    0.0%
  open_session            0.006s    1.3%
  execute_reduction       0.484s   96.7%
  publish                 0.000s    0.0%
  verify_output           0.006s    1.2%
  write_provenance        0.001s    0.1%
  total                   0.500s
```

Provenance goes to stdout as JSONL — one whole record per line, one line per day
produced — so a SLURM `--output` file stays machine-readable and greppable while the
operator reads timings in `--error`. `--quiet` suppresses the tables; skipped and failed
days are still reported, since a range that vanished in silence is what a wrong
`--dataset` looks like.

Phase timings say *which phase* was slow. When that phase is `execute_reduction`,
`--sql-profile` additionally writes DuckDB's per-operator profile to
`_duckdb_profile.json` beside the product, which says whether the cost was `READ_PARQUET`
(I/O-bound) or `HASH_GROUP_BY` (compute-bound) — different problems, different fixes.

## Safety and restartability

The raw dataset is read-only input for every downstream product, so `run` refuses to
start if `output.root` resolves inside `--dataset`.

Output is published atomically: DuckDB writes `10sec.parquet.tmp`, which is `os.replace`d
onto `10sec.parquet` only after the statement completes, and `_success.json` is written
last. A killed job therefore leaves a stray `.tmp` and no success marker, and simply
reruns. A rerun with an existing `_success.json` returns the prior provenance without
recomputing; `--force` overrides that.

## Layout

```
src/crocus_qc/
  reduce.py      the Stage 1 SQL builder — where all the analytical logic lives
  config.py      frozen dataclasses over YAML; no Pydantic
  pipeline.py    walk a station's calendar: execute, finalise atomically, record provenance
  provenance.py  _success.json
  timing.py      per-phase wall clock
  cli.py         argparse
  profiles/      wxt536.yaml — how to find and reduce each variable
tests/
  test_reduce.py    statistical behaviour, asserted on the product's columns
  test_pipeline.py  publication, idempotency, determinism, timing, discovery
  test_timing.py    the phase stopwatch
  test_cli.py       argument handling and subcommands
hpc/              cluster deployment: config, SLURM array, two-day trial script
benchmarks/       full-scale performance harness
docs/             benchmark results
```

```bash
pytest
```
