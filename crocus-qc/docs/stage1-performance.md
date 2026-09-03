# Stage 1 performance verification

Reproduce with:

```bash
python benchmarks/stage1_benchmark.py --work-dir "$TMPDIR/crocus-bench"
```

## What was measured, and what was not

| | |
|---|---|
| Date | 2026-09-03 |
| Machine | macOS 26.6.2, arm64, 14 logical CPUs (**not** an HPC node) |
| DuckDB | 1.5.5 |
| Python | 3.13.11 |
| Data | **synthetic**, at production row counts |

The production dataset root
(`/nfs/gce/.../wxt-aqt-production-v5`) is not mounted on this machine, so the benchmark
synthesises one Hive partition per instrument at the row counts observed in the ADQAT
pilot on W08D: WXT536 at ~10 Hz across 11 variables (9,504,000 rows/day) and AQT530 at
~0.05 Hz across 12 variables (51,840 rows/day). Values are plausible but arbitrary, with
sentinels every 997th row and NULLs every 1499th so the sentinel `CASE` and the `FILTER`
predicates stay on their real code paths.

**Structural findings below are exact** — partition pruning, column projection, scan
count, and plan shape are properties of the generated SQL and hold wherever it runs.
**Absolute wall times are not transferable**: they were measured on a laptop against
local NVMe, and must be re-measured on the cluster against NFS, where I/O rather than
the `GROUP BY` may dominate. Run the same script there with `--dataset`.

## Structural findings

### One Parquet scan per work unit ✅

Both plans contain exactly one `TABLE_SCAN / Function: READ_PARQUET`, reporting
`Total Files Read: 1`. Every statistic for every variable is produced by the single
downstream `HASH_GROUP_BY`; there is no separate query per statistic.

### Hive partition pruning ✅

The scan's `Filename(s)` is the single glob the work unit names:

```
raw/sensor=vaisala-wxt536/vsn=W08D/instrument=*/date=2025-12-15/*.parquet
```

Pinning `sensor`, `vsn`, and `date` in the path means DuckDB never opens, and never
lists, any other partition. This is stronger than a predicate on the Hive columns —
there is nothing left to prune.

### Column projection ✅

The raw fact schema has 13 columns. The scan projects only what Stage 1 reads:

| instrument | projected columns |
|---|---|
| WXT536 | `time`, `measurement`, `field`, `value_type`, `value_float64` (5 of 13) |
| AQT530 | the same plus `value_string` (6 of 13) — it has one string variable |

`value_string` is correctly absent for WXT536, which has none. `series_id`,
`instrument_id`, `value_int64`, `value_uint64`, `value_bool`, and the Hive key columns
are never read.

### Predicate pushdown ✅ (with one note)

`measurement IN (...)` appears in the scan's `Filters:` list as `optional:`, so row
groups whose statistics exclude every listed measurement are skipped at read time.

Note: DuckDB rewrote the 11-element `IN` list into a `HASH_JOIN (Join Type: MARK)`
against an 11-row `COLUMN_DATA_SCAN`, costing 0.06 s of the WXT run. This is DuckDB's
own choice for large `IN` lists and is not worth working around at this cost.

## Cost distribution (WXT536, 9.5 M rows)

| operator | cumulative CPU |
|---|---|
| `READ_PARQUET` | 0.15 s |
| `HASH_JOIN` (the `IN` list rewrite) | 0.06 s |
| `HASH_GROUP_BY` (11 variables × up to 5 statistics) | 1.95 s |
| `HASH_JOIN` onto the dense grid, `ORDER BY`, `COPY` | ~0.00 s |

The reduction, not the scan, is the cost — as intended. The dense-grid join and the
write are free at this size: the grid is 8,640 rows.

## Thread scaling

`memory_limit = 8GB`, wall clock for the whole statement including the Parquet write.

| threads | WXT536 (9.5 M rows) | speedup | AQT530 (51.8 k rows) | speedup |
|---|---|---|---|---|
| 1 | 2.00 s | ×1.00 | 0.05 s | ×1.00 |
| 2 | 1.14 s | ×1.74 | 0.05 s | ×0.95 |
| 4 | 0.66 s | ×3.04 | 0.05 s | ×0.84 |
| 8 | 0.44 s | ×4.50 | 0.07 s | ×0.68 |

WXT536 scales usefully to 8 threads (×4.5). AQT530 gets *slower* with more threads —
51.8 k rows is far below the point where parallel scheduling pays for itself.

**Implication for SLURM sizing:** `--cpus-per-task=8` for WXT536 work units;
`--cpus-per-task=1` or `2` for AQT530. Requesting 8 CPUs for an AQT day wastes an
allocation for no gain.

## Memory and spill

| | |
|---|---|
| WXT536 at `memory_limit=8GB` | 0.44 s |
| WXT536 at `memory_limit=256MB` | 0.49 s, completed, **no spill files** |
| AQT530 at `memory_limit=256MB` | 0.07 s, completed, no spill |

Stage 1 has a small, bounded working set regardless of input size: the `GROUP BY` has at
most 8,640 groups and the aggregate state per group is a fixed handful of doubles. A day
is streamed, not accumulated. 9.5 M rows completed in 256 MB with no temp files at all.

`--mem=4G` is generous for a single work unit. `temp_directory` still matters as
insurance, but nothing observed here needs it.

## Output

| | WXT536 | AQT530 |
|---|---|---|
| raw input (zstd Parquet) | 24,195,549 B | 127,492 B |
| product (zstd Parquet) | 2,143,328 B | 1,024,471 B |
| rows | 8,640 | 8,640 |
| variables | 11 | 12 |
| reduction | ×11.3 | ×0.12 |

Both products are exactly 8,640 rows — one complete UTC day at 10 seconds.

The AQT530 product is *larger than its input*, which is expected and not a defect: a
0.05 Hz instrument produces roughly one observation per 20 s, so most of the 8,640
buckets hold zero or one sample, and the dense grid stores 8,640 rows regardless. The
dense grid is a deliberate requirement — a 1 MB daily file is a fine price for it.

## Not yet verified

- Everything above against the real production partitions on the cluster. Re-run with
  `--dataset` on an HPC node; NFS read throughput is the variable most likely to change
  the picture.
- Behaviour when a day's partition spans many Parquet files rather than one. The
  synthetic fixture writes a single file per partition; the real ingest may not.
- Days with genuinely irregular or clustered sampling. The synthetic data is uniformly
  spaced, which is the easiest case for the `GROUP BY`.
