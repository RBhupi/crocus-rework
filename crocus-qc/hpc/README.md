# Running `crocus-qc` on the cluster

## 1. Install

The package has two runtime dependencies (`duckdb`, `PyYAML`) and needs Python 3.12+.

```bash
python -m venv ~/venvs/crocus-qc
source ~/venvs/crocus-qc/bin/activate
pip install -e /path/to/crocus-rework/crocus-qc
crocus-qc profiles          # smoke test: lists aqt530 and wxt536
```

`-e` is deliberate: provenance records the git commit of the checkout the code runs
from, and a copied-in wheel has no checkout, so `git_commit` comes out `null`. While the
pipeline is still being verified, knowing exactly which commit produced a file is worth
more than the isolation of a non-editable install.

## 2. Point the output somewhere safe

Edit `output.root` in [`pipeline.yaml`](pipeline.yaml) to your own project space.

The production facts tree is read-only input:

```
/nfs/gce/projects/crocus-server-admins/data-rework/crocus-rework-output/wxt-aqt-production-v5
```

`crocus-qc run` refuses to start if `output.root` is inside it. That check is the last
line of defence, not the plan — set the path correctly.

Leave `threads` and `memory_limit` unset and they follow the SLURM allocation
(`SLURM_CPUS_PER_TASK`, 80% of `SLURM_MEM_PER_NODE`). `temp_dir` follows `$TMPDIR`.

## 3. First run: two days, in the foreground

```bash
salloc --cpus-per-task=8 --mem=8G --time=01:00:00
bash hpc/two_day_trial.sh 2025-12-15 2025-12-16
```

This runs WXT536 and AQT530 for each day with `--sql-profile`. For each work unit you get:

| where | what |
|---|---|
| stdout | the provenance record as JSON (row count, elapsed, timings, versions) |
| stderr | the phase timing table — which phase was slow |
| `<out>/<sensor>/<vsn>/<date>/10sec.parquet` | the product, exactly 8640 rows |
| `<out>/.../_success.json` | the same provenance record, written last |
| `<out>/.../_duckdb_profile.json` | DuckDB's per-operator breakdown |

### Reading the timings

`_success.json` carries `timings_seconds`, one entry per phase:

| phase | if it dominates |
|---|---|
| `load_config`, `load_profile` | should be milliseconds; anything more means NFS latency on the config |
| `build_sql` | should be microseconds; it is pure string assembly |
| `open_session` | DuckDB startup plus creating `temp_dir` — slow means the filesystem |
| `execute_reduction` | the actual work: scan + `GROUP BY` + write. Expected to dominate |
| `publish` | a `rename(2)`; slow means the output filesystem is struggling |
| `verify_output` | reading the product's row count back |
| `write_provenance` | writing `_success.json` |

If `execute_reduction` is the outlier, open `_duckdb_profile.json` to see whether it was
`READ_PARQUET` (I/O-bound on NFS) or `HASH_GROUP_BY` (compute-bound). The two need
different fixes: more bandwidth versus more CPUs.

## 4. Scaling up

Build a manifest (directory listing only, no Parquet is read):

```bash
mkdir -p manifests logs
crocus-qc discover --dataset "$DATASET" --sensor vaisala-wxt536 \
    --start 2025-12-01 --end 2025-12-31 > manifests/wxt.tsv
crocus-qc discover --dataset "$DATASET" --sensor vaisala-aqt530 \
    --start 2025-12-01 --end 2025-12-31 > manifests/aqt.tsv
```

Then submit one array per instrument, sized by the measured thread scaling
(see [`../docs/stage1-performance.md`](../docs/stage1-performance.md)): WXT536 scales
to ×4.5 at 8 threads, AQT530 gets *slower* past 2.

```bash
MANIFEST=manifests/wxt.tsv PROFILE=wxt536 \
  sbatch --array=1-$(wc -l < manifests/wxt.tsv) --cpus-per-task=8 hpc/stage1_array.sbatch

MANIFEST=manifests/aqt.tsv PROFILE=aqt530 \
  sbatch --array=1-$(wc -l < manifests/aqt.tsv) --cpus-per-task=2 hpc/stage1_array.sbatch
```

## 5. Restarting

A work unit with a `_success.json` is skipped. Resubmitting the same array after a
partial failure reprocesses only what did not finish; no bookkeeping is needed beyond
the output tree itself. A killed job leaves a `10sec.parquet.tmp` and no marker, so it
is simply rerun. Use `--force` to deliberately recompute.
