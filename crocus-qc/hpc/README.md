# Running `crocus-qc` on the cluster

Written for the Argonne GCE environment the earlier stages ran in (`/nfs/gce/...`,
compute host `compute-386-07`, quota via `dfq`). Two conventions are inherited from
`docs/HANDOVER.md` and the ADQAT HPC handoff: environments live in project space, not
`$HOME`, and outputs go to a version-suffixed tree that is never the input tree.

> **The scheduler may not exist here.** The ADQAT handoff records, verbatim,
> `Slurm: sbatch was not available` on `compute-386-07`, and both earlier stages ran
> under `nohup`. Check with `command -v sbatch` before planning around
> [`stage1_array.sbatch`](stage1_array.sbatch); if it is missing, use
> [`run_manifest.sh`](run_manifest.sh), which is the same one-process-per-work-unit
> model with `xargs` in place of the scheduler.

## 0. Paths

| | |
|---|---|
| Input (read-only) | `/nfs/gce/projects/crocus-server-admins/data-rework/crocus-rework-output/wxt-aqt-production-v5` |
| Output | `/nfs/gce/projects/crocus-server-admins/data-rework/crocus-qc-output/10sec-v0.1.0` |
| Environment | `/nfs/gce/projects/crocus-server-admins/data-rework/envs/crocus-qc` |
| Checkout | `/nfs/gce/projects/crocus-server-admins/data-rework/crocus-rework/crocus-qc` |

`--dataset` is the **version root**, not the `facts/` directory inside it. The Hive tree
begins one level down (`.../wxt-aqt-production-v5/facts/sensor=.../vsn=.../instrument=.../date=...`),
and `crocus-qc` appends `facts/` itself. Passing the version root also means the
"never write inside the raw dataset" guard covers the whole versioned tree.

Output follows the same version-suffix convention as the input (`-v5` there, `-v0.1.0`
here): a changed product definition gets a new output root rather than overwriting an
existing one.

## 1. Install

**There is no `python3.12` on the GCE compute nodes.** The system packages are 3.10 and
3.11; the 3.12 the earlier stages used lives inside an environment, not on `PATH`. Find
one before creating anything:

```bash
BASE=/nfs/gce/projects/crocus-server-admins/data-rework
for p in "$BASE"/envs/*/bin/python; do printf '%-60s ' "$p"; "$p" -V 2>&1; done
```

`envs/adqat-braut` is the one the ADQAT handoff pins to 3.12. Use its interpreter as the
base of a fresh venv — a separate environment, so a `crocus-qc` dependency can never
perturb a working ADQAT install:

```bash
"$BASE/envs/adqat-braut/bin/python" -m venv "$BASE/envs/crocus-qc"
"$BASE/envs/crocus-qc/bin/pip" install -e "$BASE/crocus-rework/crocus-qc"
"$BASE/envs/crocus-qc/bin/crocus-qc" profiles
```

If no 3.12 turns up, create one with micromamba, which is how the `data` environment was
built (`HANDOVER.md`):

```bash
micromamba create -y -p "$BASE/envs/crocus-qc" python=3.12
```

Two runtime dependencies (`duckdb`, `PyYAML`), Python 3.12+.

`-e` is deliberate: provenance records the git commit of the checkout the code runs
from, and a copied-in wheel has no checkout, so `git_commit` comes out `null`.

Following `HANDOVER.md`, no activation is needed if you call `$ENV/bin/crocus-qc`
directly. To use the bare `crocus-qc` name in the scripts below, either activate the
venv or `export PATH="$BASE/envs/crocus-qc/bin:$PATH"`.

## 2. Confirm the input is where you think it is

```bash
crocus-qc discover --dataset "$DATASET" --start 2025-12-15 --end 2025-12-16
```

Directory listing only — no Parquet is opened. Matching nothing is an error, not an
empty result: `discover` exits non-zero and prints the glob it tried, so you can see
which level of the tree stopped matching. The usual cause is `--dataset` pointing at
`.../facts` instead of the version root above it.

Confirm the checkout is current before believing a negative result — a stale working
copy produces exactly the same empty listing as a wrong path:

```bash
git -C "$BASE/crocus-rework" log --oneline -1
```

Also check headroom before a large campaign:

```bash
dfq
```

## 3. Point the output somewhere safe

Edit `output.root` in [`pipeline.yaml`](pipeline.yaml). `run` refuses to start if it
resolves inside `--dataset`; that check is the last line of defence, not the plan.

Leave `threads` and `memory_limit` unset and they follow the SLURM allocation
(`SLURM_CPUS_PER_TASK`, 80% of `SLURM_MEM_PER_NODE`). **With no scheduler there is no
allocation to follow** — `threads` then defaults to every core on the node, which is
wrong as soon as you run more than one work unit at a time. Set both explicitly when
using `run_manifest.sh`.

## 4. First run: two days, in the foreground

```bash
bash hpc/two_day_trial.sh 2025-12-15 2025-12-16
```

Four work units: WXT536 and AQT530, two days each, with `--sql-profile`. Under SLURM,
wrap it in `salloc --cpus-per-task=8 --mem=8G --time=01:00:00` first.

For each work unit you get:

| where | what |
|---|---|
| stdout | the provenance record as JSON (row count, elapsed, timings, versions) |
| stderr | the phase timing table — which phase was slow |
| `<out>/<sensor>/<vsn>/<date>/10sec.parquet` | the product, exactly 8640 rows |
| `<out>/.../_success.json` | the same provenance record, written last |
| `<out>/.../_duckdb_profile.json` | DuckDB's per-operator breakdown |

Check every product is a complete day:

```bash
find "$OUT" -name '_success.json' -exec grep -h output_row_count {} \; | sort | uniq -c
```

### Reading the timings

`_success.json` carries `timings_seconds`, one entry per phase:

| phase | if it dominates |
|---|---|
| `load_config`, `load_profile` | should be milliseconds; more means NFS latency on the config |
| `build_sql` | should be microseconds; it is pure string assembly |
| `open_session` | DuckDB startup plus creating `temp_dir` — slow means the filesystem |
| `execute_reduction` | the actual work: scan + `GROUP BY` + write. Expected to dominate |
| `publish` | a `rename(2)`; slow means the output filesystem is struggling |
| `verify_output` | reading the product's row count back |
| `write_provenance` | writing `_success.json` |

If `execute_reduction` is the outlier, open `_duckdb_profile.json` to see whether it was
`READ_PARQUET` (I/O-bound on NFS) or `HASH_GROUP_BY` (compute-bound). The two need
different fixes: more bandwidth versus more CPUs.

## 5. Scaling up

Build manifests (directory listing only):

```bash
mkdir -p manifests logs
crocus-qc discover --dataset "$DATASET" --sensor vaisala-wxt536 > manifests/wxt.tsv
crocus-qc discover --dataset "$DATASET" --sensor vaisala-aqt530 > manifests/aqt.tsv
```

### Without a scheduler

```bash
bash hpc/run_manifest.sh manifests/aqt.tsv aqt530 8
bash hpc/run_manifest.sh manifests/wxt.tsv wxt536 4
```

The last argument is how many work units run at once. Size it against the node, not the
manifest: each unit asks DuckDB for `execution.threads`, so 4 concurrent units on a
`threads: 8` config want 32 cores. AQT is tiny, so run more of it at lower thread counts.
For a long campaign, put it under `nohup` as the earlier stages did:

```bash
nohup bash hpc/run_manifest.sh manifests/wxt.tsv wxt536 4 > logs/wxt-campaign.log 2>&1 &
```

A failing work unit is logged and counted, not fatal — one bad day does not cost the
other 956. The script exits non-zero if any unit failed.

### With SLURM

One array task per work unit, sized by the measured thread scaling
(see [`../docs/stage1-performance.md`](../docs/stage1-performance.md)): WXT536 scales to
×4.5 at 8 threads, AQT530 gets *slower* past 2.

```bash
MANIFEST=manifests/wxt.tsv PROFILE=wxt536 \
  sbatch --array=1-$(wc -l < manifests/wxt.tsv) --cpus-per-task=8 hpc/stage1_array.sbatch
```

```bash
MANIFEST=manifests/aqt.tsv PROFILE=aqt530 \
  sbatch --array=1-$(wc -l < manifests/aqt.tsv) --cpus-per-task=2 hpc/stage1_array.sbatch
```

`stage1_array.sbatch` sets no `--partition`, `--account`, or `--qos`, because none is
recorded anywhere in this project. Add whatever this cluster requires before submitting.

## 6. Restarting

A work unit with a `_success.json` is skipped without touching DuckDB, so rerunning the
same manifest or resubmitting the same array reprocesses only what did not finish. No
bookkeeping is needed beyond the output tree itself. A killed job leaves a
`10sec.parquet.tmp` and no marker, so it is simply rerun. `--force` recomputes
deliberately.
