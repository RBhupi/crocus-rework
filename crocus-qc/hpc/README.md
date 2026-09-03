# Running `crocus-qc` on the cluster

Written for the Argonne GCE environment the earlier stages ran in (`/nfs/gce/...`,
compute host `compute-386-07`, quota via `dfq`). Two conventions are inherited from
`docs/HANDOVER.md` and the ADQAT HPC handoff: environments live in project space, not
`$HOME`, and outputs go to a version-suffixed tree that is never the input tree.

> **The scheduler may not exist here.** The ADQAT handoff records, verbatim,
> `Slurm: sbatch was not available` on `compute-386-07`, and both earlier stages ran
> under `nohup`. Check with `command -v sbatch` before planning around
> [`stage1_array.sbatch`](stage1_array.sbatch); if it is missing, use
> [`run_vsns.sh`](run_vsns.sh), which is the same one-process-per-station model
> with `xargs` in place of the scheduler.

## 0. Paths

| | |
|---|---|
| Input (read-only) | `/nfs/gce/projects/crocus-server-admins/data-rework/crocus-rework-output/wxt-aqt-production-v5` |
| Output | `/nfs/gce/projects/crocus-server-admins/data-rework/crocus-qc-output/wxt536-10sec-v0.2.0` |
| Environment | `/nfs/gce/projects/crocus-server-admins/data-rework/envs/crocus-qc` |
| Checkout | `/nfs/gce/projects/crocus-server-admins/data-rework/crocus-rework/crocus-qc` |

`--dataset` is the **version root**, not the `facts/` directory inside it. The Hive tree
begins one level down (`.../wxt-aqt-production-v5/facts/sensor=.../vsn=.../instrument=.../date=...`),
and `crocus-qc` appends `facts/` itself. Passing the version root also means the
"never write inside the raw dataset" guard covers the whole versioned tree.

Output follows the same version-suffix convention as the input (`-v5` there, `-v0.2.0`
here): a changed product definition gets a new output root rather than overwriting an
existing one. v0.2.0 is the first version where `raw_std` is NULL below two samples
rather than 0.0, so its products are not interchangeable with v0.1.0's.

This package reduces the **WXT536 only**. The AQT530 samples once per 20 seconds, so a
10-second average of it is the raw observation on a half-empty grid; its route to
publication is a `native` product, not an aggregate. There is no `--sensor` flag.

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
crocus-qc discover --dataset "$DATASET" | cut -f1 | sort | uniq -c   # days per station
```

Directory listing only — no Parquet is opened. Rows are `vsn<TAB>date`. Matching nothing
is an error, not an empty result: `discover` exits non-zero and prints the glob it tried,
so you can see which level of the tree stopped matching. The usual cause is `--dataset`
pointing at `.../facts` instead of the version root above it.

Naming stations with `--vsn W08D W08E` restricts the listing, and **a named VSN that
matches nothing is an error, reported by name** — a typo would otherwise just shorten the
list, and a campaign built from a short list completes successfully while quietly missing
a whole station.

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
wrong as soon as you run more than one station at a time. Set both explicitly when
using `run_vsns.sh`.

## 4. First run: two days, in the foreground

```bash
bash hpc/two_day_trial.sh 2025-12-15 2025-12-16
```

One station over a two-day window, with `--sql-profile`. The two arguments are the
inclusive ends of the window, not a list of days: `run` walks the calendar itself. Under
SLURM, wrap it in `salloc --cpus-per-task=8 --mem=8G --time=01:00:00` first.

For each day you get:

| where | what |
|---|---|
| stdout | one JSON provenance record per line (row count, elapsed, timings, versions) |
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

**A job is a station, not a station-day.** One process walks a VSN's whole calendar, so
~7,000 station-days collapse into ~11 processes. Seven thousand one-day jobs would be
seven thousand interpreter and DuckDB startups for the scheduler to queue and log, around
a reduction that takes about a second — and the days within a station have to be serial
anyway. Parallelism goes where the independence is: across stations.

Confirm the station list first, since it now drives the whole campaign:

```bash
mkdir -p logs
crocus-qc discover --dataset "$DATASET" | cut -f1 | sort -u > vsns.txt
cat vsns.txt
```

Days are **not** enumerated up front. `run` walks the calendar and skips days with no raw
partitions, one stderr line each, so there is no manifest to keep in step with the tree.

### Without a scheduler

```bash
bash hpc/run_vsns.sh 4 $(cat vsns.txt)
```

The first argument is how many stations run at once. Size it against the node, not the
station count: each process asks DuckDB for `execution.threads`, so 4 concurrent stations
on a `threads: 8` config want 32 cores. For a long campaign, put it under `nohup` as the
earlier stages did:

```bash
nohup bash hpc/run_vsns.sh 4 $(cat vsns.txt) > logs/wxt-campaign.log 2>&1 &
```

Per station you get `logs/<vsn>.jsonl` (one provenance record per day produced) and
`logs/<vsn>.log` (timings, skips, failures). A failing day is logged and counted, not
fatal — one bad block should not cost the other 599 days — and both the station process
and the batch exit non-zero if anything failed.

Restrict the window with `START` / `END`; omitted, each station uses its own first and
last day present:

```bash
START=2025-11-01 END=2025-11-30 bash hpc/run_vsns.sh 4 W08D W08E
```

### With SLURM

One array task per station, sized by the measured thread scaling
(see [`../docs/stage1-performance.md`](../docs/stage1-performance.md)): WXT536 scales to
×4.5 at 8 threads.

```bash
VSNS=vsns.txt sbatch --array=1-$(wc -l < vsns.txt) --cpus-per-task=8 \
  hpc/stage1_array.sbatch
```

A station is now hours rather than seconds, so `--time` in `stage1_array.sbatch` is 4h
rather than 30m. Check it against your longest station: ~1.2 s per day × its day count.

`stage1_array.sbatch` sets no `--partition`, `--account`, or `--qos`, because none is
recorded anywhere in this project. Add whatever this cluster requires before submitting.

## 6. Restarting

Rerun the identical command. A day with a `_success.json` is skipped without touching
DuckDB, and a day with no raw partitions creates nothing, so a rerun redoes exactly the
days that failed. No bookkeeping is needed beyond the output tree itself. A killed job
leaves a `10sec.parquet.tmp` and no marker, so that day is simply redone. `--force`
recomputes deliberately.

Confirm completeness from the output tree alone rather than from the logs:

```bash
find "$OUT" -name '_success.json' -exec grep -h output_row_count {} \; | sort | uniq -c
```

One line reading `8640`. Anything else is a partial campaign.
