#!/bin/bash
# Run the campaign without a scheduler: N stations at a time, each walking its own days.
#
# The ADQAT HPC handoff records "Slurm: sbatch was not available" on compute-386-07, and
# both earlier generations of this pipeline ran under nohup instead. This script is the
# same execution model as hpc/stage1_array.sbatch -- one independent OS process per VSN,
# no shared state -- with xargs standing in for the scheduler. It is not a parallel
# framework: there is still no Dask, Ray, Spark, MPI, or multiprocessing here.
#
#   bash hpc/run_vsns.sh 4 W08D W08E W096
#   bash hpc/run_vsns.sh 4 $(crocus-qc discover --dataset "$DATASET" | cut -f1 | sort -u)
#
# A job is a station, not a station-day. Each process walks that VSN's whole calendar,
# so ~7,000 interpreter and DuckDB startups collapse into one per station, and the days
# inside a station are exactly the work that has to be serial anyway.
#
# Concurrency is the first argument. Size it from the node, not from the station count:
# each process asks DuckDB for `threads` (execution.threads in the config, or every core
# if unset), so N concurrent stations on an 8-thread config want 8N cores.
#
# Re-running is safe and cheap. A day with a _success.json is skipped without touching
# DuckDB, and a day with no raw partitions is skipped without creating anything, so this
# doubles as the recovery path after a partial run: rerun the identical command and it
# redoes exactly the days that failed.
#
# Restrict the window with START/END; omitted, each station uses its own first and last
# day present in the dataset.

set -uo pipefail

CONCURRENCY="${1:?usage: run_vsns.sh CONCURRENCY VSN [VSN ...]}"
shift
[ "$#" -gt 0 ] || { echo "usage: run_vsns.sh CONCURRENCY VSN [VSN ...]" >&2; exit 2; }

: "${DATASET:=/nfs/gce/projects/crocus-server-admins/data-rework/crocus-rework-output/wxt-aqt-production-v5}"
: "${CONFIG:=hpc/pipeline.yaml}"
: "${LOG_DIR:=logs}"
: "${START:=}"
: "${END:=}"

mkdir -p "$LOG_DIR"

export DATASET CONFIG LOG_DIR START END

# One station, in its own process with its own node-local temp directory.
run_one() {
    local vsn="$1"
    local tmp
    tmp="$(mktemp -d "${TMPDIR:-/tmp}/crocus-qc-${vsn}-XXXXXX")"
    # stdout is JSONL, one record per day, so the .jsonl name is literal.
    TMPDIR="$tmp" crocus-qc run \
        --vsn "$vsn" \
        ${START:+--start "$START"} \
        ${END:+--end "$END"} \
        --dataset "$DATASET" --config "$CONFIG" \
        > "$LOG_DIR/$vsn.jsonl" 2> "$LOG_DIR/$vsn.log"
    local status=$?
    rm -rf "$tmp"
    local days
    days="$(grep -c '"status": "success"' "$LOG_DIR/$vsn.jsonl" || true)"
    if [ "$status" -eq 0 ]; then
        printf 'ok   %s (%s days)\n' "$vsn" "$days"
    else
        # Do not abort the batch: stations are independent, and one bad one should not
        # cost the others. crocus-qc has already skipped past the bad days within this
        # station and exited non-zero; failures are counted at the end.
        printf 'FAIL %s (%s days ok, exit %d, see %s)\n' \
            "$vsn" "$days" "$status" "$LOG_DIR/$vsn.log"
    fi
    return "$status"
}
export -f run_one

echo "stations:    $* ($# total)"
echo "dataset:     $DATASET"
echo "window:      ${START:-<first day present>} .. ${END:-<last day present>}"
echo "concurrency: $CONCURRENCY"
echo

# Results go through a file rather than a pipeline into grep, so the per-station lines
# stay visible as they happen and the failure count survives the subshell.
results="$LOG_DIR/.results-$$"
started="$SECONDS"
printf '%s\n' "$@" | xargs -P "$CONCURRENCY" -n 1 bash -c 'run_one "$@"' _ | tee "$results"
elapsed=$((SECONDS - started))

failures="$(grep -c '^FAIL' "$results" || true)"
rm -f "$results"

echo
echo "finished $# stations in ${elapsed}s; $failures failed"
[ "$failures" -eq 0 ]
