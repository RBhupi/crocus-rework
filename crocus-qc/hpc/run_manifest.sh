#!/bin/bash
# Process a manifest without a scheduler, N work units at a time.
#
# The ADQAT HPC handoff records "Slurm: sbatch was not available" on compute-386-07, and
# both earlier generations of this pipeline ran under nohup instead. This script is the
# same execution model as hpc/stage1_array.sbatch -- one independent OS process per work
# unit, no shared state -- with xargs standing in for the scheduler. It is not a
# parallel framework: there is still no Dask, Ray, Spark, MPI, or multiprocessing here.
#
#   bash hpc/run_manifest.sh manifests/wxt.tsv wxt536 4
#
# Concurrency is the third argument. Size it from the node, not from the manifest: each
# work unit asks DuckDB for `threads` (execution.threads in the config, or all cores if
# unset), so N concurrent units on an 8-thread config want 8N cores.
#
# Re-running is safe and cheap: a work unit with a _success.json is skipped without
# touching DuckDB, so this doubles as the recovery path after a partial run.

set -uo pipefail

MANIFEST="${1:?usage: run_manifest.sh MANIFEST PROFILE [CONCURRENCY]}"
PROFILE="${2:?usage: run_manifest.sh MANIFEST PROFILE [CONCURRENCY]}"
CONCURRENCY="${3:-4}"

: "${DATASET:=/nfs/gce/projects/crocus-server-admins/data-rework/crocus-rework-output/wxt-aqt-production-v5}"
: "${CONFIG:=hpc/pipeline.yaml}"
: "${LOG_DIR:=logs}"

mkdir -p "$LOG_DIR"

export DATASET CONFIG PROFILE LOG_DIR

# One work unit, in its own process with its own node-local temp directory.
run_one() {
    local sensor="$1" vsn="$2" day="$3"
    local tag="${sensor}-${vsn}-${day}"
    local tmp
    tmp="$(mktemp -d "${TMPDIR:-/tmp}/crocus-qc-${tag}-XXXXXX")"
    TMPDIR="$tmp" crocus-qc run \
        --sensor "$sensor" --vsn "$vsn" --date "$day" \
        --dataset "$DATASET" --config "$CONFIG" --profile "$PROFILE" \
        > "$LOG_DIR/$tag.json" 2> "$LOG_DIR/$tag.timing"
    local status=$?
    rm -rf "$tmp"
    if [ "$status" -eq 0 ]; then
        printf 'ok   %s\n' "$tag"
    else
        # Do not abort the batch: work units are independent, and one bad day should not
        # cost the other 956. Failures are counted at the end.
        printf 'FAIL %s (exit %d, see %s)\n' "$tag" "$status" "$LOG_DIR/$tag.timing"
    fi
    return "$status"
}
export -f run_one

total="$(wc -l < "$MANIFEST" | tr -d '[:space:]')"
echo "manifest:    $MANIFEST ($total work units)"
echo "profile:     $PROFILE"
echo "dataset:     $DATASET"
echo "concurrency: $CONCURRENCY"
echo

# Results go through a file rather than a pipeline into grep, so the per-unit lines stay
# visible as they happen and the failure count survives the subshell.
results="$LOG_DIR/.results-$$"
started="$SECONDS"
xargs -P "$CONCURRENCY" -n 3 bash -c 'run_one "$@"' _ < "$MANIFEST" | tee "$results"
elapsed=$((SECONDS - started))

failures="$(grep -c '^FAIL' "$results" || true)"
rm -f "$results"

echo
echo "finished $total work units in ${elapsed}s; $failures failed"
[ "$failures" -eq 0 ]
