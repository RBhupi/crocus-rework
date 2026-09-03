#!/bin/bash
# First real-data trial: a short window on one WXT536 station, run in the foreground.
#
# Deliberately not a SLURM array. The point of the first run is to read the output --
# the provenance records, the phase timings, the DuckDB operator profile -- not to get
# throughput. Once this looks right, scale up with hpc/stage1_array.sbatch or, where
# sbatch is unavailable, hpc/run_vsns.sh.
#
#   salloc --cpus-per-task=8 --mem=8G --time=01:00:00
#   bash hpc/two_day_trial.sh 2025-11-25 2025-11-26
#
# The two arguments are the inclusive ends of the window, not a list of days: `run`
# walks the calendar itself and skips days with no raw partitions.
#
# Override the station or paths from the environment:
#   VSN=W08E CONFIG=hpc/pipeline.yaml bash hpc/two_day_trial.sh <start> <end>

set -euo pipefail

: "${DATASET:=/nfs/gce/projects/crocus-server-admins/data-rework/crocus-rework-output/wxt-aqt-production-v5}"
: "${CONFIG:=hpc/pipeline.yaml}"
: "${VSN:=W08D}"

if [ "$#" -ne 2 ]; then
    echo "usage: $0 START_YYYY-MM-DD END_YYYY-MM-DD" >&2
    exit 2
fi
START="$1"
END="$2"

export TMPDIR="${TMPDIR:-/tmp}/crocus-qc-trial-$$"
mkdir -p "$TMPDIR"
trap 'rm -rf "$TMPDIR"' EXIT

echo "dataset: $DATASET"
echo "config:  $CONFIG"
echo "station: $VSN"
echo

# What is actually there. Cheap: directory names only, no Parquet is opened. Under
# `set -e` this doubles as input validation -- `discover` exits non-zero when the named
# VSN matches nothing, so a typo aborts here rather than after the first day.
echo "--- days available for $VSN in this window ---"
crocus-qc discover --dataset "$DATASET" --vsn "$VSN" --start "$START" --end "$END"
echo

echo "=============================================================="
echo "$VSN  $START .. $END"
echo "=============================================================="
# --sql-profile leaves _duckdb_profile.json next to each product, so a surprising phase
# timing can be broken down per operator without rerunning the day.
crocus-qc run \
    --vsn "$VSN" \
    --start "$START" \
    --end "$END" \
    --dataset "$DATASET" \
    --config "$CONFIG" \
    --sql-profile
