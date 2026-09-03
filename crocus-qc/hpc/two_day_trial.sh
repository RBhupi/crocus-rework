#!/bin/bash
# First real-data trial: two UTC days of WXT536 and two of AQT530, run in the foreground.
#
# Deliberately not a SLURM array. The point of the first run is to read the output --
# the provenance record, the phase timings, the DuckDB operator profile -- not to get
# throughput. Once this looks right, scale up with hpc/stage1_array.sbatch.
#
#   salloc --cpus-per-task=8 --mem=8G --time=01:00:00
#   bash hpc/two_day_trial.sh 2025-12-15 2025-12-16
#
# Override the VSNs or paths from the environment:
#   WXT_VSN=W08D AQT_VSN=W08D CONFIG=hpc/pipeline.yaml bash hpc/two_day_trial.sh <day> <day>

set -euo pipefail

: "${DATASET:=/nfs/gce/projects/crocus-server-admins/data-rework/crocus-rework-output/wxt-aqt-production-v5}"
: "${CONFIG:=hpc/pipeline.yaml}"
: "${WXT_VSN:=W08D}"
: "${AQT_VSN:=W08D}"

if [ "$#" -lt 1 ]; then
    echo "usage: $0 YYYY-MM-DD [YYYY-MM-DD ...]" >&2
    exit 2
fi

export TMPDIR="${TMPDIR:-/tmp}/crocus-qc-trial-$$"
mkdir -p "$TMPDIR"
trap 'rm -rf "$TMPDIR"' EXIT

echo "dataset: $DATASET"
echo "config:  $CONFIG"
echo

# What is actually there. Cheap: directory names only, no Parquet is opened.
echo "--- work units available for these days ---"
for day in "$@"; do
    crocus-qc discover --dataset "$DATASET" --start "$day" --end "$day"
done
echo

for day in "$@"; do
    for pair in "vaisala-wxt536:wxt536:$WXT_VSN" "vaisala-aqt530:aqt530:$AQT_VSN"; do
        IFS=: read -r sensor profile vsn <<< "$pair"
        echo "=============================================================="
        echo "$sensor $vsn $day"
        echo "=============================================================="
        # --sql-profile leaves _duckdb_profile.json next to the product, so a surprising
        # phase timing can be broken down per operator without rerunning the day.
        crocus-qc run \
            --sensor "$sensor" \
            --vsn "$vsn" \
            --date "$day" \
            --dataset "$DATASET" \
            --config "$CONFIG" \
            --profile "$profile" \
            --sql-profile
        echo
    done
done
