#!/bin/bash
# One SLURM array task = one VSN, walking that station's whole calendar.
#
# SLURM is the iterator across stations; DuckDB is the node-level parallelism within a
# day. There is no Dask, Ray, Spark, MPI, or Python multiprocessing anywhere in this
# pipeline. A task is deliberately a station rather than a station-day: ~7,000 one-day
# tasks would be 7,000 interpreter and DuckDB startups for the scheduler to queue and
# log, around a reduction that takes about a second.
#
# Build the station list once, then submit one task per line:
#
#   crocus-qc discover --dataset "$DATASET" | cut -f1 | sort -u > vsns.txt
#   VSNS=vsns.txt sbatch --array=1-$(wc -l < vsns.txt) --cpus-per-task=8 \
#       hpc/stage1_array.sbatch
#
# Restrict the window with START/END; omitted, each station uses its own first and last
# day present in the dataset. Sizing guidance is in docs/stage1-performance.md.
#
#SBATCH --job-name=crocus-stage1
#SBATCH --time=04:00:00
#SBATCH --mem=8G
#SBATCH --output=logs/stage1-%A_%a.out
#SBATCH --error=logs/stage1-%A_%a.err

set -euo pipefail

: "${VSNS:?set VSNS to a file with one VSN per line, e.g. from 'crocus-qc discover | cut -f1 | sort -u'}"
: "${DATASET:=/nfs/gce/projects/crocus-server-admins/data-rework/crocus-rework-output/wxt-aqt-production-v5}"
: "${CONFIG:=hpc/pipeline.yaml}"
: "${START:=}"
: "${END:=}"

# $TMPDIR is node-local; DuckDB spills there rather than onto NFS. crocus-qc creates the
# directory itself, but a job that inherits a stale TMPDIR should fail here, not at spill.
export TMPDIR="${TMPDIR:-/tmp}/crocus-qc-${SLURM_JOB_ID:-$$}"
mkdir -p "$TMPDIR"
trap 'rm -rf "$TMPDIR"' EXIT

read -r VSN < <(sed -n "${SLURM_ARRAY_TASK_ID}p" "$VSNS")

echo "station: $VSN   cpus=${SLURM_CPUS_PER_TASK:-?}  mem=${SLURM_MEM_PER_NODE:-?}MB"

# stdout is JSONL, one provenance record per day produced. Skipped and failed days go to
# stderr (the .err file), along with the per-day phase timings.
srun crocus-qc run \
    --vsn "$VSN" \
    ${START:+--start "$START"} \
    ${END:+--end "$END"} \
    --dataset "$DATASET" \
    --config "$CONFIG" \
    ${SQL_PROFILE:+--sql-profile}
