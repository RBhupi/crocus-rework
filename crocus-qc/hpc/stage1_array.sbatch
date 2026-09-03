#!/bin/bash
# One SLURM array task = one work unit (sensor x VSN x UTC day).
#
# SLURM is the iterator; DuckDB is the node-level parallelism. There is no Dask, Ray,
# Spark, MPI, or Python multiprocessing anywhere in this pipeline.
#
# Submit with the manifest and profile supplied from outside, so the same script serves
# both instruments at their different CPU sizings (see docs/stage1-performance.md):
#
#   MANIFEST=manifests/wxt.tsv PROFILE=wxt536 \
#     sbatch --array=1-$(wc -l < manifests/wxt.tsv) --cpus-per-task=8 hpc/stage1_array.sbatch
#
#   MANIFEST=manifests/aqt.tsv PROFILE=aqt530 \
#     sbatch --array=1-$(wc -l < manifests/aqt.tsv) --cpus-per-task=2 hpc/stage1_array.sbatch
#
#SBATCH --job-name=crocus-stage1
#SBATCH --time=00:30:00
#SBATCH --mem=8G
#SBATCH --output=logs/stage1-%A_%a.out
#SBATCH --error=logs/stage1-%A_%a.err

set -euo pipefail

: "${MANIFEST:?set MANIFEST to a sensor<TAB>vsn<TAB>date file from 'crocus-qc discover'}"
: "${PROFILE:?set PROFILE to an instrument profile name, e.g. wxt536}"
: "${DATASET:=/nfs/gce/projects/crocus-server-admins/data-rework/crocus-rework-output/wxt-aqt-production-v5}"
: "${CONFIG:=hpc/pipeline.yaml}"

# $TMPDIR is node-local; DuckDB spills there rather than onto NFS. crocus-qc creates the
# directory itself, but a job that inherits a stale TMPDIR should fail here, not at spill.
export TMPDIR="${TMPDIR:-/tmp}/crocus-qc-${SLURM_JOB_ID:-$$}"
mkdir -p "$TMPDIR"
trap 'rm -rf "$TMPDIR"' EXIT

read -r SENSOR VSN DAY < <(sed -n "${SLURM_ARRAY_TASK_ID}p" "$MANIFEST")

echo "work unit: $SENSOR $VSN $DAY   cpus=${SLURM_CPUS_PER_TASK:-?}  mem=${SLURM_MEM_PER_NODE:-?}MB"

# stdout is the provenance JSON; the phase timing table goes to stderr (the .err file).
srun crocus-qc run \
    --sensor "$SENSOR" \
    --vsn "$VSN" \
    --date "$DAY" \
    --dataset "$DATASET" \
    --config "$CONFIG" \
    --profile "$PROFILE" \
    ${SQL_PROFILE:+--sql-profile}
