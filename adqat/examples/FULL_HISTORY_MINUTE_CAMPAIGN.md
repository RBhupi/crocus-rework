# CROCUS full-history one-minute campaign

This runbook creates dense one-minute WXT536 and AQT530 products while leaving
the native CROCUS Parquet facts read-only. The quality rules remain marked
`pilot`; manufacturer limits are sourced from the bundled Vaisala datasheets,
while broader physical screens are engineering candidates pending science
approval.

## Product contract

For each instrument and UTC processing day, ADQAT writes:

- `minute_data.parquet`: one row per minute and configured variable, including
  explicit rows for entirely absent minutes;
- `findings.parquet`: sparse raw check evidence;
- `qc_flags.parquet`: sparse nonzero raw observation masks;
- `check_results.parquet`: raw check totals;
- `success.json`: atomic resume marker and row counts.

There is no 1-second product, raw timestamp grid, interpolation, synthetic raw
observation, or NetCDF output. Daily directories are restartable physical
partitions of one logical per-instrument Parquet table.

## 1. Install the checked-out revision

```bash
cd /nfs/gce/projects/crocus-server-admins/data-rework/crocus-rework/adqat
git pull --ff-only

ADQAT_ENV=/nfs/gce/projects/crocus-server-admins/data-rework/envs/adqat-braut
"$ADQAT_ENV/bin/python" -m pip install -e '.[dev]'
"$ADQAT_ENV/bin/adqat" --help
dfq
```

## 2. Validate and run the two-day minute pilots

```bash
"$ADQAT_ENV/bin/adqat" validate \
  examples/processing_run.w08d_wxt_20251215_20251216_minute_pilot.yaml
"$ADQAT_ENV/bin/adqat" validate \
  examples/processing_run.w08d_aqt_20251215_20251216_minute_pilot.yaml

"$ADQAT_ENV/bin/adqat" run \
  examples/processing_run.w08d_wxt_20251215_20251216_minute_pilot.yaml \
  --work-unit w08d_wxt536_20251215_20251216_minute \
  --run-id w08d-wxt-minute-20251215-16-v1

"$ADQAT_ENV/bin/adqat" run \
  examples/processing_run.w08d_aqt_20251215_20251216_minute_pilot.yaml \
  --work-unit w08d_aqt530_20251215_20251216_minute \
  --run-id w08d-aqt-minute-20251215-16-v1
```

Expected dense row counts are 31,680 for WXT (2 days × 1,440 minutes ×
11 variables) and 34,560 for AQT (2 × 1,440 × 12). Missing source coverage is
represented in those totals, not omitted.

Inspect representative values and QC:

```bash
OUT=/nfs/gce/projects/crocus-server-admins/data-rework/crocus-rework-output-tests-only/adqat-minute-pilot-output

"$ADQAT_ENV/bin/python" - "$OUT" <<'PY'
import duckdb, sys
root = sys.argv[1]
connection = duckdb.connect()
for run_id in (
    "w08d-wxt-minute-20251215-16-v1",
    "w08d-aqt-minute-20251215-16-v1",
):
    pattern = f"{root}/runs/{run_id}/work_units/*/*/minute_data.parquet"
    row = connection.execute("""
        SELECT count(*) AS row_count,
               sum(total_count = 0) missing_rows,
               sum(qc_bits <> 0) flagged_rows,
               sum(valid_count) valid_observations
        FROM read_parquet(?)
    """, [pattern]).fetchone()
    print(run_id, row)
PY
```

Before the full campaign, inspect wind wraparound and cumulative/categorical
aggregation with DuckDB. `wind_direction` uses `circular_mean`, rain/hail use
`last`, and heater status uses `mode`.

## 3. Generate the AQT campaign without running it

```bash
DATASET_OK=/nfs/gce/projects/crocus-server-admins/data-rework/crocus-rework-output/wxt-aqt-production-v5
MINUTE_OUT=/nfs/gce/projects/crocus-server-admins/data-rework/crocus-rework-output-tests-only/adqat-minute-full-output

"$ADQAT_ENV/bin/python" examples/full_history_minute_campaign.py \
  --dataset-root "$DATASET_OK" \
  --output-root "$MINUTE_OUT" \
  --kind aqt \
  --campaign-id minute-aqt-full-v1 \
  --adqat "$ADQAT_ENV/bin/adqat"
```

Review the generated `manifest.json` and several YAML files below
`$MINUTE_OUT/campaigns/minute-aqt-full-v1/`. The inventory must contain exactly
one instrument per VSN; the generator rejects duplicates.

## 4. Run AQT under nohup

```bash
nohup "$ADQAT_ENV/bin/python" examples/full_history_minute_campaign.py \
  --dataset-root "$DATASET_OK" \
  --output-root "$MINUTE_OUT" \
  --kind aqt \
  --campaign-id minute-aqt-full-v1 \
  --max-parallel 4 \
  --adqat "$ADQAT_ENV/bin/adqat" \
  --execute \
  > "$MINUTE_OUT/campaigns/minute-aqt-full-v1/nohup.log" 2>&1 &

echo $!
tail -f "$MINUTE_OUT/campaigns/minute-aqt-full-v1/nohup.log"
```

The launcher starts one process per instrument, bounded by `--max-parallel`.
If it is restarted, an existing run directory is resumed and completed period
markers are trusted without rereading source data.

## 5. Run WXT after AQT acceptance

WXT is much larger. Start with two concurrent jobs; increase only after checking
memory, I/O behavior, and quota on the compute node.

```bash
mkdir -p "$MINUTE_OUT/campaigns/minute-wxt-full-v1"

nohup "$ADQAT_ENV/bin/python" examples/full_history_minute_campaign.py \
  --dataset-root "$DATASET_OK" \
  --output-root "$MINUTE_OUT" \
  --kind wxt \
  --campaign-id minute-wxt-full-v1 \
  --max-parallel 2 \
  --adqat "$ADQAT_ENV/bin/adqat" \
  --execute \
  > "$MINUTE_OUT/campaigns/minute-wxt-full-v1/nohup.log" 2>&1 &
```

## 6. Query the logical products

```sql
SELECT vsn, variable,
       count(*) AS minute_rows,
       sum(total_count = 0) AS entirely_missing_minutes,
       sum(qc_bits <> 0) AS flagged_minutes
FROM read_parquet(
  '/nfs/gce/projects/crocus-server-admins/data-rework/crocus-rework-output-tests-only/adqat-minute-full-output/runs/*/work_units/*/*/minute_data.parquet',
  union_by_name = true
)
GROUP BY vsn, variable
ORDER BY vsn, variable;
```

Sparse raw evidence remains independently queryable from the corresponding
`qc_flags.parquet` dataset. A raw observation absent from that sparse table
passed the configured direct-value checks.
