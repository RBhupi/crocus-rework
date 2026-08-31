# W08D WXT/AQT two-day sparse-QC pilot

This pilot covers the half-open UTC interval
`[2025-12-15T00:00:00Z, 2025-12-17T00:00:00Z)`. It reads only the W08D,
instrument, and `date=2025-12-15|16` partitions on a host where the CROCUS NFS
is mounted.

The production input tree under `crocus-rework-output/wxt-aqt-production-v5`
is read-only to ADQAT. All pilot artifacts go to the separate sibling tree
`crocus-rework-output-tests-only/adqat-pilot-output`.

The workflow writes restartable sparse Parquet QA/QC evidence, one dataset file
per UTC day. It does not create NetCDF or Level 2 aggregates. Raw timestamps
and values are never regularized, interpolated, or rewritten. Only actual
invalid values and configured physical/instrument range failures are flagged.
A raw observation absent from `qc_flags.parquet` is valid under these rules; no
row is synthesized for a nominal 10 Hz timestamp.

The candidate limits are based on Vaisala documentation but are not approved
CROCUS science rules. Every run snapshots the rules with status `pilot`.

## 1. Install on the data host

From the checked-out `adqat/` directory:

```bash
cd /nfs/gce/projects/crocus-server-admins/data-rework/crocus-rework/adqat
mamba env create -f environment.yml
mamba run -n adqat adqat --help
```

For an existing environment, refresh the editable package after pulling:

```bash
mamba run -n adqat python -m pip install -e '.[dev]'
```

## 2. Validate and run WXT first

```bash
mamba run -n adqat adqat validate \
  examples/processing_run.w08d_wxt_20251215_20251216_pilot.yaml

/usr/bin/time -v mamba run -n adqat adqat run \
  examples/processing_run.w08d_wxt_20251215_20251216_pilot.yaml \
  --work-unit w08d_wxt536_20251215_20251216 \
  --run-id w08d-wxt-20251215-16-sparse-qc-v1

WXT_RUN=/nfs/gce/projects/crocus-server-admins/data-rework/crocus-rework-output-tests-only/adqat-pilot-output/runs/w08d-wxt-20251215-16-sparse-qc-v1
mamba run -n adqat adqat report "$WXT_RUN" | tee /tmp/w08d-wxt-sparse-qc-report.txt
mamba run -n adqat adqat report "$WXT_RUN" --json > /tmp/w08d-wxt-sparse-qc-report.json
```

Use a new immutable run ID. Do not reuse or modify the earlier successful
NetCDF pilot runs.

Every successful period must contain the same four artifacts:

```text
findings.parquet
check_results.parquet
qc_flags.parquet
success.json
```

List them without reopening the source:

```bash
find "$WXT_RUN/work_units" -type f \
  \( -name success.json -o -name '*.parquet' \) -print | sort
```

## 3. Query and verify the sparse QC dataset

Query both daily files as one logical table:

```bash
mamba run -n adqat python - "$WXT_RUN" <<'PY'
import sys
import duckdb

run = sys.argv[1]
checks = f"{run}/work_units/*/*/check_results.parquet"
flags = f"{run}/work_units/*/*/qc_flags.parquet"
con = duckdb.connect()

print(con.execute("""
    SELECT variable, check_id, flag_name,
           sum(units_tested) AS tested,
           sum(units_failed) AS failed,
           round(sum(units_failed) / nullif(sum(units_tested), 0), 8)
             AS fraction_failed
    FROM read_parquet(?)
    GROUP BY variable, check_id, flag_name
    ORDER BY variable, check_id
""", [checks]).fetchdf().to_string(index=False))

print(con.execute("""
    SELECT sensor, vsn, instrument_id, variable, qc_bits,
           run_id, work_unit_id, config_hash
    FROM read_parquet(?, union_by_name = true)
    WHERE vsn = 'W08D' AND qc_bits <> 0
    ORDER BY time, variable
""", [flags]).fetchdf().to_string(index=False))
PY
```

Check per-period counts and require all masks to be nonzero:

```bash
mamba run -n adqat python - "$WXT_RUN" <<'PY'
import glob
import json
import sys
import pyarrow.parquet as pq

for marker in sorted(glob.glob(f"{sys.argv[1]}/work_units/*/*/success.json")):
    with open(marker, encoding="utf-8") as stream:
        success = json.load(stream)
    flags = pq.read_table(marker.replace("success.json", "qc_flags.parquet"))
    masks = flags["qc_bits"].to_pylist()
    assert all(mask > 0 for mask in masks)
    assert len(masks) == success["flagged_observations"]
    print(marker, "rows=", success["rows_processed"], "flagged=", len(masks))
PY
```

WXT gate before AQT:

- two successful periods, each with all three Parquet tables;
- `flagged_observations` equals the sparse flag-table row count;
- every persisted `qc_bits` value is greater than zero;
- broad clean pilot data may produce typed zero-row flag tables;
- bit 1 is absent because cadence QC is disabled;
- zero-unit variables are reviewed as unavailable, not called missing samples;
- unexpectedly large missing/range fractions are investigated before AQT;
- peak memory and elapsed time are acceptable.

Resume the same run after an interruption:

```bash
mamba run -n adqat adqat resume "$WXT_RUN"
```

## 4. Run AQT after the WXT gate passes

```bash
mamba run -n adqat adqat validate \
  examples/processing_run.w08d_aqt_20251215_20251216_pilot.yaml

/usr/bin/time -v mamba run -n adqat adqat run \
  examples/processing_run.w08d_aqt_20251215_20251216_pilot.yaml \
  --work-unit w08d_aqt530_20251215_20251216 \
  --run-id w08d-aqt-20251215-16-sparse-qc-v1

AQT_RUN=/nfs/gce/projects/crocus-server-admins/data-rework/crocus-rework-output-tests-only/adqat-pilot-output/runs/w08d-aqt-20251215-16-sparse-qc-v1
mamba run -n adqat adqat report "$AQT_RUN" | tee /tmp/w08d-aqt-sparse-qc-report.txt
```

Repeat the Parquet queries and verification above with `AQT_RUN`.

## 5. Full-history rollout after scientific approval

Keep the quality-rules file reusable. Create one processing file per selected
VSN/instrument and change only the source partition/glob, UTC selection,
work-unit identity and filters, output root, and immutable run ID.

Replace pilot limits with reviewed values and set metadata status to
`approved`. Keep daily periods so resume scope and memory remain bounded. Do
not enable cadence or aggregation until each variable's expected interval and
minimum coverage are explicit.
