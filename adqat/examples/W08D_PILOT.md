# W08D WXT/AQT two-day A2Z pilot

This pilot covers the half-open UTC interval
`[2025-12-15T00:00:00Z, 2025-12-17T00:00:00Z)`. It reads only the W08D,
instrument, and `date=2025-12-15|16` partitions. Run it on a host where the
CROCUS production NFS is mounted.

The production input tree under `crocus-rework-output/wxt-aqt-production-v5`
is read-only to ADQAT. All pilot artifacts are written to the separate sibling
tree `crocus-rework-output-tests-only/adqat-pilot-output`.

The pilot creates restartable Parquet QA/QC evidence plus one native Level 1
NetCDF file per UTC day. It does **not** create Level 2 aggregates. Numeric AQT
variables receive missing/range checks; the string instrument clock
(`aqt.house.datetime`) receives missing/sentinel checks without a meaningless
numeric range.

The candidate limits are based on Vaisala documentation but are not approved
CROCUS science rules. The snapshotted rule status in every run is `pilot`.

## 1. Install on the data host

First make this updated subproject available on the data host (through your
normal Git or file-sync workflow), then run from its `adqat/` directory. The
handover's NFS checkout is normally:

```bash
cd /nfs/gce/projects/crocus-server-admins/data-rework/crocus-rework/adqat
```

Create the environment:

```bash
mamba env create -f environment.yml
mamba run -n adqat adqat --help
```

If the environment already exists, update the editable installation after a
code change:

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
  --run-id w08d-wxt-20251215-16-pilot-v2
```

The run directory is:

```text
/nfs/gce/projects/crocus-server-admins/data-rework/crocus-rework-output-tests-only/adqat-pilot-output/runs/w08d-wxt-20251215-16-pilot-v2
```

Generate the persisted-evidence report:

```bash
WXT_RUN=/nfs/gce/projects/crocus-server-admins/data-rework/crocus-rework-output-tests-only/adqat-pilot-output/runs/w08d-wxt-20251215-16-pilot-v2
mamba run -n adqat adqat report "$WXT_RUN" | tee /tmp/w08d-wxt-pilot-report.txt
mamba run -n adqat adqat report "$WXT_RUN" --json > /tmp/w08d-wxt-pilot-report.json
```

Check the success markers and NetCDF files without opening the source again:

```bash
find "$WXT_RUN/work_units" -type f \
  \( -name success.json -o -name '*.nc' \) -print | sort
```

Expected filename shape:

```text
neiu.wxt536.W08D.native.a1.20251215T000000Z-20251216T000000Z.nc
neiu.wxt536.W08D.native.a1.20251216T000000Z-20251217T000000Z.nc
```

Inspect the two daily result tables with DuckDB:

```bash
mamba run -n adqat python - "$WXT_RUN" <<'PY'
import sys
import duckdb

run = sys.argv[1]
checks = f"{run}/work_units/*/*/check_results.parquet"
flags = f"{run}/work_units/*/*/qc_flags.parquet"

con = duckdb.connect()
result = con.execute("""
    SELECT variable, check_id, flag_name,
           sum(units_tested) AS tested,
           sum(units_failed) AS failed,
           round(sum(units_failed) / nullif(sum(units_tested), 0), 8) AS fraction_failed
    FROM read_parquet(?)
    GROUP BY variable, check_id, flag_name
    ORDER BY variable, check_id
""", [checks])
print("\t".join(column[0] for column in result.description))
for row in result.fetchall():
    print("\t".join(str(value) for value in row))
print(con.execute("""
    SELECT qc_bits, count(*) AS observations
    FROM read_parquet(?)
    GROUP BY qc_bits
    ORDER BY qc_bits
""", [flags]).fetchall())
PY
```

Inspect the NetCDF contract and QC distribution:

```bash
mamba run -n adqat python - "$WXT_RUN" <<'PY'
import glob
import sys
import numpy as np
from netCDF4 import Dataset

for path in sorted(glob.glob(f"{sys.argv[1]}/work_units/*/*/*.nc")):
    with Dataset(path) as nc:
        qc = np.asarray(nc.variables["qc_bits"][:], dtype=np.uint64)
        values, counts = np.unique(qc, return_counts=True)
        print(path)
        print(" observations:", len(nc.dimensions["observation"]))
        print(" actual coverage:", nc.time_coverage_start, nc.time_coverage_end)
        print(" config hash:", nc.qaqc_rule_fingerprint)
        print(" qc distribution:", dict(zip(values.tolist(), counts.tolist(), strict=True)))
PY
```

WXT gate before AQT:

- two successful periods and two NetCDF files;
- every NetCDF observation count equals the corresponding `rows_processed`;
- no timestamp is outside its nominal UTC day (the writer verifies this);
- bit 1 is absent because cadence QC is disabled;
- zero-unit variables are reviewed as unavailable for W08D, not called missing
  samples (the inventory reports 9 WXT variables for W08D versus 11 globally);
- unexpectedly large missing/range fractions are investigated before AQT or a
  full-history run;
- peak memory and elapsed time from `/usr/bin/time -v` are acceptable.

Resume the same immutable run after an interruption:

```bash
mamba run -n adqat adqat resume "$WXT_RUN"
```

## 3. Run AQT after the WXT gate passes

```bash
mamba run -n adqat adqat validate \
  examples/processing_run.w08d_aqt_20251215_20251216_pilot.yaml

/usr/bin/time -v mamba run -n adqat adqat run \
  examples/processing_run.w08d_aqt_20251215_20251216_pilot.yaml \
  --work-unit w08d_aqt530_20251215_20251216 \
  --run-id w08d-aqt-20251215-16-pilot-v1

AQT_RUN=/nfs/gce/projects/crocus-server-admins/data-rework/crocus-rework-output-tests-only/adqat-pilot-output/runs/w08d-aqt-20251215-16-pilot-v1
mamba run -n adqat adqat report "$AQT_RUN" | tee /tmp/w08d-aqt-pilot-report.txt
```

Repeat the Parquet and NetCDF checks above with `AQT_RUN`. The expected NetCDF
filenames use `neiu.aqt530.W08D...`.

## 4. Full-history rollout after scientific approval

Keep the quality-rules file reusable. Create one processing file per selected
VSN/instrument and change only:

- source partition/glob;
- UTC selection;
- work-unit ID and equality filters;
- reviewed `site`/instrument filename tokens;
- output root and run ID.

Replace the pilot rules with reviewed values and set their metadata status to
`approved`. Use daily periods so resume scope and memory stay bounded. Do not
enable cadence or aggregation until each variable's expected interval and
minimum coverage are explicit.
