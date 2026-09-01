# ADQAT current fixed-period QA/QC and NetCDF: implementation and HPC handoff

**Status:** Implemented locally; real-data two-day aggregate validation is the next
required gate before the full-history campaign.

**ADQAT target:** 0.1.4 current pipeline. This is not the proposed ADQAT 0.2
architecture.

**Purpose:** Preserve the decisions, implementation context, data inventory,
commands, acceptance criteria, and recovery procedure so work can continue in a
new session on the remote compute host without reconstructing the discussion.

## 1. Objective

Process the complete CROCUS WXT536 and AQT530 long-format Parquet facts into:

1. sparse evidence for invalid native observations; and
2. a dense, queryable fixed-period Parquet product for every configured variable; and
3. an optional wide NetCDF product with a paired unsigned 8-bit QC variable.

Native input must remain read-only. The current campaign does not create a
1-second product. QA/QC limits are candidate pilot rules based on the
manufacturer datasheets and sensible physical screens; they are not yet an
approved CROCUS scientific standard.

## 2. Final decisions from the design discussion

- Keep original raw timestamps and values unchanged.
- Do not regularize raw observations to a 0.1-second or 1-second grid.
- Do not interpolate, create synthetic raw rows, or infer exact missing native
  samples.
- Apply only direct-value checks at raw resolution:
  - actual null, NaN, or configured sentinel;
  - physical range;
  - manufacturer instrument/operating range.
- A raw observation is eligible for aggregation only when its raw QC
  mask is zero.
- Retain only bad raw observations in the sparse `qc_flags.parquet` table.
- Create one dense row for every configured UTC interval and variable. The
  interval is configured as `{period: <integer>, units: seconds|minutes|hours}`.
- Aggregate QC is independent from raw QC and is stored as `uint8`. Bit 0 is
  insufficient coverage; bit 1 excessive variability; bit 2 stuck/constant;
  bits 3/4 below/above physical limits; bits 5/6 below/above instrument limits;
  bit 7 is reserved and always zero.
- A missing or insufficient interval has a null Parquet value. In NetCDF the
  data variable contains `_FillValue=-999.0` and its QC value is 1 (bit 0), not
  missing.
- Daily directories are atomic processing/resume partitions. They form one
  logical per-instrument table when read through a Parquet glob; they are not
  independent scientific products.
- Keep `instrument_id` as immutable provenance and an equality filter. It is not
  required in the source glob because the current inventory contains one
  instrument per VSN.
- NetCDF uses a dense time coordinate, paired `qc_<variable>` unsigned 8-bit
  fields, count/statistic diagnostics, provenance, and atomic publication.

## 3. Why nominal 10 Hz is not a current completeness rule

The stored WXT facts can appear near 8–10 rows/s, but the WXT530 datasheet says
wind sampling is configurable at 1, 2, or 4 Hz. Stored telemetry frequency is
therefore not sufficient evidence of independent sensor cadence.

Current behavior:

- `observed_rate_hz = total_count / aggregation_period_seconds` is diagnostic metadata;
- `maximum_gap_seconds` records the largest observed within-minute timestamp
  separation;
- a completely absent interval sets aggregate bit 0;
- no exact timestamp expectation or partial-minute completeness threshold is
  enforced.

Coverage is controlled by explicit minimum valid count and optional minimum
valid fraction in the separate aggregate-rule YAML, without changing ingestion.

## 4. Input and compute environment

Remote host information observed during the pilot:

```text
host: compute-386-07
memory: 251 GiB total, approximately 238 GiB available at inspection
CPUs: 56
Slurm: sbatch was not available
quota command: dfq
```

Read-only input root:

```text
/nfs/gce/projects/crocus-server-admins/data-rework/crocus-rework-output/wxt-aqt-production-v5
```

Required test/full output roots are outside the source tree:

```text
/nfs/gce/projects/crocus-server-admins/data-rework/crocus-rework-output-tests-only/adqat-minute-pilot-output
/nfs/gce/projects/crocus-server-admins/data-rework/crocus-rework-output-tests-only/adqat-minute-full-output
```

Never point `output.root` under the production facts tree. ADQAT also rejects
source/output overlap before processing.

The facts have the following relevant schema:

```text
time             timestamp[ns, tz=UTC]
sensor           string
vsn              string
instrument_id    string
measurement      string
field            string
series_id        fixed_size_binary[16]
value_type       string
value_float64    double nullable
value_string     string nullable
```

The source adapter preserves nanosecond timestamps. The earlier PyArrow sample
display failure was only Python `datetime` conversion of nanoseconds; reading
timestamp values as integer nanoseconds confirmed the source was valid.

## 5. Existing real-data pilot evidence

The earlier native-resolution, sparse-QC W08D pilots completed successfully.
They did not yet create the new minute product.

### WXT W08D, 2025-12-15 through 2025-12-16

```text
instrument: W08D--vaisala-wxt536--core--934c67f6166a
raw rows: 9,534,537
periods: 2
findings: 0 under the broad pilot limits
elapsed: approximately 8 seconds
maximum resident memory: approximately 6.3 GiB
```

The first day contained data only from approximately 19:20 UTC onward and the
second day ended around 20:00 UTC. The dense minute product must still contain
all 2,880 requested minutes for every WXT variable.

### AQT W08D, 2025-12-15 through 2025-12-16

```text
instrument: W08D--vaisala-aqt530--core--df6b0090a23b
raw rows: 53,796
string observations: 4,483
periods: 2
findings: 0 under the broad pilot limits
elapsed: approximately 1.6 seconds
maximum resident memory: approximately 0.4 GiB
```

These runs established source pushdown, raw engine compatibility, sparse
Parquet persistence, atomic period publication, and input immutability.

## 6. Full inventory used by the campaign generator

The checked-in coverage inventories report:

- 22 WXT VSN/instrument work units;
- 15 AQT VSN/instrument work units;
- one instrument per VSN in each inventory;
- 37 total independent jobs.

Inventory files:

```text
wxt-aqt-production-v5-report/wxt_vsn_instrument_coverage.csv
wxt-aqt-production-v5-report/aqt_vsn_instrument_coverage.csv
```

Estimated dense long-table size from the recorded date ranges:

```text
WXT: 9,411 instrument-days × 1,440 × 11 = 149,070,240 rows
AQT: 6,278 instrument-days × 1,440 × 12 = 108,483,840 rows
total: 257,554,080 minute-variable rows before Parquet compression
```

These are much smaller than native facts but are not trivial. Check `dfq`
before launch and monitor output growth during AQT before starting WXT.

## 7. Raw and aggregate QC bits

```text
bit 0: missing_value
bit 1: missing_sample (declared but unused by current raw processing)
bit 2: physical_range
bit 3: instrument_range
```

Raw `qc_flags.parquet` contains only nonzero masks. Multiple failures for one
raw observation are compiled using unsigned 64-bit `BIT_OR`.

The aggregate `qc_bits` column is independent and strictly unsigned 8-bit:

```text
bit 0: insufficient coverage or no valid aggregate
bit 1: excessive variability within the aggregation interval
bit 2: stuck/constant value within the aggregation interval
bit 3: aggregate below physical minimum
bit 4: aggregate above physical maximum
bit 5: aggregate below instrument minimum
bit 6: aggregate above instrument maximum
bit 7: reserved; always zero
```

Raw failures only exclude inputs and affect the count diagnostics. They are not
ORed into the aggregate mask. `aggregate_valid` is false exactly when bit 0 is
set; other aggregate flags preserve the calculated value for review.

## 8. Fixed-period output schema and statistics

`aggregate_data.parquet` contains identity/provenance, representative values,
counts, diagnostics, statistics, and aggregate QC:

```text
time                         UTC aggregation-interval start, timestamp[ns]
sensor, vsn, instrument_id
variable, units, aggregation_method
aggregation_period, aggregation_period_units, aggregation_period_seconds
value_float64, value_string
total_count, valid_count, invalid_count
missing_value_count
physical_range_count
instrument_range_count
valid_fraction
observed_rate_hz
maximum_gap_seconds
mean, median, standard_deviation
minimum, maximum, q25, q75, iqr
circular_resultant_length
aggregate_valid
qc_bits
run_id, work_unit_id, config_hash
```

Statistics use only valid raw observations. Standard deviation is population
standard deviation (`ddof=0`). For circular variables, `mean` is circular,
standard deviation is circular, and linear median/quartile/min/max/IQR fields
are null because their ordering would be misleading.

## 9. Variable aggregation policy

### WXT536

| Variable | CROCUS measurement | Method |
|---|---|---|
| air temperature | `wxt.env.temp` | mean |
| relative humidity | `wxt.env.humidity` | mean |
| air pressure | `wxt.env.pressure` | mean |
| wind speed | `wxt.wind.speed` | mean |
| wind direction | `wxt.wind.direction` | circular mean |
| rain accumulation | `wxt.rain.accumulation` | last valid cumulative value |
| hail accumulation | `wxt.hail.accumulation` | last valid cumulative value |
| heater temperature | `wxt.heater.temp` | mean |
| heater voltage | `wxt.heater.volt` | mean |
| heater status | `wxt.heater.status` | deterministic mode |
| supply voltage | `wxt.voltage.supply` | mean |

Wind direction must never use an arithmetic mean. For example, 359° and 1°
produce a circular mean of 0°, not 180°.

The WXT530 datasheet documents instrument ranges including temperature
−52–60 °C, RH 0–100%, pressure 500–1100 hPa, wind speed observation range
0–60 m/s, and direction 0–360°. Rain and hail are documented as cumulative
counters but do not have manufacturer maximums in the supplied datasheet;
their finite screens are explicitly labeled engineering candidates.

### AQT530

| Variable | CROCUS measurement | Method |
|---|---|---|
| air temperature | `aqt.env.temp` | mean |
| relative humidity | `aqt.env.humidity` | mean |
| air pressure | `aqt.env.pressure` | mean |
| carbon monoxide | `aqt.gas.co` | mean |
| nitric oxide | `aqt.gas.no` | mean |
| nitrogen dioxide | `aqt.gas.no2` | mean |
| ozone | `aqt.gas.ozone` | mean |
| instrument datetime | `aqt.house.datetime` | last valid string |
| instrument uptime | `aqt.house.uptime` | last valid value |
| PM1 | `aqt.particle.pm1` | mean |
| PM2.5 | `aqt.particle.pm2.5` | mean |
| PM10 | `aqt.particle.pm10` | mean |

The AQT530 datasheet ranges used include temperature −30–40 °C, RH 15–100%,
pressure 800–1150 hPa, CO 0–10 ppm, NO/NO2/O3 0–2 ppm, PM2.5 0–1000 µg/m³,
and PM10 0–2500 µg/m³. The datasheet does not publish PM1 or uptime ranges, so
the rules must not label invented bounds as manufacturer limits.

## 10. Implemented files

Core implementation:

```text
src/adqat/minute.py
src/adqat/config.py
src/adqat/runner.py
src/adqat/store.py
src/adqat/report.py
src/adqat/cli.py
```

Current rules and two-day configs:

```text
examples/quality_rules.crocus_wxt_aqt_pilot.yaml
examples/aggregate_quality_rules.crocus_wxt_aqt_minute_pilot.yaml
examples/processing_run.w08d_wxt_20251215_20251216_minute_pilot.yaml
examples/processing_run.w08d_aqt_20251215_20251216_minute_pilot.yaml
```

Campaign tooling and operational runbook:

```text
examples/full_history_minute_campaign.py
examples/FULL_HISTORY_MINUTE_CAMPAIGN.md
```

Tests:

```text
tests/test_aggregate.py
tests/test_pilot_examples.py
```

ADQAT was bumped to version 0.1.4. Configuration remains schema version 1.
Fixed-period aggregation is opt-in:

```yaml
processing: {period: 1d, aggregation: {period: 1, units: minutes}}
```

Every selected profile variable must then declare one of `mean`,
`circular_mean`, `mode`, or `last`. Strict validation rejects missing or
type-incompatible methods.

## 11. Local verification already completed

```text
pytest: 46 tests passed
Ruff: passed
git diff --check: passed
generated campaign configurations: 37
generated configurations loaded through the strict ADQAT schema: passed
```

The local default Python was 3.13, while ADQAT targets Python 3.12. Ruff was
available from another local environment. The remote Python 3.12 environment
must run the complete release checks, including mypy, import-linter, coverage,
and package build.

## 12. Required remote execution plan

### Gate A: inspect revision and install

```bash
REPO=/nfs/gce/projects/crocus-server-admins/data-rework/crocus-rework/adqat
ADQAT_ENV=/nfs/gce/projects/crocus-server-admins/data-rework/envs/adqat-braut

cd "$REPO"
git branch --show-current
git rev-parse --short HEAD
git status --short
git pull --ff-only

"$ADQAT_ENV/bin/python" -V
"$ADQAT_ENV/bin/python" -m pip install -e '.[dev]'
"$ADQAT_ENV/bin/adqat" --help
dfq
```

Confirm Python is 3.12 and the output filesystem has adequate quota.

### Gate B: run release checks

```bash
"$ADQAT_ENV/bin/python" -m pytest --cov=adqat --cov-report=term-missing
"$ADQAT_ENV/bin/python" -m ruff check .
"$ADQAT_ENV/bin/python" -m mypy src/adqat
"$ADQAT_ENV/bin/lint-imports"
"$ADQAT_ENV/bin/python" -m build
```

Stop if any command fails. Capture its complete output before changing code.

### Gate C: validate two-day real-data configurations

```bash
"$ADQAT_ENV/bin/adqat" validate \
  examples/processing_run.w08d_wxt_20251215_20251216_minute_pilot.yaml

"$ADQAT_ENV/bin/adqat" validate \
  examples/processing_run.w08d_aqt_20251215_20251216_minute_pilot.yaml
```

### Gate D: execute two-day real-data minute pilots

Use new immutable run IDs. If these IDs already exist, use `adqat resume` or
choose a new version suffix; never overwrite a successful run.

```bash
"$ADQAT_ENV/bin/adqat" run \
  examples/processing_run.w08d_wxt_20251215_20251216_minute_pilot.yaml \
  --work-unit w08d_wxt536_20251215_20251216_minute \
  --run-id w08d-wxt-minute-20251215-16-v1

"$ADQAT_ENV/bin/adqat" run \
  examples/processing_run.w08d_aqt_20251215_20251216_minute_pilot.yaml \
  --work-unit w08d_aqt530_20251215_20251216_minute \
  --run-id w08d-aqt-minute-20251215-16-v1
```

Expected minute row counts:

```text
WXT: 2 × 1,440 × 11 = 31,680
AQT: 2 × 1,440 × 12 = 34,560
```

### Gate E: inspect pilot artifacts

```bash
MINUTE_PILOT=/nfs/gce/projects/crocus-server-admins/data-rework/crocus-rework-output-tests-only/adqat-minute-pilot-output

"$ADQAT_ENV/bin/adqat" report \
  "$MINUTE_PILOT/runs/w08d-wxt-minute-20251215-16-v1"
"$ADQAT_ENV/bin/adqat" report \
  "$MINUTE_PILOT/runs/w08d-aqt-minute-20251215-16-v1"
```

Run the verification snippet in
`examples/FULL_HISTORY_MINUTE_CAMPAIGN.md`. Additionally inspect:

- WXT direction around north to prove circular wraparound;
- rain/hail output to confirm last-value behavior;
- heater status to confirm mode behavior;
- AQT instrument datetime and uptime to confirm last-value behavior;
- missing rows before and after actual partial-day coverage;
- `total_count`, `valid_count`, and `valid_fraction` consistency;
- all rows with `aggregate_valid=false`;
- all nonzero minute masks and the raw sparse evidence behind them;
- source file size and mtime unchanged.

Do not proceed to the full campaign until both pilot products pass inspection.

### Gate F: generate and inspect full AQT jobs

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

Expected: 15 generated AQT job YAML files. Review `manifest.json`, W08D, the
short W039 deployment, and one long deployment before execution.

### Gate G: launch full AQT under nohup

Create the campaign directory before shell redirection because the shell opens
the log before the Python process can create it:

```bash
mkdir -p "$MINUTE_OUT/campaigns/minute-aqt-full-v1"

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

The launcher uses a stable run ID per instrument. If restarted, it calls
`adqat resume` for existing run directories. Successful daily markers are
trusted; unfinished periods are recomputed atomically.

### Gate H: accept AQT, then launch WXT conservatively

Check AQT failures, counts, disk use, and queryability before WXT. WXT is much
larger and previously required about 6.3 GiB for one two-day raw pilot process.
Start with two concurrent WXT jobs.

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

Increase concurrency only after observing stable memory, NFS throughput, and
quota. More CPUs do not imply that more NFS readers will be faster.

## 13. Monitoring and recovery

Useful checks:

```bash
ps -ef | grep '[f]ull_history_minute_campaign.py'
dfq
du -sh "$MINUTE_OUT"
find "$MINUTE_OUT/runs" -name success.json | wc -l
find "$MINUTE_OUT/campaigns" -name '*.log' -type f -size +0 -print
```

Inspect failed job logs under the campaign `logs/` directory. The top-level
`nohup.log` reports `PASS` or `FAIL(return-code)` per instrument.

If the launcher or host stops:

1. confirm no old launcher/process is still running;
2. rerun the exact same campaign command and campaign ID;
3. existing instrument runs will resume;
4. completed daily periods will be skipped without touching source;
5. corrupt/missing markers cause only that period to be recomputed.

Do not delete or modify successful period directories during normal recovery.

## 14. Logical dataset queries

All minute periods across all runs can be queried directly:

```sql
SELECT vsn, variable,
       count(*) AS aggregate_rows,
       sum(total_count = 0) AS entirely_missing_intervals,
       sum(aggregate_valid) AS valid_aggregate_minutes,
       sum(qc_bits <> 0) AS flagged_intervals
FROM read_parquet(
  '/nfs/gce/projects/crocus-server-admins/data-rework/crocus-rework-output-tests-only/adqat-minute-full-output/runs/*/work_units/*/*/aggregate_data.parquet',
  union_by_name = true
)
GROUP BY vsn, variable
ORDER BY vsn, variable;
```

Sparse raw flags remain a separate logical table:

```sql
SELECT sensor, vsn, instrument_id, variable, qc_bits, count(*)
FROM read_parquet(
  '/nfs/gce/projects/crocus-server-admins/data-rework/crocus-rework-output-tests-only/adqat-minute-full-output/runs/*/work_units/*/*/qc_flags.parquet',
  union_by_name = true
)
GROUP BY sensor, vsn, instrument_id, variable, qc_bits
ORDER BY sensor, vsn, variable, qc_bits;
```

No database import or catalog build is required. Parquet projection and filter
pushdown apply through DuckDB.

## 15. Acceptance criteria for the full campaign

- All 15 AQT and 22 WXT instrument jobs complete successfully.
- Every expected period contains `success.json`, `aggregate_data.parquet`,
  `findings.parquet`, `qc_flags.parquet`, and `check_results.parquet`.
- NetCDF is produced only when an approved VSN-to-site mapping is configured;
  no 1-second intermediate product is produced.
- Output remains entirely below the tests-only output root.
- Production source files retain their original size and modification time.
- Aggregate rows are unique by `time` and `variable` within a work unit.
- `total_count >= valid_count`; `invalid_count = total_count - valid_count`.
- `aggregate_valid` is false exactly when aggregate bit 0 is set.
- Missing or insufficient intervals have bit 0 and null representative values.
- Sparse raw masks are always nonzero and unique by raw observation identity.
- Wind direction behaves correctly across 0/360°.
- Cumulative and categorical variables use their configured methods.
- All files are readable together with `read_parquet(..., union_by_name=true)`.
- Reports and manifests preserve run ID, work unit, config hash, dependency
  versions, source fingerprint, and rules status.

## 16. Known limitations and explicitly deferred work

- Rules are `pilot`, not scientifically approved production QA/QC.
- Coverage thresholds must be explicit in aggregate-rule YAML; no native sample
  timestamps are inferred from a nominal cadence.
- No stuck/constant or excessive-variability flags are enabled. The aggregate
  product contains statistics needed to study and approve those thresholds.
- No cross-variable consistency tests are enabled, such as PM ordering or
  wind-speed/direction coupling.
- No aggregation across daily atomic partitions is required; DuckDB provides
  the logical table. Optional later compaction must preserve atomic provenance.
- Wide NetCDF requires reviewed site IDs; the campaign generator never invents
  them and enables NetCDF only with `--netcdf-site-map`.
- NetCDF/CSV/database input adapters remain out of scope; current input is the
  long Parquet adapter.
- The proposed ADQAT 0.2 product architecture remains a separate future plan.

## 17. Ready-to-paste prompt for the new remote session

Copy the following into the new session after opening the repository on the
compute host:

```text
Continue the ADQAT current fixed-period QA/QC rollout using
docs/plans/adqat-current-minute-qc-hpc-handoff.md as the source of truth.

We are on the HPC/compute host with access to the production CROCUS Parquet
facts. Do not modify the native input tree. All output must remain below
/nfs/gce/projects/crocus-server-admins/data-rework/crocus-rework-output-tests-only/.

First inspect the current branch, commit, and dirty status. Then run the Python
3.12 release checks and validate the two W08D minute YAMLs. Guide me one command
block at a time. Do not launch the full-history campaign until both two-day
minute pilots have been run and their Parquet artifacts have passed the checks
in the handoff document. After pilot acceptance, run AQT full history first,
inspect it, and only then start WXT with conservative concurrency. Use the same
campaign IDs for automatic resume, never overwrite successful runs, and report
exact commands, paths, counts, failures, memory/quota observations, and the next
gate after every step.
```

## 18. Immediate next action

Push the current repository changes, pull them on `compute-386-07`, open the
new remote session with the prompt above, and execute Gate A only. Continue one
gate at a time based on observed output.
