# ADQAT

**ADQAT — Automated Data Quality Assessment for Time-Series** (pronounced
“adequate”) turns configured quality checks into sparse, queryable evidence and
bitwise QC flags.

Version 1 is a deliberately small vertical slice for long-format CROCUS
Parquet facts. It processes one explicitly selected work unit per run, divides
the requested UTC interval into restartable periods, executes Pointblank checks,
and writes only below the configured output root.

> [!WARNING]
> The files under `examples/` use synthetic profiles and dummy limits for
> software testing. They are not approved scientific QC rules and must not be
> used for production conclusions.

## Install

Create the Python 3.12 Mamba environment:

```bash
mamba env create -f environment.yml
mamba activate adqat
```

Alternatively, from an existing Python 3.12 environment:

```bash
python -m pip install -e '.[dev]'
```

## Configure

ADQAT uses two YAML documents:

- `quality_rules.yaml` defines flags, profiles, variable mappings, checks, and
  pipelines.
- `processing_run.yaml` defines the Parquet source, UTC selection, processing
  period, work units, selected pipeline, and output root.

For the current CROCUS facts, each variable maps a long-format
`measurement`/`field` pair to its typed-value column:

```yaml
temperature:
  column: value_float64
  where:
    measurement: wxt.env.temp
    field: value
    value_type: float64
  checks:
    - id: temperature_missing
      method: col_vals_not_null
      flag: missing_value
    - id: temperature_physical
      method: col_vals_between
      flag: physical_range
      args: {left: -80, right: 70}
```

Range checks always pass nulls. This ensures an absent value receives the
missing-value flag without also receiving every range flag.

`sampling.expected_frequency_hz` is accepted as provenance metadata only.
ADQAT does not infer native cadence or create native observations.

Real sources may encode missing observations as numeric sentinels. Declare them
per variable so they become null before Pointblank runs:

```yaml
missing_values: [-9999.9]
```

String variables declare `data_type: string` and may use
`col_vals_not_null` plus `missing_strings`. Range checks remain numeric-only.

An optional dense fixed-period product is enabled with an explicit duration:

```yaml
quality:
  rules: quality_rules.yaml
  aggregate_rules: aggregate_quality_rules.yaml
  pipeline: basic_qc
processing:
  period: 1d
  aggregation: {period: 10, units: seconds}
```

Aggregation units are `seconds`, `minutes`, or `hours`. For daily NetCDF
publication the duration must divide one UTC day exactly. A one-minute product
uses `{period: 1, units: minutes}`.

Every variable in the selected profile then declares `aggregation` as `mean`,
`circular_mean`, `mode`, or `last`. Wind direction uses `circular_mean`;
cumulative rain/hail and uptime use `last`; categorical status uses `mode`.
Only raw observations passing all configured direct-value checks contribute to
the representative aggregate and statistics. Raw flag bits are not propagated:
the separate aggregate rules produce an unsigned 8-bit mask with bits 0–6
defined and bit 7 reserved/always zero.

## Run

```bash
adqat validate examples/processing_run.synthetic.yaml
adqat config show examples/processing_run.synthetic.yaml

adqat run processing_run.yaml --work-unit site01_wxt
adqat run processing_run.yaml --work-unit site01_wxt --run-id selected-id

adqat resume /results/adqat/runs/<run-id>
adqat compile /results/adqat/runs/<run-id>
adqat compile /results/adqat/runs/<run-id> --period <period-id>
adqat report /results/adqat/runs/<run-id>
```

Scientific QC findings are successful evidence products, so a run containing
findings exits with status zero. Configuration, source, engine, compilation,
and persistence failures exit nonzero. A period with no matching observations
also succeeds, emits a warning, and contains typed empty output tables.

Each work unit must provide non-empty string filters for `sensor`, `vsn`, and
`instrument_id`. These become immutable identity columns in the sparse QC
dataset and make cross-run queries self-describing.

## Outputs and queries

Each run represents one work unit:

```text
runs/<run-id>/
  run.json
  quality_rules.yaml
  aggregate_quality_rules.yaml  # when aggregation is enabled
  processing_run.yaml
  work_units/<work-unit-id>/<period-id>/
    findings.parquet
    check_results.parquet
    qc_flags.parquet
    aggregate_data.parquet  # when processing.aggregation is configured
    <site>.<instrument>.<vsn>.native.a1.<start>-<end>.nc  # when enabled
    <site>.<instrument>.<vsn>.<interval>.b1.<start>-<end>.nc  # aggregate NetCDF
    success.json
```

NetCDF is opt-in through `output.netcdf`. It requires complete
UTC-day selections, carries nanosecond source timestamps, joins sparse flags
back to every selected observation for the backward-compatible native `a1`
product, or writes a wide aggregate `b1` product with paired `qc_<variable>`
unsigned 8-bit fields. Numeric aggregate variables use `_FillValue=-999.0`;
their QC field remains present and sets bit 0 when coverage is insufficient.
Every file is reopened and verified before atomic publication.

`aggregate_data.parquet` contains one row for every configured fixed interval
and variable, including intervals with no source observation. Empty intervals
have null values, zero counts, `aggregate_valid=false`, and aggregate QC bit 0.
Observed intervals include total/valid/invalid counts,
per-raw-flag counts, valid fraction, observed row rate, maximum within-interval
timestamp gap, mean, median, population standard deviation, minimum, maximum,
quartiles, and IQR. Raw timestamps are neither regularized nor interpolated.
Coverage, variability, stuck, and aggregate range thresholds are explicit in
the separate aggregate-rule YAML.

Aggregate QC bits are fixed: 0 insufficient coverage, 1 excessive variability,
2 stuck/constant, 3 below physical minimum, 4 above physical maximum, 5 below
instrument minimum, 6 above instrument maximum, and 7 reserved/always zero.

Daily directories are atomic restart units, not separate logical products.
Query all aggregate files for an instrument as one Parquet relation:

```sql
SELECT *
FROM read_parquet(
  '/results/adqat/runs/<run-id>/work_units/*/*/aggregate_data.parquet',
  union_by_name = true
)
ORDER BY time, variable;
```

The concrete W08D two-day WXT-first/AQT-second runbook is
[`examples/W08D_PILOT.md`](examples/W08D_PILOT.md). Its shared pilot rules are
engineering candidates, not approved scientific thresholds.

The current dense one-minute pilots and restartable full-history AQT/WXT
campaign are documented in
[`examples/FULL_HISTORY_MINUTE_CAMPAIGN.md`](examples/FULL_HISTORY_MINUTE_CAMPAIGN.md).
The complete implementation context and new-session HPC handoff are in
[`docs/plans/adqat-current-minute-qc-hpc-handoff.md`](docs/plans/adqat-current-minute-qc-hpc-handoff.md).

For AQT-only testing, the manufacturer-datasheet profile is
[`examples/quality_rules.crocus_aqt530_datasheet_test.yaml`](examples/quality_rules.crocus_aqt530_datasheet_test.yaml),
with a two-day W08D processing example in
[`examples/processing_run.w08d_aqt_20251215_20251216_datasheet_test.yaml`](examples/processing_run.w08d_aqt_20251215_20251216_datasheet_test.yaml).
It converts documented gas limits from ppb to the CROCUS ppm representation.
The datasheet provides no PM1 concentration or uptime range, so the profile
does not invent those instrument checks. It remains a `pilot` standard until
scientific review.

The proposed, not-yet-implemented ADQAT 0.2 configuration and product
architecture is recorded in
[`docs/requirements/adqat-v0.2-product-architecture.md`](docs/requirements/adqat-v0.2-product-architecture.md).

`qc_flags.parquet` is the canonical operational QC dataset. It contains only
observations whose combined unsigned 64-bit mask is nonzero, plus observation
keys, instrument identity, variable, run/work-unit identity, and configuration
hash. A source observation with no matching row is valid under the configured
raw rules. Clean and empty periods still contain a typed zero-row file.

```text
configured observation keys
sensor             string
vsn                string
instrument_id      string
variable           string
qc_bits            uint64, always > 0 for nonempty files
run_id             string
work_unit_id       string
config_hash        string
```

Rows are unique by configured observation keys plus `variable`; multiple
findings for that identity are combined with `BIT_OR`.

Query every period and run directly as one logical table with DuckDB; no import
or catalog-building step is required:

```sql
SELECT *
FROM read_parquet(
  '/results/adqat/runs/*/work_units/*/*/qc_flags.parquet',
  union_by_name = true
)
WHERE vsn = 'W08D'
  AND qc_bits <> 0
ORDER BY time, variable;
```

Parquet projection and filter pushdown apply to these sparse files. Query
detailed findings without reopening source data when check-level evidence is
needed:

```sql
SELECT variable, check_id, count(*) AS failures
FROM read_parquet('/results/adqat/runs/*/work_units/*/*/findings.parquet')
GROUP BY variable, check_id
ORDER BY variable, check_id;
```

Compile flags with unsigned 64-bit semantics:

```text
qc_bits = BIT_OR(1::UBIGINT << bit)
```

`success.json` is the resume marker. A matching successful marker is trusted
without restating source files; changed inputs require a deliberate new run if
they are to be reprocessed. It records both check findings and unique flagged
observations.

## Development

```bash
python -m pytest
ruff check .
mypy src/adqat
lint-imports
python -m build
```

The source selection layer uses Arrow dataset pushdown to preserve CROCUS
`timestamp[ns, UTC]` values losslessly. Current DuckDB releases convert this
specific Parquet type to microsecond-resolution `TIMESTAMPTZ`; DuckDB remains
the QC-bit compilation engine, with timestamp keys represented internally as
nanosecond integers during grouping.

Future partial-minute completeness thresholds remain disabled unless cadence
is explicit. The current minute product flags an entirely absent minute but
does not classify a minute as incomplete merely because samples do not land on
an inferred grid.
