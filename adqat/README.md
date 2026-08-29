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
Version 1 does not infer cadence, create missing observations, or evaluate
coverage.

Real sources may encode missing observations as numeric sentinels. Declare them
per variable so they become null before Pointblank runs:

```yaml
missing_values: [-9999.9]
```

String variables declare `data_type: string` and may use
`col_vals_not_null` plus `missing_strings`. Range checks remain numeric-only.

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

## Outputs and queries

Each run represents one work unit:

```text
runs/<run-id>/
  run.json
  quality_rules.yaml
  processing_run.yaml
  work_units/<work-unit-id>/<period-id>/
    findings.parquet
    check_results.parquet
    qc_flags.parquet
    <site>.<instrument>.<vsn>.native.a1.<start>-<end>.nc  # when enabled
    success.json
```

Native Level 1 NetCDF is opt-in through `output.netcdf`. It requires complete
UTC-day selections, carries nanosecond source timestamps, joins sparse flags
back to every selected observation, writes required CROCUS provenance,
and reopens each file for verification before publishing the period. This is a
native `a1` slice; Level 2 aggregation remains deferred.

The concrete W08D two-day WXT-first/AQT-second runbook is
[`examples/W08D_PILOT.md`](examples/W08D_PILOT.md). Its shared pilot rules are
engineering candidates, not approved scientific thresholds.

Query sparse findings without reopening source data:

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
they are to be reprocessed.

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

Future temporal aggregation will remain disabled unless cadence is explicit.
For example, 10 Hz with a one-second window and an 80% minimum requires at
least eight observations; an insufficient window will yield a null aggregate,
not invented observation rows.
