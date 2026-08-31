# ADQAT 0.2 Product Architecture Requirements

| Field | Value |
| --- | --- |
| Status | **Proposed / Not Implemented** |
| Target | ADQAT 0.2.0 |
| Implemented baseline | ADQAT 0.1.1 |
| Document type | Product requirements and architecture decision record |

This document defines the intended ADQAT 0.2 configuration, source-adapter,
quality-standard, and NetCDF-product boundaries. It is a future implementation
contract, not documentation of currently available behavior. Until 0.2 is
implemented and accepted, the ADQAT 0.1.1 source code, schemas, CLI, and README
remain authoritative.

## 1. Validated baseline

ADQAT 0.1.1 is a working vertical slice for long-format CROCUS Parquet facts.
It validates one selected work unit per run, uses Arrow Dataset predicate and
projection pushdown, materializes one bounded period as Polars, executes
Pointblank checks, persists sparse evidence and compiled bit masks, and can
write one native Level 1 NetCDF file per complete UTC day.

The W08D real-data pilot used the half-open interval
`[2025-12-15T00:00:00Z, 2025-12-17T00:00:00Z)` and wrote only to the separate
`crocus-rework-output-tests-only` tree.

| Pilot | Input and checks | Result | Resource observation |
| --- | --- | --- | --- |
| WXT536 | 82 Parquet files; 9,534,537 rows; nine available variables with 1,059,393 rows each | Two daily NetCDF files; exact row and nanosecond-time preservation; zero findings under pilot limits | 7.91 seconds wall time; 6,321,248 KiB peak RSS; no swap |
| AQT530 | Two Parquet files; 53,796 rows; twelve variables with 4,483 rows each, including 4,483 string clock values | Two daily NetCDF files; exact row, string, QC, and nanosecond-time preservation; zero findings under pilot limits | 1.62 seconds wall time; 409,676 KiB peak RSS; no swap |

The configured WXT hail-accumulation and supply-voltage variables were absent
for W08D and correctly reported zero tested units. This absence was not called
a missing-sample failure because cadence QC was not configured. The pilot rule
status remains `pilot`; zero findings do not constitute scientific approval.

The pilot proves that one engine can process two instrument families and both
numeric and string facts through YAML profiles. It does not prove that the
current configuration and product boundaries are sufficiently general for an
external user.

## 2. Current constraints to remove

ADQAT 0.1.1 intentionally contains the following product-specific assumptions:

- The only source type is a Parquet Arrow Dataset containing long facts.
- A variable selector must include columns named `measurement` and `field`.
- Source storage mappings and scientific QA/QC rules are in the same profile.
- Native NetCDF output requires work-unit filters named `sensor`, `vsn`, and
  `instrument_id`.
- NetCDF output is restricted to complete UTC-day periods and one fixed
  CROCUS native-long observation layout.
- The filename, Level 1 terminology, global metadata, compression, variable
  names, numeric types, and QC representation are implemented in Python.
- Every observation uses a common `observed_value` or
  `observed_value_string` field and a common `qc_bits` field instead of a
  user-selected data-product schema.
- Only null/missing and between-range Pointblank checks are supported.
- Sampling frequency is provenance only; cadence, coverage, delta, alignment,
  and aggregation are not implemented.

ADQAT 0.2 must remove the source and product naming assumptions without adding
new input formats or silently changing native observations.

## 3. Goals and user workflow

### 3.1 One-step operation

Once standards and a job are configured, a single-work-unit job must run with:

```bash
adqat run job.yaml
```

The command must resolve and validate all referenced documents, validate the
source contract, generate a run ID when one is not supplied, execute QA/QC,
write evidence and products, reopen and validate each product, and write a
machine-readable manifest and human-readable report.

When a job contains exactly one work unit, the CLI must infer it. When it
contains more than one, `--work-unit` remains required and omission must fail
before scanning or writing. Existing `validate`, `config show`, `resume`,
`compile`, and `report` workflows remain available.

### 3.2 Three-document model

ADQAT 0.2 uses three conceptual documents:

1. A versioned instrument QA/QC standard maintained by ADQAT or an
   organization.
2. A versioned NetCDF product standard maintained by the producing
   organization or user.
3. A small job document containing source location and binding, selection,
   work-unit identity and filters, and output location.

Most users of a curated instrument and product standard should edit only the
job document. A physical source binding may be defined inline or referenced as
a packaged preset; it is not a fourth mandatory user-maintained file.

All references must use an exact version or a local file path. Floating aliases
such as `latest`, remote HTTP retrieval, and executable templates are excluded
from 0.2.

## 4. Public configuration contracts

ADQAT 0.2 introduces strict schema-version-2 documents. Unknown fields are
rejected. The examples below illustrate the required boundaries; final field
names must follow these shapes unless an implementation ADR records a
compatible refinement.

### 4.1 Instrument QA/QC standard

An instrument standard is storage-independent. It defines logical variables,
scientific metadata, named limits, checks, QC semantics, citations, version,
and approval status. It must not contain CROCUS paths or physical column names.

```yaml
schema_version: 2
kind: instrument_standard
id: vaisala-wxt536
version: 1.0.0
status: candidate
models: [WXT536]
references:
  operating_limits: https://example.invalid/reviewed-reference

flags:
  missing_value: {bit: 0, description: Existing observation has no usable value}
  physical_range: {bit: 2, description: Value is physically implausible}
  instrument_range: {bit: 3, description: Value is outside instrument limits}

variables:
  air_temperature:
    data_type: numeric
    metadata:
      standard_name: air_temperature
      long_name: Air temperature
      units: degree_Celsius
    missing_values: [-9999.9]
    limits:
      physical: {minimum: -90, maximum: 70}
      instrument: {minimum: -52, maximum: 60}
    checks:
      - {id: air_temperature_missing, method: not_null, flag: missing_value}
      - {id: air_temperature_physical, method: between, limits: physical, flag: physical_range}
      - {id: air_temperature_instrument, method: between, limits: instrument, flag: instrument_range}
```

Named limits are the single source of truth. Product metadata such as
`valid_min` and `valid_max` may be derived from a specifically selected named
limit; the same numbers must not be independently copied into multiple
documents.

An exact version is referenced with either a local path or a packaged URI such
as `adqat://instruments/vaisala-wxt536/1.0.0`. Packaged standards must be
immutable within a released ADQAT version. Curated WXT536 and AQT530 standards
remain `candidate` until scientifically reviewed; ADQAT must not label them
approved based on software tests.

### 4.2 Job and physical source binding

The job owns storage-specific column names and maps them to logical variables
from the instrument standard.

```yaml
schema_version: 2
kind: job
id: w08d-wxt-december-2025

source:
  adapter: parquet_long
  uri: /data/facts/**/*.parquet
  options: {partitioning: hive, union_by_name: true}
  contract:
    time: {column: time, timezone: UTC}
    observation_keys: [time, series_id]
    value_channels:
      numeric: value_float64
      string: value_string
  binding: adqat://bindings/crocus-facts-v4/wxt536/1.0.0

quality:
  standard: adqat://instruments/vaisala-wxt536/1.0.0

product:
  standard: ./organization-native-level1.yaml

selection:
  start: 2025-12-15T00:00:00Z
  end: 2025-12-17T00:00:00Z
  period: 1d

work_units:
  - id: W08D-wxt536
    filters:
      sensor: vaisala-wxt536
      vsn: W08D
      instrument_id: W08D--vaisala-wxt536--core--934c67f6166a
    identity:
      site: neiu
      platform: W08D
      instrument: wxt536
      instrument_id: W08D--vaisala-wxt536--core--934c67f6166a

output:
  root: /results/wxt-qaqc
```

Filters are arbitrary scalar equality predicates used by the source adapter.
Identity is an explicit metadata and filename namespace and must not be inferred
from filter names. This removes the current dependency on `sensor`, `vsn`, and
`instrument_id` as special Python keys.

For a non-preset source, the job may define `source.bindings` inline. Each
logical variable binding identifies an arbitrary equality selector and one
configured value channel. Selectors are not required to use columns named
`measurement` or `field`, but selector maps must remain unique within the
selected instrument standard.

### 4.3 NetCDF product standard

A product standard defines representation rather than scientific truth. The
instrument standard remains authoritative for logical units, standard names,
scientific limits, and QC meaning.

```yaml
schema_version: 2
kind: netcdf_product_standard
id: organization-native-level1
version: 1.0.0

layout:
  type: wide_per_variable_time

filename:
  template: "{site}.{instrument}.{platform}.{level}.{start:%Y%m%d.%H%M%S}.nc"

netcdf:
  format: NETCDF4
  compression: {enabled: true, level: 4}
  unlimited_dimensions: true

qc:
  variable_name: "qc_{variable}"
  dtype: uint64
  zero_means_good: true
  add_ancillary_variables: true

global_attributes:
  Conventions: "CF-1.10, ACDD-1.3"
  institution: Example organization
  project: Example project
  license: Example license
  processing_level: a1
```

Filename and metadata templates use a non-executable, validated placeholder
language with a fixed allowlist of run, period, identity, and product fields.
Unknown placeholders, path separators produced by a filename template, and
filename collisions are errors. Jinja, Python evaluation, shell expansion, and
arbitrary environment-variable expansion are prohibited.

Global metadata from the job may fill fields declared by the product standard,
but it may not override protected runtime provenance. Instrument scientific
metadata may be changed only by referencing or creating a different instrument
standard, not by an untracked job-level override.

## 5. Source-adapter boundary

ADQAT 0.2 implements a source-adapter protocol with these responsibilities:

- Validate configuration and source schema without writing.
- Expose observation-key types and lossless timestamp semantics.
- Apply work-unit, time, and logical-variable filters with predicate pushdown
  whenever supported.
- Project only observation keys, selectors, selected value channels, and source
  identity needed by the run.
- Return one normalized bounded period and its contributing immutable source
  objects.
- Produce a deterministic input fingerprint appropriate to the source type.
- Expose no write operation and always open database-backed sources read-only.

The only required 0.2 adapter is `parquet_long`, implemented through the
current Arrow Dataset path and supporting Hive or directory-partitioned
Parquet. It generalizes selector and value-channel names without changing the
lossless timestamp or period-bounded materialization behavior already proven
by the pilot.

NetCDF, CSV, SQL services, and DuckDB database-file inputs are deferred. The
protocol must make later adapters possible, but 0.2 schemas must reject an
unimplemented adapter rather than fall back or partially process it.

## 6. Product layouts and time semantics

The product standard must select one layout explicitly.

### 6.1 `native_long`

This is the lossless equivalent of the 0.1.1 observation layout. It preserves
every selected native timestamp, logical-variable identity, typed value, and
QC mask. It remains suitable when different variables do not share exact
timestamps.

### 6.2 `wide_per_variable_time`

Each logical variable has its own time coordinate, data variable, and paired QC
variable. For example:

```text
time_air_temperature(time_air_temperature)
air_temperature(time_air_temperature)
qc_air_temperature(time_air_temperature)
```

The data variable must name its QC variable using `ancillary_variables`.
The QC variable must carry `flag_masks`, `flag_meanings`, a clear zero-is-good
description, and compatible units and coordinates. Numeric and string source
variables remain supported without inventing values.

### 6.3 `wide_shared_time`

A common time dimension is valid only when all emitted variables already share
the configured timestamps or an explicit upstream alignment/aggregation
processor has produced them. ADQAT must never silently round, bin, interpolate,
forward-fill, or discard timestamps to construct a shared axis.

ADQAT 0.2 may accept shared-time output only with an explicit `exact` alignment
policy that validates identical time axes. A non-exact policy must be rejected
as unsupported until cadence, coverage, and aggregation processors are
implemented. An ARM-like 60-second mean product is therefore future processing
behavior, not a formatting option.

## 7. ARM-style product considerations

The reviewed ARM-style NetCDF example demonstrates requirements that the
product architecture must accommodate without claiming they are currently
implemented:

- A data variable and paired `qc_<variable>` field on a shared time dimension.
- Rich per-variable metadata including units, missing values, valid bounds, and
  valid-delta metadata.
- Global data-level, site, facility, input-source, sampling, averaging, QC
  standard, process-version, and history attributes.
- A dedicated time QC field with interval and prior-sample checks.
- Separate below-minimum, above-maximum, and excessive-delta bit meanings.
- Aggregated means, standard deviations, and corrected or derived variables.

ADQAT 0.1.1 `between` checks cannot distinguish below-minimum from
above-maximum failures, and it has no delta, time-continuity, derivation, or
aggregation processor. Faithful ARM-style behavior will require new check and
processor definitions. The 0.2 writer must not fabricate these results or
derive them solely from variable attributes.

Legacy representations such as `base_time` plus `time_offset` may be expressed
by a future product template when required by an organizational standard. They
are not the default and must not reduce native timestamp precision.

## 8. Output and publication contract

The user-facing root separates products from execution evidence:

```text
output-root/
  products/
    <validated-user-filename>.nc
  runs/<run-id>/
    run.json
    resolved-job.yaml
    resolved-instrument-standard.yaml
    resolved-product-standard.yaml
    manifest.json
    report.json
    work_units/<work-unit-id>/<period-id>/
      findings.parquet
      check_results.parquet
      qc_flags.parquet
      success.json
```

Every product must be staged below the output root, reopened and structurally
verified, checksummed, and recorded in `manifest.json` before publication is
reported successful. Publication must be interruption-safe: an incomplete
product cannot have a valid success marker, and resume must clean or replace
only staging artifacts for the same run and period.

Existing products and immutable successful periods must never be overwritten.
A deterministic filename collision is a configuration or selection error, not
permission to replace a file. All paths and template results must be validated
beneath the configured output root, which must not overlap the source base.

The report must summarize processed and empty periods, rows, variables,
checks, failures, QC-bit distributions, produced products, checksums, rule
status, and warnings. QC findings remain successful scientific evidence and do
not change the process exit status.

## 9. Reproducibility and provenance

For every run, ADQAT 0.2 must:

- Reject unknown configuration fields and unresolved or floating standards.
- Resolve local paths relative to the referencing document.
- Snapshot the fully resolved job, source binding, instrument standard, and
  product standard.
- Compute the configuration hash over all resolved behavior, including source
  contract and binding, selected work unit, interval, processing policy,
  scientific rules, output layout, metadata policy, QC representation, and
  filename template.
- Exclude run ID and output root from the behavioral configuration hash.
- Record ADQAT, Python, engine, writer, and dependency versions.
- Record the source fingerprint and output checksum for every period product.
- Preserve exact UTC half-open period semantics and nanosecond source times.
- Generate required provenance attributes even when the user provides minimal
  metadata.
- Reject any attempt to override `run_id`, configuration/rule fingerprints,
  source fingerprint, software version, creation time, history, or product
  checksum.

Resume trusts a valid matching success marker without reopening completed
source periods. Changed configuration or a deliberate reprocessing of changed
source requires a new immutable run ID.

## 10. Migration and compatibility

- Schema-version-1 files continue to load through the 0.2 minor release with
  their existing semantics and a deprecation notice.
- Version 1 must not be silently reinterpreted as version 2.
- Existing W08D WXT and AQT examples remain regression fixtures.
- A migration command may create new version-2 documents, but it must write new
  files, preserve the original, mark curated limits `pilot` or `candidate`, and
  require review of product metadata and source binding before execution.
- Version-1 support must not be removed before a later major/minor release has
  a documented migration path and deprecation period.

## 11. Phased implementation

1. **Configuration and reference resolution:** add strict version-2 models,
   exact local/package reference resolution, identity separation, canonical
   snapshots and hashing, and version-1 compatibility tests.
2. **Generic current-source adapter:** introduce the adapter protocol and move
   the existing Arrow Dataset implementation behind `parquet_long`; generalize
   selectors, bindings, filters, and value channels.
3. **Product writer contract:** introduce the writer protocol, protected
   metadata, validated filename templates, product directories, manifests,
   checksums, and atomic publication.
4. **NetCDF layouts:** preserve `native_long`, implement lossless
   `wide_per_variable_time`, implement paired QC fields, and allow
   `wide_shared_time` only with verified exact alignment.
5. **Curated resources and CLI:** package candidate WXT536/AQT530 standards and
   CROCUS bindings, infer a sole work unit for `adqat run job.yaml`, and update
   reports and documentation.
6. **Acceptance and rollout:** run synthetic schema tests, ARM-shaped writer
   fixtures, and the W08D two-day pilot before wider representative-node
   benchmarking.

Each phase must use test-first development and leave the source tree read-only
at runtime.

## 12. Acceptance criteria

ADQAT 0.2 is complete when:

- A single-work-unit version-2 job completes with `adqat run job.yaml`.
- Instrument rules contain no storage paths or CROCUS physical column names.
- The Parquet adapter processes arbitrary selector-column names with pushdown
  and preserves nanosecond timestamps.
- Work-unit identity and metadata do not depend on filter-key names.
- Two instruments run through the same adapter and writer registrations using
  only document changes.
- Native-long and per-variable-wide files preserve exact row counts, values,
  strings, timestamps, QC masks, and logical-variable identity.
- Per-variable-wide data fields contain correctly named ancillary QC fields
  with validated masks and meanings.
- Shared-time output rejects differing time axes and performs no implicit
  alignment.
- Filename and metadata templates reject unknown placeholders, path traversal,
  protected-attribute overrides, and collisions before publication.
- Every product is reopened, verified, checksummed, and listed in a manifest.
- Configuration snapshots and hashes reproduce an identical behavioral plan
  independent of output root and run ID.
- Resume skips valid completed periods without scanning source data, and QC
  flags can be rebuilt from persisted findings.
- The W08D WXT/AQT pilot still passes under candidate rules, and all existing
  version-1 tests remain green.

## 13. Explicit non-goals for 0.2

- NetCDF, CSV, web-service, SQL-service, or DuckDB database-file input adapters.
- Scientifically approved WXT536 or AQT530 limits without formal domain review.
- Cadence-based missing-sample or minimum-coverage findings.
- Delta, spike, persistence, prior-file continuity, or time-QC checks.
- Resampling, interpolation, aggregation, derived variables, or Level 2/b1
  product generation.
- Silent transformation of native time axes to satisfy an output layout.
- Remote standard registries, executable templates, custom Python processors,
  dashboards, HPC scheduling, or multiprocessing orchestration.

These exclusions preserve a small, reproducible 0.2 boundary while ensuring
the adapter, standard, and writer interfaces can be extended deliberately in a
later version.

## 14. Decisions held for later work

The following concepts are fixed, although their implementation is deferred:

- Cadence QC remains disabled unless an expected cadence is explicit.
- At 10 Hz with a one-second window and `minimum_fraction: 0.8`, at least
  `ceil(10 × 1 × 0.8) = 8` observations are required.
- Insufficient coverage produces a null aggregate and a coverage/missing-value
  finding; it does not synthesize individual absent observations.
- Irregular series receive no cadence finding without an explicit expectation.
- Shared-time products require an explicit, reviewable alignment or aggregation
  processor and coverage policy.
