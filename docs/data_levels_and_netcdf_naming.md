# CROCUS data levels and NetCDF naming

## Scope

This standard defines CROCUS processing levels and the naming of final daily
NetCDF files. It is inspired by ARM datastream conventions but intentionally
uses CROCUS-specific level meanings and filename components. It must not be
described as an official ARM datastream convention.

The convention applies only to final NetCDF files. Existing partitioned
Parquet files retain their current names and Hive layout.

## Processing levels

| Processing level | Medallion | Meaning |
| --- | --- | --- |
| Level 0 | Bronze | Original observation timestamps and source values; no QA/QC or temporal aggregation |
| Level 1 | Silver | Original observation timestamps with QA/QC flags and approved standardization |
| Level 2 | Gold | QA/QC-aware temporal aggregates and interval statistics |
| Level 3 | Reserved | Future scientifically derived variables or algorithms |

The current CROCUS Parquet facts are Level 0. Processing level and file format
are separate concepts.

## NetCDF data-level codes

NetCDF representations use an `a` prefix:

| NetCDF code | Source level | Product |
| --- | --- | --- |
| `a0` | Level 0 | Source-preserving native-timestamp NetCDF |
| `a1` | Level 1 | QA/QC-flagged native-timestamp NetCDF |
| `a2` | Level 2 | QA/QC-aware one-minute or fifteen-minute aggregate NetCDF |

The integration component distinguishes one-minute and fifteen-minute `a2` products.

Every file must include this global-attribute statement:

```text
CROCUS ARM-inspired naming and processing levels; not an ARM datastream level designation.
```

## Filename convention

Final daily NetCDF files use:

```text
{site}.{instrument}.{vsn}.{integration}.{level}.{start_utc}-{end_utc}.nc
```

Example:

```text
neiu.wxt536.W08E.native.a1.20251215T000000Z-20251216T000000Z.nc
```

### Components

| Component | Definition | Examples |
| --- | --- | --- |
| `site` | Reviewed site identifier, lowercase, 3–8 ASCII letters | `neiu`, `atmos` |
| `instrument` | Stable lowercase instrument-model abbreviation | `wxt536`, `aqt530` |
| `vsn` | Authoritative four-character uppercase VSN | `W08E`, `W0A1` |
| `integration` | Temporal representation | `native`, `10sec`, `1min`, `15min` |
| `level` | CROCUS NetCDF data-level code | `a0`, `a1`, `a2` |
| `start_utc` | Inclusive nominal daily start | `20251215T000000Z` |
| `end_utc` | Exclusive nominal daily end | `20251216T000000Z` |
| extension | NetCDF extension | `nc` |

Filename tokens must not contain dots, whitespace, path separators, or Unicode
characters. NetCDF publication requires a reviewed VSN-to-site mapping; an
`unknown` site is not allowed in a final filename.

## Daily interval semantics

Each filename describes one nominal UTC day as a half-open interval:

```text
[start_utc, end_utc)
```

For example:

```text
[2025-12-15T00:00:00Z, 2025-12-16T00:00:00Z)
```

Filename bounds remain deterministic even when the first observation is late,
the last observation is early, or the day contains no valid observations.

## Time representation inside NetCDF

### Primary CF time coordinate

Every final NetCDF file must use `time` as its primary coordinate and encode it
according to the CF conventions. For a file whose nominal UTC day begins on
2026-09-02, the declaration is equivalent to:

```text
double time(time)
time:long_name = "Time offset from midnight"
time:units = "seconds since 2026-09-02 00:00:00 0:00"
time:standard_name = "time"
```

The reference date changes to the nominal UTC date represented by each file.
The reference instant is always midnight UTC; local time zones and daylight
saving offsets are not allowed. The `time` values must be:

- UTC;
- monotonically increasing;
- strictly non-repeating;
- finite, with no missing value, `_FillValue`, NaN, or infinity; and
- inside the filename's half-open interval `[start_utc, end_utc)`.

For aggregate products, `time` represents the interval midpoint. For example,
the first 10-second interval `[00:00:00, 00:00:10)` has `time=5.0`. Every
aggregate has a corresponding two-element `time_bounds` row in the same units,
and `time` declares `bounds = "time_bounds"`. Native products retain source
timestamp precision; they must not round observation timestamps merely to make
the coordinate integral.

### Legacy ARM time variables

For full ARM compatibility, final processed files retain `base_time` and
`time_offset` in addition to the CF `time` coordinate:

- `base_time` is a scalar containing the Unix time of the file's nominal UTC
  midnight and uses `units = "seconds since 1970-01-01 00:00:00 0:00"`.
- `time_offset(time)` is a double-precision offset in seconds from
  `base_time` and uses the same daily epoch as `time`.
- `time`, `base_time`, and `time_offset` must describe identical instants;
  disagreement is a publication error.

`time` remains the authoritative CF coordinate. The legacy variables exist for
ARM tooling and compatibility and must not replace or alter it.

### Coverage metadata

The filename contains nominal daily bounds. The NetCDF file must additionally
record the actual occupied time coverage at whole-second resolution.

Required global attributes:

```text
time_coverage_start
time_coverage_end
time_coverage_duration
time_coverage_resolution
```

Coverage uses half-open semantics and must enclose every observation:

- `time_coverage_start` is the actual earliest included observation rounded
  down to the containing UTC second.
- `time_coverage_end` is one whole-second boundary strictly after the actual
  latest included observation.
- Both values use `YYYY-MM-DDTHH:MM:SSZ`.
- `time_coverage_end` is exclusive.

Example for observations from `00:00:00.003271202Z` through
`23:59:59.995721481Z`:

```text
time_coverage_start = "2025-12-15T00:00:00Z"
time_coverage_end = "2025-12-16T00:00:00Z"
```

Rounding applies only to the coverage attributes, not to individual timestamps
or aggregate interval bounds.

## Variable names and metadata

NetCDF variable names must be descriptive, use lowercase ASCII where practical,
separate words with `_`, and contain no more than 64 characters. Names must be
stable within a product version. A renamed variable requires a new product/DOD
version and an explicit migration note.

Every data variable must define:

```text
long_name
units
```

Units must use UDUNITS-compatible spelling. Dimensionless quantities use
`units = "1"`; an empty unit string is not allowed. A variable must also define
an official CF `standard_name` whenever an applicable name exists. This is
required for a primary geophysical variable when CF provides an appropriate
standard name. Project-specific variables without a CF name retain a precise
`long_name` and must not invent a `standard_name`.

Each primary data variable with a paired QC field must identify it with the CF
`ancillary_variables` attribute. QC variables use the approved `qc_<variable>`
naming and unsigned 8-bit representation defined by the applicable CROCUS QC
standard.

### Missing-value representation

When a numeric sentinel represents missing data, define `_FillValue` or
`missing_value`. ARM-compatible publication uses `-9999` where appropriate. If
both attributes are present, they must have the same value and the same numeric
type as the variable. The production CROCUS aggregate writer will therefore use
`-9999` (or the exactly representable typed equivalent) unless a reviewed
variable standard requires a different sentinel.

The current ADQAT prototype uses `-999.0` for aggregate NetCDF variables. That
prototype value is not the final publication convention and must be migrated
before the production 10-second files are generated. Missing sentinels are
representation values only: they must not participate in statistics or range
checks.

String variables use an explicitly documented string missing representation;
numeric sentinels such as `-9999` must not be written into string data.

## Data quality and QC flags

Every science variable with QC has one paired variable named:

```text
qc_<variable_name>
```

The science variable links to it using:

```text
ancillary_variables = "qc_<variable_name>"
```

QC values are bit-packed integers and declare `flag_method = "bit"`. ARM
generally prefers a 32-bit integer container. CROCUS deliberately uses unsigned
8-bit QC variables because its approved aggregate QC contract contains exactly
eight bits. This is a documented CROCUS representation decision; unused storage
bits must not be added or assigned implicitly.

Each QC variable must define:

```text
long_name
units = "1"
description
flag_method = "bit"
standard_name = "quality_flag"
```

It must also define `bit_<n>_description` and `bit_<n>_assessment` for every
assigned bit. ARM attribute numbering is one-based: `bit_1` describes mask
value 1 and corresponds to ADQAT internal bit index 0; `bit_8` describes mask
value 128 and corresponds to internal bit index 7. Assessments are exactly
`Bad` or `Indeterminate`. The product/DOD standard must explicitly assign the
assessment for each test; the writer must not infer an assessment from bit
position.

Example:

```text
qc_temperature:units = "1"
qc_temperature:flag_method = "bit"
qc_temperature:standard_name = "quality_flag"
qc_temperature:bit_1_description = "Insufficient valid source observations"
qc_temperature:bit_1_assessment = "Bad"
```

For CF interoperability, QC variables should also provide `flag_masks` and
`flag_meanings`. They must encode exactly the same masks and meanings as the
ARM `bit_<n>_*` attributes. A QC value of zero means that no configured QC test
failed; it does not assert that every conceivable quality test was performed.

Range metadata distinguishes failure severity:

- `fail_min` and `fail_max` define **Bad** limits;
- `warn_min` and `warn_max` define **Indeterminate** limits; and
- `valid_min` and `valid_max` must not be used as ordinary QC thresholds.

All limits use the science variable's declared units. The applicable immutable
QC standard is the source of truth; values must not be copied into a product
DOD independently without a consistency check.

## Current implementation status

This document is the publication contract for the planned production files; it
does not claim that the current prototype writer is already compliant. ADQAT
0.1.4 currently differs in several material ways:

- aggregate `time` is an `int64` Unix timestamp at the interval start rather
  than a double offset from daily midnight at the interval midpoint;
- `base_time` and `time_offset` are not written;
- numeric aggregate fill values use `-999.0` rather than the production
  convention above;
- QC variables do not yet provide the exact `flag_method`, `standard_name`,
  `bit_<n>_description`, and `bit_<n>_assessment` attribute contract; and
- the complete process-version, DOD-version, datastream, DOI, and history
  validation contract is not implemented.

These are required writer and acceptance-test changes before any 10-second
NetCDF file is labeled publication-ready or ARM compliant. Existing prototype
files remain test artifacts and must not be renamed into the final namespace.

## Required daily products

For every reviewed site, VSN, instrument, and UTC day:

| Integration | Level | Contents |
| --- | --- | --- |
| `native` | `a0` | Original values and timestamps converted to NetCDF |
| `native` | `a1` | Original values and timestamps with QA/QC variables |
| `10sec` | `a2` | Ten-second QA/QC-aware aggregates and statistics |
| `1min` | `a2` | One-minute QA/QC-aware aggregates and statistics |
| `15min` | `a2` | Fifteen-minute QA/QC-aware aggregates and statistics |

Examples:

```text
neiu.wxt536.W08E.native.a0.20251215T000000Z-20251216T000000Z.nc
neiu.wxt536.W08E.native.a1.20251215T000000Z-20251216T000000Z.nc
neiu.wxt536.W08E.10sec.a2.20251215T000000Z-20251216T000000Z.nc
neiu.wxt536.W08E.1min.a2.20251215T000000Z-20251216T000000Z.nc
neiu.wxt536.W08E.15min.a2.20251215T000000Z-20251216T000000Z.nc

neiu.aqt530.W08E.native.a0.20251215T000000Z-20251216T000000Z.nc
neiu.aqt530.W08E.native.a1.20251215T000000Z-20251216T000000Z.nc
neiu.aqt530.W08E.10sec.a2.20251215T000000Z-20251216T000000Z.nc
neiu.aqt530.W08E.1min.a2.20251215T000000Z-20251216T000000Z.nc
neiu.aqt530.W08E.15min.a2.20251215T000000Z-20251216T000000Z.nc
```

## Required provenance

Every NetCDF file must record:

```text
Conventions
process_version
dod_version
datastream
crocus_filename_convention
crocus_processing_level
crocus_data_level
crocus_medallion
source_dataset_fingerprint
source_snapshot
qaqc_rule_fingerprint
aggregation_rule_fingerprint
processing_software_version
site_id
vsn
instrument_id
instrument_model
time_coverage_start
time_coverage_end
date_created
history
```

`Conventions` must identify the applicable CF convention and any approved ACDD
or ARM convention used by the product. `process_version` identifies the
processing software/release, `dod_version` identifies the exact data-object
definition, and `datastream` identifies the ARM-style datastream represented by
the file. Include `doi` whenever a DOI has been assigned or is required for the
datastream. `history` is append-only and records processing time, software
version, and the transformation that created the file.

Level-specific metadata:

| File level | `crocus_medallion` | `crocus_processing_level` |
| --- | --- | --- |
| `a0` | `bronze` | `0` |
| `a1` | `silver` | `1` |
| `a2` | `gold` | `2` |

An `a0` file must not claim QA/QC or aggregation fingerprints. An `a1` file
must identify its QA/QC rules. An `a2` file must identify both its QA/QC and
aggregation rules.

## Publication checks

Before publication, the generator must verify:

1. Filename site matches the reviewed VSN-to-site registry.
2. Filename instrument matches the sensor-to-model registry.
3. Filename VSN matches every observation in the file.
4. Filename bounds describe exactly one UTC day.
5. All observation times fall inside the filename's half-open interval.
6. Actual coverage attributes enclose all included times.
7. Native timestamps retain source precision.
8. `10sec` files contain exactly 8,640 aligned intervals for a complete UTC day.
9. `1min` files contain exactly 1,440 aligned intervals for a complete UTC day.
10. `15min` files contain exactly 96 aligned intervals for a complete UTC day.
11. Data-level, medallion, QA/QC, aggregation, and provenance metadata agree.
12. No two files have the same name within one immutable release.
13. `time` is double precision, UTC, strictly increasing, unique, finite, and
    has the required CF attributes.
14. `base_time`, `time_offset`, and `time` describe identical instants.
15. Aggregate `time` values are interval midpoints and lie inside their declared
    `time_bounds`.
16. Variable names contain at most 64 characters and every data variable has
    `long_name` and UDUNITS-compatible `units`.
17. Applicable primary variables use official CF `standard_name` values and
    paired QC variables are identified through `ancillary_variables`.
18. `_FillValue` and `missing_value`, when both present, are identical in value
    and type; missing sentinels never enter computed statistics.
19. Required ARM/CROCUS global metadata, including process, DOD, datastream,
    DOI when applicable, and processing history, is present.
20. Every QC variable follows `qc_<variable_name>`, is linked through
    `ancillary_variables`, uses the approved integer width, and declares
    `flag_method = "bit"`.
21. ARM `bit_<n>_description`/`bit_<n>_assessment` attributes agree with CF
    `flag_masks`/`flag_meanings`, and every assessment is `Bad` or
    `Indeterminate`.
22. `fail_min`/`fail_max` and `warn_min`/`warn_max` agree with the immutable QC
    standard; `valid_min`/`valid_max` are not substituted for QC thresholds.

## References

- ARM Formatting and File Naming Protocols:
  <https://www.arm.gov/guidance/datause/formatting-and-file-naming-protocols>
- ARM Data File Standards, DOE/SC-ARM-15-004:
  <https://armgov.svcs.arm.gov/publications/programdocs/doe-sc-arm-15-004.pdf>
