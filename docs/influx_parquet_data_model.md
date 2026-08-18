# InfluxDB archive and Parquet data model findings

## Scope and evidence

This note records a read-only inspection of the backup copied to
`/Volumes/Extreme SSD/2025-12-17`. No source archive was modified and no full
restore or full export was attempted.

Evidence used:

- backup manifest `20251217T150420Z.manifest`;
- representative tar listings and compressed/uncompressed sizes;
- InfluxDB OSS `v2.7.11` `influxd inspect report-tsm`, `dump-tsm`, and
  `export-lp`, run from the official container image;
- shard `1004` (`2024-12-30` through `2025-01-05`) and shard `2293`
  (`2025-12-15` through the backup time);
- narrow exports from December 15-17, 2025;
- experimental repository `seanshahkarami/sage-data-rework` at commit
  `16e27e4abacd54a66be39673d81fa489591fe6a6`.

Counts below describe the archive or the sampled shards, not necessarily a
CROCUS-only subset. The archive does not contain an explicit `crocus` tag.

## Archive structure

The copy is an InfluxDB OSS 2 backup, not a directory that `influxd` can query
in place.

| Item | Observed role |
| --- | --- |
| `*.manifest` | JSON backup catalog: organizations, buckets, retention policies, shard groups, time ranges, and the file assigned to every shard. |
| `*.tar.gz` | One shard backup per file. Each contains paths such as `<bucket-id>/autogen/<shard-id>/<generation>.tsm`. The numeric suffix is the shard ID. |
| `*.bolt.gz` | Compressed InfluxDB key/value metadata, including organizations, buckets, tasks, dashboards, authorizations, and credentials. It is sensitive and is not needed for point conversion. |
| `*.sqlite.gz` | Compressed SQL metadata. In this backup it contains migrations plus empty annotations, notebooks, remotes, replications, and streams tables. |

There are 976 shard tarballs. The non-AppleDouble backup files total
482,017,394,488 bytes (448.91 GiB). Finder-created `._*` files are not part of
the Influx backup and must be ignored.

The manifest declares ten buckets:

| Bucket | ID | Shards with files | Compressed bytes | Manifest time range |
| --- | --- | ---: | ---: | --- |
| `waggle` | `b3a4e89ad74c5acc` | 246 | 471,269,876,646 | 1999-12-27 to 2025-12-22 |
| `grafana-agent` | `c1c93ca186ca3ccd` | 90 | 10,736,778,303 | 2025-09-04 to 2025-12-18 |
| `plugin-stats` | `02e5bb0565afd948` | 220 | 6,019,779 | 2021-09-27 to 2025-12-22 |
| `upload-stats` | `a67e1f034764859b` | 212 | 1,464,370 | 2021-04-26 to 2025-12-22 |
| `health-check-test` | `cb466950d9ea381b` | 102 | 1,563,742 | 2024-01-01 to 2025-12-22 |
| `downsampled-test` | `cec418af9531e534` | 102 | 1,204,321 | 2024-01-01 to 2025-12-22 |
| `_monitoring` | `21605256038a9184` | 4 | 1,519 | retained recent data |
| `scheduler-events` | `3e58d5be308f41c1` | 0 | 0 | empty |
| `waggle-recent` | `8031269d10503bf0` | 0 | 0 | empty in this backup |
| `_tasks` | `9eff17653b2f6e26` | 0 | 0 | empty in this backup |

The existing experiment exports bucket `b3a4e89ad74c5acc` (`waggle`), and the
scientific measurements inspected below are present there. That establishes
`waggle` as the source bucket, but not which nodes or measurements constitute
the desired CROCUS subset.

The `waggle` bucket uses seven-day shard groups and dominates the archive. Its
246 compressed shards range from 32 bytes to 5.89 GB, with a mean of 1.92 GB.
The apparent 1999 shard is only 315 bytes and is an outlier that must be
verified before treating the manifest minimum as the scientific time range.

## Actual point model

Influx stores a point identity as:

```text
bucket + measurement + complete tag set + field + timestamp
```

The field value has an Influx physical type. Tag order is not semantically
significant, but every tag key and value is.

In the sampled `waggle` shards, names such as `wxt.env.temp` are measurements,
not fields. Every sampled measurement had exactly one field, named `value`.
This is a strong current convention, not a format invariant; the raw model
must still retain the field name.

Sample line protocol:

```text
wxt.env.temp,host=000048b02d35a87e.ws-nxcore,job=waggle-wxt536-4630,missing=-9999.9,node=000048b02d35a87e,plugin=registry.sagecontinuum.org/jrobrien/waggle-wxt536:0.24.11.14,sensor=vaisala-wxt536,task=waggle-wxt536,units=degree\ Celsius,vsn=W08E,zone=core value=-14.1 1765756800003271202
```

The timestamp is an integer nanosecond Unix timestamp in UTC. The example is
`2025-12-15T00:00:00.003271202Z`.

### Representative schema inventory

Shard `1004` covered a complete week and contained an estimated 6.47 million
series, 768 measurement names, 768 measurement/field entries, and 58 tag keys.
Shard `2293` was a partial backup week ending near December 18 and contained
313,916 exact series, 1,319 measurement names, 1,319 measurement/field entries,
and 66 tag keys.

Between those samples, 580 measurement names were common, 188 occurred only in
the early sample, and 739 only in the late sample. Many names are dynamically
expanded inference outputs such as `env.detection.avian.<species>`. This is
material schema evolution and makes one physical file per measurement per day
undesirable for the raw layer.

Representative measurement families include:

- weather and air quality: `wxt.*`, `aqt.*`, `env.temperature`;
- soil and water: `soilvwc`, `soilt`, `water_depth`, `water_temperature`;
- system telemetry: `sys.gps.*`, `sys.mem.*`, `sys.net.*`;
- computer vision and acoustic inference: `env.count.*`,
  `env.detection.*`, `object.detections.all`;
- application/log output: `upload`, `status`, `error`, `caption`;
- research instruments: `sonic.*`, `cdp.*`, radiative flux and gas names.

The inspected TSM files contained float64 and string blocks. Values that look
integral, for example `sys.gps.mode value=2`, are stored as float64 in the
sample. Influx also supports signed integer, unsigned integer, and boolean
fields, so the generic target schema must support those types even though they
were not observed in the two representative TSM files.

String values can contain JSON, URLs, descriptions, log messages, and embedded
newlines. A one-day selected export contained 698 newlines inside quoted string
values. Physical line count is therefore not a safe record count. The Go
`influxdata/line-protocol` stream parser used by the experiment explicitly
supports newlines inside quoted field strings.

### Tags and identifiers

The late sample had these 66 tag keys:

```text
applicationId applicationName camera channel chip class command cpu creator
description devAddr devEui device deviceName deviceProfileId deviceProfileName
event file_hash filename fstype gatewayId gateway_id host image_frac input
interval_seconds job last_modified_timestamp_source loop_num m_type missing
mode model mountpoint name node nsegments_asked nsegments_found organization
original_path packet_type plugin position project quality sample_count score
seg_id seg_rank seg_size sensor serial_number_tag server severity site
size_bytes status task tenantId tenantName thres_otsu type units upload_name
vsn zone
```

Tags are sparse and measurement-dependent. Common scientific identity tags
are `node`, `vsn`, `host`, `plugin`, `task`, `job`, `sensor`, `site`, `zone`,
and `units`. `missing` is a tag containing a sentinel description, not a null
value. `plugin` includes a version in many series, so a plugin upgrade creates
a new series. `host` and `node` can differ (for example RPi data attributed to
a node), so neither may be derived from the other.

One measurement can contain multiple nodes and instruments. In the late
sample, `wxt.env.temp` had 11 series and `env.temperature` had 118. A
measurement name alone is therefore not a complete scientific identity.

Tag schema changed between the two sample weeks. The early sample included
`direction` and `unit`, while the late sample added keys including `class`,
`event`, `model`, `score`, `sample_count`, and `gateway_id`. All tags must be
preserved without a fixed allowlist.

### Sampling and scale examples

Ten minutes beginning `2025-12-15T00:00:00Z` produced:

| Measurement | Series | Rows | Median within-series interval | Rough full-day rows if continuous |
| --- | ---: | ---: | ---: | ---: |
| `wxt.env.temp` | 10 | 63,410 | 0.0800 s | 9.13 million |
| `aqt.env.temp` | 7 | 189 | 20.015 s | 27,216 |

A different ten-minute window contained 2,744 `env.temperature`, 533
`object.detections.all`, 411 `upload`, and 326 `sys.gps.mode` points. These are
examples, not stable rates.

A disposable Parquet benchmark of the 190,238 WXT rows used the proposed
nanosecond long schema, common promoted tags, the complete tag map, Zstandard,
and two row groups. It was 1.38 MB versus 58.6 MB of exported line protocol
(7.24 Parquet bytes/row). Extrapolating that unusually compressible sample to
24 hours gives about 27.4 million rows and 0.185 GiB for three WXT
measurements. String-heavy and high-cardinality measurements will compress
less. Production file sizing must be based on a representative full-day pilot.

## Daily range semantics

`influxd inspect export-lp` in OSS 2.7.11 applies:

```go
if ts < start || ts > end { continue }
```

Both bounds are inclusive. The existing exporter passes adjacent midnights as
`--start` and `--end`; this can duplicate points exactly at midnight. The
sample contains such points, including a `date` point at
`2025-12-15T00:00:00.000000000Z`.

The canonical Parquet day must be UTC and half-open:

```text
[YYYY-MM-DDT00:00:00Z, next-dayT00:00:00Z)
```

The conversion layer must enforce that predicate after parsing, regardless of
the exporter's inclusive end behavior. It should report how many exported rows
were removed at the upper boundary. A two-second boundary sample confirmed
nanosecond timestamps on both sides of midnight. A ten-minute WXT sample had
zero duplicate measurement/tag-set/field/timestamp identities.

## Information required for losslessness

The raw Parquet representation must retain:

- source bucket ID and name;
- timestamp at nanosecond resolution with UTC semantics;
- measurement name;
- field name;
- exact Influx physical value type and value;
- the complete tag map, including unknown future tags;
- promoted copies of frequently filtered tags only as a convenience;
- source backup snapshot, manifest, shard IDs/files, and extraction bounds;
- warnings, parse errors, and boundary-filter counts.

Exact original line-protocol byte spelling and tag order are not scientific
semantics and need not be preserved. The typed point identity and values must
be reconstructable.

## Existing experiment assessment

### Reusable ideas

- `export-wxt-data.py` uses the supported `influxd inspect export-lp` command,
  explicit bucket and engine paths, measurement filters, UTC date strings,
  compressed output, temporary output names, and atomic rename.
- Its small stateless `export_chunk(start, end, measurement)` shape is a useful
  interface once configuration and range semantics are corrected.
- `convert-lp-to-json` promotes common tags while retaining other tags in a
  metadata map and keeps nanosecond timestamps.
- `process-lp-files.py` uses PyArrow, Zstandard, Parquet 2.6, statistics,
  temporary files, and atomic rename. These are suitable implementation
  ingredients.

### Information dropped or changed

`convert-lp-to-parquet/main.go` is WXT-specific and not lossless:

- converts nanoseconds to microseconds;
- assumes the first field is a float64 and drops its field name and any other
  fields;
- accepts only ten hard-coded tags and aborts on every other tag;
- reuses one output struct between points, so absent tags can inherit values
  from the preceding point;
- ignores file creation, final write, close, and parser termination errors.

`convert-lp-to-json/main.go` is more generic but still keeps only the first
field and drops the field name. It also treats every parser error as normal end
of input.

`process-lp-files.py` groups physically by plugin, measurement, and date, but
its streaming `groupby` is only correct if input is already sorted by that
entire key. TSM export is series-key ordered, not plugin-group ordered. It can
therefore emit many fragments for the same logical partition. Inferred Arrow
map/struct schemas can also differ between batches. File IDs based on current
directory contents are unsafe under concurrent writers.

The experimental scripts hard-code paths, bucket, dates, concurrency, and WXT
tag/value assumptions. They should be treated as evidence and prototypes, not
copied into production.

## Recommended raw Parquet schema

The natural semantic unit is a measurement-field point identified by its full
tag set and timestamp. The natural physical partition for the stated access
pattern is a UTC day, not a measurement, node, or instrument.

Recommended stable long schema:

| Column | Arrow type | Notes |
| --- | --- | --- |
| `time` | `timestamp[ns, tz=UTC]` | Required; never downcast. |
| `measurement` | `string` | Required. |
| `field` | `string` | Required even while usually `value`. |
| `value_type` | dictionary string or small integer enum | Required discriminator. |
| `value_float64` | `float64` nullable | Exactly one typed value column is non-null. |
| `value_int64` | `int64` nullable | Preserve signed integer. |
| `value_uint64` | `uint64` nullable | Preserve unsigned integer. |
| `value_bool` | `bool` nullable | Preserve boolean. |
| `value_string` | `string` nullable | Preserve strings, JSON, and newlines. |
| `tags` | `map<string,string>` | Required complete canonical tag set. |
| common tags | nullable `string` | Promoted copies of `node`, `vsn`, `host`, `plugin`, `task`, `job`, `sensor`, `site`, `zone`, `units`, `missing`; never the sole copy. |

A wide table would align fields at equal timestamps and tag sets. It is not
recommended for the raw layer because fields may have different types,
different cadence, sparse presence, and schema evolution. It also requires a
join/pivot that is not reversible without extra rules. Wide measurement- or
instrument-specific views can be produced later for xarray and scientific
analysis.

The stable long table supports pandas, Polars, PyArrow, and DuckDB. Sorting or
clustering rows by `measurement`, `field`, promoted identity tags, then `time`
improves compression and row-group predicate pushdown. Daily partitions give
date pruning. The tag map retains future metadata without changing the core
schema.

## Risks and unresolved facts

- The CROCUS node/site allowlist is not derivable from this backup alone.
- The manifest's 1999 start is likely an anomalous point or empty shard and is
  not a trustworthy scientific start date without targeted inspection.
- Field-type consistency was verified only for sampled files. A preflight must
  inventory type by measurement/field across selected shards and reject
  conflicts from accidental coercion.
- The supported exporter scans TSM files even for narrow ranges and filters;
  one invocation per measurement would repeatedly scan the same shard.
- The Bolt metadata contains credentials and must be access-controlled,
  excluded from logs, and omitted from conversion artifacts.
- Multiline strings make Unix physical-line tools unsuitable for record
  counting or splitting, although the selected Go stream parser supports them.
- The final partial shard contains data after the backup filename timestamp;
  selection must follow point timestamps and manifest/TSM ranges, not filenames.
