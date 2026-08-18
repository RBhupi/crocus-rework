# InfluxDB to Parquet implementation plan

## Recommendation

Build a raw, lossless-enough Parquet layer with one stable long schema and UTC
daily partitions. Keep measurement and field as columns. Do not create one
file or partition per measurement: the sampled schema grew from 768 to 1,319
measurements, many of them sparse, dynamically named inference classes.

Recommended dataset layout:

```text
raw/
  schema_version=1/
    bucket=waggle/
      date=2025-12-15/
        part-00000.parquet
        part-00001.parquet
        _manifest.json
      date=2025-12-16/
        ...
```

Partition by bucket and date. `schema_version` may be a release directory
rather than a Hive partition if the storage system makes that clearer. Write
multiple parts only when needed to meet a target compressed size, initially
128-512 MiB. Use row groups near 64-128 MiB and Zstandard compression. Tune
these values from the pilot rather than treating them as fixed requirements.

Within each day, cluster by `measurement`, `field`, promoted identity tags, and
`time`. This lets row-group statistics skip unrelated measurements while
avoiding hundreds or thousands of tiny daily files.

Terminology:

- **Parquet file:** one `part-*.parquet` object with one schema and one or more
  row groups.
- **Parquet dataset:** all compatible files under the schema-version root.
- **Partition:** a directory value such as `bucket=waggle/date=2025-12-15` used
  for pruning; it is not necessarily one file.
- **Row group:** an independently encoded/statistical chunk inside a file.
- **Table/schema:** the Arrow logical columns shared by every raw data file.

## Long versus wide

Use long form for the raw layer:

```text
time | measurement | field | value_type | typed value columns | tags | promoted tags
```

Reasons:

- it maps directly to Influx measurement/tag/field/timestamp identity;
- it preserves different physical field types without coercing them to string;
- new measurements, fields, instruments, and tags do not require new columns;
- sparse inference outputs do not create extremely wide null-heavy tables;
- date partitioning, row-group measurement statistics, and promoted tags still
  support pruning and predicate pushdown;
- later QA/QC can group or pivot without the raw layer deciding alignment rules.

Use five nullable value columns (`float64`, `int64`, `uint64`, `bool`, and
`string`) plus a required `value_type`. Exactly one value column must be
non-null per row. Arrow/Parquet has no portable scalar union type, and encoding
all values as strings would lose type and numeric statistics.

Keep the complete tag set in `map<string,string>`. Duplicate common tags into
nullable top-level columns for filtering, but validate that every promoted
value equals the map entry. Do not normalize units, missing sentinels, node
names, plugin versions, or measurement names in this raw layer.

## Daily extraction semantics

The canonical day is:

```text
[day 00:00:00 UTC, next day 00:00:00 UTC)
```

InfluxDB OSS 2.7.11 `export-lp` includes both `--start` and `--end`. Therefore:

1. request through the next midnight;
2. parse all returned points without changing nanosecond timestamps;
3. retain only `start <= time < end` in the conversion core;
4. count and report points removed at `time == end`;
5. fail if any point is outside the requested inclusive export envelope.

This deliberately makes the converter, not the CLI's undocumented boundary
behavior, authoritative. Never split or count exported data by physical
newline because quoted strings can contain newlines.

Do one export per bucket/day or per bounded group of measurements, not one
export per measurement. Each exporter invocation scans relevant TSM files, so
measurement-level invocations multiply I/O. Whether all-measurement daily
exports fit the streaming parser's throughput is a pilot question.

## Provenance

Use a small partition sidecar as the authoritative record and put only compact
identifiers in Parquet key/value metadata.

`_manifest.json` should contain:

- status: `writing`, `complete`, or `failed`;
- raw schema version and schema fingerprint;
- source backup snapshot name and source manifest path;
- source bucket name/ID, retention policy, shard IDs, tarball names, sizes, and
  optionally checksums when checksum cost is accepted;
- requested and effective UTC range;
- measurement/field filters, or `all`;
- `influxd` version and container digest;
- extractor/converter version and Git commit;
- row count, distinct measurement and field counts;
- minimum/maximum timestamp;
- counts by value type, null counts, duplicate identity count, and upper-bound
  rows removed;
- warnings and errors;
- output file names, byte sizes, row counts, row groups, and checksums.

Parquet metadata should repeat schema version, partition ID, run ID, Git
commit, and source snapshot ID so a detached file remains identifiable. Avoid
large JSON metadata in every file. A global catalog is not needed for the
prototype; completed sidecars can be scanned into one later.

The Bolt and SQLite metadata backups are not required converter inputs. In
particular, the Bolt backup contains credentials and must never be copied into
Parquet metadata or provenance logs.

## Validation strategy

Validation is a release gate, not QA/QC. It proves faithful representation.

For one representative day, compare source export summaries with Parquet:

1. total logical point rows after the half-open boundary filter;
2. counts grouped by measurement, field, and value type;
3. distinct complete tag maps and values for every tag key;
4. minimum and maximum nanosecond timestamp globally and per measurement;
5. null counts and the exactly-one-value-column invariant;
6. numeric count/min/max/sum and string count/min-length/max-length or hashes;
7. duplicate identity count using measurement + canonical tag map + field +
   timestamp;
8. promoted-tag equality with the canonical tag map;
9. schema and physical Parquet types;
10. deterministic hashes of sorted canonical rows for small selected streams.

Use independent implementations where practical: the converter emits its
manifest, while a validation command reads the export and Parquet separately.
Record mismatches; do not silently coerce or drop malformed points.

The pilot day must include:

- high-frequency numeric WXT data;
- slower AQT or system telemetry;
- strings with JSON and embedded newlines (`object.detections.all`, `status`,
  or `caption`);
- several nodes/plugins and sparse tags;
- at least one exact-midnight point;
- measurements present in only one of the compared historical samples.

The ten-minute WXT inspection found zero duplicate source identities, but the
validator must still detect and report duplicates rather than assuming none.

## Restartability and idempotency

Write each partition as an isolated transaction:

```text
_staging/<run-id>/...part files...
_staging/<run-id>/_manifest.json
```

After validation:

1. finalize the manifest with `status=complete`;
2. atomically rename the staging directory into the final partition when the
   filesystem supports directory rename;
3. otherwise copy immutable files and publish `_SUCCESS` or the complete
   manifest last.

Rules:

- skip only when a complete existing manifest has the same source snapshot,
  time range, schema version, converter version, and file checksums;
- never append into an existing complete partition;
- never mix files from different run IDs or schema fingerprints;
- leave failed staging output for diagnosis or remove it only via an explicit
  cleanup command;
- a code/schema change writes a new schema-version root or requires explicit
  replacement, never silent overwrite;
- derive deterministic part names from the run plan, not directory file count.

## Proposed modules and interfaces

Prefer a small functional core with a CLI shell.

```text
src/crocus_raw/
  archive.py       # read/validate backup manifest and resolve shards
  extract.py       # build and run version-pinned influxd export command
  line_protocol.py # stream logical points and expose parse errors/positions
  model.py         # typed canonical Point and Arrow schema
  transform.py     # boundary filter, tag canonicalization, Arrow batches
  write.py         # clustered Parquet parts and atomic staging
  validate.py      # independent export-vs-Parquet summaries
  provenance.py    # schema fingerprint and partition manifest
  cli.py           # inspect, convert-day, validate-day
```

Suggested interfaces:

```python
load_backup_manifest(path) -> BackupManifest
resolve_shards(manifest, bucket_id, start, end) -> list[ShardSource]
build_export_command(config, bucket_id, start, end, measurements) -> list[str]
iter_points(stream) -> Iterator[InfluxPoint]
normalize_point(point, start, end) -> CanonicalPoint | BoundaryDrop
points_to_batches(points, schema, max_rows) -> Iterator[pyarrow.RecordBatch]
write_partition(batches, destination, run_manifest) -> OutputSummary
summarize_export(stream, start, end) -> ValidationSummary
summarize_dataset(path) -> ValidationSummary
compare_summaries(source, target) -> ValidationReport
```

Keep subprocess/filesystem handling outside parsing and normalization. Use
plain data structures and functions; classes are justified only for streaming
writer/parser state.

## Dependencies and environment

Use `mamba` for environment creation. Initial runtime dependencies:

- Python 3.12 or the project's agreed supported version;
- `pyarrow` for the explicit schema, record batches, dataset writing, and
  validation reads;
- a pinned InfluxDB OSS 2.7.11 `influxd`, preferably by container digest or a
  verified binary;
- one mature line-protocol parser with verified multiline strings and all
  Influx scalar types.

Polars is optional for exploratory validation, not required in the conversion
path. Pandas is not required. Avoid an additional framework or database until
the pilot demonstrates a need.

## Small-scale prototype

Implement only one `waggle` UTC day after the human scope decisions below.

1. Resolve the one or two source shards from the backup manifest and restore
   only their TSM files into a temporary read-only engine layout.
2. Export the chosen day once with InfluxDB 2.7.11, including next midnight.
3. Stream-parse all logical records, preserve tags/types/nanoseconds, apply the
   half-open filter, and write Arrow batches.
4. Write clustered Parquet parts to staging with the stable long schema.
5. Produce the partition manifest.
6. Run the independent comparisons listed above.
7. Measure wall time, peak memory, export bytes, rows, Parquet bytes, row-group
   selectivity, and number of files.
8. Stop and review results before converting another day.

December 15, 2025 is a useful technical stress day because the inspected shard
contains high-frequency WXT, strings/JSON, 66 tag keys, and exact-midnight
points. It may not be the correct scientific pilot until the CROCUS node scope
is supplied.

## Human decisions required

1. **CROCUS scope:** provide the authoritative node/VSN/site allowlist and say
   whether system/plugin outputs are included or only scientific instruments.
2. **Bucket scope:** confirm that only `waggle` is in the raw scientific layer,
   excluding `grafana-agent`, statistics, monitoring, and test buckets.
3. **History scope:** choose the first trusted date and disposition of the 1999
   outlier and pre-CROCUS data.
4. **Raw tags:** approve storing the complete tag map plus promoted common tags,
   accepting the small duplication for usability.
5. **File target:** choose an initial target in the proposed 128-512 MiB range,
   or authorize the pilot to select it empirically.
6. **Source checksums:** decide whether expensive SHA-256 checksums of multi-GB
   tarballs are required or whether manifest filename/size/snapshot identity is
   sufficient initially.
7. **Sensitive backup handling:** define access and retention rules for the
   Bolt metadata file containing credentials.

