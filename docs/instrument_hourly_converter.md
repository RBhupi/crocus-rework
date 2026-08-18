# Instrument/hour converter

For the complete index catalog and direct WXT export commands, see
`docs/catalog_and_wxt_export.md`.

## Purpose

The prototype reads one UTC day of InfluxDB line protocol exactly once and
routes records into immutable hourly Parquet datasets for stable instruments.
It does not load a day into memory and does not append to existing Parquet
files.

Output layout:

```text
schema_version=1/
  instrument=<instrument-id>/
    date=YYYY-MM-DD/
      hour=HH/
        part-00000.parquet
        _manifest.json
```

Buffers are bounded globally. Full buffers become new `part-*.parquet` files.
Completed hour directories are published from a run-specific staging tree only
after all records have been parsed and written successfully.

## Environment

```bash
mamba env create -f environment.yml
mamba activate crocus-raw
```

For an existing environment:

```bash
python -m pip install -e '.[test]'
```

## Instrument registry

Use an explicit registry for production. Rules match an exact subset of tags;
the first matching rule supplies the stable instrument ID. See
`config/instruments.example.json`.

Without a match, the prototype derives a deterministic fallback from node,
sensor/task/device, and zone. Plugin version is deliberately excluded. Use
`--require-registry` to reject every unmatched point.

## Convert an existing export

```bash
crocus-raw \
  --date 2025-12-15 \
  --input 2025-12-15.lp.gz \
  --output /path/to/parquet \
  --source-snapshot 20251217T150420Z \
  --bucket waggle \
  --instrument-registry config/instruments.json \
  --require-registry
```

Use `--input -` to stream from `influxd inspect export-lp`. Request the next
midnight as `--end`; InfluxDB 2.7.11 includes that endpoint, and the converter
records and removes points at or after the next midnight.

```bash
influxd inspect export-lp \
  --bucket-id b3a4e89ad74c5acc \
  --engine-path /path/to/engine \
  --start 2025-12-15T00:00:00Z \
  --end 2025-12-16T00:00:00Z \
  --output-path - \
| crocus-raw \
    --date 2025-12-15 \
    --input - \
    --output /path/to/parquet \
    --source-snapshot 20251217T150420Z \
    --instrument-registry config/instruments.json \
    --require-registry
```

Do not launch 24 hourly `influxd` exports. The converter creates hour
partitions while consuming one daily stream, avoiding 24 repeated TSM scans.

## WXT compatibility

The `sage-data-rework` WXT example establishes the commonly used measurements
`wxt.env.humidity`, `wxt.env.pressure`, and `wxt.env.temp`. It also identifies
hail, heater, rain, supply-voltage, wind-direction, and wind-speed measurements
that may be enabled in later exports. Do not hard-code only the first three:
the converter accepts every WXT measurement and keeps the measurement name in
each row.

The example's WXT tags are `host`, `missing`, `node`, `plugin`, `sensor`,
`task`, `units`, `vsn`, and `zone`; some archive records also contain `job`.
All tags are retained in the `tags` map and these common tags are also promoted
to nullable columns. In particular, `missing` remains a string so downstream
QA/QC can interpret the instrument-specific sentinel without modifying raw
values.

Use `node` + `sensor` + `zone` as the WXT registry match. Do not include
`plugin`, `job`, or `host` in instrument identity because software versions,
runs, and host naming can change while the physical instrument remains the
same. The example registry follows this rule.

The older example writes only floating-point `value` fields and converts
nanoseconds to microseconds. This implementation retains nanosecond timestamps,
supports all Influx field types and multiple fields, and preserves tags not
known in advance.

## Restart behavior

The default is to fail if any target hour already exists. `--on-existing skip`
skips a completed hour only when its source snapshot, bucket, schema version,
converter version, and registry fingerprint match. Incomplete or incompatible
partitions always fail.

Failed runs remain under `schema_version=1/_staging/<run-id>` for diagnosis.
They are never mixed with completed partitions.

## Current boundary

This implements streaming conversion and structural provenance, not scientific
QA/QC. Before a production run, supply the authoritative CROCUS instrument
registry and validate one full day against the source summaries described in
`docs/influx_parquet_plan.md`.
