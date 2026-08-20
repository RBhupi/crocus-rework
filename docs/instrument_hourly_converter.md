# Sensor/VSN/instrument/day converter

For the recommended selective export workflow, see `docs/selective_export.md`.

## Purpose

The prototype reads one UTC day of InfluxDB line protocol exactly once and
routes records into immutable daily Parquet datasets by sensor, authoritative
VSN, and stable instrument.
It does not load a day into memory and does not append to existing Parquet
files.

Output layout:

```text
facts/
  sensor=<sensor>/
    vsn=<Wxxx>/
      instrument=<instrument-id>/
        date=YYYY-MM-DD/
          part-00000.parquet
          _manifest.json
```

Buffers are bounded globally. Full buffers become new `part-*.parquet` files.
Completed daily directories are published from a run-specific staging tree only
after all records have been parsed and written successfully.

## Environment

```bash
mamba env create -f environment.yml
mamba activate crocus
```

For an existing environment:

```bash
python -m pip install -e '.[test]'
```

## Instrument registry

Use an explicit registry for production. Rules match an exact subset of tags;
the first matching rule supplies the stable instrument ID. See
`config/instruments.example.json`.

Without a match, the converter derives a deterministic fallback from VSN,
sensor/task/device, zone, and device identity. Plugin version is deliberately excluded. Use
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

Do not launch 24 hourly `influxd` exports. The converter creates daily
partitions while consuming one daily stream, avoiding repeated TSM scans.

## WXT compatibility

The `sage-data-rework` WXT example establishes the commonly used measurements
`wxt.env.humidity`, `wxt.env.pressure`, and `wxt.env.temp`. It also identifies
hail, heater, rain, supply-voltage, wind-direction, and wind-speed measurements
that may be enabled in later exports. Do not hard-code only the first three:
the converter accepts every WXT measurement and keeps the measurement name in
each row.

The source WXT tags include `host`, `missing`, a numeric location tag, `plugin`, `sensor`,
`task`, `units`, `vsn`, and `zone`; some archive records also contain `job`.
The numeric location tag is discarded. Other tags are stored once per
`series_id` in `_series`; fact rows keep only required identity and value
columns. In particular, `missing` remains a string in series metadata so
downstream QA/QC can interpret the instrument-specific sentinel.

Use `vsn` + `sensor` + `zone` as the WXT registry match. Do not include
`plugin`, `job`, or `host` in instrument identity because software versions,
runs, and host naming can change while the physical instrument remains the
same. The example registry follows this rule.

The older example writes only floating-point `value` fields and converts
nanoseconds to microseconds. This implementation retains nanosecond timestamps,
supports all Influx field types and multiple fields, and preserves tags not
known in advance.

## Restart behavior

The default is to fail if any target day partition already exists. `--on-existing skip`
skips a completed partition only when its source snapshot, bucket, schema version,
converter version, registry fingerprint, and selection fingerprint match. Incomplete or incompatible
partitions always fail.

Failed `crocus-export` staging is removed. Published partitions are never mixed
with incomplete staging and are recovered through the daily completion marker.

## Current boundary

This implements streaming conversion and structural provenance, not scientific
QA/QC. Before a production run, supply the authoritative CROCUS instrument
registry and validate one full day against the source summaries described in
`docs/influx_parquet_plan.md`.
