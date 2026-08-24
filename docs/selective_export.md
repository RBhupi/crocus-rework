# Selective VSN-first export

For the current production status, HPC paths, monitoring, restart, validation,
and next-session instructions, begin with `docs/HANDOVER.md`.

## Recommended workflow

The exhaustive inventory is optional. Use a curated selection containing exact
measurement names and sensor predicates. In backup mode, `crocus-export` stages
each overlapping shard once, passes the deduplicated measurement union to one
`influxd inspect export-lp` process, filters the stream, and writes Parquet
immediately. Line protocol is never retained.

```json
{
  "selection_version": 1,
  "selectors": [
    {
      "measurement": "wxt.env.temp",
      "fields": ["value", "quality"],
      "tags": {
        "sensor": ["vaisala-wxt536"],
        "vsn": ["W0*"]
      }
    }
  ]
}
```

Every selector requires an exact measurement. Conditions within one selector
are ANDed; values within a list and separate selectors are ORed. Field and tag
patterns use case-sensitive shell globs. `vsn` is authoritative and preserves
uppercase Wxxx values. The discarded numeric source tag cannot be selected or
stored.

Use `config/vaisala-wxt536-exact.selection.json` for the 11 WXT measurements
observed in the pilot. The complete 246-shard catalog identifies every AQT
series as `vaisala-aqt530`; use `config/aqt-exact.selection.json` for its 12
reviewed measurements. Accepted facts still require the raw `sensor` and `vsn`
tags; missing identities are quarantined. Add both measurement sets to one
selection file to stage each shard only once for a combined WXT/AQT run.

## Mac pilot

`--end-date` is exclusive:

```bash
cd /Users/bhupendra/projects/crocus-rework
micromamba run -n data python -m pip install -e '.[test]'

/usr/bin/time -lp micromamba run -n data crocus-export \
  --backup-dir '/Volumes/Extreme SSD/2025-12-17' \
  --bucket-id b3a4e89ad74c5acc \
  --start-date 2025-12-15 \
  --end-date 2025-12-16 \
  --selection-file config/vaisala-wxt536-exact.selection.json \
  --output '/Volumes/Extreme SSD/crocus-rework-output/wxt-vsn-v4' \
  --work-dir '/Volumes/Extreme SSD/crocus-rework-output/_work-wxt-vsn-v4' \
  --influxd '/Volumes/Extreme SSD/crocus-rework-output/tools/influxdb2-2.7.11/influxd' \
  --workers 1
```

Start with one worker. Multiple workers decompress independent shards and run
independent InfluxDB scans, so benchmark them only after the one-worker run.
Completed day and shard markers prevent compatible work from being repeated.

## Dataset layout

```text
_dataset.json
_selection.json
_days/date=YYYY-MM-DD.json
_shards/shard=*.json
_series/run=<run-id>/part-*.parquet
_quarantine/reason=<reason>/date=YYYY-MM-DD/run=<run-id>/part-*.parquet
_catalog/
  selected_sensors.csv
  selected_instruments.csv
  selected_variables.csv
  selected_instrument_variables.csv
  selected_series.csv
  metadata_conflicts.csv
facts/
  sensor=<sensor>/
    vsn=<Wxxx>/
      instrument=<instrument-id>/
        date=YYYY-MM-DD/
          _manifest.json
          part-*.parquet
```

Facts use long format with nanosecond time, sensor, VSN, instrument ID,
measurement, field, typed value columns, and a deterministic 128-bit
`series_id`. Complete retained tags are normalized into `_series`; they are not
repeated on every fact row. Missing-sensor and missing-VSN rows go to
`_quarantine` and mark the export for review.

Do not concatenate all dates. Query the Hive tree as one table:

```sql
SELECT time, vsn, instrument_id, measurement, field, value_float64
FROM read_parquet(
  '/path/to/output/facts/sensor=*/vsn=*/instrument=*/date=*/*.parquet',
  hive_partitioning = true
)
WHERE sensor = 'vaisala-wxt536'
  AND measurement = 'wxt.env.temp';
```

Compact only within one sensor/VSN/instrument/day partition if small files
become a problem.
