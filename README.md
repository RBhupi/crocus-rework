# CROCUS Rework

Selective, resumable conversion of CROCUS InfluxDB OSS backups into
analysis-ready Parquet. The production workflow stages each backup shard once,
exports only curated measurements, streams line protocol directly into Python,
and publishes immutable Hive-style partitions without retaining intermediate
LP files.

## Data Model

Facts use long format and nanosecond UTC timestamps:

```text
facts/
  sensor=<sensor>/
    vsn=<Wxxx>/
      instrument=<instrument-id>/
        date=YYYY-MM-DD/
          part-*.parquet
```

Each fact contains sensor, authoritative VSN, stable instrument ID,
measurement, field, typed value columns, and a deterministic `series_id`.
Complete retained tags are normalized under `_series`; missing sensor/VSN rows
are written to `_quarantine`. The numeric `node` tag is intentionally
discarded.

Reviewed selections are provided for:

- Vaisala WXT536: `config/vaisala-wxt536-exact.selection.json`
- Vaisala AQT530: `config/aqt-exact.selection.json`

## Commands

- `crocus-export`: selective backup/engine export to Parquet; primary workflow
- `crocus-raw`: streaming line-protocol-to-Parquet converter
- `crocus-inventory`: optional index-only SQLite catalog builder

Install and test:

```bash
python -m pip install -e '.[test]'
python -m pytest
```

InfluxDB OSS 2.7.11 or 2.7.12 is required. A typical backup export is:

```bash
crocus-export \
  --backup-dir /path/to/influx-backup \
  --bucket-id b3a4e89ad74c5acc \
  --start-date 2025-12-15 \
  --end-date 2025-12-16 \
  --selection-file config/vaisala-wxt536-exact.selection.json \
  --output /path/to/parquet-output \
  --work-dir /path/to/temporary-work \
  --influxd /path/to/influxd \
  --workers 1
```

`--end-date` is exclusive. Compatible shard/day markers make the same command
safe to resume. Do not run multiple orchestrators against one output root.

## Documentation

- [`docs/HANDOVER.md`](docs/HANDOVER.md): current production status, HPC paths,
  monitoring, restart, validation, results, lessons, and next-session prompt
- [`docs/selective_export.md`](docs/selective_export.md): selection format,
  export behavior, layout, and query examples
- [`docs/data_levels_and_netcdf_naming.md`](docs/data_levels_and_netcdf_naming.md):
  Bronze/Silver/Gold processing levels and final daily NetCDF filenames
- [`docs/catalog_and_wxt_export.md`](docs/catalog_and_wxt_export.md): optional
  inventory workflow and InfluxDB setup
- [`docs/instrument_hourly_converter.md`](docs/instrument_hourly_converter.md):
  converter behavior, identity rules, and restart semantics
- [`docs/influx_parquet_data_model.md`](docs/influx_parquet_data_model.md): source
  investigation and data-model background
- [`docs/influx_parquet_plan.md`](docs/influx_parquet_plan.md): original design
  and validation considerations
