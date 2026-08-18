# CROCUS catalog and WXT export

## Overview

The workflow has two commands:

- `crocus-inventory` reads TSM indexes and creates instrument and variable lists.
- `crocus-export` runs daily `influxd inspect export-lp` streams and writes
  instrument/hour Parquet through the existing converter.

The inventory command never requests `dump-tsm --blocks` or `dump-tsm --all`.
Backup archives still have to be decompressed because each TSM index is stored
inside its TSM file.

## Install InfluxDB OSS on the Mac

InfluxDB OSS 2.7.11 provides a Darwin AMD64 archive. It runs through Rosetta on
Apple Silicon. Download it into the output drive and verify the published
checksum before extracting it:

```bash
cd '/Volumes/Extreme SSD/crocus-rework-output'
mkdir -p tools
curl -fL \
  https://dl.influxdata.com/influxdb/releases/influxdb2-2.7.11_darwin_amd64.tar.gz \
  -o tools/influxdb2-2.7.11_darwin_amd64.tar.gz
echo '224926fd77736a364cf28128f18927dda00385f0b6872a108477246a1252ae1b  tools/influxdb2-2.7.11_darwin_amd64.tar.gz' \
  | shasum -a 256 -c -
tar -xzf tools/influxdb2-2.7.11_darwin_amd64.tar.gz -C tools

INFLUXD='/Volumes/Extreme SSD/crocus-rework-output/tools/influxdb2-2.7.11/influxd'
"$INFLUXD" version
```

Only InfluxDB OSS 2.7.11 and 2.7.12 are accepted. The detected version is
stored in catalog, run, partition, and Parquet metadata.

Install the Python commands from the repository:

```bash
python -m pip install -e '.[test]'
```

## Build the complete catalog on the Mac

```bash
crocus-inventory \
  --backup-dir '/Volumes/Extreme SSD/2025-12-17' \
  --bucket-id b3a4e89ad74c5acc \
  --output '/Volumes/Extreme SSD/crocus-rework-output/catalog' \
  --work-dir '/Volumes/Extreme SSD/crocus-rework-output/_inventory_work' \
  --influxd '/Volumes/Extreme SSD/crocus-rework-output/tools/influxdb2-2.7.11/influxd' \
  --resume
```

The selected `waggle` bucket contains 246 shard archives and approximately
471 GB of compressed data. The SQLite database commits one complete shard at a
time. Repeating the command with `--resume` skips completed shards and retries
failed shards.

Catalog outputs:

```text
catalog/
  inventory.sqlite
  inventory_manifest.json
  inventory_errors.csv
  instruments.csv
  instrument_variables.csv
  measurements.csv
  wxt_instruments.txt
  wxt_measurements.txt
```

`instrument_variables.csv` is the primary requested list. Its stable key is:

```text
instrument_id::measurement::field
```

Fallback identities are retained and marked with `confidence` and
`review_required` in `instruments.csv`.

## Run the Mac WXT pilot

`--end-date` is exclusive. The following processes one UTC day:

```bash
crocus-export \
  --backup-dir '/Volumes/Extreme SSD/2025-12-17' \
  --bucket-id b3a4e89ad74c5acc \
  --start-date 2025-12-15 \
  --end-date 2025-12-16 \
  --measurement-file '/Volumes/Extreme SSD/crocus-rework-output/catalog/wxt_measurements.txt' \
  --instrument-file '/Volumes/Extreme SSD/crocus-rework-output/catalog/wxt_instruments.txt' \
  --output '/Volumes/Extreme SSD/crocus-rework-output/parquet' \
  --work-dir '/Volumes/Extreme SSD/crocus-rework-output/_export_work' \
  --influxd '/Volumes/Extreme SSD/crocus-rework-output/tools/influxdb2-2.7.11/influxd' \
  --workers 1
```

Backup mode stages one weekly shard, processes its requested days, and removes
the staged engine before moving to the next shard. All WXT measurements are
passed to one daily `export-lp` invocation. Line protocol is read from stdout
and is never retained.

The converter removes the inclusive next-midnight row reported by InfluxDB and
filters rows using `wxt_instruments.txt`. Completed compatible partitions are
skipped on a rerun.

## Run on the production engine

The production engine path must contain
`data/<bucket-id>/<retention-policy>/<shard-id>/*.tsm`.
Use a stable backup or engine snapshot identifier for restart compatibility:

```bash
crocus-inventory \
  --engine-dir /path/to/influxdb/engine \
  --bucket-id b3a4e89ad74c5acc \
  --bucket-name waggle \
  --source-snapshot 20251217T150420Z \
  --output /path/to/crocus/catalog \
  --work-dir /path/to/crocus/_inventory_work \
  --influxd /home/sean/influxdb2-2.7.12/usr/bin/influxd \
  --resume

crocus-export \
  --engine-dir /path/to/influxdb/engine \
  --bucket-id b3a4e89ad74c5acc \
  --bucket-name waggle \
  --source-snapshot 20251217T150420Z \
  --start-date 2025-01-01 \
  --end-date 2025-02-01 \
  --measurement-file /path/to/crocus/catalog/wxt_measurements.txt \
  --instrument-file /path/to/crocus/catalog/wxt_instruments.txt \
  --output /path/to/crocus/parquet \
  --influxd /home/sean/influxdb2-2.7.12/usr/bin/influxd \
  --workers 1
```

Do not start with eight workers. Both workers would scan the same TSM files.
Increase to `--workers 2` only after measuring storage throughput; larger values
are rejected.

## Completion checks

- `inventory_manifest.json` must have `status: complete`, 246 completed
  sources, and no error sources.
- `inventory_errors.csv` must contain only its header.
- The export manifest under `parquet/export_runs` must have
  `status: complete` and one completed entry for every requested day.
- For each day, `parsed_point_rows` must equal `output_rows` plus
  `filtered_instrument_rows` plus `upper_boundary_rows`.
