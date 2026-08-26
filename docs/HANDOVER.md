# CROCUS rework handover

Last updated: 2026-08-24

## Purpose

This document is the starting point for a new development or operations
session. It records what was implemented, the current production run, how to
monitor or resume it, what output to expect, how to validate it, and the main
technical conclusions from the work.

The repository implements selective, streaming conversion of an InfluxDB OSS
backup into a long-format Parquet dataset. The exhaustive inventory is no
longer required for extraction, although its completed SQLite catalog remains
useful for discovering and reviewing instruments and variables.

## Repository state

- Repository: `https://github.com/RBhupi/crocus-rework`
- Validated commit at handover: `f504957` (`Add reviewed AQT extraction workflow`)
- Package: `crocus-raw==0.5.0`
- Python: 3.11 or newer
- InfluxDB OSS accepted versions: 2.7.11 and 2.7.12
- Test status: 50 tests passing
- Primary commands: `crocus-export`, `crocus-raw`, and `crocus-inventory`

Before continuing development:

```bash
cd /Users/bhupendra/projects/crocus-rework
git status --short
git pull --ff-only origin main
micromamba run -n data python -m pip install -e '.[test]'
micromamba run -n data python -m pytest -q
```

## Source data

### Mac

```text
/Volumes/Extreme SSD/2025-12-17
/Volumes/Extreme SSD/crocus-rework-output
```

### HPC

```text
Backup:  /nfs/gce/globalscratch/influx
Base:    /nfs/gce/projects/crocus-server-admins/data-rework
Repo:    /nfs/gce/projects/crocus-server-admins/data-rework/crocus-rework
Env:     /nfs/gce/projects/crocus-server-admins/data-rework/envs/crocus
Influx:  /nfs/gce/projects/crocus-server-admins/data-rework/tools/influxdb2-2.7.11/usr/bin/influxd
```

Source identity:

- Bucket ID: `b3a4e89ad74c5acc`
- Bucket name: `waggle`
- Backup snapshot: `20251217T150420Z`
- Complete backup catalog: 246 shards

The raw backup under global scratch is read-only. Temporary extracted engines
belong under the project `work` directory and are deleted after their shard
finishes.

## Implemented workflow

The production workflow is direct selective export:

1. Select exact measurement names with a versioned JSON file.
2. Stage each overlapping backup shard once.
3. Pass the deduplicated measurement union to one `influxd inspect export-lp`
   process for that shard interval.
4. Stream uncompressed line protocol from stdout directly into Python.
5. Cache decoded series metadata and apply field/tag selection during parsing.
6. Require raw `sensor` and `vsn` tags for accepted facts.
7. Write bounded Arrow buffers directly to immutable Parquet partitions.
8. Publish normalized series metadata, quarantine records, manifests, and
   catalogs atomically.
9. Resume from compatible shard and day completion markers.

No line-protocol files are retained, and dates are never concatenated into one
giant Parquet file.

## Dataset model

Internal storage schema version 4 is stored in Parquet and manifest metadata,
not in directory names.

```text
<dataset-root>/
  _dataset.json
  _selection.json
  _days/date=YYYY-MM-DD.json
  _shards/shard=*.json
  _runs/*.json
  _series/run=<run-id>/part-*.parquet
  _quarantine/reason=<reason>/date=YYYY-MM-DD/run=<run-id>/part-*.parquet
  _catalog/
    selected_sensors.csv
    selected_instruments.csv
    selected_variables.csv
    selected_instrument_variables.csv
    selected_series.csv
    metadata_conflicts.csv
  export_runs/*.json
  facts/
    sensor=<sensor>/
      vsn=<Wxxx>/
        instrument=<instrument-id>/
          date=YYYY-MM-DD/
            _manifest.json
            part-*.parquet
```

Fact columns:

- `time`: nanosecond UTC timestamp
- `sensor`
- `vsn`
- `instrument_id`
- `measurement`
- `field`
- `series_id`: deterministic 128-bit hash
- `value_type`
- `value_float64`, `value_int64`, `value_uint64`, `value_bool`, `value_string`

`series_id` is the first 128 bits of SHA-256 over the measurement and sorted
retained Influx tags. Field is excluded because Influx fields belong to a
series. The discarded numeric `node` tag is excluded from facts, series
metadata, identities, catalogs, selection files, and series hashes.

Complete retained tags live once in `_series`; they are not repeated on every
fact row. Missing-sensor and missing-VSN rows go to `_quarantine` and mark the
run for review.

## Instrument and metadata decisions

`vsn` is the authoritative location identity and preserves uppercase Wxxx
values. Fallback instrument IDs use VSN, sensor/type, zone, and device identity.
Plugin, job, task, and host changes do not change a physical instrument ID.

Metadata consistency is checked per `sensor + measurement + field` using:

- description
- units
- missing-value declaration
- observed value types

Conflicts are retained as distinct series, written to
`_catalog/metadata_conflicts.csv`, and cause `requires_review=true`.

## Reviewed measurements

### WXT

Sensor: `vaisala-wxt536`

Selection: `config/vaisala-wxt536-exact.selection.json`

The 11 reviewed measurements are:

```text
wxt.env.humidity
wxt.env.pressure
wxt.env.temp
wxt.hail.accumulation
wxt.heater.status
wxt.heater.temp
wxt.heater.volt
wxt.rain.accumulation
wxt.voltage.supply
wxt.wind.direction
wxt.wind.speed
```

### AQT

The full 246-shard inventory proved that the archived sensor value is always
`vaisala-aqt530`, not `vaisala-aqt560`.

Selection: `config/aqt-exact.selection.json`

The 12 reviewed measurements are:

```text
aqt.env.humidity
aqt.env.pressure
aqt.env.temp
aqt.gas.co
aqt.gas.no
aqt.gas.no2
aqt.gas.ozone
aqt.house.datetime
aqt.house.uptime
aqt.particle.pm1
aqt.particle.pm10
aqt.particle.pm2.5
```

Full-catalog AQT findings:

- 636 raw series records
- 15 VSNs
- one sensor value: `vaisala-aqt530`
- one zone value: `core`
- one field per measurement: `value`
- consistent description, units, and missing sentinel per measurement
- common missing declaration: `-9999.9`
- `aqt.house.datetime` is a string; the other pilot values were numeric
- runtime variation: 15 hosts, 30 jobs, 2 plugins, and 5 tasks

Scientific metadata is `description + units + missing`. Runtime tags `host`,
`job`, `plugin`, and `task` must not define instrument identity.

HPC catalog artifacts:

```text
/nfs/gce/projects/crocus-server-admins/data-rework/crocus-rework-output/catalog/inventory.sqlite
/nfs/gce/projects/crocus-server-admins/data-rework/crocus-rework-output/catalog/aqt_inventory.sqlite
/nfs/gce/projects/crocus-server-admins/data-rework/crocus-rework-output/catalog/aqt_variable_registry.csv
```

The complete inventory reported `complete|246`. At the time it was inspected,
the main database was 39 GB with an 85 MB SHM and 14 GB WAL. Never delete or
copy only the WAL-backed main database while those files are present. Query it
read-only in place or use SQLite's backup mechanism to create a consistent
snapshot.

## Local acceptance results

### Dense WXT day

Date: 2025-12-15

- 85,531,255 fact rows
- 11 measurements
- 11 VSNs
- runtime: 520.48 seconds
- maximum resident memory: 1.21 GB
- zero incorrect-sensor rows
- zero quarantined rows
- zero metadata conflicts
- exact reconciliation with the earlier source/output summaries
- compatible resume: 1.36 seconds without restaging

Output:

```text
/Volumes/Extreme SSD/crocus-rework-output/pilot-vaisala-wxt536-2025-12-15-vsn-v5
```

### AQT day

Date: 2025-12-15

- 338,376 fact rows
- 12 measurements, each with 28,198 rows
- 8 VSNs active that day
- 96 normalized series
- 310,178 float rows
- 28,198 string rows from `aqt.house.datetime`
- runtime: approximately 44 seconds, including approximately 9.5 seconds of
  shard staging
- zero filtered rows
- zero quarantined rows
- zero metadata conflicts
- compatible resume: 0.96 seconds

Validated output:

```text
/Volumes/Extreme SSD/crocus-rework-output/pilot-aqt-2025-12-15-v5-sensor-agnostic
```

The production AQT selection is now sensor-constrained because the exhaustive
catalog confirmed one consistent sensor value across all shards.

## Active HPC production run

The combined WXT+AQT production export was started with eight shard workers on
`compute-386-07` using `nohup`.

```text
Date range: 2023-05-05 through 2025-12-16
End date:   2025-12-17, exclusive
Days:       957
Output:     /nfs/gce/projects/crocus-server-admins/data-rework/crocus-rework-output/wxt-aqt-production-v5
Work:       /nfs/gce/projects/crocus-server-admins/data-rework/work/wxt-aqt-production-v5
Log:        /nfs/gce/projects/crocus-server-admins/data-rework/wxt-aqt-production-v5.log
PID file:   /nfs/gce/projects/crocus-server-admins/data-rework/wxt-aqt-production-v5.pid
Selection:  /nfs/gce/projects/crocus-server-admins/data-rework/wxt-aqt-exact.selection.json
Host:       compute-386-07
Workers:    8
```

Progress snapshot on 2026-08-24:

```text
completed shards: 112
completed days: 780 of 957
successful archives: 112
errors: 0
```

This snapshot is time-sensitive. Recalculate progress rather than assuming it
is current. The run processes only shards overlapping the requested date range,
not necessarily all 246 backup shards. Use the manifest command below to
calculate the exact required number.

At the snapshot, one Python `crocus-export` orchestrator was active with
multiple `influxd inspect export-lp` children. Interleaved `stage` and `ok`
messages are normal with eight workers. Never launch two orchestrators against
the same output root.

## Restore shell variables on HPC

A new login loses shell variables but not the NFS environment or data:

```bash
BASE=/nfs/gce/projects/crocus-server-admins/data-rework
REPO="$BASE/crocus-rework"
ENV="$BASE/envs/crocus"
BACKUP=/nfs/gce/globalscratch/influx
OUTPUT="$BASE/crocus-rework-output/wxt-aqt-production-v5"
WORK="$BASE/work/wxt-aqt-production-v5"
LOG="$BASE/wxt-aqt-production-v5.log"
PIDFILE="$BASE/wxt-aqt-production-v5.pid"
SELECTION="$BASE/wxt-aqt-exact.selection.json"
INFLUXD="$BASE/tools/influxdb2-2.7.11/usr/bin/influxd"
```

No environment activation is required when commands use `$ENV/bin/python` or
`$ENV/bin/crocus-export` directly.

## Monitor the active run

The log and output live on NFS and are visible from any node:

```bash
tail -F "$LOG"
```

`Ctrl-C` stops only `tail`; it does not stop the export.

The process itself must be checked on its execution host:

```bash
ssh compute-386-07 \
  "pgrep -af '[c]rocus-export|[i]nfluxd inspect export-lp' || echo 'no export process running'"
```

Progress:

```bash
echo -n "completed shards: "
find "$OUTPUT/_shards" -type f -name '*.json' 2>/dev/null | wc -l

echo -n "completed days: "
find "$OUTPUT/_days" -type f -name '*.json' 2>/dev/null | wc -l

echo -n "successful archives: "
grep -c '^ok ' "$LOG"

echo -n "errors: "
grep -c '^error ' "$LOG"

grep '^error ' "$LOG" | tail -20
tail -30 "$LOG"
```

Calculate the exact number of overlapping shards without reading value data:

```bash
"$ENV/bin/python" - <<'PY'
from datetime import UTC, datetime
from pathlib import Path

from crocus_raw.backup import load_backup_bucket

backup = load_backup_bucket(
    Path("/nfs/gce/globalscratch/influx"),
    "b3a4e89ad74c5acc",
)
start = datetime(2023, 5, 5, tzinfo=UTC)
end = datetime(2025, 12, 17, tzinfo=UTC)
selected = [
    shard
    for shard in backup.shards
    if shard.end_time > start and shard.start_time < end
]
print("all backup shards:", len(backup.shards))
print("required overlapping shards:", len(selected))
PY
```

The log can remain quiet while a dense shard is being exported. A running
process, advancing completion markers, or a changing log modification time are
all valid activity signals.

## Why the final manifest may be absent during processing

`export_runs/` and `_catalog/` are generated after all worker futures finish.
Their absence during an active run is expected. Facts, series fragments, shard
markers, and day markers are published incrementally and can be inspected
before finalization.

## Production command and restart

The original combined selection was created by merging the two reviewed
selection files:

```bash
"$ENV/bin/python" - \
  "$REPO/config/vaisala-wxt536-exact.selection.json" \
  "$REPO/config/aqt-exact.selection.json" \
  "$SELECTION" <<'PY'
import json
import sys

selectors = []
for path in sys.argv[1:3]:
    with open(path) as stream:
        selectors.extend(json.load(stream)["selectors"])

deduplicated = {
    json.dumps(selector, sort_keys=True, separators=(",", ":")): selector
    for selector in selectors
}
document = {
    "selection_version": 1,
    "selectors": [deduplicated[key] for key in sorted(deduplicated)],
}
with open(sys.argv[3], "w") as stream:
    json.dump(document, stream, indent=2, sort_keys=True)
    stream.write("\n")
PY
```

The production command is:

```bash
nohup /usr/bin/time -v "$ENV/bin/crocus-export" \
  --backup-dir "$BACKUP" \
  --bucket-id b3a4e89ad74c5acc \
  --start-date 2023-05-05 \
  --end-date 2025-12-17 \
  --selection-file "$SELECTION" \
  --output "$OUTPUT" \
  --work-dir "$WORK" \
  --influxd "$INFLUXD" \
  --workers 8 \
  --rows-per-file 500000 \
  --max-buffer-rows 1000000 \
  --on-existing skip \
  > "$LOG" 2>&1 &

echo $! | tee "$PIDFILE"
```

Only restart if no orchestrator is running. Use the exact same selection,
source snapshot, output root, registry, converter, schema version, and InfluxDB
version. Compatible shard/day markers are skipped before restaging. A changed
selection requires a new output root.

## Inspect Parquet without Pandas

Pandas is not installed in the HPC environment and is not required. Use
PyArrow directly:

```bash
FILE=$(find "$OUTPUT/facts" -type f -name '*.parquet' | head -1)

"$ENV/bin/python" - "$FILE" <<'PY'
import sys
import pyarrow.parquet as pq

parquet = pq.ParquetFile(sys.argv[1])
batch = next(parquet.iter_batches(batch_size=5))

print("file:", sys.argv[1])
print("rows:", parquet.metadata.num_rows)
print(parquet.schema_arrow)
for row in batch.to_pylist():
    row["series_id"] = row["series_id"].hex()
    print(row)
PY
```

## Completion checks

The job is finished when the orchestrator and its `influxd` children are gone
and the latest export manifest exists. First inspect the log:

```bash
pgrep -af '[c]rocus-export|[i]nfluxd inspect export-lp' \
  || echo "no export process running"
tail -100 "$LOG"
```

Then inspect the final manifest:

```bash
LATEST=$(find "$OUTPUT/export_runs" -type f -name '*.json' | sort | tail -1)
echo "$LATEST"

"$ENV/bin/python" - "$LATEST" <<'PY'
import json
import sys

document = json.load(open(sys.argv[1]))
for key in (
    "status",
    "expected_days",
    "completed_days",
    "quarantined_rows",
    "requires_review",
    "catalog",
    "errors",
):
    print(f"{key}: {document.get(key)}")
PY
```

Production acceptance requires:

- `status == "complete"`
- `errors == []`
- `completed_days == expected_days == 957`
- zero unexplained quarantined rows
- zero incorrect-sensor rows
- zero unexplained metadata conflicts
- all requested measurements represented
- all partition file sizes and SHA-256 hashes match manifests
- fact, partition-manifest, day-manifest, catalog, and export row counts reconcile
- timestamps remain inside each half-open requested interval
- a compatible rerun completes without staging archives

If the manifest is incomplete, inspect errors, verify no process remains, and
rerun the production command unchanged. Do not delete compatible completed
partitions or markers.

## Expected final output

The result is one logical Hive dataset, not one giant Parquet file:

- sensor-partitioned WXT and AQT facts
- VSN/instrument/date pruning
- 23 requested measurements: 11 WXT and 12 AQT
- normalized full series metadata
- generated sensor, instrument, variable, instrument-variable, and series
  catalogs
- explicit metadata-conflict report
- quarantine data separated from accepted facts
- source, selection, registry, converter, schema, and InfluxDB provenance
- daily and shard restart markers

The final size is expected to be several hundred GB, dominated by dense WXT
data. Do not concatenate it. Query the Hive tree directly with Arrow, DuckDB,
Polars, or Spark. Optional compaction must stay within a single
sensor/VSN/instrument/day partition.

## Important lessons

1. Direct selective export is more useful than waiting for an exhaustive
   inventory when exact measurement names are known.
2. Exact measurement union per shard is faster than exporting one variable at
   a time because `export-lp` otherwise rescans the same TSM files repeatedly.
3. WXT and AQT should be combined in one shard pass and separated by the
   `sensor` Hive partition and fact column.
4. Python is sufficient after optimization. The dense-day pilot met the
   ten-minute and two-GB gates, so Rust is not currently justified.
5. The main Python speedups were cached series decoding, buffered line reads,
   direct byte parsing, direct Arrow column buffers, removal of fact tag maps,
   removal of global sorting, cached partition routing, and disabling cyclic GC
   only inside the hot loop.
6. `vsn`, not numeric `node`, is the authoritative CROCUS location identity.
7. Complete arbitrary tags belong in the normalized series table, not on every
   high-density fact row.
8. AQT is `vaisala-aqt530` throughout this snapshot; no `vaisala-aqt560` series
   were found.
9. Runtime tags change and must not define instruments or scientific variables.
10. One orchestrator with multiple workers is safe; multiple orchestrators
    writing the same output root are not.
11. Eight workers improve shard concurrency but multiply memory, extraction
    space, and shared-filesystem I/O. Do not increase concurrency without a new
    benchmark and code change.
12. `export_runs/` is a finalization artifact. Its absence while workers are
    active does not imply failure.

## Next steps after production completion

1. Run the completion checks above.
2. Save a compact validation summary containing counts, timestamps, checksums,
   sensor purity, quarantine, conflicts, runtime, and peak memory.
3. Review any quarantine or metadata conflicts before declaring the dataset
   production-ready.
4. Perform representative WXT and AQT Hive queries by sensor, VSN, instrument,
   date, measurement, and field.
5. Measure query performance and identify only genuinely small daily
   partitions for optional within-partition compaction.
6. Preserve the raw backup, selection JSON, dataset manifest, final export
   manifest, catalog, and validation summary together.
7. Add another instrument only after curating its exact measurements and sensor
   identity from the completed inventory or a representative shard index.

The approved Bronze/Silver/Gold level definitions, NetCDF codes, daily filename
pattern, and time-coverage rules are defined in
`docs/data_levels_and_netcdf_naming.md`.

## Suggested prompt for a new session

```text
Continue the CROCUS WXT+AQT production workflow using docs/HANDOVER.md as the
authoritative handover. First inspect the active/final HPC run without changing
or deleting any output. Re-establish the documented HPC paths, check whether
the orchestrator on compute-386-07 is still running, count completed shard/day
markers, inspect errors, and validate the final export manifest if it exists.
Use the existing immutable output root and do not launch a second orchestrator
against it. Proceed through the documented completion and reconciliation checks.
```
