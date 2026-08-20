from __future__ import annotations

import csv
import hashlib
import json
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import tarfile
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime
from itertools import groupby
from pathlib import Path, PurePosixPath
from typing import Iterable, Iterator

from crocus_raw import __version__
from crocus_raw.backup import BackupBucket, BackupShard, load_backup_bucket
from crocus_raw.instruments import InstrumentResolver
from crocus_raw.line_protocol import parse_series_key, unescape_identifier
from crocus_raw.model import InfluxPoint, ParsedValue
from crocus_raw.runtime import describe_error, sha256_file, write_text_atomic


INDEX_ROW = re.compile(
    r"^\s*\d+\t(?P<minimum>[^\t]+)\t(?P<maximum>[^\t]+)\t"
    r"\d+\t\d+\t(?P<series>.*)\t(?P<field>[^\t]+)\s*$"
)


@dataclass(frozen=True)
class IndexEntry:
    measurement: str
    field: str
    tags: dict[str, str]
    minimum_time_ns: int
    maximum_time_ns: int


@dataclass(frozen=True)
class InventoryConfig:
    output_dir: Path
    work_dir: Path
    bucket_id: str
    bucket_name: str
    influxd: Path
    influxd_version: str
    source_snapshot: str
    source_fingerprint: str
    resume: bool = False


def parse_dump_tsm_index(lines: Iterable[str]) -> Iterator[IndexEntry]:
    for line_number, line in enumerate(lines, start=1):
        match = INDEX_ROW.match(line)
        if not match:
            if re.match(r"^\s*\d+\t", line):
                raise ValueError(f"malformed dump-tsm index row at line {line_number}: {line.rstrip()!r}")
            continue
        measurement, tags = parse_series_key(match.group("series"), f"dump-tsm line {line_number}")
        yield IndexEntry(
            measurement=measurement,
            field=unescape_identifier(match.group("field").strip()),
            tags=tags,
            minimum_time_ns=parse_rfc3339_ns(match.group("minimum")),
            maximum_time_ns=parse_rfc3339_ns(match.group("maximum")),
        )


def parse_rfc3339_ns(value: str) -> int:
    match = re.fullmatch(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?Z", value)
    if not match:
        raise ValueError(f"invalid RFC3339 nanosecond timestamp: {value!r}")
    seconds = int(datetime.fromisoformat(f"{match.group(1)}+00:00").timestamp())
    fraction = (match.group(2) or "").ljust(9, "0")
    return seconds * 1_000_000_000 + int(fraction or "0")


def format_rfc3339_ns(value: int) -> str:
    seconds, nanoseconds = divmod(value, 1_000_000_000)
    base = datetime.fromtimestamp(seconds, tz=UTC).strftime("%Y-%m-%dT%H:%M:%S")
    return f"{base}.{nanoseconds:09d}Z"


class InventoryDatabase:
    def __init__(self, path: Path, config: InventoryConfig):
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists() and not config.resume:
            raise FileExistsError(f"inventory database already exists; use --resume: {path}")
        self.connection = sqlite3.connect(path)
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=FULL")
        self._create_schema()
        self._validate_metadata(config)

    def close(self) -> None:
        self.connection.close()

    def _create_schema(self) -> None:
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS shards (
                source_key TEXT PRIMARY KEY,
                shard_id TEXT NOT NULL,
                start_time TEXT,
                end_time TEXT,
                compressed_size INTEGER,
                status TEXT NOT NULL,
                error TEXT,
                tsm_files INTEGER NOT NULL DEFAULT 0,
                index_entries INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS series (
                series_hash TEXT PRIMARY KEY,
                measurement TEXT NOT NULL,
                field TEXT NOT NULL,
                tags_json TEXT NOT NULL,
                minimum_time_ns INTEGER NOT NULL,
                maximum_time_ns INTEGER NOT NULL,
                index_entries INTEGER NOT NULL,
                first_source TEXT NOT NULL,
                last_source TEXT NOT NULL
            );
            """
        )

    def _validate_metadata(self, config: InventoryConfig) -> None:
        expected = {
            "catalog_version": "1",
            "converter_version": __version__,
            "bucket_id": config.bucket_id,
            "bucket_name": config.bucket_name,
            "influxd_version": config.influxd_version,
            "source_snapshot": config.source_snapshot,
            "source_fingerprint": config.source_fingerprint,
        }
        current = dict(self.connection.execute("SELECT key, value FROM metadata"))
        if current:
            mismatches = {
                key: (current.get(key), value)
                for key, value in expected.items()
                if current.get(key) != value
            }
            if mismatches:
                raise ValueError(f"inventory database metadata mismatch: {mismatches}")
        else:
            self.connection.executemany("INSERT INTO metadata(key, value) VALUES (?, ?)", expected.items())
            self.connection.commit()

    def is_complete(self, source_key: str) -> bool:
        row = self.connection.execute(
            "SELECT status FROM shards WHERE source_key = ?", (source_key,)
        ).fetchone()
        return bool(row and row[0] == "complete")

    def begin_source(
        self,
        source_key: str,
        shard_id: str,
        start_time: str | None,
        end_time: str | None,
        compressed_size: int | None,
    ) -> None:
        self.connection.execute("BEGIN IMMEDIATE")
        self.connection.execute(
            """
            INSERT INTO shards(source_key, shard_id, start_time, end_time, compressed_size, status, error, updated_at)
            VALUES (?, ?, ?, ?, ?, 'running', NULL, ?)
            ON CONFLICT(source_key) DO UPDATE SET
                shard_id=excluded.shard_id,
                start_time=excluded.start_time,
                end_time=excluded.end_time,
                compressed_size=excluded.compressed_size,
                status='running', error=NULL, tsm_files=0, index_entries=0,
                updated_at=excluded.updated_at
            """,
            (source_key, shard_id, start_time, end_time, compressed_size, _now()),
        )

    def add_entry(self, source_key: str, entry: IndexEntry) -> None:
        tags_json = json.dumps(entry.tags, separators=(",", ":"), sort_keys=True)
        identity = json.dumps(
            [entry.measurement, entry.field, entry.tags], separators=(",", ":"), sort_keys=True
        )
        series_hash = hashlib.sha256(identity.encode()).hexdigest()
        self.connection.execute(
            """
            INSERT INTO series(
                series_hash, measurement, field, tags_json, minimum_time_ns,
                maximum_time_ns, index_entries, first_source, last_source
            ) VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?)
            ON CONFLICT(series_hash) DO UPDATE SET
                minimum_time_ns=MIN(minimum_time_ns, excluded.minimum_time_ns),
                maximum_time_ns=MAX(maximum_time_ns, excluded.maximum_time_ns),
                index_entries=index_entries + 1,
                last_source=excluded.last_source
            """,
            (
                series_hash,
                entry.measurement,
                entry.field,
                tags_json,
                entry.minimum_time_ns,
                entry.maximum_time_ns,
                source_key,
                source_key,
            ),
        )

    def complete_source(self, source_key: str, tsm_files: int, index_entries: int) -> None:
        self.connection.execute(
            """
            UPDATE shards SET status='complete', error=NULL, tsm_files=?, index_entries=?, updated_at=?
            WHERE source_key=?
            """,
            (tsm_files, index_entries, _now(), source_key),
        )
        self.connection.commit()

    def fail_source(
        self,
        source_key: str,
        shard_id: str,
        start_time: str | None,
        end_time: str | None,
        compressed_size: int | None,
        error: Exception,
    ) -> None:
        self.connection.rollback()
        self.connection.execute(
            """
            INSERT INTO shards(source_key, shard_id, start_time, end_time, compressed_size, status, error, updated_at)
            VALUES (?, ?, ?, ?, ?, 'error', ?, ?)
            ON CONFLICT(source_key) DO UPDATE SET status='error', error=excluded.error, updated_at=excluded.updated_at
            """,
            (
                source_key,
                shard_id,
                start_time,
                end_time,
                compressed_size,
                describe_error(error),
                _now(),
            ),
        )
        self.connection.commit()


def inventory_backup(
    backup: BackupBucket,
    config: InventoryConfig,
    resolver: InstrumentResolver,
) -> dict[str, object]:
    database_path = config.output_dir / "inventory.sqlite"
    database = InventoryDatabase(database_path, config)
    config.work_dir.mkdir(parents=True, exist_ok=True)
    try:
        for shard in backup.shards:
            source_key = shard.archive.name
            if config.resume and database.is_complete(source_key):
                print(f"skip {source_key}", file=sys.stderr, flush=True)
                continue
            print(f"scan {source_key}", file=sys.stderr, flush=True)
            try:
                _scan_backup_shard(database, shard, config)
                print(f"ok {source_key}", file=sys.stderr, flush=True)
            except Exception as error:
                print(f"error {source_key}: {describe_error(error)}", file=sys.stderr, flush=True)
                database.fail_source(
                    source_key,
                    str(shard.shard_id),
                    shard.start_time.isoformat(),
                    shard.end_time.isoformat(),
                    shard.compressed_size,
                    error,
                )
        return write_catalog_outputs(database.connection, config, resolver, len(backup.shards))
    finally:
        database.close()


def inventory_engine(
    engine_dir: Path,
    config: InventoryConfig,
    resolver: InstrumentResolver,
) -> dict[str, object]:
    database = InventoryDatabase(config.output_dir / "inventory.sqlite", config)
    shard_directories = _engine_shard_directories(engine_dir, config.bucket_id)
    try:
        for shard_directory in shard_directories:
            source_key = str(shard_directory.relative_to(engine_dir))
            if config.resume and database.is_complete(source_key):
                print(f"skip {source_key}", file=sys.stderr, flush=True)
                continue
            print(f"scan {source_key}", file=sys.stderr, flush=True)
            try:
                database.begin_source(source_key, shard_directory.name, None, None, None)
                count = 0
                files = sorted(shard_directory.glob("*.tsm"))
                for path in files:
                    for entry in inspect_tsm(config.influxd, path):
                        database.add_entry(source_key, entry)
                        count += 1
                database.complete_source(source_key, len(files), count)
                print(f"ok {source_key}", file=sys.stderr, flush=True)
            except Exception as error:
                print(f"error {source_key}: {describe_error(error)}", file=sys.stderr, flush=True)
                database.fail_source(source_key, shard_directory.name, None, None, None, error)
        return write_catalog_outputs(database.connection, config, resolver, len(shard_directories))
    finally:
        database.close()


def inspect_tsm(influxd: Path, path: Path) -> Iterator[IndexEntry]:
    with tempfile.TemporaryFile(mode="w+t") as error_stream:
        process = subprocess.Popen(
            [str(influxd), "inspect", "dump-tsm", "--index", "--file-path", str(path)],
            stdout=subprocess.PIPE,
            stderr=error_stream,
            text=True,
        )
        assert process.stdout is not None
        try:
            yield from parse_dump_tsm_index(process.stdout)
        except Exception:
            process.terminate()
            process.wait()
            raise
        finally:
            process.stdout.close()
        return_code = process.wait()
        if return_code:
            error_stream.seek(0)
            raise subprocess.CalledProcessError(
                return_code,
                process.args,
                stderr=error_stream.read(),
            )


def write_catalog_outputs(
    connection: sqlite3.Connection,
    config: InventoryConfig,
    resolver: InstrumentResolver,
    expected_sources: int,
) -> dict[str, object]:
    _rebuild_resolved_series(connection, resolver)
    paths = {
        "database": config.output_dir / "inventory.sqlite",
        "instruments": config.output_dir / "instruments.csv",
        "variables": config.output_dir / "instrument_variables.csv",
        "measurements": config.output_dir / "measurements.csv",
        "errors": config.output_dir / "inventory_errors.csv",
        "wxt_measurements": config.output_dir / "wxt_measurements.txt",
        "wxt_instruments": config.output_dir / "wxt_instruments.txt",
    }
    _write_instruments(connection, paths["instruments"])
    _write_variables(connection, paths["variables"])
    _write_measurements(connection, paths["measurements"])
    _write_errors(connection, paths["errors"])
    _write_wxt_lists(connection, paths["wxt_measurements"], paths["wxt_instruments"])
    connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()

    shard_counts = dict(connection.execute("SELECT status, COUNT(*) FROM shards GROUP BY status"))
    output_counts = {
        "instruments": connection.execute("SELECT COUNT(DISTINCT instrument_id) FROM resolved_series").fetchone()[0],
        "variables": connection.execute(
            "SELECT COUNT(*) FROM (SELECT 1 FROM resolved_series GROUP BY instrument_id, measurement, field)"
        ).fetchone()[0],
        "measurements": connection.execute(
            "SELECT COUNT(*) FROM (SELECT 1 FROM resolved_series GROUP BY measurement, field)"
        ).fetchone()[0],
        "raw_series": connection.execute("SELECT COUNT(*) FROM series").fetchone()[0],
    }
    status = "complete" if shard_counts.get("complete", 0) == expected_sources else "incomplete"
    manifest = {
        "status": status,
        "catalog_version": 1,
        "converter_version": __version__,
        "bucket_id": config.bucket_id,
        "bucket_name": config.bucket_name,
        "source_snapshot": config.source_snapshot,
        "source_fingerprint": config.source_fingerprint,
        "influxd_version": config.influxd_version,
        "expected_sources": expected_sources,
        "source_status_counts": shard_counts,
        "counts": output_counts,
        "registry_fingerprint": resolver.fingerprint,
        "outputs": {
            name: {"path": path.name, "sha256": sha256_file(path)} for name, path in paths.items()
        },
        "finished_at": _now(),
    }
    write_text_atomic(
        config.output_dir / "inventory_manifest.json",
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
    )
    return manifest


def _scan_backup_shard(
    database: InventoryDatabase,
    shard: BackupShard,
    config: InventoryConfig,
) -> None:
    source_key = shard.archive.name
    database.begin_source(
        source_key,
        str(shard.shard_id),
        shard.start_time.isoformat(),
        shard.end_time.isoformat(),
        shard.compressed_size,
    )
    tsm_files = 0
    index_entries = 0
    with tarfile.open(shard.archive, mode="r|gz") as archive:
        for member in archive:
            member_path = PurePosixPath(member.name)
            if member_path.suffix != ".tsm":
                continue
            if not member.isfile() or ".." in member_path.parts or member_path.is_absolute():
                raise ValueError(f"unsafe TSM archive member: {member.name!r}")
            if not member_path.parts or member_path.parts[0] != config.bucket_id:
                continue
            extracted = archive.extractfile(member)
            if extracted is None:
                raise ValueError(f"could not read TSM member: {member.name!r}")
            with tempfile.NamedTemporaryFile(
                dir=config.work_dir,
                prefix=f"{shard.shard_id}-",
                suffix=".tsm",
                delete=False,
            ) as temporary_file:
                temporary_path = Path(temporary_file.name)
            try:
                with temporary_path.open("wb") as output:
                    shutil.copyfileobj(extracted, output, length=1024 * 1024)
                for entry in inspect_tsm(config.influxd, temporary_path):
                    database.add_entry(source_key, entry)
                    index_entries += 1
                tsm_files += 1
            finally:
                temporary_path.unlink(missing_ok=True)
    database.complete_source(source_key, tsm_files, index_entries)


def _engine_shard_directories(engine_dir: Path, bucket_id: str) -> list[Path]:
    bucket_path = engine_dir / "data" / bucket_id
    if not bucket_path.is_dir():
        raise FileNotFoundError(f"bucket engine directory not found: {bucket_path}")
    directories = sorted({path.parent for path in bucket_path.rglob("*.tsm")})
    return directories


def _rebuild_resolved_series(connection: sqlite3.Connection, resolver: InstrumentResolver) -> None:
    connection.executescript(
        """
        DROP TABLE IF EXISTS resolved_series;
        CREATE TABLE resolved_series (
            series_hash TEXT PRIMARY KEY,
            instrument_id TEXT NOT NULL,
            identity_source TEXT NOT NULL,
            confidence TEXT NOT NULL,
            review_required INTEGER NOT NULL,
            identity_vsn TEXT NOT NULL,
            identity_kind TEXT NOT NULL,
            identity_zone TEXT NOT NULL,
            measurement TEXT NOT NULL,
            field TEXT NOT NULL,
            tags_json TEXT NOT NULL,
            sensor TEXT,
            units TEXT,
            missing TEXT,
            minimum_time_ns INTEGER NOT NULL,
            maximum_time_ns INTEGER NOT NULL,
            index_entries INTEGER NOT NULL
        );
        """
    )
    select_cursor = connection.execute(
        "SELECT series_hash, measurement, field, tags_json, minimum_time_ns, maximum_time_ns, index_entries FROM series"
    )
    insert = """
        INSERT INTO resolved_series VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    batch: list[tuple[object, ...]] = []
    for series_hash, measurement, field, tags_json, minimum, maximum, entries in select_cursor:
        tags = json.loads(tags_json)
        tags.pop("node", None)
        tags_json = json.dumps(tags, separators=(",", ":"), sort_keys=True)
        point = InfluxPoint(0, measurement, field, ParsedValue("float64", 0.0), tags)
        identity = resolver.resolve_identity(point)
        batch.append(
            (
                series_hash,
                identity.instrument_id,
                identity.identity_source,
                identity.confidence,
                int(identity.review_required),
                identity.vsn,
                identity.kind,
                identity.zone,
                measurement,
                field,
                tags_json,
                tags.get("sensor"),
                tags.get("units"),
                tags.get("missing"),
                minimum,
                maximum,
                entries,
            )
        )
        if len(batch) >= 10_000:
            connection.executemany(insert, batch)
            batch.clear()
    if batch:
        connection.executemany(insert, batch)
    connection.executescript(
        """
        CREATE INDEX resolved_instrument_idx ON resolved_series(instrument_id, measurement, field);
        CREATE INDEX resolved_measurement_idx ON resolved_series(measurement, field);
        """
    )
    connection.commit()


def _write_instruments(connection: sqlite3.Connection, path: Path) -> None:
    query = """
        SELECT instrument_id, identity_source, confidence, review_required,
               identity_vsn, identity_kind, identity_zone,
               MIN(minimum_time_ns), MAX(maximum_time_ns), COUNT(*),
               COUNT(DISTINCT measurement || char(0) || field)
        FROM resolved_series
        GROUP BY instrument_id, identity_source, confidence, review_required,
                 identity_vsn, identity_kind, identity_zone
        ORDER BY instrument_id
    """
    _write_csv(
        path,
        [
            "instrument_id", "identity_source", "confidence", "review_required", "vsn",
            "kind", "zone", "first_time", "last_time", "raw_series_count", "variable_count",
        ],
        (
            (*row[:7], format_rfc3339_ns(row[7]), format_rfc3339_ns(row[8]), *row[9:])
            for row in connection.execute(query)
        ),
    )


def _write_variables(connection: sqlite3.Connection, path: Path) -> None:
    rows = connection.execute(
        """
        SELECT instrument_id, measurement, field, units, missing,
               minimum_time_ns, maximum_time_ns, index_entries
        FROM resolved_series
        ORDER BY instrument_id, measurement, field, units, missing
        """
    )

    def aggregated() -> Iterator[tuple[object, ...]]:
        for key, group in groupby(rows, key=lambda row: row[:3]):
            units: set[str] = set()
            missing_values: set[str] = set()
            minimum: int | None = None
            maximum: int | None = None
            raw_series_count = 0
            index_entries = 0
            for row in group:
                if row[3] is not None:
                    units.add(row[3])
                if row[4] is not None:
                    missing_values.add(row[4])
                minimum = row[5] if minimum is None else min(minimum, row[5])
                maximum = row[6] if maximum is None else max(maximum, row[6])
                index_entries += row[7]
                raw_series_count += 1
            instrument_id, measurement, field = key
            yield (
                f"{instrument_id}::{measurement}::{field}",
                instrument_id,
                measurement,
                field,
                json.dumps(sorted(units)),
                json.dumps(sorted(missing_values)),
                format_rfc3339_ns(minimum or 0),
                format_rfc3339_ns(maximum or 0),
                raw_series_count,
                index_entries,
            )

    _write_csv(
        path,
        [
            "variable_id", "instrument_id", "measurement", "field", "units", "missing_values",
            "first_time", "last_time", "raw_series_count", "index_entries",
        ],
        aggregated(),
    )


def _write_measurements(connection: sqlite3.Connection, path: Path) -> None:
    query = """
        SELECT measurement, field, COUNT(DISTINCT instrument_id), COUNT(*),
               MIN(minimum_time_ns), MAX(maximum_time_ns)
        FROM resolved_series
        GROUP BY measurement, field
        ORDER BY measurement, field
    """
    _write_csv(
        path,
        ["measurement", "field", "instrument_count", "raw_series_count", "first_time", "last_time"],
        (
            (*row[:4], format_rfc3339_ns(row[4]), format_rfc3339_ns(row[5]))
            for row in connection.execute(query)
        ),
    )


def _write_errors(connection: sqlite3.Connection, path: Path) -> None:
    _write_csv(
        path,
        ["source", "shard_id", "start_time", "end_time", "error", "updated_at"],
        connection.execute(
            """
            SELECT source_key, shard_id, start_time, end_time, error, updated_at
            FROM shards WHERE status='error' ORDER BY source_key
            """
        ),
    )


def _write_wxt_lists(connection: sqlite3.Connection, measurements_path: Path, instruments_path: Path) -> None:
    condition = "measurement LIKE 'wxt.%' OR lower(COALESCE(sensor, '')) = 'vaisala-wxt536'"
    measurements = [
        row[0]
        for row in connection.execute(
            f"SELECT DISTINCT measurement FROM resolved_series WHERE {condition} ORDER BY measurement"
        )
    ]
    instruments = [
        row[0]
        for row in connection.execute(
            f"SELECT DISTINCT instrument_id FROM resolved_series WHERE {condition} ORDER BY instrument_id"
        )
    ]
    write_text_atomic(measurements_path, "".join(f"{value}\n" for value in measurements))
    write_text_atomic(instruments_path, "".join(f"{value}\n" for value in instruments))


def _write_csv(path: Path, fieldnames: list[str], rows: Iterable[Iterable[object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_name(f".{path.name}.tmp")
    with temporary_path.open("w", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(fieldnames)
        writer.writerows(rows)
    os.replace(temporary_path, path)


def _now() -> str:
    return datetime.now(UTC).isoformat()
