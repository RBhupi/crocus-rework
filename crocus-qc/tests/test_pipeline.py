"""Work-unit orchestration: atomic publication, idempotency, determinism.

The seam is ``run_work_unit()``: synthetic raw Parquet plus a config in, files on disk
plus a provenance record out. Tests assert on what lands in the output directory, never
on how the SQL is assembled.
"""

from __future__ import annotations

from datetime import date as Date
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pytest

from crocus_qc.config import TEN_SECONDS, PipelineConfig
from crocus_qc.pipeline import (
    PROFILE_NAME,
    PRODUCT_NAME,
    discover_work_units,
    run_work_unit,
    work_unit_dir,
)
from crocus_qc.provenance import SUCCESS_NAME
from crocus_qc.timing import Stopwatch

from conftest import DAY, SENSOR, VSN, Obs, subset, write_raw


@pytest.fixture
def dataset(tmp_path: Path) -> Path:
    """A small but real work unit: a few minutes of temperature and humidity."""
    observations = [
        Obs(offset, "aqt.env.temp", 20.0 + offset / 100.0) for offset in range(0, 120, 3)
    ] + [Obs(offset, "aqt.env.humidity", 55.0) for offset in range(0, 120, 3)]
    return write_raw(tmp_path / "raw", observations)


@pytest.fixture
def config(tmp_path: Path) -> PipelineConfig:
    return PipelineConfig(
        output_root=tmp_path / "out",
        threads=2,
        memory_limit="1GB",
        temp_dir=str(tmp_path / "scratch"),
        config_hash="testhash00000000",
    )


@pytest.fixture
def profile(aqt_profile):
    """Two variables is enough to exercise orchestration; reduction has its own tests."""
    from dataclasses import replace

    return replace(
        aqt_profile, variables=subset(aqt_profile, ["air_temperature", "relative_humidity"])
    )


def out_dir(config: PipelineConfig) -> Path:
    return work_unit_dir(config, SENSOR, VSN, DAY.date())


def run(config, profile, dataset, **kwargs):
    return run_work_unit(
        sensor=SENSOR,
        vsn=VSN,
        day=DAY.date(),
        dataset_root=dataset,
        config=config,
        profile=profile,
        **kwargs,
    )


def read_product(path: Path, order: bool = False) -> list[tuple]:
    suffix = " ORDER BY time" if order else ""
    with duckdb.connect() as conn:
        conn.execute("SET TimeZone = 'UTC';")
        return conn.execute(f"SELECT * FROM read_parquet('{path}'){suffix}").fetchall()


# ------------------------------------------------------------------------------------
# Publication
# ------------------------------------------------------------------------------------


def test_run_publishes_product_and_success_marker(config, profile, dataset):
    run(config, profile, dataset)

    directory = out_dir(config)
    assert (directory / PRODUCT_NAME).is_file()
    assert (directory / SUCCESS_NAME).is_file()


def test_run_leaves_no_temporary_files(config, profile, dataset):
    run(config, profile, dataset)

    leftovers = sorted(p.name for p in out_dir(config).iterdir() if p.name.endswith(".tmp"))
    assert leftovers == []


def test_product_holds_one_full_utc_day(config, profile, dataset):
    record = run(config, profile, dataset)

    assert record["output_row_count"] == TEN_SECONDS.rows_per_day == 8640
    assert len(read_product(out_dir(config) / PRODUCT_NAME)) == 8640


def test_product_rows_are_written_in_time_order(config, profile, dataset):
    """Downstream reads row *i* as bucket *i*, so file order is part of the contract."""
    run(config, profile, dataset)

    rows = read_product(out_dir(config) / PRODUCT_NAME)
    times = [row[0] for row in rows]
    assert times == sorted(times)


def test_provenance_records_what_produced_the_file(config, profile, dataset):
    record = run(config, profile, dataset)

    assert record["status"] == "success"
    assert record["sensor"] == SENSOR
    assert record["vsn"] == VSN
    assert record["date"] == f"{DAY:%Y-%m-%d}"
    assert record["aggregation_period"] == "10 seconds"
    assert record["config_hash"] == "testhash00000000"
    assert record["variables"] == ["air_temperature", "relative_humidity"]
    assert record["duckdb_version"]


# ------------------------------------------------------------------------------------
# Timing
# ------------------------------------------------------------------------------------


def test_provenance_breaks_the_run_down_by_phase(config, profile, dataset):
    """Diagnosing a slow day on the cluster must not need anything but its output dir."""
    record = run(config, profile, dataset)

    timings = record["timings_seconds"]
    assert set(timings) >= {"build_sql", "open_session", "execute_reduction", "publish"}
    assert all(seconds >= 0.0 for seconds in timings.values())


def test_a_caller_supplied_stopwatch_collects_the_phases(config, profile, dataset):
    """The CLI times config loading before the run, into the same breakdown."""
    watch = Stopwatch()
    with watch.phase("load_config"):
        pass

    run(config, profile, dataset, stopwatch=watch)

    names = [name for name, _ in watch.phases]
    assert names[0] == "load_config"
    assert "execute_reduction" in names
    assert "write_provenance" in names


def test_phases_are_recorded_even_when_the_run_fails(config, profile, tmp_path):
    watch = Stopwatch()

    with pytest.raises(duckdb.Error):
        run(config, profile, tmp_path / "does-not-exist", stopwatch=watch)

    assert "execute_reduction" in watch.as_dict()


def test_sql_profile_writes_duckdb_operator_timings(config, profile, dataset):
    """DuckDB's own profile answers 'which operator', where phases answer 'which phase'."""
    record = run(config, profile, dataset, sql_profile=True)

    written = Path(record["duckdb_profile_path"])
    assert written.is_file()
    assert "READ_PARQUET" in written.read_text()


def test_no_duckdb_profile_is_written_unless_asked(config, profile, dataset):
    record = run(config, profile, dataset)

    assert "duckdb_profile_path" not in record
    assert not (out_dir(config) / PROFILE_NAME).exists()


# ------------------------------------------------------------------------------------
# Discovery
# ------------------------------------------------------------------------------------


def test_discover_lists_the_work_units_present(dataset):
    assert discover_work_units(dataset) == [(SENSOR, VSN, DAY.date())]


def test_discover_collapses_instruments_into_one_work_unit(tmp_path):
    """A work unit spans every instrument directory, so it must not be listed twice."""
    root = tmp_path / "raw"
    for instrument in ("aqt530-001", "aqt530-002"):
        write_raw(root, [Obs(0, "aqt.env.temp", 20.0)], instrument=instrument)

    assert discover_work_units(root) == [(SENSOR, VSN, DAY.date())]


def test_discover_restricts_to_the_requested_days(tmp_path):
    root = tmp_path / "raw"
    days = [datetime(2025, 12, d, tzinfo=timezone.utc) for d in (14, 15, 16)]
    for day in days:
        write_raw(root, [Obs(0, "aqt.env.temp", 20.0)], day=day)

    found = discover_work_units(
        root, start=Date(2025, 12, 15), end=Date(2025, 12, 16)
    )

    assert [day for _, _, day in found] == [Date(2025, 12, 15), Date(2025, 12, 16)]


def test_discover_ignores_a_sensor_it_was_not_asked_for(tmp_path):
    root = tmp_path / "raw"
    write_raw(root, [Obs(0, "aqt.env.temp", 20.0)])
    write_raw(root, [Obs(0, "wxt.env.temp", 20.0)], sensor="vaisala-wxt536")

    assert [sensor for sensor, _, _ in discover_work_units(root, sensor=SENSOR)] == [SENSOR]


# ------------------------------------------------------------------------------------
# Failure leaves nothing publishable
# ------------------------------------------------------------------------------------


def test_failed_run_writes_no_success_marker(config, profile, tmp_path):
    """A missing partition must not look like a completed work unit on rerun."""
    with pytest.raises(duckdb.Error):
        run(config, profile, tmp_path / "does-not-exist")

    assert not (out_dir(config) / SUCCESS_NAME).exists()
    assert not (out_dir(config) / PRODUCT_NAME).exists()


def test_output_inside_the_raw_dataset_is_refused(profile, dataset, tmp_path):
    """The raw dataset is read-only input for every downstream product."""
    unsafe = PipelineConfig(
        output_root=dataset / "sensor=vaisala-aqt530" / "products",
        threads=2,
        memory_limit="1GB",
        temp_dir=str(tmp_path / "scratch"),
        config_hash="testhash00000000",
    )
    with pytest.raises(ValueError, match="never be written to"):
        run(unsafe, profile, dataset)


# ------------------------------------------------------------------------------------
# Idempotency and determinism
# ------------------------------------------------------------------------------------


def test_rerun_without_force_does_not_recompute(config, profile, dataset):
    first = run(config, profile, dataset)

    product = out_dir(config) / PRODUCT_NAME
    product.write_bytes(b"deliberately corrupted")
    second = run(config, profile, dataset)

    assert product.read_bytes() == b"deliberately corrupted"
    assert second == first


def test_force_recomputes(config, profile, dataset):
    run(config, profile, dataset)

    product = out_dir(config) / PRODUCT_NAME
    product.write_bytes(b"deliberately corrupted")
    run(config, profile, dataset, force=True)

    assert len(read_product(product)) == 8640


def test_recomputation_reproduces_the_same_product(config, profile, dataset):
    run(config, profile, dataset)
    product = out_dir(config) / PRODUCT_NAME
    before = read_product(product, order=True)

    run(config, profile, dataset, force=True)

    assert read_product(product, order=True) == before
