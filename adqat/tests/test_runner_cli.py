from __future__ import annotations

import json
from pathlib import Path

import duckdb
import netCDF4
import numpy as np
import pyarrow.parquet as pq
import pytest
import yaml
from conftest import BASE_NS, aggregate_quality_document, write_configuration, write_facts

from adqat.cli import main
from adqat.runner import compile_run, resume_run, run_new
from adqat.source import SourceError
from adqat.store import StoreError


def test_end_to_end_run_resume_and_recompile(tmp_path: Path) -> None:
    run_path = write_configuration(
        tmp_path,
        start="2025-01-01T00:00:00Z",
        end="2025-01-03T00:00:00Z",
        period="1d",
    )
    source_path = write_facts(tmp_path, [{"time": BASE_NS + 1, "value": 100.0}])
    before = (source_path.stat().st_size, source_path.stat().st_mtime_ns)
    summary = run_new(run_path, "demo_wxt_work_unit", "test-run")
    assert summary.processed_periods == 2
    assert summary.empty_periods == 1
    assert summary.findings == 2
    assert summary.flagged_observations == 1
    assert (source_path.stat().st_size, source_path.stat().st_mtime_ns) == before

    run_dir = tmp_path / "results" / "runs" / "test-run"
    periods = sorted((run_dir / "work_units" / "demo_wxt_work_unit").iterdir())
    assert len(periods) == 2
    assert all((period / "success.json").is_file() for period in periods)
    assert pq.read_table(periods[1] / "findings.parquet").num_rows == 0
    assert pq.read_table(periods[1] / "qc_flags.parquet").num_rows == 0
    first_flags = pq.read_table(periods[0] / "qc_flags.parquet")
    assert first_flags["qc_bits"][0].as_py() == (1 << 2) | (1 << 3)
    assert first_flags["sensor"][0].as_py() == "vaisala-wxt536"
    assert first_flags["vsn"][0].as_py() == "W08E"
    assert first_flags["instrument_id"][0].as_py() == "W08E--demo"
    assert first_flags["run_id"][0].as_py() == "test-run"
    assert first_flags["work_unit_id"][0].as_py() == "demo_wxt_work_unit"
    success = json.loads((periods[0] / "success.json").read_text())
    assert success["source_file_count"] == 1
    assert success["flagged_observations"] == 1
    assert first_flags["config_hash"][0].as_py() == success["config_hash"]
    assert len(success["input_fingerprint"]) == 64
    assert (run_dir / "run.json").is_file()
    assert (run_dir / "quality_rules.yaml").is_file()
    assert (run_dir / "processing_run.yaml").is_file()

    source_path.touch()
    resumed = resume_run(run_dir)
    assert resumed.processed_periods == 0
    assert resumed.skipped_periods == 2

    flags = periods[0] / "qc_flags.parquet"
    before_compile = pq.read_table(flags)
    flags.unlink()
    assert compile_run(run_dir, periods[0].name) == 1
    assert flags.is_file()
    assert pq.read_table(flags).equals(before_compile)

    relation = (
        duckdb.connect()
        .execute(
            """
        SELECT vsn, qc_bits, count(*) AS observations
        FROM read_parquet(?, union_by_name = true)
        WHERE vsn = ? AND qc_bits <> 0
        GROUP BY vsn, qc_bits
        """,
            [str(run_dir / "work_units" / "*" / "*" / "qc_flags.parquet"), "W08E"],
        )
        .fetchall()
    )
    assert relation == [("W08E", (1 << 2) | (1 << 3), 1)]


def test_corrupt_success_marker_is_recomputed(tmp_path: Path) -> None:
    run_path = write_configuration(tmp_path)
    write_facts(tmp_path, [{"time": BASE_NS + 1, "value": 10.0}])
    summary = run_new(run_path, "demo_wxt_work_unit", "resume-run")
    period = next((summary.run_dir / "work_units" / "demo_wxt_work_unit").iterdir())
    (period / "success.json").write_text("not-json", encoding="utf-8")
    resumed = resume_run(summary.run_dir)
    assert resumed.processed_periods == 1
    assert json.loads((period / "success.json").read_text())["status"] == "success"


def test_cli_exit_codes_and_output(tmp_path: Path, capsys: object) -> None:
    run_path = write_configuration(tmp_path)
    write_facts(tmp_path, [{"time": BASE_NS + 1, "value": 100.0}])
    assert main(["validate", str(run_path)]) == 0
    assert (
        main(
            [
                "run",
                str(run_path),
                "--work-unit",
                "demo_wxt_work_unit",
                "--run-id",
                "cli-run",
            ]
        )
        == 0
    )
    output = capsys.readouterr()
    assert "findings=2" in output.out
    assert "flagged_observations=1" in output.out
    assert main(["run", str(run_path), "--work-unit", "missing"]) == 1


def test_two_profiles_use_same_runner(tmp_path: Path) -> None:
    run_path = write_configuration(tmp_path)
    write_facts(
        tmp_path,
        [
            {
                "time": BASE_NS + 1,
                "sensor": "vaisala-aqt530",
                "instrument_id": "W08E--aqt-demo",
                "measurement": "aqt.gas.co",
                "value": 150.0,
            }
        ],
    )
    summary = run_new(run_path, "demo_aqt_work_unit", "aqt-run")
    assert summary.findings == 1


def test_source_is_validated_before_output_creation(tmp_path: Path) -> None:
    run_path = write_configuration(tmp_path)
    with pytest.raises(SourceError, match="matched no Parquet"):
        run_new(run_path, "demo_wxt_work_unit", "invalid-source")
    assert not (tmp_path / "results").exists()


def test_rejects_unsafe_and_existing_run_ids(tmp_path: Path) -> None:
    run_path = write_configuration(tmp_path)
    write_facts(tmp_path, [{"time": BASE_NS + 1, "value": 10.0}])
    with pytest.raises(Exception, match="path-safe"):
        run_new(run_path, "demo_wxt_work_unit", "../unsafe")
    run_new(run_path, "demo_wxt_work_unit", "same-run")
    with pytest.raises(StoreError, match="already exists"):
        run_new(run_path, "demo_wxt_work_unit", "same-run")


def test_failed_staging_is_not_visible_and_resume_recovers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_path = write_configuration(tmp_path)
    write_facts(tmp_path, [{"time": BASE_NS + 1, "value": 10.0}])

    def fail_compile(*args: object, **kwargs: object) -> None:
        raise RuntimeError("injected compile failure")

    monkeypatch.setattr("adqat.store.compile_findings", fail_compile)
    with pytest.raises(RuntimeError, match="injected"):
        run_new(run_path, "demo_wxt_work_unit", "atomic-run")
    run_dir = tmp_path / "results" / "runs" / "atomic-run"
    work_root = run_dir / "work_units" / "demo_wxt_work_unit"
    assert list(work_root.iterdir()) == []
    assert any((run_dir / ".staging").iterdir())

    monkeypatch.undo()
    resumed = resume_run(run_dir)
    assert resumed.processed_periods == 1
    assert len(list(work_root.iterdir())) == 1
    assert list((run_dir / ".staging").iterdir()) == []


def test_writes_and_verifies_native_a1_netcdf(tmp_path: Path) -> None:
    run_path = write_configuration(tmp_path)
    run = yaml.safe_load(run_path.read_text(encoding="utf-8"))
    run["output"]["netcdf"] = {"enabled": True, "site": "neiu", "instrument": "wxt536"}
    run_path.write_text(yaml.safe_dump(run, sort_keys=False), encoding="utf-8")
    write_facts(
        tmp_path,
        [
            {"time": BASE_NS + 123_456_789, "value": 100.0},
            {"time": BASE_NS + 1_987_654_321, "value": 100.0},
        ],
    )

    summary = run_new(run_path, "demo_wxt_work_unit", "netcdf-run")
    period = next((summary.run_dir / "work_units" / "demo_wxt_work_unit").iterdir())
    filename = "neiu.wxt536.W08E.native.a1.20250101T000000Z-20250102T000000Z.nc"
    netcdf_path = period / filename
    assert netcdf_path.is_file()
    with netCDF4.Dataset(netcdf_path) as dataset:
        assert len(dataset.dimensions["observation"]) == 2
        assert dataset.getncattr("crocus_data_level") == "a1"
        assert dataset.getncattr("site_id") == "neiu"
        assert dataset.getncattr("time_coverage_start") == ("2025-01-01T00:00:00.123456789Z")
        assert dataset.getncattr("time_coverage_end") == ("2025-01-01T00:00:01.987654321Z")
        assert dataset.getncattr("time_coverage_duration") == "PT1.864197532S"
        series_id = np.asarray(dataset.variables["series_id"][0], dtype=np.uint8).tobytes()
        assert series_id.hex() == "00000000000000000000000000000000"
        assert int(dataset.variables["qc_bits"][0]) == (1 << 2) | (1 << 3)
        assert int(dataset.variables["time"][0]) == BASE_NS + 123_456_789
    success = json.loads((period / "success.json").read_text(encoding="utf-8"))
    assert success["netcdf_file"] == filename
    assert main(["report", str(summary.run_dir)]) == 0


def test_writes_arm_style_aggregate_netcdf_with_unsigned_eight_bit_qc(
    tmp_path: Path,
) -> None:
    run_path = write_configuration(tmp_path)
    raw_rules = yaml.safe_load((tmp_path / "quality_rules.yaml").read_text(encoding="utf-8"))
    for profile in raw_rules["profiles"].values():
        for variable in profile["variables"].values():
            variable["aggregation"] = "mean"
    (tmp_path / "quality_rules.yaml").write_text(
        yaml.safe_dump(raw_rules, sort_keys=False), encoding="utf-8"
    )
    (tmp_path / "aggregate_quality_rules.yaml").write_text(
        yaml.safe_dump(aggregate_quality_document(), sort_keys=False), encoding="utf-8"
    )
    run = yaml.safe_load(run_path.read_text(encoding="utf-8"))
    run["quality"]["aggregate_rules"] = "aggregate_quality_rules.yaml"
    run["processing"]["aggregation"] = {"period": 1, "units": "minutes"}
    run["output"]["netcdf"] = {
        "enabled": True,
        "product": "aggregate",
        "site": "neiu",
        "instrument": "wxt536",
        "data_level": "b1",
    }
    run_path.write_text(yaml.safe_dump(run, sort_keys=False), encoding="utf-8")
    write_facts(tmp_path, [{"time": BASE_NS + 1_000_000_000, "value": 10.0}])

    summary = run_new(run_path, "demo_wxt_work_unit", "aggregate-netcdf-run")
    assert (summary.run_dir / "aggregate_quality_rules.yaml").is_file()
    period = next((summary.run_dir / "work_units" / "demo_wxt_work_unit").iterdir())
    filename = "neiu.wxt536.W08E.1min.b1.20250101T000000Z-20250102T000000Z.nc"
    netcdf_path = period / filename
    assert netcdf_path.is_file()
    with netCDF4.Dataset(netcdf_path) as dataset:
        dataset.set_auto_mask(False)
        assert len(dataset.dimensions["time"]) == 1_440
        assert dataset.getncattr("crocus_data_level") == "b1"
        assert dataset.getncattr("aggregation_period") == 1
        assert dataset.getncattr("aggregation_period_units") == "minutes"
        assert dataset.getncattr("aggregation_period_seconds") == 60
        assert int(dataset.variables["time"][1] - dataset.variables["time"][0]) == 60
        temperature = dataset.variables["temperature"]
        qc_temperature = dataset.variables["qc_temperature"]
        assert temperature.getncattr("ancillary_variables") == "qc_temperature"
        assert float(temperature[0]) == 10.0
        assert float(temperature[1]) == -999.0
        assert qc_temperature.dtype == np.dtype("uint8")
        assert int(qc_temperature[0]) == 0
        assert int(qc_temperature[1]) == 1
        assert int(dataset.variables["qc_wind_speed"][0]) == 1
        assert list(np.asarray(qc_temperature.flag_masks, dtype=np.uint8)) == [
            1,
            2,
            4,
            8,
            16,
            32,
            64,
            128,
        ]
        assert qc_temperature.getncattr("qc_bit_8_description").startswith("Reserved")
        assert qc_temperature.getncattr("qc_bit_8_assessment") == "Indeterminate"
        assert "temperature_mean" in dataset.variables
        assert "temperature_maximum_gap" in dataset.variables
    success = json.loads((period / "success.json").read_text(encoding="utf-8"))
    assert success["netcdf_file"] == filename
