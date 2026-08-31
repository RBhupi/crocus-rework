from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

import polars as pl
import pyarrow.parquet as pq
import yaml
from conftest import BASE_NS, run_document, write_facts

from adqat.config import load_config
from adqat.report import build_run_report
from adqat.runner import run_new

EXAMPLE_RULES = (
    Path(__file__).parents[1] / "examples" / "quality_rules.crocus_wxt_aqt_pilot.yaml"
)
EXAMPLES_DIR = Path(__file__).parents[1] / "examples"
AQT_DATASHEET_RULES = EXAMPLES_DIR / "quality_rules.crocus_aqt530_datasheet_test.yaml"
AQT_DATASHEET_RUN = (
    EXAMPLES_DIR / "processing_run.w08d_aqt_20251215_20251216_datasheet_test.yaml"
)


def test_hpc_pilot_outputs_are_isolated_from_production_tree() -> None:
    for filename in (
        "processing_run.w08d_wxt_20251215_20251216_pilot.yaml",
        "processing_run.w08d_aqt_20251215_20251216_pilot.yaml",
    ):
        document = yaml.safe_load((EXAMPLES_DIR / filename).read_text(encoding="utf-8"))
        assert "/crocus-rework-output/wxt-aqt-production-v5/" in document["source"][
            "path"
        ]
        assert document["output"]["root"] == (
            "/nfs/gce/projects/crocus-server-admins/data-rework/"
            "crocus-rework-output-tests-only/adqat-pilot-output"
        )
        assert "netcdf" not in document["output"]


def test_minute_pilot_examples_are_dense_parquet_only() -> None:
    expected = {
        "processing_run.w08d_wxt_20251215_20251216_minute_pilot.yaml": (
            "crocus_wxt536_pilot",
            "circular_mean",
        ),
        "processing_run.w08d_aqt_20251215_20251216_minute_pilot.yaml": (
            "crocus_aqt530_pilot",
            "mean",
        ),
    }
    for filename, (profile_name, representative_method) in expected.items():
        loaded = load_config(EXAMPLES_DIR / filename)
        assert loaded.run.processing.period == "1d"
        assert loaded.run.processing.aggregation == "1minute"
        assert loaded.run.output.netcdf is None
        profile = loaded.rules.profiles[profile_name]
        assert all(variable.aggregation is not None for variable in profile.variables.values())
        assert representative_method in {
            variable.aggregation for variable in profile.variables.values()
        }
        assert "/vsn=W08D/**/date=" in loaded.source_path


def test_aqt_datasheet_test_rules_are_sourced_and_nonprovisional() -> None:
    document = yaml.safe_load(AQT_DATASHEET_RULES.read_text(encoding="utf-8"))
    assert document["metadata"]["status"] == "pilot"
    assert document["metadata"]["references"]["aqt530_document_id"] == (
        "Vaisala B211817EN-F, published 2023"
    )
    variables = document["profiles"]["crocus_aqt530_datasheet_test"]["variables"]

    def check_args(variable: str, check_id: str) -> dict[str, Any]:
        checks = variables[variable]["checks"]
        return next(check["args"] for check in checks if check["id"] == check_id)

    assert check_args("air_temperature", "aqt_air_temperature_instrument") == {
        "left": -30,
        "right": 40,
        "inclusive": [True, True],
    }
    assert check_args("relative_humidity", "aqt_relative_humidity_instrument")["left"] == 15
    assert check_args("air_pressure", "aqt_air_pressure_instrument")["right"] == 1150
    assert check_args("carbon_monoxide", "aqt_carbon_monoxide_instrument")["right"] == 10
    assert check_args("nitric_oxide", "aqt_nitric_oxide_instrument")["right"] == 2
    assert check_args("nitrogen_dioxide", "aqt_nitrogen_dioxide_instrument")["right"] == 2
    assert check_args("ozone", "aqt_ozone_instrument")["right"] == 2
    assert check_args("particulate_matter_pm2_5", "aqt_pm2_5_instrument")["right"] == 1000
    assert check_args("particulate_matter_pm10", "aqt_pm10_instrument")["right"] == 2500
    assert all(
        check["flag"] != "instrument_range"
        for check in variables["particulate_matter_pm1"]["checks"]
    )
    assert len(variables["instrument_uptime"]["checks"]) == 1

    loaded = load_config(AQT_DATASHEET_RUN)
    config = loaded.resolve_work_unit("w08d_aqt530_datasheet_test_20251215_20251216")
    assert config.profile is loaded.rules.profiles["crocus_aqt530_datasheet_test"]
    assert config.run.output.netcdf is None
    assert str(config.output_root).endswith("adqat-aqt530-datasheet-test-output")


def test_aqt_datasheet_profile_runs_end_to_end(tmp_path: Path) -> None:
    root = tmp_path / "aqt-datasheet"
    root.mkdir()
    shutil.copyfile(AQT_DATASHEET_RULES, root / AQT_DATASHEET_RULES.name)
    instrument_id = "W08D--vaisala-aqt530--core--df6b0090a23b"
    run = run_document(str(root / "facts" / "*.parquet"), str(root / "results"))
    run["quality"]["rules"] = AQT_DATASHEET_RULES.name
    run["work_units"] = [
        {
            "id": "w08d_aqt_datasheet",
            "profile": "crocus_aqt530_datasheet_test",
            "filters": {
                "sensor": "vaisala-aqt530",
                "vsn": "W08D",
                "instrument_id": instrument_id,
            },
        }
    ]
    run_path = root / "processing_run.yaml"
    run_path.write_text(yaml.safe_dump(run, sort_keys=False), encoding="utf-8")
    common = {
        "sensor": "vaisala-aqt530",
        "vsn": "W08D",
        "instrument_id": instrument_id,
    }
    write_facts(
        root,
        [
            {**common, "measurement": "aqt.env.humidity", "value": 10.0},
            {**common, "measurement": "aqt.gas.co", "value": 11.0},
            {**common, "measurement": "aqt.particle.pm2.5", "value": 1200.0},
            {**common, "measurement": "aqt.particle.pm1", "value": 1200.0},
            {**common, "measurement": "aqt.house.uptime", "value": 5_000_000_000.0},
        ],
    )

    summary = run_new(run_path, "w08d_aqt_datasheet", "aqt-datasheet-test")
    period = next(
        (summary.run_dir / "work_units" / "w08d_aqt_datasheet").iterdir()
    )
    findings = pl.read_parquet(period / "findings.parquet")
    assert set(findings.select("check_id", "bit").rows()) == {
        ("aqt_relative_humidity_instrument", 3),
        ("aqt_carbon_monoxide_instrument", 3),
        ("aqt_pm2_5_instrument", 3),
    }
    assert summary.flagged_observations == 3


def _write_pilot_config(
    root: Path,
    *,
    profile: str,
    work_unit_id: str,
    sensor: str,
    instrument_id: str,
) -> Path:
    root.mkdir()
    shutil.copyfile(EXAMPLE_RULES, root / "quality_rules.crocus_wxt_aqt_pilot.yaml")
    run: dict[str, Any] = run_document(
        str(root / "facts" / "*.parquet"),
        str(root / "results"),
        end="2025-01-03T00:00:00Z",
    )
    run["quality"]["rules"] = "quality_rules.crocus_wxt_aqt_pilot.yaml"
    run["work_units"] = [
        {
            "id": work_unit_id,
            "profile": profile,
            "filters": {
                "sensor": sensor,
                "vsn": "W08D",
                "instrument_id": instrument_id,
            },
        }
    ]
    path = root / "processing_run.yaml"
    path.write_text(yaml.safe_dump(run, sort_keys=False), encoding="utf-8")
    return path


def test_wxt_then_aqt_pilot_profiles_run_through_identical_pipeline(tmp_path: Path) -> None:
    wxt_root = tmp_path / "wxt"
    wxt_instrument = "W08D--vaisala-wxt536--core--934c67f6166a"
    wxt_config = _write_pilot_config(
        wxt_root,
        profile="crocus_wxt536_pilot",
        work_unit_id="w08d_wxt_pilot",
        sensor="vaisala-wxt536",
        instrument_id=wxt_instrument,
    )
    write_facts(
        wxt_root,
        [
            {
                "time": BASE_NS + 1,
                "vsn": "W08D",
                "instrument_id": wxt_instrument,
                "measurement": "wxt.env.temp",
                "value": 10.0,
            },
            {
                "time": BASE_NS + 2,
                "vsn": "W08D",
                "instrument_id": wxt_instrument,
                "measurement": "wxt.env.temp",
                "value": -9999.9,
            },
            {
                "time": BASE_NS + 3,
                "vsn": "W08D",
                "instrument_id": wxt_instrument,
                "measurement": "wxt.wind.speed",
                "value": 80.0,
            },
            {
                "time": BASE_NS + 86_400_000_000_001,
                "vsn": "W08D",
                "instrument_id": wxt_instrument,
                "measurement": "wxt.env.temp",
                "value": 9.0,
            },
        ],
    )
    wxt_summary = run_new(wxt_config, "w08d_wxt_pilot", "wxt-pilot")
    wxt_period = sorted(
        (wxt_summary.run_dir / "work_units" / "w08d_wxt_pilot").iterdir()
    )[0]
    wxt_findings = pl.read_parquet(wxt_period / "findings.parquet")
    assert set(wxt_findings.select("check_id", "bit").rows()) == {
        ("wxt_air_temperature_missing", 0),
        ("wxt_wind_speed_instrument", 3),
    }
    wxt_report = build_run_report(wxt_summary.run_dir)
    assert wxt_report["rule_status"] == "pilot"
    assert wxt_report["flagged_observations"] == 2
    assert wxt_report["netcdf_files"] == []
    wxt_flags = pq.read_table(wxt_period / "qc_flags.parquet")
    assert set(wxt_flags["qc_bits"].to_pylist()) == {1, 1 << 3}
    assert set(wxt_flags["instrument_id"].to_pylist()) == {wxt_instrument}

    aqt_root = tmp_path / "aqt"
    aqt_instrument = "W08D--vaisala-aqt530--core--df6b0090a23b"
    aqt_config = _write_pilot_config(
        aqt_root,
        profile="crocus_aqt530_pilot",
        work_unit_id="w08d_aqt_pilot",
        sensor="vaisala-aqt530",
        instrument_id=aqt_instrument,
    )
    write_facts(
        aqt_root,
        [
            {
                "time": BASE_NS + 1,
                "sensor": "vaisala-aqt530",
                "vsn": "W08D",
                "instrument_id": aqt_instrument,
                "measurement": "aqt.gas.co",
                "value": 12.0,
            },
            {
                "time": BASE_NS + 2,
                "sensor": "vaisala-aqt530",
                "vsn": "W08D",
                "instrument_id": aqt_instrument,
                "measurement": "aqt.particle.pm2.5",
                "value": -1.0,
            },
            {
                "time": BASE_NS + 3,
                "sensor": "vaisala-aqt530",
                "vsn": "W08D",
                "instrument_id": aqt_instrument,
                "measurement": "aqt.house.datetime",
                "value_type": "string",
                "value": "2025-01-01T00:00:00",
            },
            {
                "time": BASE_NS + 4,
                "sensor": "vaisala-aqt530",
                "vsn": "W08D",
                "instrument_id": aqt_instrument,
                "measurement": "aqt.house.datetime",
                "value_type": "string",
                "value": "-9999.9",
            },
            {
                "time": BASE_NS + 86_400_000_000_001,
                "sensor": "vaisala-aqt530",
                "vsn": "W08D",
                "instrument_id": aqt_instrument,
                "measurement": "aqt.gas.co",
                "value": 0.1,
            },
        ],
    )
    aqt_summary = run_new(aqt_config, "w08d_aqt_pilot", "aqt-pilot")
    aqt_period = sorted(
        (aqt_summary.run_dir / "work_units" / "w08d_aqt_pilot").iterdir()
    )[0]
    aqt_findings = pl.read_parquet(aqt_period / "findings.parquet")
    assert set(aqt_findings.select("check_id", "bit").rows()) == {
        ("aqt_carbon_monoxide_instrument", 3),
        ("aqt_pm2_5_physical", 2),
        ("aqt_pm2_5_instrument", 3),
        ("aqt_instrument_datetime_missing", 0),
    }
    aqt_report = build_run_report(aqt_summary.run_dir)
    assert aqt_report["flagged_observations"] == 3
    assert aqt_report["netcdf_files"] == []
    aqt_flags = pq.read_table(aqt_period / "qc_flags.parquet")
    assert sorted(aqt_flags["qc_bits"].to_pylist()) == [1, 1 << 3, (1 << 2) | (1 << 3)]
    assert set(aqt_flags["instrument_id"].to_pylist()) == {aqt_instrument}
