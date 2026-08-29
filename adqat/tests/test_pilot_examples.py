from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

import netCDF4
import polars as pl
import yaml
from conftest import BASE_NS, run_document, write_facts

from adqat.report import build_run_report
from adqat.runner import run_new

EXAMPLE_RULES = (
    Path(__file__).parents[1] / "examples" / "quality_rules.crocus_wxt_aqt_pilot.yaml"
)
EXAMPLES_DIR = Path(__file__).parents[1] / "examples"


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


def _write_pilot_config(
    root: Path,
    *,
    profile: str,
    work_unit_id: str,
    sensor: str,
    instrument_id: str,
    instrument_code: str,
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
    run["output"]["netcdf"] = {
        "enabled": True,
        "site": "neiu",
        "instrument": instrument_code,
    }
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
        instrument_code="wxt536",
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
    assert len(wxt_report["netcdf_files"]) == 2

    aqt_root = tmp_path / "aqt"
    aqt_instrument = "W08D--vaisala-aqt530--core--df6b0090a23b"
    aqt_config = _write_pilot_config(
        aqt_root,
        profile="crocus_aqt530_pilot",
        work_unit_id="w08d_aqt_pilot",
        sensor="vaisala-aqt530",
        instrument_id=aqt_instrument,
        instrument_code="aqt530",
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
    assert len(aqt_report["netcdf_files"]) == 2
    with netCDF4.Dataset(aqt_report["netcdf_files"][0]) as dataset:
        assert "2025-01-01T00:00:00" in dataset.variables["observed_value_string"][:].tolist()
