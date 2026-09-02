from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

STANDARD = (
    Path(__file__).parents[1]
    / "standards"
    / "crocus-wxt536-aqt530-aggregate-qc-v1.0.0.yaml"
)

VARIABLE_FLAGS = {
    "raw_insufficient_coverage": 0,
    "raw_high_stdev": 1,
    "raw_low_stdev": 2,
    "aggregate_spike": 3,
    "aggregate_stuck": 4,
    "instrument_range": 5,
    "negative_impossible": 6,
    "reserved": 7,
}

CONDITION_FLAGS = {
    "heater_active": 0,
    "temp_below_operating": 1,
    "temp_above_operating": 2,
    "rh_below_operating": 3,
    "high_humidity": 4,
    "pressure_below_operating": 5,
    "pressure_above_operating": 6,
    "instrument_status_error": 7,
}

APPLICABLE_CHECKS = {
    "raw_insufficient_coverage",
    "raw_high_stdev",
    "raw_low_stdev",
    "aggregate_spike",
    "aggregate_stuck",
    "instrument_range",
    "negative_impossible",
}


def _load() -> dict[str, Any]:
    document = yaml.safe_load(STANDARD.read_text(encoding="utf-8"))
    assert isinstance(document, dict)
    return document


def test_approved_standard_has_exact_uint8_flag_contracts() -> None:
    standard = _load()
    assert standard["status"] == "approved"
    assert standard["version"] == "1.0.0"
    assert standard["runtime_compatibility"]["accepted_by_current_runtime"] is False
    assert {
        name: definition["bit"] for name, definition in standard["variable_qc_flags"].items()
    } == VARIABLE_FLAGS
    assert {
        name: definition["value"] for name, definition in standard["variable_qc_flags"].items()
    } == {name: 1 << bit for name, bit in VARIABLE_FLAGS.items()}
    assert {
        name: definition["bit"]
        for name, definition in standard["instrument_condition_flags"].items()
    } == CONDITION_FLAGS
    assert standard["variable_qc_flags"]["reserved"]["always_zero"] is True


def test_every_variable_declares_complete_applicability_and_pending_checks_are_off() -> None:
    standard = _load()
    pending = {
        "raw_insufficient_coverage",
        "raw_high_stdev",
        "raw_low_stdev",
        "aggregate_spike",
        "aggregate_stuck",
    }
    for profile in standard["profiles"].values():
        for variable in profile["variables"].values():
            checks = variable["checks"]
            assert set(checks) == APPLICABLE_CHECKS
            for name in pending:
                assert checks[name]["enabled"] is False


def test_reviewed_manufacturer_ranges_and_operating_conditions_are_exact() -> None:
    standard = _load()
    aqt = standard["profiles"]["crocus_aqt530"]
    wxt = standard["profiles"]["crocus_wxt536"]

    expected_ranges = {
        ("aqt", "air_temperature"): (-30, 40),
        ("aqt", "relative_humidity"): (15, 100),
        ("aqt", "air_pressure"): (800, 1150),
        ("aqt", "carbon_monoxide"): (0, 10),
        ("aqt", "nitric_oxide"): (0, 2),
        ("aqt", "nitrogen_dioxide"): (0, 2),
        ("aqt", "ozone"): (0, 2),
        ("aqt", "particulate_matter_pm2_5"): (0, 1000),
        ("aqt", "particulate_matter_pm10"): (0, 2500),
        ("wxt", "air_temperature"): (-52, 60),
        ("wxt", "relative_humidity"): (0, 100),
        ("wxt", "air_pressure"): (500, 1100),
        ("wxt", "wind_speed"): (0, 60),
        ("wxt", "wind_direction"): (0, 360),
        ("wxt", "heater_voltage"): (10.8, 31.2),
        ("wxt", "supply_voltage"): (5.4, 31.2),
    }
    profiles = {"aqt": aqt, "wxt": wxt}
    for (profile_name, variable_name), bounds in expected_ranges.items():
        check = profiles[profile_name]["variables"][variable_name]["checks"]["instrument_range"]
        assert check["enabled"] is True
        assert (check["minimum"], check["maximum"]) == bounds

    assert aqt["instrument_conditions"]["temperature"] == {
        "variable": "air_temperature",
        "below": -30,
        "above": 40,
        "units": "degree_Celsius",
    }
    assert aqt["instrument_conditions"]["relative_humidity"]["high_humidity_above"] == 90
    assert wxt["instrument_conditions"]["pressure"]["below"] == 500
    assert wxt["instrument_conditions"]["pressure"]["above"] == 1100


def test_accumulator_negative_check_requires_reset_aware_increment() -> None:
    standard = _load()
    variables = standard["profiles"]["crocus_wxt536"]["variables"]
    for name in ("rain_accumulation", "hail_accumulation"):
        check = variables[name]["checks"]["negative_impossible"]
        assert check["applicable"] is True
        assert check["enabled"] is False
        assert check["apply_to"] == "reset_aware_interval_increment"
