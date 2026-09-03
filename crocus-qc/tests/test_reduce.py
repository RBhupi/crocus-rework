"""Stage 1 statistical reduction behaviour.

Every expected value here is worked out by hand from the fixture, never recomputed the
way the implementation computes it. Stage 1 has no QA/QC, so there are no QC assertions.
"""

from __future__ import annotations

import math

import pytest

from conftest import DAY, Obs, bucket, run_stage1, subset

TEMP = "aqt.env.temp"
RH = "aqt.env.humidity"
UPTIME = "aqt.house.uptime"
DATETIME = "aqt.house.datetime"
WDIR = "wxt.wind.direction"
HSTATUS = "wxt.heater.status"


def circular_distance(actual: float, expected: float) -> float:
    """Shortest angular separation, so 359.9999 and 0.0 compare as adjacent."""
    return abs((actual - expected + 180.0) % 360.0 - 180.0)


# ---------------------------------------------------------------------------------
# Core statistics
# ---------------------------------------------------------------------------------


def test_mean_and_sample_count(tmp_path, aqt_profile):
    """10, 10, 14, 14 -> mean 12, min 10, max 14, population std 2."""
    rows = run_stage1(
        tmp_path,
        [
            Obs(1.0, TEMP, 10.0),
            Obs(2.0, TEMP, 10.0),
            Obs(3.0, TEMP, 14.0),
            Obs(4.0, TEMP, 14.0),
        ],
        subset(aqt_profile, ["air_temperature"]),
    )
    first = bucket(rows, 0)
    assert first["air_temperature"] == 12.0
    assert first["air_temperature_n_samples"] == 4
    assert first["air_temperature_raw_min"] == 10.0
    assert first["air_temperature_raw_max"] == 14.0
    # Summation order makes this 1.9999999999999998, not exactly 2.
    assert first["air_temperature_raw_std"] == pytest.approx(2.0)


def test_population_std_not_sample_std(tmp_path, aqt_profile):
    """Guard against ddof=1: for 10 and 14, pop std is 2.0 but sample std is ~2.83."""
    rows = run_stage1(
        tmp_path,
        [Obs(1.0, TEMP, 10.0), Obs(2.0, TEMP, 14.0)],
        subset(aqt_profile, ["air_temperature"]),
    )
    assert bucket(rows, 0)["air_temperature_raw_std"] == 2.0


def test_single_sample_std_is_zero(tmp_path, aqt_profile):
    """One observation has zero population spread (sample std would be undefined)."""
    rows = run_stage1(
        tmp_path, [Obs(1.0, TEMP, 10.0)], subset(aqt_profile, ["air_temperature"])
    )
    first = bucket(rows, 0)
    assert first["air_temperature_n_samples"] == 1
    assert first["air_temperature_raw_std"] == 0.0
    assert first["air_temperature_raw_min"] == 10.0
    assert first["air_temperature_raw_max"] == 10.0


# ---------------------------------------------------------------------------------
# Missing-value normalization -- the only preprocessing Stage 1 is allowed to do
# ---------------------------------------------------------------------------------


def test_missing_sentinel_is_normalized_to_null(tmp_path, aqt_profile):
    """-9999.9 must not reach AVG/MIN/STDDEV_POP; only 10 and 14 count."""
    rows = run_stage1(
        tmp_path,
        [Obs(1.0, TEMP, 10.0), Obs(2.0, TEMP, -9999.9), Obs(3.0, TEMP, 14.0)],
        subset(aqt_profile, ["air_temperature"]),
    )
    first = bucket(rows, 0)
    assert first["air_temperature_n_samples"] == 2
    assert first["air_temperature"] == 12.0
    assert first["air_temperature_raw_min"] == 10.0


def test_explicit_null_is_excluded(tmp_path, aqt_profile):
    rows = run_stage1(
        tmp_path,
        [Obs(1.0, TEMP, 10.0), Obs(2.0, TEMP, None), Obs(3.0, TEMP, 14.0)],
        subset(aqt_profile, ["air_temperature"]),
    )
    assert bucket(rows, 0)["air_temperature_n_samples"] == 2


def test_bucket_of_only_missing_values(tmp_path, aqt_profile):
    """All-sentinel bucket is indistinguishable from an empty one, by design."""
    rows = run_stage1(
        tmp_path,
        [Obs(1.0, TEMP, -9999.9), Obs(2.0, TEMP, None)],
        subset(aqt_profile, ["air_temperature"]),
    )
    first = bucket(rows, 0)
    assert first["air_temperature"] is None
    assert first["air_temperature_n_samples"] == 0
    assert first["air_temperature_raw_std"] is None


def test_out_of_range_values_are_retained(tmp_path, aqt_profile):
    """Stage 1 applies no bounds: an absurd 500 C reading still contributes.

    This is the explicit acceptance criterion 'no physical/instrument filtering exists'.
    """
    rows = run_stage1(
        tmp_path,
        [Obs(1.0, TEMP, 10.0), Obs(2.0, TEMP, 500.0)],
        subset(aqt_profile, ["air_temperature"]),
    )
    first = bucket(rows, 0)
    assert first["air_temperature_n_samples"] == 2
    assert first["air_temperature"] == 255.0
    assert first["air_temperature_raw_max"] == 500.0


# ---------------------------------------------------------------------------------
# Dense 10-second UTC grid
# ---------------------------------------------------------------------------------


def test_full_day_has_exactly_8640_rows(tmp_path, aqt_profile):
    rows = run_stage1(
        tmp_path, [Obs(1.0, TEMP, 10.0)], subset(aqt_profile, ["air_temperature"])
    )
    assert len(rows) == 8640


def test_empty_interval_is_explicit(tmp_path, aqt_profile):
    """Bucket 1 has no observations: nulls and a zero count, never interpolation."""
    rows = run_stage1(
        tmp_path,
        [Obs(1.0, TEMP, 10.0), Obs(25.0, TEMP, 30.0)],
        subset(aqt_profile, ["air_temperature"]),
    )
    empty = bucket(rows, 10)
    assert empty["air_temperature"] is None
    assert empty["air_temperature_n_samples"] == 0
    assert empty["air_temperature_raw_min"] is None
    assert empty["air_temperature_raw_max"] is None
    assert empty["air_temperature_raw_std"] is None
    # The neighbours are untouched -- no smearing across empty buckets.
    assert bucket(rows, 0)["air_temperature"] == 10.0
    assert bucket(rows, 20)["air_temperature"] == 30.0


def test_grid_is_utc_anchored_and_evenly_spaced(tmp_path, aqt_profile):
    rows = run_stage1(
        tmp_path, [Obs(1.0, TEMP, 10.0)], subset(aqt_profile, ["air_temperature"])
    )
    assert rows[0]["time"] == DAY
    assert rows[1]["time"].second == 10
    assert rows[-1]["time"] == DAY.replace(hour=23, minute=59, second=50)


def test_day_boundaries(tmp_path, aqt_profile):
    """First and last instants of the day land in the first and last buckets."""
    rows = run_stage1(
        tmp_path,
        [Obs(0.0, TEMP, 1.0), Obs(86399.9, TEMP, 2.0)],
        subset(aqt_profile, ["air_temperature"]),
    )
    assert bucket(rows, 0)["air_temperature"] == 1.0
    assert bucket(rows, 86390)["air_temperature"] == 2.0
    assert sum(r["air_temperature_n_samples"] for r in rows) == 2


# ---------------------------------------------------------------------------------
# Irregular sampling
# ---------------------------------------------------------------------------------


def test_irregular_timestamps_land_in_correct_buckets(tmp_path, aqt_profile):
    """Real ~10 Hz timestamps are irregular; each observation simply falls in its bucket."""
    rows = run_stage1(
        tmp_path,
        [
            Obs(0.03, TEMP, 2.0),
            Obs(3.71, TEMP, 4.0),
            Obs(9.99, TEMP, 6.0),
            Obs(10.01, TEMP, 100.0),
            Obs(19.999, TEMP, 200.0),
        ],
        subset(aqt_profile, ["air_temperature"]),
    )
    assert bucket(rows, 0)["air_temperature_n_samples"] == 3
    assert bucket(rows, 0)["air_temperature"] == 4.0
    assert bucket(rows, 10)["air_temperature_n_samples"] == 2
    assert bucket(rows, 10)["air_temperature"] == 150.0


# ---------------------------------------------------------------------------------
# Variable-specific aggregation
# ---------------------------------------------------------------------------------


def test_circular_mean_across_north(tmp_path, wxt_profile):
    """359 and 1 average to 0 (north), not 180 (south)."""
    rows = run_stage1(
        tmp_path,
        [Obs(1.0, WDIR, 359.0), Obs(2.0, WDIR, 1.0)],
        subset(wxt_profile, ["wind_direction"]),
    )
    first = bucket(rows, 0)
    assert circular_distance(first["wind_direction"], 0.0) < 1e-6
    assert first["wind_direction_n_samples"] == 2


def test_circular_mean_is_normalized_into_0_360(tmp_path, wxt_profile):
    """ATAN2 returns (-180, 180]; the product must never expose a negative bearing."""
    rows = run_stage1(
        tmp_path,
        [Obs(1.0, WDIR, 350.0), Obs(2.0, WDIR, 340.0)],
        subset(wxt_profile, ["wind_direction"]),
    )
    value = bucket(rows, 0)["wind_direction"]
    assert 0.0 <= value < 360.0
    assert circular_distance(value, 345.0) < 1e-6


def test_opposing_directions_give_large_circular_spread(tmp_path, wxt_profile):
    """90 and 270 cancel: the mean is meaningless and the circular std says so."""
    rows = run_stage1(
        tmp_path,
        [Obs(1.0, WDIR, 90.0), Obs(2.0, WDIR, 270.0)],
        subset(wxt_profile, ["wind_direction"]),
    )
    assert bucket(rows, 0)["wind_direction_raw_std"] > 100.0


def test_mode_aggregation(tmp_path, wxt_profile):
    rows = run_stage1(
        tmp_path,
        [
            Obs(1.0, HSTATUS, 3.0),
            Obs(2.0, HSTATUS, 3.0),
            Obs(3.0, HSTATUS, 9.0),
        ],
        subset(wxt_profile, ["heater_status"]),
    )
    first = bucket(rows, 0)
    assert first["heater_status"] == 3.0
    assert first["heater_status_n_samples"] == 3


def test_last_value_uses_latest_timestamp_not_file_order(tmp_path, aqt_profile):
    """MAX_BY on the real timestamp: the 7 s observation wins regardless of row order."""
    rows = run_stage1(
        tmp_path,
        [Obs(7.0, UPTIME, 700.0), Obs(1.0, UPTIME, 100.0), Obs(4.0, UPTIME, 400.0)],
        subset(aqt_profile, ["instrument_uptime"]),
    )
    first = bucket(rows, 0)
    assert first["instrument_uptime"] == 700.0
    assert first["instrument_uptime_n_samples"] == 3


def test_last_string_value(tmp_path, aqt_profile):
    rows = run_stage1(
        tmp_path,
        [
            Obs(1.0, DATETIME, text="2025-12-15T00:00:01"),
            Obs(6.0, DATETIME, text="2025-12-15T00:00:06"),
        ],
        subset(aqt_profile, ["instrument_datetime"]),
    )
    first = bucket(rows, 0)
    assert first["instrument_datetime"] == "2025-12-15T00:00:06"
    assert first["instrument_datetime_n_samples"] == 2


def test_missing_strings_are_normalized(tmp_path, aqt_profile):
    """The empty string and the textual sentinel are missing, not data."""
    rows = run_stage1(
        tmp_path,
        [
            Obs(1.0, DATETIME, text="2025-12-15T00:00:01"),
            Obs(6.0, DATETIME, text=""),
            Obs(8.0, DATETIME, text="-9999.9"),
        ],
        subset(aqt_profile, ["instrument_datetime"]),
    )
    first = bucket(rows, 0)
    assert first["instrument_datetime"] == "2025-12-15T00:00:01"
    assert first["instrument_datetime_n_samples"] == 1


# ---------------------------------------------------------------------------------
# Schema: meaningless statistics are omitted rather than filled
# ---------------------------------------------------------------------------------


def test_mean_variable_carries_all_spread_statistics(tmp_path, aqt_profile):
    rows = run_stage1(
        tmp_path, [Obs(1.0, TEMP, 10.0)], subset(aqt_profile, ["air_temperature"])
    )
    assert set(rows[0]) == {
        "time",
        "air_temperature",
        "air_temperature_n_samples",
        "air_temperature_raw_min",
        "air_temperature_raw_max",
        "air_temperature_raw_std",
    }


@pytest.mark.parametrize(
    "profile_name,variable,measurement,omitted",
    [
        ("wxt", "wind_direction", WDIR, ["raw_min", "raw_max"]),
        ("wxt", "heater_status", HSTATUS, ["raw_min", "raw_max", "raw_std"]),
        ("aqt", "instrument_uptime", UPTIME, ["raw_min", "raw_max", "raw_std"]),
        ("aqt", "instrument_datetime", DATETIME, ["raw_min", "raw_max", "raw_std"]),
    ],
)
def test_meaningless_statistics_are_absent(
    tmp_path, aqt_profile, wxt_profile, profile_name, variable, measurement, omitted
):
    profile = aqt_profile if profile_name == "aqt" else wxt_profile
    text = "x" if variable == "instrument_datetime" else None
    value = None if text else 1.0
    rows = run_stage1(
        tmp_path,
        [Obs(1.0, measurement, value, text)],
        subset(profile, [variable]),
    )
    for suffix in omitted:
        assert f"{variable}_{suffix}" not in rows[0]
    assert f"{variable}_n_samples" in rows[0]


def test_circular_variable_keeps_std_but_not_min_max(tmp_path, wxt_profile):
    rows = run_stage1(
        tmp_path, [Obs(1.0, WDIR, 10.0)], subset(wxt_profile, ["wind_direction"])
    )
    assert set(rows[0]) == {
        "time",
        "wind_direction",
        "wind_direction_n_samples",
        "wind_direction_raw_std",
    }


# ---------------------------------------------------------------------------------
# Multi-variable single scan
# ---------------------------------------------------------------------------------


def test_variables_are_independent(tmp_path, aqt_profile):
    """One scan, many variables: each reduces only its own measurement's rows."""
    rows = run_stage1(
        tmp_path,
        [
            Obs(1.0, TEMP, 10.0),
            Obs(2.0, TEMP, 14.0),
            Obs(1.5, RH, 40.0),
            Obs(2.5, RH, 60.0),
            Obs(3.0, RH, 80.0),
        ],
        subset(aqt_profile, ["air_temperature", "relative_humidity"]),
    )
    first = bucket(rows, 0)
    assert first["air_temperature"] == 12.0
    assert first["air_temperature_n_samples"] == 2
    assert first["relative_humidity"] == 60.0
    assert first["relative_humidity_n_samples"] == 3


def test_variable_absent_from_raw_data_yields_nulls(tmp_path, aqt_profile):
    """A profile variable the instrument never reported is all-null, not an error."""
    rows = run_stage1(
        tmp_path,
        [Obs(1.0, TEMP, 10.0)],
        subset(aqt_profile, ["air_temperature", "ozone"]),
    )
    first = bucket(rows, 0)
    assert first["air_temperature"] == 10.0
    assert first["ozone"] is None
    assert first["ozone_n_samples"] == 0
    assert sum(r["ozone_n_samples"] for r in rows) == 0
