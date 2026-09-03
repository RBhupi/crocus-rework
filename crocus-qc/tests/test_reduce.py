"""Stage 1 statistical reduction behaviour.

Every expected value here is worked out by hand from the fixture, never recomputed the
way the implementation computes it. Stage 1 has no QA/QC, so there are no QC assertions.
"""

from __future__ import annotations

import math

import pytest

from conftest import DAY, Obs, bucket, run_stage1, subset
from crocus_qc.config import VariableSpec

TEMP = "wxt.env.temp"
RH = "wxt.env.humidity"
RAIN = "wxt.rain.accumulation"
WDIR = "wxt.wind.direction"
HSTATUS = "wxt.heater.status"

#: A text variable, built here rather than taken from a profile.
#:
#: String handling belongs to the SQL builder, not to an instrument: the WXT536 reports
#: no text measurement, and adding one to the shipped YAML purely to give these two tests
#: something to read would put a measurement in the profile that the instrument never
#: sends. The spec lives next to the tests that need it instead.
TEXT = "synthetic.text.value"
TEXT_SPEC = VariableSpec(
    name="text_value",
    measurement=TEXT,
    field="value",
    value_type="string",
    units="1",
    aggregation="last",
    data_type="string",
    missing_strings=("-9999.9", ""),
)


def circular_distance(actual: float, expected: float) -> float:
    """Shortest angular separation, so 359.9999 and 0.0 compare as adjacent."""
    return abs((actual - expected + 180.0) % 360.0 - 180.0)


# ---------------------------------------------------------------------------------
# Core statistics
# ---------------------------------------------------------------------------------


def test_mean_and_sample_count(tmp_path, wxt_profile):
    """10, 10, 14, 14 -> mean 12, min 10, max 14, population std 2."""
    rows = run_stage1(
        tmp_path,
        [
            Obs(1.0, TEMP, 10.0),
            Obs(2.0, TEMP, 10.0),
            Obs(3.0, TEMP, 14.0),
            Obs(4.0, TEMP, 14.0),
        ],
        subset(wxt_profile, ["air_temperature"]),
    )
    first = bucket(rows, 0)
    assert first["air_temperature"] == 12.0
    assert first["air_temperature_n_samples"] == 4
    assert first["air_temperature_raw_min"] == 10.0
    assert first["air_temperature_raw_max"] == 14.0
    # Summation order makes this 1.9999999999999998, not exactly 2.
    assert first["air_temperature_raw_std"] == pytest.approx(2.0)


def test_population_std_not_sample_std(tmp_path, wxt_profile):
    """Guard against ddof=1: for 10 and 14, pop std is 2.0 but sample std is ~2.83."""
    rows = run_stage1(
        tmp_path,
        [Obs(1.0, TEMP, 10.0), Obs(2.0, TEMP, 14.0)],
        subset(wxt_profile, ["air_temperature"]),
    )
    assert bucket(rows, 0)["air_temperature_raw_std"] == 2.0


def test_single_sample_has_no_std(tmp_path, wxt_profile):
    """One observation measures no spread at all -- which is not the same as zero spread.

    ``STDDEV_POP`` returns 0.0 here, and 0.0 is a claim: it says the instrument was
    perfectly steady across the bucket. A downstream ``raw_low_stdev`` check reading that
    column cannot tell the claim apart from a genuinely stuck sensor. NULL says the only
    true thing, which is that one sample supports no spread statistic.

    ``raw_min`` and ``raw_max`` stay populated: they really are the observed extremes.
    """
    rows = run_stage1(
        tmp_path, [Obs(1.0, TEMP, 10.0)], subset(wxt_profile, ["air_temperature"])
    )
    first = bucket(rows, 0)
    assert first["air_temperature_n_samples"] == 1
    assert first["air_temperature_raw_std"] is None
    assert first["air_temperature_raw_min"] == 10.0
    assert first["air_temperature_raw_max"] == 10.0
    assert first["air_temperature"] == 10.0


# ---------------------------------------------------------------------------------
# Missing-value normalization -- the only preprocessing Stage 1 is allowed to do
# ---------------------------------------------------------------------------------


def test_missing_sentinel_is_normalized_to_null(tmp_path, wxt_profile):
    """-9999.9 must not reach AVG/MIN/STDDEV_POP; only 10 and 14 count."""
    rows = run_stage1(
        tmp_path,
        [Obs(1.0, TEMP, 10.0), Obs(2.0, TEMP, -9999.9), Obs(3.0, TEMP, 14.0)],
        subset(wxt_profile, ["air_temperature"]),
    )
    first = bucket(rows, 0)
    assert first["air_temperature_n_samples"] == 2
    assert first["air_temperature"] == 12.0
    assert first["air_temperature_raw_min"] == 10.0


def test_explicit_null_is_excluded(tmp_path, wxt_profile):
    rows = run_stage1(
        tmp_path,
        [Obs(1.0, TEMP, 10.0), Obs(2.0, TEMP, None), Obs(3.0, TEMP, 14.0)],
        subset(wxt_profile, ["air_temperature"]),
    )
    assert bucket(rows, 0)["air_temperature_n_samples"] == 2


def test_bucket_of_only_missing_values(tmp_path, wxt_profile):
    """All-sentinel bucket is indistinguishable from an empty one, by design."""
    rows = run_stage1(
        tmp_path,
        [Obs(1.0, TEMP, -9999.9), Obs(2.0, TEMP, None)],
        subset(wxt_profile, ["air_temperature"]),
    )
    first = bucket(rows, 0)
    assert first["air_temperature"] is None
    assert first["air_temperature_n_samples"] == 0
    assert first["air_temperature_raw_std"] is None


def test_out_of_range_values_are_retained(tmp_path, wxt_profile):
    """Stage 1 applies no bounds: an absurd 500 C reading still contributes.

    This is the explicit acceptance criterion 'no physical/instrument filtering exists'.
    """
    rows = run_stage1(
        tmp_path,
        [Obs(1.0, TEMP, 10.0), Obs(2.0, TEMP, 500.0)],
        subset(wxt_profile, ["air_temperature"]),
    )
    first = bucket(rows, 0)
    assert first["air_temperature_n_samples"] == 2
    assert first["air_temperature"] == 255.0
    assert first["air_temperature_raw_max"] == 500.0


# ---------------------------------------------------------------------------------
# Dense 10-second UTC grid
# ---------------------------------------------------------------------------------


def test_full_day_has_exactly_8640_rows(tmp_path, wxt_profile):
    rows = run_stage1(
        tmp_path, [Obs(1.0, TEMP, 10.0)], subset(wxt_profile, ["air_temperature"])
    )
    assert len(rows) == 8640


def test_empty_interval_is_explicit(tmp_path, wxt_profile):
    """Bucket 1 has no observations: nulls and a zero count, never interpolation."""
    rows = run_stage1(
        tmp_path,
        [Obs(1.0, TEMP, 10.0), Obs(25.0, TEMP, 30.0)],
        subset(wxt_profile, ["air_temperature"]),
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


def test_grid_is_utc_anchored_and_evenly_spaced(tmp_path, wxt_profile):
    rows = run_stage1(
        tmp_path, [Obs(1.0, TEMP, 10.0)], subset(wxt_profile, ["air_temperature"])
    )
    assert rows[0]["time"] == DAY
    assert rows[1]["time"].second == 10
    assert rows[-1]["time"] == DAY.replace(hour=23, minute=59, second=50)


def test_day_boundaries(tmp_path, wxt_profile):
    """First and last instants of the day land in the first and last buckets."""
    rows = run_stage1(
        tmp_path,
        [Obs(0.0, TEMP, 1.0), Obs(86399.9, TEMP, 2.0)],
        subset(wxt_profile, ["air_temperature"]),
    )
    assert bucket(rows, 0)["air_temperature"] == 1.0
    assert bucket(rows, 86390)["air_temperature"] == 2.0
    assert sum(r["air_temperature_n_samples"] for r in rows) == 2


# ---------------------------------------------------------------------------------
# Irregular sampling
# ---------------------------------------------------------------------------------


def test_irregular_timestamps_land_in_correct_buckets(tmp_path, wxt_profile):
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
        subset(wxt_profile, ["air_temperature"]),
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


def test_single_direction_has_no_circular_spread(tmp_path, wxt_profile):
    """The circular form reaches 0.0 by a different route than STDDEV_POP, same result.

    With one direction the mean resultant length is exactly 1, so ``LN(1)`` is 0 and the
    circular sigma is 0.0 -- "the wind held perfectly steady" from a single reading. The
    bearing itself is real and stays; only the spread is unsupported.
    """
    rows = run_stage1(
        tmp_path, [Obs(1.0, WDIR, 137.0)], subset(wxt_profile, ["wind_direction"])
    )
    first = bucket(rows, 0)
    assert first["wind_direction_n_samples"] == 1
    assert circular_distance(first["wind_direction"], 137.0) < 1e-6
    assert first["wind_direction_raw_std"] is None


def test_many_identical_directions_do_not_overflow_the_resultant(tmp_path, wxt_profile):
    """A bucket of identical directions has zero spread, not a domain error.

    The mean resultant length R is bounded by 1 by definition, but floating-point
    summation of many identical unit vectors rounds it to 1 + 2e-16. That makes
    ``LN(R)`` positive, ``-2*LN(R)`` negative, and ``SQRT`` of a negative number an
    ``OutOfRangeException`` -- which is exactly how a full station run died on a calm
    10-second bucket (W069, 2025-02-11 at 17.4 degrees). Angle and count here are chosen
    to land R just above 1.0; the honest answer is zero angular spread.
    """
    obs = [Obs(round(i * 0.1, 1), WDIR, 17.4) for i in range(100)]
    rows = run_stage1(tmp_path, obs, subset(wxt_profile, ["wind_direction"]))
    first = bucket(rows, 0)
    assert first["wind_direction_n_samples"] == 100
    assert circular_distance(first["wind_direction"], 17.4) < 1e-6
    assert abs(first["wind_direction_raw_std"]) < 1e-9


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


def test_last_value_uses_latest_timestamp_not_file_order(tmp_path, wxt_profile):
    """MAX_BY on the real timestamp: the 7 s observation wins regardless of row order."""
    rows = run_stage1(
        tmp_path,
        [Obs(7.0, RAIN, 700.0), Obs(1.0, RAIN, 100.0), Obs(4.0, RAIN, 400.0)],
        subset(wxt_profile, ["rain_accumulation"]),
    )
    first = bucket(rows, 0)
    assert first["rain_accumulation"] == 700.0
    assert first["rain_accumulation_n_samples"] == 3


def test_last_string_value(tmp_path):
    rows = run_stage1(
        tmp_path,
        [
            Obs(1.0, TEXT, text="2025-12-15T00:00:01"),
            Obs(6.0, TEXT, text="2025-12-15T00:00:06"),
        ],
        (TEXT_SPEC,),
    )
    first = bucket(rows, 0)
    assert first["text_value"] == "2025-12-15T00:00:06"
    assert first["text_value_n_samples"] == 2


def test_missing_strings_are_normalized(tmp_path):
    """The empty string and the textual sentinel are missing, not data."""
    rows = run_stage1(
        tmp_path,
        [
            Obs(1.0, TEXT, text="2025-12-15T00:00:01"),
            Obs(6.0, TEXT, text=""),
            Obs(8.0, TEXT, text="-9999.9"),
        ],
        (TEXT_SPEC,),
    )
    first = bucket(rows, 0)
    assert first["text_value"] == "2025-12-15T00:00:01"
    assert first["text_value_n_samples"] == 1


# ---------------------------------------------------------------------------------
# Schema: meaningless statistics are omitted rather than filled
# ---------------------------------------------------------------------------------


def test_mean_variable_carries_all_spread_statistics(tmp_path, wxt_profile):
    rows = run_stage1(
        tmp_path, [Obs(1.0, TEMP, 10.0)], subset(wxt_profile, ["air_temperature"])
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
    "variable,measurement,omitted",
    [
        ("wind_direction", WDIR, ["raw_min", "raw_max"]),
        ("heater_status", HSTATUS, ["raw_min", "raw_max", "raw_std"]),
        ("rain_accumulation", RAIN, ["raw_min", "raw_max", "raw_std"]),
    ],
)
def test_meaningless_statistics_are_absent(
    tmp_path, wxt_profile, variable, measurement, omitted
):
    rows = run_stage1(
        tmp_path,
        [Obs(1.0, measurement, 1.0)],
        subset(wxt_profile, [variable]),
    )
    for suffix in omitted:
        assert f"{variable}_{suffix}" not in rows[0]
    assert f"{variable}_n_samples" in rows[0]


def test_string_variable_carries_no_spread_statistics(tmp_path):
    """Min, max, and spread are undefined on text, so the columns are absent."""
    rows = run_stage1(tmp_path, [Obs(1.0, TEXT, text="x")], (TEXT_SPEC,))
    assert set(rows[0]) == {"time", "text_value", "text_value_n_samples"}


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


def test_variables_are_independent(tmp_path, wxt_profile):
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
        subset(wxt_profile, ["air_temperature", "relative_humidity"]),
    )
    first = bucket(rows, 0)
    assert first["air_temperature"] == 12.0
    assert first["air_temperature_n_samples"] == 2
    assert first["relative_humidity"] == 60.0
    assert first["relative_humidity_n_samples"] == 3


def test_variable_absent_from_raw_data_yields_nulls(tmp_path, wxt_profile):
    """A profile variable the instrument never reported is all-null, not an error."""
    rows = run_stage1(
        tmp_path,
        [Obs(1.0, TEMP, 10.0)],
        subset(wxt_profile, ["air_temperature", "air_pressure"]),
    )
    first = bucket(rows, 0)
    assert first["air_temperature"] == 10.0
    assert first["air_pressure"] is None
    assert first["air_pressure_n_samples"] == 0
    assert sum(r["air_pressure_n_samples"] for r in rows) == 0
