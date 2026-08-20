import json

import pytest

from crocus_raw.model import InfluxPoint, ParsedValue
from crocus_raw.selection import Selection


def _point(measurement="wxt.env.temp", field="value", tags=None):
    return InfluxPoint(
        time_ns=1,
        measurement=measurement,
        field=field,
        parsed_value=ParsedValue("float64", 1.0),
        tags=tags or {},
    )


def test_selection_matches_fields_arbitrary_tags_and_globs():
    selection = Selection.from_document(
        {
            "selection_version": 1,
            "selectors": [
                {
                    "measurement": "wxt.env.temp",
                    "fields": ["value", "quality*"],
                    "tags": {
                        "custom.metadata": ["alpha"],
                        "vsn": ["W0*", "W10"],
                    },
                }
            ],
        }
    )

    assert selection.matches(
        _point(tags={"custom.metadata": "alpha", "vsn": "W08D"})
    )
    assert selection.matches(
        _point(field="quality_flag", tags={"custom.metadata": "alpha", "vsn": "W10"})
    )
    assert not selection.matches(_point(tags={"vsn": "W08D"}))
    assert not selection.matches(
        _point(field="other", tags={"custom.metadata": "alpha", "vsn": "W08D"})
    )


def test_selection_ors_selectors_and_deduplicates_measurements():
    selection = Selection.from_document(
        {
            "selection_version": 1,
            "selectors": [
                {"measurement": "wxt.env.temp", "tags": {"site": ["NEIU"]}},
                {"measurement": "wxt.env.temp", "tags": {"site": ["CSU"]}},
                {"measurement": "wxt.env.humidity"},
            ],
        }
    )

    assert selection.measurements == ("wxt.env.humidity", "wxt.env.temp")
    assert selection.matches(_point(tags={"site": "CSU"}))
    assert selection.matches(_point(measurement="wxt.env.humidity"))


def test_selection_fingerprint_is_canonical(tmp_path):
    first = Selection.from_document(
        {
            "selection_version": 1,
            "selectors": [
                {"measurement": "b"},
                {"measurement": "a", "fields": ["z", "x", "x"]},
            ],
        }
    )
    path = tmp_path / "selection.json"
    path.write_text(
        json.dumps(
            {
                "selectors": [
                    {"fields": ["x", "z"], "measurement": "a"},
                    {"measurement": "b"},
                ],
                "selection_version": 1,
            }
        )
    )

    second = Selection.from_json(path)

    assert second.fingerprint == first.fingerprint
    assert second.canonical_json == first.canonical_json


def test_selection_v2_discovers_measurements_by_sensor_and_glob():
    selection = Selection.from_document(
        {
            "selection_version": 2,
            "selectors": [
                {
                    "measurement_glob": "wxt.*",
                    "tags": {"sensor": ["vaisala-wxt536"]},
                }
            ],
        }
    )

    assert selection.requires_discovery is True
    assert selection.measurements == ()
    assert selection.matches_parts(
        "wxt.env.temp", "value", {"sensor": "vaisala-wxt536"}
    )
    assert not selection.matches_parts(
        "wxt.env.temp", "value", {"sensor": "other"}
    )
    assert not selection.matches_parts(
        "sys.net.up", "value", {"sensor": "vaisala-wxt536"}
    )


@pytest.mark.parametrize(
    "document",
    [
        {},
        {"selection_version": 3, "selectors": [{"measurement": "x"}]},
        {"selection_version": 1, "selectors": [{"measurement_glob": "x*"}]},
        {"selection_version": 1, "selectors": []},
        {"selection_version": 1, "selectors": [{"measurement": "wxt.*"}]},
        {"selection_version": 1, "selectors": [{"measurement": "x", "fields": []}]},
        {
            "selection_version": 1,
            "selectors": [{"measurement": "x", "tags": {"site": []}}],
        },
        {
            "selection_version": 1,
            "selectors": [{"measurement": "x", "tags": {"node": ["42"]}}],
        },
    ],
)
def test_selection_rejects_invalid_documents(document):
    with pytest.raises(ValueError):
        Selection.from_document(document)
