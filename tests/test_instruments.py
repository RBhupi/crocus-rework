import json

import pytest

from crocus_raw.instruments import InstrumentResolver
from crocus_raw.model import InfluxPoint, ParsedValue


def _point(tags):
    return InfluxPoint(1, "wxt.env.temp", "value", ParsedValue("float64", 2.0), tags)


def test_registry_override_and_stable_fallback(tmp_path):
    registry = tmp_path / "registry.json"
    registry.write_text(
        json.dumps(
            {
                "rules": [
                    {
                        "instrument_id": "known-wxt",
                        "match": {"vsn": "W001", "sensor": "wxt"},
                    }
                ]
            }
        )
    )
    resolver = InstrumentResolver.from_json(registry)

    assert resolver.resolve(_point({"vsn": "W001", "sensor": "wxt", "plugin": "image:v1"})) == "known-wxt"
    first = resolver.resolve(_point({"vsn": "W002", "sensor": "wxt", "plugin": "image:v1"}))
    second = resolver.resolve(_point({"vsn": "W002", "sensor": "wxt", "plugin": "image:v2"}))
    assert first == second


def test_wxt_identity_ignores_runtime_and_image_tags():
    resolver = InstrumentResolver([])
    stable_tags = {
        "vsn": "W08E",
        "sensor": "vaisala-wxt536",
        "zone": "core",
    }

    first = resolver.resolve(
        _point({**stable_tags, "plugin": "image:0.24.11.14", "job": "run-1"})
    )
    second = resolver.resolve(
        _point({**stable_tags, "plugin": "image:0.25.0", "job": "run-2"})
    )

    assert first == second


def test_fallback_identity_uses_uppercase_vsn_and_device_identity():
    resolver = InstrumentResolver()
    common = {"vsn": "W08E", "sensor": "vaisala-wxt536", "zone": "core"}

    first = resolver.resolve(_point({**common, "device": "left"}))
    second = resolver.resolve(_point({**common, "device": "right"}))

    assert first.startswith("W08E--vaisala-wxt536--core--left--")
    assert second.startswith("W08E--vaisala-wxt536--core--right--")
    assert first != second


def test_identity_metadata_flags_fallback_quality():
    resolver = InstrumentResolver()

    high = resolver.resolve_identity(_point({"vsn": "W001", "sensor": "wxt", "zone": "core"}))
    low = resolver.resolve_identity(_point({"host": "host1"}))

    assert high.confidence == "high"
    assert high.review_required is False
    assert high.identity_source == "vsn+sensor"
    assert low.confidence == "low"
    assert low.review_required is True


def test_registry_rejects_node_identity_rules(tmp_path):
    registry = tmp_path / "registry.json"
    registry.write_text(
        json.dumps(
            {
                "rules": [
                    {"instrument_id": "old-id", "match": {"node": "numeric-id"}}
                ]
            }
        )
    )

    with pytest.raises(ValueError, match="use 'vsn'"):
        InstrumentResolver.from_json(registry)
