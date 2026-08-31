from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from conftest import quality_document, run_document, write_configuration, write_facts

from adqat.config import ConfigError, load_config


def test_loads_and_resolves_paths_with_stable_hash(tmp_path: Path) -> None:
    run_path = write_configuration(tmp_path)
    write_facts(tmp_path, [{"value": 1.0}])
    first = load_config(run_path).resolve_work_unit("demo_wxt_work_unit")
    second = load_config(run_path).resolve_work_unit("demo_wxt_work_unit")
    assert first.source_path == str((tmp_path / "facts" / "*.parquet").resolve())
    assert first.output_root == (tmp_path / "results").resolve()
    assert first.config_hash == second.config_hash
    assert len(first.config_hash) == 64


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda rules: rules["flags"]["physical_range"].update(bit=0), "unique"),
        (
            lambda rules: rules["profiles"]["demo_wxt"]["variables"]["temperature"]["checks"][
                0
            ].update(flag="unknown"),
            "unknown flag",
        ),
        (
            lambda rules: rules["profiles"]["demo_wxt"]["variables"]["temperature"]["checks"][1][
                "args"
            ].update(na_pass=False),
            "na_pass=false",
        ),
    ],
)
def test_rejects_invalid_quality_rules(tmp_path: Path, mutate: object, message: str) -> None:
    rules = quality_document()
    mutate(rules)
    (tmp_path / "quality_rules.yaml").write_text(yaml.safe_dump(rules), encoding="utf-8")
    run = run_document(str(tmp_path / "facts" / "*.parquet"), str(tmp_path / "results"))
    run_path = tmp_path / "processing_run.yaml"
    run_path.write_text(yaml.safe_dump(run), encoding="utf-8")
    with pytest.raises(ConfigError, match=message):
        load_config(run_path)


def test_rejects_naive_time_and_output_source_overlap(tmp_path: Path) -> None:
    (tmp_path / "quality_rules.yaml").write_text(
        yaml.safe_dump(quality_document()), encoding="utf-8"
    )
    run = run_document(str(tmp_path / "facts" / "*.parquet"), str(tmp_path / "facts/out"))
    run["selection"]["start"] = "2025-01-01T00:00:00"
    run_path = tmp_path / "processing_run.yaml"
    run_path.write_text(yaml.safe_dump(run), encoding="utf-8")
    with pytest.raises(ConfigError, match="timezone-aware"):
        load_config(run_path)
    run["selection"]["start"] = "2025-01-01T00:00:00Z"
    run_path.write_text(yaml.safe_dump(run), encoding="utf-8")
    with pytest.raises(ConfigError, match="must not overlap"):
        load_config(run_path)


def test_rejects_unknown_work_unit_and_profile(tmp_path: Path) -> None:
    run_path = write_configuration(tmp_path)
    loaded = load_config(run_path)
    with pytest.raises(ConfigError, match="unknown work unit"):
        loaded.resolve_work_unit("missing")

    run = yaml.safe_load(run_path.read_text())
    run["work_units"][0]["profile"] = "missing"
    run_path.write_text(yaml.safe_dump(run), encoding="utf-8")
    with pytest.raises(ConfigError, match="unknown profile"):
        load_config(run_path)


def test_work_unit_requires_cross_run_identity_filters(tmp_path: Path) -> None:
    (tmp_path / "quality_rules.yaml").write_text(
        yaml.safe_dump(quality_document()), encoding="utf-8"
    )
    run = run_document(str(tmp_path / "facts" / "*.parquet"), str(tmp_path / "results"))
    del run["work_units"][0]["filters"]["instrument_id"]
    run_path = tmp_path / "processing_run.yaml"
    run_path.write_text(yaml.safe_dump(run), encoding="utf-8")
    with pytest.raises(ConfigError, match="instrument_id"):
        load_config(run_path)


def test_netcdf_requires_complete_utc_days(tmp_path: Path) -> None:
    rules_path = tmp_path / "quality_rules.yaml"
    rules_path.write_text(yaml.safe_dump(quality_document()), encoding="utf-8")
    run = run_document(
        str(tmp_path / "facts" / "*.parquet"),
        str(tmp_path / "results"),
        start="2025-01-01T00:30:00Z",
    )
    run["output"]["netcdf"] = {"enabled": True, "site": "neiu", "instrument": "wxt536"}
    run_path = tmp_path / "processing_run.yaml"
    run_path.write_text(yaml.safe_dump(run), encoding="utf-8")
    with pytest.raises(ConfigError, match="UTC midnight"):
        load_config(run_path)
