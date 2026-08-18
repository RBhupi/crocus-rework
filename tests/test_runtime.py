from pathlib import Path

import pytest

from crocus_raw.runtime import validate_influxd


def _binary(path: Path, version: str) -> Path:
    path.write_text(f'#!/bin/sh\necho "InfluxDB v{version}"\n')
    path.chmod(0o755)
    return path


def test_supported_influxd_version(tmp_path):
    assert validate_influxd(_binary(tmp_path / "influxd", "2.7.12")) == "2.7.12"


def test_unsupported_influxd_version(tmp_path):
    with pytest.raises(ValueError, match="unsupported"):
        validate_influxd(_binary(tmp_path / "influxd", "2.7.10"))


def test_empty_influxd_path_has_actionable_error():
    with pytest.raises(FileNotFoundError, match="empty path"):
        validate_influxd(Path("."))
