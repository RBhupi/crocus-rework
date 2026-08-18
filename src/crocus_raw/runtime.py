from __future__ import annotations

import hashlib
import os
import re
import subprocess
from pathlib import Path


SUPPORTED_INFLUXD_VERSIONS = {"2.7.11", "2.7.12"}


def validate_influxd(path: Path) -> str:
    if path == Path("."):
        raise FileNotFoundError(
            "--influxd received an empty path; set INFLUXD in this terminal or pass the full binary path"
        )
    if not path.is_file():
        raise FileNotFoundError(f"influxd binary not found: {path}")
    result = subprocess.run(
        [str(path), "version"],
        check=True,
        capture_output=True,
        text=True,
    )
    output = f"{result.stdout}\n{result.stderr}"
    match = re.search(r"\b(?:InfluxDB\s+)?v?(2\.7\.\d+)\b", output, re.IGNORECASE)
    if not match:
        raise ValueError(f"could not determine influxd version from: {output.strip()!r}")
    version = match.group(1)
    if version not in SUPPORTED_INFLUXD_VERSIONS:
        supported = ", ".join(sorted(SUPPORTED_INFLUXD_VERSIONS))
        raise ValueError(f"unsupported influxd {version}; expected one of {supported}")
    return version


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def write_text_atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_name(f".{path.name}.tmp")
    temporary_path.write_text(text)
    os.replace(temporary_path, path)


def describe_error(error: BaseException) -> str:
    stderr = getattr(error, "stderr", None)
    if isinstance(stderr, bytes):
        stderr = stderr.decode("utf-8", errors="replace")
    detail = stderr.strip() if isinstance(stderr, str) else ""
    return f"{error}: {detail}" if detail else str(error)
