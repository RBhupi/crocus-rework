"""Per-work-unit provenance, written as one small JSON file.

There is deliberately no central job database: a work unit is one sensor/VSN/day, and a
file next to its own output is enough to answer "did this run, with what, and when".
That keeps thousands of concurrent SLURM jobs from contending on shared state.

``_success.json`` is written **last** and is therefore the idempotency gate: if it
exists, the product beside it is complete.
"""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from typing import Any

SUCCESS_NAME = "_success.json"


def git_commit(repo: Path | None = None) -> str | None:
    """Short commit of the checkout the code is running from, if it is a git tree."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=str(repo or Path(__file__).resolve().parent),
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return result.stdout.strip() or None


def write_json_atomic(payload: dict[str, Any], path: Path) -> Path:
    """Write JSON to ``path`` via a temp file and a POSIX rename.

    A reader therefore sees either no file or a complete one, never a half-written
    marker that would make a failed run look successful.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    os.replace(tmp, path)
    return path


def read_provenance(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())
