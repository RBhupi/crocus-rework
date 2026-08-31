#!/usr/bin/env python3
"""Generate and optionally run the CROCUS WXT/AQT full-history minute campaign."""

from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import yaml

INSTRUMENTS = {
    "wxt": {
        "sensor": "vaisala-wxt536",
        "model": "wxt536",
        "profile": "crocus_wxt536_pilot",
        "inventory": "wxt_vsn_instrument_coverage.csv",
    },
    "aqt": {
        "sensor": "vaisala-aqt530",
        "model": "aqt530",
        "profile": "crocus_aqt530_pilot",
        "inventory": "aqt_vsn_instrument_coverage.csv",
    },
}


def build_parser() -> argparse.ArgumentParser:
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--kind", choices=("wxt", "aqt", "all"), default="all")
    parser.add_argument("--campaign-id", default="minute-full-v1")
    parser.add_argument("--max-parallel", type=int, default=4)
    parser.add_argument("--adqat", default="adqat", help="path to the adqat executable")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument(
        "--inventory-dir", type=Path, default=root / "wxt-aqt-production-v5-report"
    )
    parser.add_argument(
        "--rules", type=Path, default=root / "examples/quality_rules.crocus_wxt_aqt_pilot.yaml"
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    if arguments.max_parallel < 1:
        raise SystemExit("--max-parallel must be at least 1")
    dataset_root = arguments.dataset_root.expanduser().resolve()
    output_root = arguments.output_root.expanduser().resolve()
    rules_path = arguments.rules.expanduser().resolve()
    if _overlap(dataset_root, output_root):
        raise SystemExit("output root must not overlap the read-only dataset root")
    campaign_root = output_root / "campaigns" / arguments.campaign_id
    config_root = campaign_root / "configs"
    log_root = campaign_root / "logs"
    config_root.mkdir(parents=True, exist_ok=True)
    log_root.mkdir(parents=True, exist_ok=True)

    kinds = tuple(INSTRUMENTS) if arguments.kind == "all" else (arguments.kind,)
    jobs: list[dict[str, str]] = []
    for kind in kinds:
        definition = INSTRUMENTS[kind]
        inventory_path = arguments.inventory_dir / definition["inventory"]
        for row in _read_inventory(inventory_path):
            job = _write_job(
                kind,
                definition,
                row,
                dataset_root,
                output_root,
                rules_path,
                config_root,
                arguments.campaign_id,
            )
            jobs.append(job)
    manifest = {
        "campaign_id": arguments.campaign_id,
        "dataset_root": str(dataset_root),
        "output_root": str(output_root),
        "rules": str(rules_path),
        "jobs": jobs,
    }
    (campaign_root / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(f"generated {len(jobs)} job configuration(s) in {config_root}")
    if not arguments.execute:
        print("generation only; add --execute after reviewing the manifest and YAML files")
        return 0

    failures: list[tuple[str, int]] = []
    with ThreadPoolExecutor(max_workers=arguments.max_parallel) as executor:
        futures = {
            executor.submit(_execute_job, job, arguments.adqat, output_root, log_root): job
            for job in jobs
        }
        for future in as_completed(futures):
            job = futures[future]
            return_code = future.result()
            state = "PASS" if return_code == 0 else f"FAIL({return_code})"
            print(f"{state} {job['run_id']}", flush=True)
            if return_code != 0:
                failures.append((job["run_id"], return_code))
    if failures:
        print(f"campaign completed with {len(failures)} failed job(s)", file=sys.stderr)
        return 1
    print(f"campaign completed successfully: {len(jobs)} job(s)")
    return 0


def _read_inventory(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    required = {"vsn", "instrument_id", "start_time_utc", "end_time_utc"}
    if not rows or not required.issubset(rows[0]):
        raise ValueError(f"inventory {path} is empty or lacks {sorted(required)}")
    seen: set[str] = set()
    for row in rows:
        if row["vsn"] in seen:
            raise ValueError(f"inventory {path} has multiple instruments for VSN {row['vsn']}")
        seen.add(row["vsn"])
    return rows


def _write_job(
    kind: str,
    definition: dict[str, str],
    row: dict[str, str],
    dataset_root: Path,
    output_root: Path,
    rules_path: Path,
    config_root: Path,
    campaign_id: str,
) -> dict[str, str]:
    vsn = row["vsn"]
    model = definition["model"]
    work_unit = f"{vsn.lower()}_{model}_full_history"
    run_id = f"{campaign_id}-{model}-{vsn.lower()}"
    start = date.fromisoformat(row["start_time_utc"][:10])
    end = date.fromisoformat(row["end_time_utc"][:10]) + timedelta(days=1)
    source = (
        dataset_root
        / "facts"
        / f"sensor={definition['sensor']}"
        / f"vsn={vsn}"
        / "**"
        / "date=*"
        / "part-*.parquet"
    )
    document: dict[str, Any] = {
        "schema_version": 1,
        "source": {
            "type": "parquet",
            "path": str(source),
            "options": {"hive_partitioning": True, "union_by_name": True},
            "time": {"column": "time", "timezone": "UTC"},
            "observation_keys": ["time", "series_id"],
        },
        "quality": {"rules": str(rules_path), "pipeline": "basic_qc"},
        "selection": {
            "start": f"{start.isoformat()}T00:00:00Z",
            "end": f"{end.isoformat()}T00:00:00Z",
        },
        "processing": {"period": "1d", "aggregation": "1minute"},
        "work_units": [
            {
                "id": work_unit,
                "profile": definition["profile"],
                "filters": {
                    "sensor": definition["sensor"],
                    "vsn": vsn,
                    "instrument_id": row["instrument_id"],
                },
            }
        ],
        "output": {"root": str(output_root)},
    }
    config_path = config_root / f"{model}-{vsn}.yaml"
    config_path.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")
    return {
        "kind": kind,
        "vsn": vsn,
        "instrument_id": row["instrument_id"],
        "work_unit_id": work_unit,
        "run_id": run_id,
        "config": str(config_path),
        "selection_start": document["selection"]["start"],
        "selection_end": document["selection"]["end"],
    }


def _execute_job(job: dict[str, str], executable: str, output_root: Path, log_root: Path) -> int:
    run_dir = output_root / "runs" / job["run_id"]
    command = (
        [executable, "resume", str(run_dir)]
        if run_dir.exists()
        else [
            executable,
            "run",
            job["config"],
            "--work-unit",
            job["work_unit_id"],
            "--run-id",
            job["run_id"],
        ]
    )
    with (log_root / f"{job['run_id']}.log").open("a", encoding="utf-8") as log:
        log.write(f"command: {' '.join(command)}\n")
        log.flush()
        completed = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, check=False)
    return completed.returncode


def _overlap(first: Path, second: Path) -> bool:
    return _contains(first, second) or _contains(second, first)


def _contains(parent: Path, child: Path) -> bool:
    try:
        child.relative_to(parent)
    except ValueError:
        return False
    return True


if __name__ == "__main__":
    raise SystemExit(main())
