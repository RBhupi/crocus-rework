from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import polars as pl

from adqat.store import open_existing_run


class ReportError(RuntimeError):
    """Raised when persisted run evidence cannot be summarized."""


def build_run_report(run_dir: str | Path) -> dict[str, Any]:
    existing = open_existing_run(run_dir)
    periods = existing.store.list_period_dirs()
    if not periods:
        raise ReportError("run contains no persisted periods")
    frames = [pl.read_parquet(path / "check_results.parquet") for path in periods]
    checks = pl.concat(frames, how="vertical_relaxed")
    grouped = (
        checks.group_by("variable", "check_id", "flag_name", "bit", maintain_order=True)
        .agg(
            pl.col("units_tested").sum(),
            pl.col("units_passed").sum(),
            pl.col("units_failed").sum(),
        )
        .with_columns(
            pl.when(pl.col("units_tested") > 0)
            .then(pl.col("units_failed") / pl.col("units_tested"))
            .otherwise(0.0)
            .alias("fraction_failed")
        )
        .sort("variable", "bit", "check_id")
    )
    successes = []
    for period in periods:
        try:
            successes.append(json.loads((period / "success.json").read_text(encoding="utf-8")))
        except (OSError, json.JSONDecodeError) as error:
            raise ReportError(f"invalid success marker in {period}: {error}") from error
    return {
        "run_id": existing.store.run_id,
        "work_unit_id": existing.store.work_unit_id,
        "config_hash": existing.config.config_hash,
        "rule_status": (
            existing.config.rules.metadata.status
            if existing.config.rules.metadata is not None
            else "unspecified"
        ),
        "periods": len(periods),
        "rows_processed": sum(int(item["rows_processed"]) for item in successes),
        "findings": sum(int(item["findings"]) for item in successes),
        "netcdf_files": [
            str(period / item["netcdf_file"])
            for period, item in zip(periods, successes, strict=True)
            if item.get("netcdf_file")
        ],
        "checks": grouped.to_dicts(),
    }


def format_run_report(report: dict[str, Any]) -> str:
    lines = [
        f"run_id: {report['run_id']}",
        f"work_unit_id: {report['work_unit_id']}",
        f"rule_status: {report['rule_status']}",
        f"periods: {report['periods']}",
        f"rows_processed: {report['rows_processed']}",
        f"findings: {report['findings']}",
        f"netcdf_files: {len(report['netcdf_files'])}",
        "",
        "variable\tcheck_id\tflag\tbit\ttested\tfailed\tfraction_failed",
    ]
    for check in report["checks"]:
        lines.append(
            "\t".join(
                [
                    str(check["variable"]),
                    str(check["check_id"]),
                    str(check["flag_name"]),
                    str(check["bit"]),
                    str(check["units_tested"]),
                    str(check["units_failed"]),
                    f"{float(check['fraction_failed']):.8f}",
                ]
            )
        )
    if report["netcdf_files"]:
        lines.extend(["", "NetCDF:", *[str(path) for path in report["netcdf_files"]]])
    return "\n".join(lines)
