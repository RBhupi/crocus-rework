from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from adqat.config import LoadedConfig, load_config, snapshot_documents
from adqat.report import build_run_report, format_run_report
from adqat.runner import RunSummary, compile_run, resume_run, run_new, validate_configuration


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="adqat",
        description="Automated Data Quality Assessment for Time-Series",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    validate = commands.add_parser("validate", help="validate configuration and source schema")
    validate.add_argument("processing_run", type=Path)

    config = commands.add_parser("config", help="inspect resolved configuration")
    config_commands = config.add_subparsers(dest="config_command", required=True)
    show = config_commands.add_parser("show", help="show resolved configuration")
    show.add_argument("processing_run", type=Path)

    run = commands.add_parser("run", help="start a new one-work-unit run")
    run.add_argument("processing_run", type=Path)
    run.add_argument("--work-unit", required=True)
    run.add_argument("--run-id")

    resume = commands.add_parser("resume", help="resume an existing run")
    resume.add_argument("run_dir", type=Path)

    compile_parser = commands.add_parser("compile", help="recompile QC bits from findings")
    compile_parser.add_argument("run_dir", type=Path)
    compile_parser.add_argument("--period")

    report = commands.add_parser("report", help="summarize persisted check evidence")
    report.add_argument("run_dir", type=Path)
    report.add_argument("--json", action="store_true", dest="as_json")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    arguments = parser.parse_args(argv)
    try:
        if arguments.command == "validate":
            loaded = validate_configuration(arguments.processing_run)
            print(
                f"valid: {len(loaded.run.work_units)} work unit(s), "
                f"pipeline {loaded.run.quality.pipeline!r}"
            )
            _print_sampling_notices(loaded)
            return 0
        if arguments.command == "config":
            loaded = load_config(arguments.processing_run)
            rules, run = snapshot_documents(loaded)
            document: dict[str, Any] = {"quality_rules": rules, "processing_run": run}
            if loaded.aggregate_rules is not None:
                document["aggregate_quality_rules"] = loaded.aggregate_rules.model_dump(mode="json")
            print(json.dumps(document, indent=2))
            return 0
        if arguments.command == "run":
            summary = run_new(
                arguments.processing_run,
                arguments.work_unit,
                arguments.run_id,
            )
            _print_summary(summary)
            return 0
        if arguments.command == "resume":
            summary = resume_run(arguments.run_dir)
            _print_summary(summary)
            return 0
        if arguments.command == "compile":
            count = compile_run(arguments.run_dir, arguments.period)
            print(f"compiled {count} period(s)")
            return 0
        if arguments.command == "report":
            report = build_run_report(arguments.run_dir)
            print(json.dumps(report, indent=2) if arguments.as_json else format_run_report(report))
            return 0
    except Exception as error:
        print(f"adqat: error: {error}", file=sys.stderr)
        return 1
    parser.error("unhandled command")
    return 2


def _print_summary(summary: RunSummary) -> None:
    print(f"run directory: {summary.run_dir}")
    print(
        f"processed={summary.processed_periods} skipped={summary.skipped_periods} "
        f"empty={summary.empty_periods} findings={summary.findings} "
        f"flagged_observations={summary.flagged_observations}"
    )
    if summary.aggregate_rows:
        print(
            f"aggregate_rows={summary.aggregate_rows} "
            f"missing_aggregate_rows={summary.missing_aggregate_rows} "
            f"flagged_aggregate_rows={summary.flagged_aggregate_rows}"
        )
    for warning in summary.warnings:
        print(f"warning: {warning}", file=sys.stderr)


def _print_sampling_notices(loaded: LoadedConfig) -> None:
    profiles = {
        work_unit.profile
        for work_unit in loaded.run.work_units
        if loaded.rules.profiles[work_unit.profile].sampling is not None
    }
    for profile in sorted(profiles):
        print(
            f"notice: profile {profile!r} records sampling cadence; "
            "temporal coverage QC is disabled in Version 1",
            file=sys.stderr,
        )


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
