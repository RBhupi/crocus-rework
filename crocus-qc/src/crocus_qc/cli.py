"""Command-line entry point. ``argparse`` only -- no Click, no plugin registry.

Four subcommands, each answering one operational question:

``run``       produce the 10-second product for one work unit (what SLURM invokes)
``explain``   show the DuckDB plan for that same work unit, publishing nothing
``discover``  list the work units present in a dataset (a SLURM array manifest)
``profiles``  list the bundled instrument profiles and their variables

``run`` prints the provenance record as JSON on **stdout** and the phase timing table on
**stderr**, so a SLURM job's ``--output`` file stays machine-readable while the operator
still sees where the time went in the ``--error`` file.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import sys
from collections.abc import Iterable, Iterator
from datetime import date as Date
from datetime import datetime
from pathlib import Path

from .config import PROFILE, PROFILE_DIR, SENSOR, load_config, load_profile
from .pipeline import (
    discover_work_units,
    explain_work_unit,
    run_work_unit,
    work_unit_pattern,
)
from .timing import Stopwatch


def _profile_lines() -> Iterator[str]:
    for path in sorted(PROFILE_DIR.glob("*.yaml")):
        profile = load_profile(path)
        yield f"{path.stem}  ({profile.sensor})"
        for spec in profile.variables:
            yield f"    {spec.name:<26} {spec.measurement:<24} {spec.aggregation}"


def _emit(lines: Iterable[str]) -> int:
    """Write a listing to stdout, tolerating a reader that stops early.

    ``discover`` and ``profiles`` are both meant to be piped into ``head``, ``grep``, or
    ``awk``, any of which may close the pipe before the last line. That is ordinary shell
    behaviour, not an error, so it must not surface as a traceback.
    """
    try:
        for line in lines:
            print(line)
        sys.stdout.flush()
    except BrokenPipeError:
        # The interpreter flushes stdout again on exit and would report a second, more
        # confusing BrokenPipeError from outside our control. Point the file descriptor
        # at the null device so that final flush lands somewhere harmless.
        with contextlib.suppress(AttributeError, OSError, ValueError):
            os.dup2(os.open(os.devnull, os.O_WRONLY), sys.stdout.fileno())
    return 0


def _iso_date(text: str) -> Date:
    try:
        return datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"expected YYYY-MM-DD, got {text!r}") from exc


def _add_work_unit_args(parser: argparse.ArgumentParser) -> None:
    """The two coordinates of a work unit, plus where to read and where to write.

    No ``--sensor`` and no ``--profile``: this package reduces the WXT536, so both are
    module constants (see ``config.SENSOR``).
    """
    parser.add_argument("--vsn", required=True, help="e.g. W08D")
    parser.add_argument("--date", required=True, type=_iso_date, help="UTC day, YYYY-MM-DD")
    parser.add_argument("--dataset", required=True, type=Path, help="raw Parquet dataset root")
    parser.add_argument("--config", required=True, type=Path, help="pipeline YAML")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="crocus-qc",
        description="Reduce raw CROCUS observations to a dense 10-second statistical product.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="produce the 10-second product for one work unit")
    _add_work_unit_args(run)
    run.add_argument(
        "--force", action="store_true", help="recompute even if _success.json is present"
    )
    run.add_argument(
        "--sql-profile",
        action="store_true",
        help="also write DuckDB's per-operator profile to _duckdb_profile.json",
    )
    run.add_argument("--quiet", action="store_true", help="suppress the phase timing table")

    explain = sub.add_parser("explain", help="print the DuckDB query plan; publish nothing")
    _add_work_unit_args(explain)
    explain.add_argument(
        "--analyze",
        action="store_true",
        help="execute the statement and report real timings and cardinalities",
    )

    discover = sub.add_parser(
        "discover", help="list sensor/vsn/date work units present in a dataset"
    )
    discover.add_argument("--dataset", required=True, type=Path, help="raw Parquet dataset root")
    discover.add_argument("--sensor", help="restrict to one sensor")
    discover.add_argument("--vsn", help="restrict to one VSN")
    discover.add_argument("--start", type=_iso_date, help="earliest UTC day, inclusive")
    discover.add_argument("--end", type=_iso_date, help="latest UTC day, inclusive")

    sub.add_parser("profiles", help="list bundled instrument profiles")
    return parser


def main(argv: list[str] | None = None) -> int:
    watch = Stopwatch()
    args = build_parser().parse_args(argv)

    if args.command == "profiles":
        return _emit(_profile_lines())

    if args.command == "discover":
        units = discover_work_units(
            args.dataset, sensor=args.sensor, vsn=args.vsn, start=args.start, end=args.end
        )
        if not units:
            print(
                f"no work units matched {args.dataset}/"
                f"{work_unit_pattern(args.sensor, args.vsn)}",
                file=sys.stderr,
            )
            return 1
        return _emit(f"{sensor}\t{vsn}\t{day:%Y-%m-%d}" for sensor, vsn, day in units)

    with watch.phase("load_config"):
        config = load_config(args.config)
    with watch.phase("load_profile"):
        profile = load_profile(PROFILE)
    common = dict(
        sensor=SENSOR,
        vsn=args.vsn,
        day=args.date,
        dataset_root=args.dataset,
        config=config,
        profile=profile,
    )

    if args.command == "explain":
        print(explain_work_unit(**common, analyze=args.analyze))
        return 0

    record = run_work_unit(
        **common, force=args.force, stopwatch=watch, sql_profile=args.sql_profile
    )
    print(json.dumps(record, indent=2, sort_keys=True))
    if not args.quiet:
        print(f"phase timings for {args.vsn} {args.date:%Y-%m-%d}", file=sys.stderr)
        print(watch.table(), file=sys.stderr)
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
