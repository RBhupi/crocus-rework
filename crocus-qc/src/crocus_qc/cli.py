"""Command-line entry point. ``argparse`` only -- no Click, no plugin registry.

Four subcommands, each answering one operational question:

``run``       produce the 10-second products for one VSN over a range of days (SLURM)
``explain``   show the DuckDB plan for a single day, publishing nothing
``discover``  list the work units present in a dataset (which VSNs, which days)
``profiles``  list the bundled instrument profiles and their variables

A SLURM task is a VSN, not a day, so ``run`` walks a calendar and prints **one JSON
record per line** on **stdout** -- JSONL, greppable, one whole record per line -- with
the phase timing tables on **stderr**. A job's ``--output`` file stays machine-readable
while the operator still sees where the time went in the ``--error`` file.
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
    run_vsn,
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


def _add_io_args(parser: argparse.ArgumentParser) -> None:
    """Where to read and where to write.

    No ``--sensor`` and no ``--profile``: this package reduces the WXT536, so both are
    module constants (see ``config.SENSOR``).
    """
    parser.add_argument("--dataset", required=True, type=Path, help="raw Parquet dataset root")
    parser.add_argument("--config", required=True, type=Path, help="pipeline YAML")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="crocus-qc",
        description="Reduce raw CROCUS observations to a dense 10-second statistical product.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="produce the 10-second product for one VSN's days")
    run.add_argument("--vsn", required=True, help="e.g. W08D")
    run.add_argument(
        "--start",
        type=_iso_date,
        help="first UTC day, inclusive; default: this VSN's earliest day in the dataset",
    )
    run.add_argument(
        "--end",
        type=_iso_date,
        help="last UTC day, inclusive; default: this VSN's latest day in the dataset",
    )
    _add_io_args(run)
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
    explain.add_argument("--vsn", required=True, help="e.g. W08D")
    explain.add_argument("--date", required=True, type=_iso_date, help="UTC day, YYYY-MM-DD")
    _add_io_args(explain)
    explain.add_argument(
        "--analyze",
        action="store_true",
        help="execute the statement and report real timings and cardinalities",
    )

    discover = sub.add_parser(
        "discover", help="list vsn/date work units present in a dataset"
    )
    discover.add_argument("--dataset", required=True, type=Path, help="raw Parquet dataset root")
    discover.add_argument(
        "--vsn",
        nargs="+",
        metavar="VSN",
        help="the VSNs to work on, e.g. --vsn W08D W08E; omit to list every WXT536 VSN",
    )
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
        try:
            units = discover_work_units(
                args.dataset, vsns=args.vsn, start=args.start, end=args.end
            )
        except LookupError as exc:
            # Nothing is written to stdout: `discover > manifest.tsv` must not leave a
            # manifest that is short by exactly the VSN the operator got wrong.
            print(exc, file=sys.stderr)
            return 1
        if not units:
            print(
                f"no work units matched {args.dataset}/{work_unit_pattern()}",
                file=sys.stderr,
            )
            return 1
        return _emit(f"{vsn}\t{day:%Y-%m-%d}" for vsn, day in units)

    with watch.phase("load_config"):
        config = load_config(args.config)
    with watch.phase("load_profile"):
        profile = load_profile(PROFILE)

    if args.command == "explain":
        print(
            explain_work_unit(
                sensor=SENSOR,
                vsn=args.vsn,
                day=args.date,
                dataset_root=args.dataset,
                config=config,
                profile=profile,
                analyze=args.analyze,
            )
        )
        return 0

    if not args.quiet:
        # Startup is paid once for the whole job, so it is reported once, before the
        # calendar starts -- not folded into the first day's numbers.
        print(f"startup timings for {args.vsn}", file=sys.stderr)
        print(watch.table(), file=sys.stderr)

    failed = 0
    for record, day_watch in run_vsn(
        vsn=args.vsn,
        start=args.start,
        end=args.end,
        dataset_root=args.dataset,
        config=config,
        profile=profile,
        force=args.force,
        sql_profile=args.sql_profile,
    ):
        # One whole record per line. A job now spans hundreds of days, so stdout is a
        # stream rather than a document: pretty-printed objects concatenated together
        # are not parseable, and `jq` and `grep` both want a line to be a record.
        print(json.dumps(record, sort_keys=True))
        # Flushed per day so a redirected log stays live for hours-long jobs instead of
        # arriving in 8 KiB blocks.
        sys.stdout.flush()
        failed += record["status"] != "success"
        if not args.quiet:
            print(f"phase timings for {args.vsn} {record['date']}", file=sys.stderr)
            print(day_watch.table(), file=sys.stderr)

    if failed:
        # The job kept going past the bad days, so the exit code is the only thing left
        # that can stop a campaign script from reading this as complete.
        print(f"{args.vsn}: {failed} day(s) failed", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
