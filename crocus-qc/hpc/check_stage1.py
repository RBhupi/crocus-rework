#!/usr/bin/env python3
"""Confirm the WXT536 Stage-1 campaign is complete and ready for Stage 2.

Stage 1 records its own truth in the output tree, not in the logs: each finished
station-day writes ``_success.json`` **last** (an atomic marker), carrying the row count
it verified at write time. An interrupted day leaves a ``10sec.parquet.tmp`` and no
marker. So the readiness check is a walk of the output tree -- no Parquet is opened, no
raw data is touched.

    python hpc/check_stage1.py                       # default v0.2.0 output root
    OUT=/path/to/product python hpc/check_stage1.py  # or override via $OUT
    python hpc/check_stage1.py /path/to/product      # or as an argument
    python hpc/check_stage1.py --deep                # also recount every Parquet in DuckDB

Exit status is 0 only when the campaign is ready for Stage 2; non-zero otherwise, so this
can gate a downstream step.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import defaultdict
from pathlib import Path

# The product tree Stage 1 writes: <root>/<sensor>/<VSN>/<YYYY-MM-DD>/{10sec.parquet,_success.json}
DEFAULT_OUT = (
    "/nfs/gce/projects/crocus-server-admins/data-rework"
    "/crocus-qc-output/wxt536-10sec-v0.2.0"
)
SENSOR = "vaisala-wxt536"
PRODUCT_NAME = "10sec.parquet"
SUCCESS_NAME = "_success.json"

# The 20 stations Stage 2 focuses on. "whichever are available" -- absentees are reported
# by name (a warning, not a failure), because a station with no raw data never produced a
# product and that is expected, not a campaign defect.
FOCUS_VSNS = [
    "W0AD", "W0AB", "W0A5", "W0A4", "W0A3", "W0A2", "W0A1", "W0A0",
    "W09F", "W09E", "W09D", "W08B", "W09B", "W09A", "W099", "W098",
    "W096", "W095", "W08E", "W08D",
]
FOCUS_LABELS = {
    "W0A4": "ATMOS", "W0A1": "HUM", "W0A0": "BIG", "W09E": "SHEDD", "W09D": "DOWN",
    "W08B": "CCICS", "W09B": "CSUN", "W099": "NU", "W098": "ADM", "W096": "UIC",
    "W095": "VLPK", "W08E": "CSU", "W08D": "NEIU",
}


class DayStatus:
    """One station-day, judged from its files alone."""

    __slots__ = ("vsn", "date", "has_marker", "has_parquet", "has_tmp", "rows", "expected", "status")

    def __init__(self, vsn: str, date: str) -> None:
        self.vsn = vsn
        self.date = date
        self.has_marker = False
        self.has_parquet = False
        self.has_tmp = False
        self.rows: int | None = None
        self.expected: int | None = None
        self.status: str | None = None

    @property
    def complete(self) -> bool:
        """A day is complete iff its marker says success and the row count matches."""
        return (
            self.has_marker
            and self.has_parquet
            and self.status == "success"
            and self.rows is not None
            and self.rows == self.expected
        )

    @property
    def problem(self) -> str | None:
        if self.complete:
            return None
        if self.has_tmp and not self.has_marker:
            return "interrupted (.tmp, no marker)"
        if not self.has_marker:
            return "no success marker"
        if not self.has_parquet:
            return "marker but no parquet"
        if self.status != "success":
            return f"status={self.status!r}"
        if self.rows != self.expected:
            return f"short: {self.rows} rows (expected {self.expected})"
        return "unknown"


def scan(root: Path) -> tuple[dict[str, list[DayStatus]], set[str]]:
    """Walk <root>/<sensor>/<VSN>/<date>/ and judge each day. Returns (per-VSN days, versions)."""
    sensor_dir = root / SENSOR
    if not sensor_dir.is_dir():
        sys.exit(f"ERROR: no sensor directory under {root}\n  expected: {sensor_dir}")

    by_vsn: dict[str, list[DayStatus]] = defaultdict(list)
    versions: set[str] = set()

    for vsn_dir in sorted(p for p in sensor_dir.iterdir() if p.is_dir()):
        for day_dir in sorted(p for p in vsn_dir.iterdir() if p.is_dir()):
            day = DayStatus(vsn_dir.name, day_dir.name)
            day.has_parquet = (day_dir / PRODUCT_NAME).is_file()
            day.has_tmp = (day_dir / (PRODUCT_NAME + ".tmp")).is_file()
            marker = day_dir / SUCCESS_NAME
            if marker.is_file():
                day.has_marker = True
                try:
                    rec = json.loads(marker.read_text())
                    day.status = rec.get("status")
                    day.rows = rec.get("output_row_count")
                    day.expected = rec.get("expected_rows")
                    versions.add(f"{rec.get('pipeline_version')}@{rec.get('git_commit')}")
                except (json.JSONDecodeError, OSError) as exc:
                    day.status = f"unreadable marker ({exc})"
            # A directory with neither a marker nor a .tmp nor a parquet is empty noise; skip.
            if day.has_marker or day.has_tmp or day.has_parquet:
                by_vsn[vsn_dir.name].append(day)

    return by_vsn, versions


def deep_verify(by_vsn: dict[str, list[DayStatus]]) -> list[str]:
    """Recount every published Parquet in DuckDB and flag any that disagree with its marker."""
    try:
        import duckdb
    except ImportError:
        return ["--deep requested but duckdb is not importable; skipped"]

    mismatches: list[str] = []
    conn = duckdb.connect()
    for vsn, days in by_vsn.items():
        for d in days:
            if not d.has_parquet:
                continue
            path = str(Path(ROOT) / SENSOR / vsn / d.date / PRODUCT_NAME)
            actual = conn.execute(f"SELECT count(*) FROM read_parquet('{path}')").fetchone()[0]
            if actual != d.rows:
                mismatches.append(f"{vsn}/{d.date}: parquet has {actual}, marker says {d.rows}")
    conn.close()
    return mismatches


def main() -> int:
    global ROOT
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("root", nargs="?", default=os.environ.get("OUT", DEFAULT_OUT),
                        help="product root (default: $OUT or the v0.2.0 path)")
    parser.add_argument("--deep", action="store_true",
                        help="also recount every Parquet in DuckDB (slow; verifies the marker did not lie)")
    args = parser.parse_args()

    ROOT = str(Path(args.root).resolve())
    root = Path(ROOT)
    print(f"Stage 1 status  --  {root.name}")
    print(f"root: {root}")
    if not root.is_dir():
        sys.exit(f"ERROR: root does not exist: {root}")

    by_vsn, versions = scan(root)
    if not by_vsn:
        sys.exit("ERROR: no station-days found under the tree. Campaign has produced nothing.")

    # ---- roll-ups --------------------------------------------------------------------
    all_days = [d for days in by_vsn.values() for d in days]
    complete = [d for d in all_days if d.complete]
    problems = [d for d in all_days if not d.complete]
    interrupted = [d for d in problems if d.has_tmp and not d.has_marker]

    print()
    print(f"stations with output : {len(by_vsn)}")
    print(f"station-days seen     : {len(all_days)}")
    print(f"  complete (8640 rows): {len(complete)}")
    print(f"  problems            : {len(problems)}")
    print(f"    of which .tmp/interrupted: {len(interrupted)}")
    print(f"provenance versions  : {', '.join(sorted(versions)) or 'none'}")
    if len(versions) > 1:
        print("  WARNING: products came from more than one code version/commit.")

    # ---- per-station table -----------------------------------------------------------
    print("\nper-station:")
    print(f"  {'VSN':<6} {'label':<6} {'days':>5} {'ok':>5} {'bad':>4}  {'first':<10}  {'last':<10}")
    for vsn in sorted(by_vsn):
        days = by_vsn[vsn]
        ok = sum(1 for d in days if d.complete)
        bad = len(days) - ok
        dates = sorted(d.date for d in days)
        label = FOCUS_LABELS.get(vsn, "")
        flag = "" if bad == 0 else "  <-- has problems"
        print(f"  {vsn:<6} {label:<6} {len(days):>5} {ok:>5} {bad:>4}  {dates[0]:<10}  {dates[-1]:<10}{flag}")

    # ---- problem detail --------------------------------------------------------------
    if problems:
        print(f"\nproblem days ({len(problems)}):")
        for d in sorted(problems, key=lambda d: (d.vsn, d.date))[:50]:
            print(f"  {d.vsn}/{d.date}: {d.problem}")
        if len(problems) > 50:
            print(f"  ... and {len(problems) - 50} more")

    # ---- focus-node coverage ---------------------------------------------------------
    present = set(by_vsn)
    missing_focus = [v for v in FOCUS_VSNS if v not in present]
    present_focus = [v for v in FOCUS_VSNS if v in present]
    print(f"\nfocus nodes (20): {len(present_focus)} present, {len(missing_focus)} missing")
    if missing_focus:
        pretty = ", ".join(f"{v}({FOCUS_LABELS[v]})" if v in FOCUS_LABELS else v for v in missing_focus)
        print(f"  missing: {pretty}")
    extra = sorted(present - set(FOCUS_VSNS))
    if extra:
        print(f"  also present (not in focus list): {', '.join(extra)}")

    # ---- optional deep verification --------------------------------------------------
    deep_mismatches: list[str] = []
    if args.deep:
        print("\ndeep verify: recounting every Parquet in DuckDB ...")
        deep_mismatches = deep_verify(by_vsn)
        if deep_mismatches:
            print(f"  {len(deep_mismatches)} file(s) disagree with their marker:")
            for line in deep_mismatches[:50]:
                print(f"    {line}")
        else:
            print("  all Parquet row counts match their markers.")

    # ---- verdict ---------------------------------------------------------------------
    reasons: list[str] = []
    if problems:
        reasons.append(f"{len(problems)} station-day(s) are not complete (rerun the campaign to finish them)")
    if deep_mismatches:
        reasons.append(f"{len(deep_mismatches)} Parquet file(s) do not match their marker")
    if len(versions) > 1:
        reasons.append("products span multiple code versions; confirm this is intended")

    print("\n" + "=" * 60)
    if not reasons:
        print("VERDICT: READY for Stage 2")
        print(f"  {len(complete)} complete station-days across {len(by_vsn)} stations,")
        print(f"  {len(present_focus)}/20 focus nodes present, every product is a full 8640-row day.")
        if missing_focus:
            print("  (Missing focus nodes above simply had no raw data; expected.)")
        print("=" * 60)
        return 0
    print("VERDICT: NOT READY for Stage 2")
    for r in reasons:
        print(f"  - {r}")
    print("=" * 60)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
