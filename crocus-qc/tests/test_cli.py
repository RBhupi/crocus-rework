"""CLI behaviour, at the seam ``main(argv)`` -> exit code, stdout, files on disk."""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

from crocus_qc.cli import main
from crocus_qc.pipeline import PRODUCT_NAME
from crocus_qc.provenance import SUCCESS_NAME

from conftest import DAY, SENSOR, VSN, Obs, write_raw


@pytest.fixture
def workspace(tmp_path: Path) -> dict[str, Path]:
    dataset = write_raw(
        tmp_path / "raw",
        [Obs(offset, "wxt.env.temp", 21.0) for offset in range(0, 60, 5)],
    )
    config = tmp_path / "pipeline.yaml"
    config.write_text(
        f"output:\n  root: {tmp_path / 'out'}\n"
        f"execution:\n  threads: 2\n  memory_limit: 1GB\n  temp_dir: {tmp_path / 'scratch'}\n"
    )
    return {"dataset": dataset, "config": config, "out": tmp_path / "out"}


def run_argv(workspace: dict[str, Path], *extra: str) -> int:
    """One job, one VSN, a window of days.

    There is no ``--sensor`` and no ``--profile``. This package reduces the WXT536 and
    nothing else, so both flags could only ever take one value -- and passing them
    separately is how a transposed pair produces an all-NULL product that still writes
    ``_success.json``.
    """
    return main(
        [
            "run",
            "--vsn", VSN,
            "--start", f"{DAY:%Y-%m-%d}",
            "--end", f"{DAY:%Y-%m-%d}",
            "--dataset", str(workspace["dataset"]),
            "--config", str(workspace["config"]),
            *extra,
        ]
    )


def test_run_produces_the_product(workspace):
    assert run_argv(workspace) == 0

    directory = workspace["out"] / SENSOR / VSN / f"{DAY:%Y-%m-%d}"
    assert (directory / PRODUCT_NAME).is_file()
    assert (directory / SUCCESS_NAME).is_file()


def test_run_prints_the_provenance_record(workspace, capsys):
    run_argv(workspace)

    record = json.loads(capsys.readouterr().out)
    assert record["status"] == "success"
    assert record["output_row_count"] == 8640


def test_run_covers_every_day_in_the_window(tmp_path, capsys):
    """A job is a VSN, not a day: SLURM gets one task per station, not per station-day.

    ~7,000 one-day processes is 7,000 interpreter and DuckDB startups to schedule and
    log. One process per VSN walks its own calendar.
    """
    days = [datetime(2025, 12, d, tzinfo=timezone.utc) for d in (15, 16, 17)]
    dataset = tmp_path / "raw"
    for day in days:
        write_raw(dataset, [Obs(0.0, "wxt.env.temp", 21.0)], day=day)
    config = tmp_path / "pipeline.yaml"
    config.write_text(
        f"output:\n  root: {tmp_path / 'out'}\n"
        f"execution:\n  threads: 2\n  memory_limit: 1GB\n  temp_dir: {tmp_path / 'scratch'}\n"
    )

    exit_code = main(
        [
            "run",
            "--vsn", VSN,
            "--start", "2025-12-15",
            "--end", "2025-12-17",
            "--dataset", str(dataset),
            "--config", str(config),
        ]
    )

    assert exit_code == 0
    produced = sorted(p.name for p in (tmp_path / "out" / SENSOR / VSN).iterdir())
    assert produced == ["2025-12-15", "2025-12-16", "2025-12-17"]


def test_run_prints_one_json_record_per_line(tmp_path, capsys):
    """One record per day, one line each, so a 600-day log stays greppable.

    Pretty-printed objects concatenated across days are not parseable as a stream;
    ``jq`` and ``grep`` both want a line to be a whole record.
    """
    dataset = tmp_path / "raw"
    for day in (14, 15):
        write_raw(
            dataset,
            [Obs(0.0, "wxt.env.temp", 21.0)],
            day=datetime(2025, 12, day, tzinfo=timezone.utc),
        )
    config = tmp_path / "pipeline.yaml"
    config.write_text(
        f"output:\n  root: {tmp_path / 'out'}\n"
        f"execution:\n  threads: 2\n  memory_limit: 1GB\n  temp_dir: {tmp_path / 'scratch'}\n"
    )

    main(
        [
            "run",
            "--vsn", VSN,
            "--start", "2025-12-14",
            "--end", "2025-12-15",
            "--dataset", str(dataset),
            "--config", str(config),
            "--quiet",
        ]
    )

    lines = capsys.readouterr().out.splitlines()
    assert [json.loads(line)["date"] for line in lines] == ["2025-12-14", "2025-12-15"]


def test_explain_reports_a_plan_without_publishing(workspace, capsys):
    exit_code = main(
        [
            "explain",
            "--vsn", VSN,
            "--date", f"{DAY:%Y-%m-%d}",
            "--dataset", str(workspace["dataset"]),
            "--config", str(workspace["config"]),
        ]
    )

    assert exit_code == 0
    plan = capsys.readouterr().out.lower()
    assert "physical_plan" in plan
    assert "copy_to_file" in plan
    assert not workspace["out"].exists()


def test_explain_analyze_executes_without_publishing(workspace, capsys):
    main(
        [
            "explain", "--analyze",
            "--vsn", VSN,
            "--date", f"{DAY:%Y-%m-%d}",
            "--dataset", str(workspace["dataset"]),
            "--config", str(workspace["config"]),
        ]
    )

    assert not workspace["out"].exists()


def test_the_phase_breakdown_goes_to_stderr_not_stdout(workspace, capsys):
    """stdout must stay parseable JSON; the operator reads timings in the SLURM error log."""
    run_argv(workspace)

    captured = capsys.readouterr()
    json.loads(captured.out)
    assert "execute_reduction" in captured.err
    assert "load_config" in captured.err


def test_quiet_suppresses_the_phase_breakdown(workspace, capsys):
    run_argv(workspace, "--quiet")

    assert capsys.readouterr().err == ""


def test_discover_lists_work_units_as_a_tab_separated_manifest(workspace, capsys):
    """A work unit is a VSN and a day, so a manifest row is a VSN and a day.

    The sensor column is gone with ``--sensor``: it would repeat the same constant on
    every line, and run_manifest.sh would have to skip a field it can never use.
    """
    exit_code = main(["discover", "--dataset", str(workspace["dataset"])])

    assert exit_code == 0
    assert capsys.readouterr().out == f"{VSN}\t{DAY:%Y-%m-%d}\n"


def test_discover_lists_every_named_vsn(tmp_path, capsys):
    """The operator names the VSNs to work on, and gets exactly those back."""
    root = tmp_path / "raw"
    for vsn in ("W08D", "W08E", "W096"):
        write_raw(root, [Obs(0, "wxt.env.temp", 20.0)], vsn=vsn)

    assert main(["discover", "--dataset", str(root), "--vsn", "W08D", "W08E"]) == 0

    assert capsys.readouterr().out == (
        f"W08D\t{DAY:%Y-%m-%d}\n"
        f"W08E\t{DAY:%Y-%m-%d}\n"
    )


def test_discover_without_vsns_lists_them_all(tmp_path, capsys):
    """Omitting ``--vsn`` is the exploratory case: what is in this dataset at all?"""
    root = tmp_path / "raw"
    for vsn in ("W08D", "W08E"):
        write_raw(root, [Obs(0, "wxt.env.temp", 20.0)], vsn=vsn)

    assert main(["discover", "--dataset", str(root)]) == 0

    assert capsys.readouterr().out.count("\n") == 2


def test_discover_fails_loudly_when_nothing_matches(tmp_path, capsys):
    """Silence cost a debugging session once already.

    ``discover`` globs directories, so a wrong ``--dataset`` produces exactly the same
    empty output as a real dataset with no days in range. Downstream that is worse than
    a crash: ``run_manifest.sh`` on an empty manifest processes nothing and exits 0,
    which reads as success.
    """
    exit_code = main(["discover", "--dataset", str(tmp_path / "not-a-dataset")])

    assert exit_code != 0
    assert "facts/sensor=" in capsys.readouterr().err


def test_discover_names_the_vsn_that_matched_nothing(tmp_path, capsys):
    """A typo'd VSN must not just shorten the manifest.

    The VSN list is what drives the whole campaign now. If W08E is silently dropped
    because it was mistyped, the run completes, every unit succeeds, and the campaign
    is quietly missing an entire station -- which nothing downstream can detect. The
    other VSNs are real, so the error has to say which one was not.
    """
    root = tmp_path / "raw"
    write_raw(root, [Obs(0, "wxt.env.temp", 20.0)], vsn="W08D")

    exit_code = main(["discover", "--dataset", str(root), "--vsn", "W08D", "NOPE"])

    assert exit_code != 0
    captured = capsys.readouterr()
    assert "NOPE" in captured.err
    assert "W08D" not in captured.err
    # Nothing on stdout: `discover > manifest.tsv` must not leave a short manifest
    # on disk that a later run would happily process.
    assert captured.out == ""


def test_discover_survives_a_reader_that_closes_the_pipe(workspace, monkeypatch):
    """``discover | head`` is the documented way to eyeball a manifest."""

    class ClosedPipe:
        def write(self, _text: str) -> int:
            raise BrokenPipeError

        def flush(self) -> None:
            raise BrokenPipeError

    monkeypatch.setattr(sys, "stdout", ClosedPipe())

    assert main(["discover", "--dataset", str(workspace["dataset"])]) == 0


def test_profiles_lists_bundled_instruments(capsys):
    assert main(["profiles"]) == 0

    out = capsys.readouterr().out
    assert "wxt536" in out
    assert "wind_direction" in out


def test_a_malformed_date_is_rejected(workspace):
    """A typo'd date must fail loudly, not silently process a different day."""
    with pytest.raises(SystemExit):
        main(
            [
                "run",
                "--vsn", VSN,
                "--start", "15-12-2025",
                "--end", "15-12-2025",
                "--dataset", str(workspace["dataset"]),
                "--config", str(workspace["config"]),
            ]
        )
