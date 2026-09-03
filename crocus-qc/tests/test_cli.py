"""CLI behaviour, at the seam ``main(argv)`` -> exit code, stdout, files on disk."""

from __future__ import annotations

import json
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
        [Obs(offset, "aqt.env.temp", 21.0) for offset in range(0, 60, 5)],
    )
    config = tmp_path / "pipeline.yaml"
    config.write_text(
        f"output:\n  root: {tmp_path / 'out'}\n"
        f"execution:\n  threads: 2\n  memory_limit: 1GB\n  temp_dir: {tmp_path / 'scratch'}\n"
    )
    return {"dataset": dataset, "config": config, "out": tmp_path / "out"}


def run_argv(workspace: dict[str, Path], *extra: str) -> int:
    return main(
        [
            "run",
            "--sensor", SENSOR,
            "--vsn", VSN,
            "--date", f"{DAY:%Y-%m-%d}",
            "--dataset", str(workspace["dataset"]),
            "--config", str(workspace["config"]),
            "--profile", "aqt530",
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


def test_explain_reports_a_plan_without_publishing(workspace, capsys):
    exit_code = main(
        [
            "explain",
            "--sensor", SENSOR,
            "--vsn", VSN,
            "--date", f"{DAY:%Y-%m-%d}",
            "--dataset", str(workspace["dataset"]),
            "--config", str(workspace["config"]),
            "--profile", "aqt530",
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
            "--sensor", SENSOR,
            "--vsn", VSN,
            "--date", f"{DAY:%Y-%m-%d}",
            "--dataset", str(workspace["dataset"]),
            "--config", str(workspace["config"]),
            "--profile", "aqt530",
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
    exit_code = main(["discover", "--dataset", str(workspace["dataset"])])

    assert exit_code == 0
    assert capsys.readouterr().out == f"{SENSOR}\t{VSN}\t{DAY:%Y-%m-%d}\n"


def test_profiles_lists_bundled_instruments(capsys):
    assert main(["profiles"]) == 0

    out = capsys.readouterr().out
    assert "aqt530" in out and "wxt536" in out
    assert "wind_direction" in out


def test_a_malformed_date_is_rejected(workspace):
    """A typo'd date must fail loudly, not silently process a different day."""
    with pytest.raises(SystemExit):
        main(
            [
                "run",
                "--sensor", SENSOR,
                "--vsn", VSN,
                "--date", "15-12-2025",
                "--dataset", str(workspace["dataset"]),
                "--config", str(workspace["config"]),
                "--profile", "aqt530",
            ]
        )
