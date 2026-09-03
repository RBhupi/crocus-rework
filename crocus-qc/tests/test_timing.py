"""``Stopwatch`` behaviour, at the seam ``phase()`` in -> ``as_dict()``/``table()`` out.

Assertions never compare against a wall-clock literal: a test that asserts a phase took
0.05 s is a test that fails on a loaded cluster node. What matters is that the right
phases are recorded, in order, with the right relative ordering and totals.
"""

from __future__ import annotations

import time

import pytest

from crocus_qc.timing import Stopwatch


def test_phases_are_recorded_in_the_order_they_ran():
    watch = Stopwatch()

    with watch.phase("first"):
        pass
    with watch.phase("second"):
        pass

    assert [name for name, _ in watch.phases] == ["first", "second"]


def test_a_slower_phase_measures_longer_than_a_faster_one():
    """The whole point of the breakdown is telling slow phases from fast ones."""
    watch = Stopwatch()

    with watch.phase("slow"):
        time.sleep(0.05)
    with watch.phase("fast"):
        pass

    timings = watch.as_dict()
    assert timings["slow"] > timings["fast"]


def test_a_phase_that_raises_is_still_measured():
    """A failed run must still show where its time went before it died."""
    watch = Stopwatch()

    with pytest.raises(ZeroDivisionError):
        with watch.phase("doomed"):
            1 / 0

    assert "doomed" in watch.as_dict()


def test_repeated_phase_names_are_summed_not_dropped():
    watch = Stopwatch()
    watch.record("scan", 1.0)
    watch.record("scan", 0.5)

    assert watch.as_dict()["scan"] == 1.5


def test_the_table_names_every_phase_and_a_total():
    watch = Stopwatch()
    watch.record("open_session", 2.0)
    watch.record("execute_reduction", 8.0)

    table = watch.table()

    assert "open_session" in table
    assert "execute_reduction" in table
    assert "10.000s" in table  # the total
    assert "80.0%" in table  # execute_reduction's share


def test_an_unused_stopwatch_reports_no_phases_rather_than_dividing_by_zero():
    assert Stopwatch().table() == "(no phases recorded)"
