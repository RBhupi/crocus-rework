from __future__ import annotations

from datetime import UTC, datetime

import pytest

from adqat.periods import iter_periods


@pytest.mark.parametrize(
    ("period", "end", "expected"),
    [
        (
            "1h",
            datetime(2025, 1, 1, 1, 30, tzinfo=UTC),
            ["20250101T003000Z_20250101T010000Z", "20250101T010000Z_20250101T013000Z"],
        ),
        (
            "1d",
            datetime(2025, 1, 2, 1, 30, tzinfo=UTC),
            ["20250101T003000Z_20250102T000000Z", "20250102T000000Z_20250102T013000Z"],
        ),
        (
            "all",
            datetime(2025, 1, 2, 1, 30, tzinfo=UTC),
            ["20250101T003000Z_20250102T013000Z"],
        ),
    ],
)
def test_periods_are_aligned_and_clipped(period: str, end: datetime, expected: list[str]) -> None:
    start = datetime(2025, 1, 1, 0, 30, tzinfo=UTC)
    assert [item.id for item in iter_periods(start, end, period)] == expected


def test_monthly_yearly_and_leap_boundaries() -> None:
    monthly = list(
        iter_periods(
            datetime(2024, 2, 28, tzinfo=UTC),
            datetime(2024, 3, 2, tzinfo=UTC),
            "1month",
        )
    )
    assert monthly[0].end == datetime(2024, 3, 1, tzinfo=UTC)
    yearly = list(
        iter_periods(
            datetime(2024, 12, 31, tzinfo=UTC),
            datetime(2025, 1, 2, tzinfo=UTC),
            "1year",
        )
    )
    assert yearly[0].end == datetime(2025, 1, 1, tzinfo=UTC)


def test_rejects_naive_or_reversed_boundaries() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        list(iter_periods(datetime(2025, 1, 1), datetime(2025, 1, 2), "1d"))
    with pytest.raises(ValueError, match="before"):
        list(
            iter_periods(
                datetime(2025, 1, 2, tzinfo=UTC),
                datetime(2025, 1, 1, tzinfo=UTC),
                "1d",
            )
        )
