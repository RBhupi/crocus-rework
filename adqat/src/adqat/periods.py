from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Literal

PeriodName = Literal["1h", "1d", "1month", "1year", "all"]


@dataclass(frozen=True)
class Period:
    start: datetime
    end: datetime

    @property
    def id(self) -> str:
        return f"{_format_boundary(self.start)}_{_format_boundary(self.end)}"


def iter_periods(start: datetime, end: datetime, period: PeriodName) -> Iterator[Period]:
    start = _as_utc(start)
    end = _as_utc(end)
    if start >= end:
        raise ValueError("period range start must be before end")
    if period == "all":
        yield Period(start, end)
        return

    cursor = start
    while cursor < end:
        boundary = _next_boundary(cursor, period)
        period_end = min(boundary, end)
        yield Period(cursor, period_end)
        cursor = period_end


def _next_boundary(value: datetime, period: PeriodName) -> datetime:
    if period == "1h":
        floor = value.replace(minute=0, second=0, microsecond=0)
        return floor + timedelta(hours=1)
    if period == "1d":
        floor = value.replace(hour=0, minute=0, second=0, microsecond=0)
        return floor + timedelta(days=1)
    if period == "1month":
        if value.month == 12:
            return datetime(value.year + 1, 1, 1, tzinfo=UTC)
        return datetime(value.year, value.month + 1, 1, tzinfo=UTC)
    if period == "1year":
        return datetime(value.year + 1, 1, 1, tzinfo=UTC)
    raise ValueError(f"unsupported period {period!r}")


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("period boundaries must be timezone-aware")
    return value.astimezone(UTC)


def _format_boundary(value: datetime) -> str:
    value = _as_utc(value)
    base = value.strftime("%Y%m%dT%H%M%S")
    fraction = f".{value.microsecond:06d}" if value.microsecond else ""
    return f"{base}{fraction}Z"
