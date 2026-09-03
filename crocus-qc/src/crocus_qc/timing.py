"""Wall-clock timing for a work unit's major phases.

On the cluster the interesting question is never "how long did the job take" -- SLURM
already reports that -- but "which phase was slow". A day that spends 90 s in
``execute_reduction`` is compute-bound; one that spends 90 s in ``open_session`` is
fighting the filesystem. The two need different fixes, so they are measured separately
and both land in ``_success.json``.

Phases are recorded even when the body raises, so a failed run still shows where its
time went before it died.
"""

from __future__ import annotations

import time
from contextlib import contextmanager
from typing import Iterator


class Stopwatch:
    """Ordered, non-nested wall-clock measurements of named phases."""

    __slots__ = ("_phases",)

    def __init__(self) -> None:
        self._phases: list[tuple[str, float]] = []

    @contextmanager
    def phase(self, name: str) -> Iterator[None]:
        start = time.perf_counter()
        try:
            yield
        finally:
            self._phases.append((name, time.perf_counter() - start))

    def record(self, name: str, seconds: float) -> None:
        """Record a phase measured elsewhere (e.g. by a subprocess)."""
        self._phases.append((name, seconds))

    @property
    def phases(self) -> tuple[tuple[str, float], ...]:
        return tuple(self._phases)

    def total(self) -> float:
        return sum(seconds for _, seconds in self._phases)

    def as_dict(self) -> dict[str, float]:
        """Phase name -> seconds, milliseconds precision, for the provenance record.

        A phase that somehow runs twice is summed rather than silently dropped.
        """
        merged: dict[str, float] = {}
        for name, seconds in self._phases:
            merged[name] = merged.get(name, 0.0) + seconds
        return {name: round(seconds, 3) for name, seconds in merged.items()}

    def table(self) -> str:
        """Human-readable breakdown, slowest phase obvious at a glance."""
        if not self._phases:
            return "(no phases recorded)"
        total = self.total()
        width = max(len(name) for name, _ in self._phases)
        lines = [
            f"  {name:<{width}}  {seconds:8.3f}s  {100 * seconds / total:5.1f}%"
            for name, seconds in self._phases
        ]
        lines.append(f"  {'total':<{width}}  {total:8.3f}s")
        return "\n".join(lines)
