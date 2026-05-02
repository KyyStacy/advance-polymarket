from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Iterable, Tuple

import numpy as np


@dataclass
class TimedSeries:
    """Deque of (timestamp_seconds, value) with windowed numpy access."""

    maxlen: int = 600
    _buf: Deque[Tuple[float, float]] = field(default_factory=deque)

    def __post_init__(self) -> None:
        self._buf = deque(maxlen=self.maxlen)

    def push(self, ts: float, value: float) -> None:
        self._buf.append((ts, value))

    def latest(self) -> Tuple[float, float] | None:
        if not self._buf:
            return None
        return self._buf[-1]

    def values_in_window(self, seconds: float, now: float) -> np.ndarray:
        cutoff = now - seconds
        vals = [v for (t, v) in self._buf if t >= cutoff]
        return np.asarray(vals, dtype=np.float64) if vals else np.empty(0)

    def items_in_window(self, seconds: float, now: float) -> Iterable[Tuple[float, float]]:
        cutoff = now - seconds
        return [(t, v) for (t, v) in self._buf if t >= cutoff]

    def __len__(self) -> int:
        return len(self._buf)
