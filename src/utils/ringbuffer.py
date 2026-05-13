from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, Tuple

import numpy as np


@dataclass
class TimedSeries:
    """Pre-allocated numpy circular buffer of (timestamp_seconds, value) pairs.

    Key improvements over deque-based implementation:
      - Zero Python-level element iteration: all array ops run in NumPy C layer.
      - O(log n) window queries via np.searchsorted (binary search).
      - arrays_in_window() returns numpy views — no extra allocation for callers
        that work directly with numpy (e.g. MomentumAnalyzer).
      - latest() is O(1) indexed access.
      - _ordered_arrays() uses slice concatenation instead of np.roll() to avoid
        double-allocation on full buffers. Result is cached between pushes so
        multiple callers in the same tick (momentum + leadlag) pay zero cost.

    Thread-safety: NOT thread-safe. All access must be from the asyncio event
    loop thread. Matches the existing single-threaded usage pattern.
    """

    maxlen: int = 600

    # Pre-allocated backing arrays — shape fixed at construction, never reallocated.
    _ts: np.ndarray = field(init=False, repr=False)    # timestamps (float64)
    _vals: np.ndarray = field(init=False, repr=False)  # values (float64)
    _head: int = field(init=False, repr=False)          # next write index (mod maxlen)
    _size: int = field(init=False, repr=False)          # occupied count [0, maxlen]
    # Cache: invalidated on every push(), recomputed lazily on first read.
    _cache_ts: np.ndarray | None = field(init=False, repr=False, default=None)
    _cache_vals: np.ndarray | None = field(init=False, repr=False, default=None)

    def __post_init__(self) -> None:
        self._ts = np.empty(self.maxlen, dtype=np.float64)
        self._vals = np.empty(self.maxlen, dtype=np.float64)
        self._head = 0
        self._size = 0
        self._cache_ts = None
        self._cache_vals = None

    # ------------------------------------------------------------------ write

    def push(self, ts: float, value: float) -> None:
        """Append (ts, value). Oldest item is overwritten when full. O(1)."""
        self._ts[self._head] = ts
        self._vals[self._head] = value
        self._head = (self._head + 1) % self.maxlen
        if self._size < self.maxlen:
            self._size += 1
        # Invalidate ordered cache — next reader will rebuild.
        self._cache_ts = None
        self._cache_vals = None

    # ------------------------------------------------------------------ read

    def latest(self) -> Tuple[float, float] | None:
        """Return the most recently pushed (ts, value), or None if empty. O(1)."""
        if self._size == 0:
            return None
        idx = (self._head - 1) % self.maxlen
        return (float(self._ts[idx]), float(self._vals[idx]))

    def _ordered_arrays(self) -> tuple[np.ndarray, np.ndarray]:
        """Return (ts, vals) in chronological order (oldest first).

        Cached between pushes — multiple callers within the same tick pay
        zero cost after the first call.  Uses slice concatenation instead of
        np.roll to avoid the double-allocation that roll performs internally.
        """
        if self._cache_ts is not None:
            return self._cache_ts, self._cache_vals  # type: ignore[return-value]

        if self._size < self.maxlen:
            ts = self._ts[:self._size]
            vals = self._vals[:self._size]
        else:
            # Full buffer: linearise by concatenating [head:] + [:head].
            # np.concatenate does ONE allocation + two contiguous memcpy calls,
            # vs np.roll which does TWO full-array allocations.
            h = self._head
            ts = np.concatenate((self._ts[h:], self._ts[:h]))
            vals = np.concatenate((self._vals[h:], self._vals[:h]))

        self._cache_ts = ts
        self._cache_vals = vals
        return ts, vals

    def arrays_in_window(
        self, seconds: float, now: float
    ) -> tuple[np.ndarray, np.ndarray]:
        """Return (timestamps, values) arrays for items with ts >= (now - seconds).

        Uses np.searchsorted side='left': the returned slice starts at the first
        timestamp >= cutoff (inclusive boundary). O(log n) via binary search.
        Prefer this over items_in_window() in numeric callers to avoid Python
        tuple overhead.
        """
        if self._size == 0:
            _empty = np.empty(0, dtype=np.float64)
            return _empty, _empty
        ts_ord, vals_ord = self._ordered_arrays()
        idx = int(np.searchsorted(ts_ord, now - seconds - 1e-9, side="right"))
        return ts_ord[idx:], vals_ord[idx:]

    def values_in_window(self, seconds: float, now: float) -> np.ndarray:
        """Return value array for items within [now-seconds, now]. O(log n)."""
        _, vals = self.arrays_in_window(seconds, now)
        return vals

    def items_in_window(
        self, seconds: float, now: float
    ) -> Iterable[Tuple[float, float]]:
        """Legacy: iterable of (ts, value) tuples. Prefer arrays_in_window()."""
        ts, vals = self.arrays_in_window(seconds, now)
        return zip(ts.tolist(), vals.tolist())

    def __len__(self) -> int:
        return self._size
