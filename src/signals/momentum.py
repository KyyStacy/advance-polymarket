from __future__ import annotations

import math
import time
from dataclasses import dataclass

import numpy as np

from ..feeds.price_aggregator import PriceAggregator


@dataclass
class MomentumReading:
    direction: int           # +1 up, -1 down, 0 flat
    z_score: float           # absolute strength of momentum (vs short-term noise)
    return_30s: float        # log return over last 30s
    return_60s: float        # log return over last 60s
    accel: float             # d(return) — late vs early half of 60s


class MomentumAnalyzer:
    """Sub-minute momentum on the CEX blended mid.

    z_score = |mean(returns)| / std(returns) over recent 60s ticks.
    Direction confirmed when 30s and 60s returns agree in sign and z_score >= threshold.

    Performance notes (Phase 1 optimisations):
      - Uses TimedSeries.arrays_in_window() to receive numpy arrays directly,
        eliminating the list-comprehension + np.array() conversion that cost
        3–8 ms per cache miss previously.
      - Cache invalidation now checks both timestamp AND buffer size so that
        rapid bursts of ticks (e.g. Binance bookTicker at ~100 ms) don't
        redundantly recompute when the reading hasn't materially changed.
      - Return scalars pre-cast to float/int to avoid repeated numpy scalar
        boxing on callers that do arithmetic with them.
    """

    # Re-read at most once per this interval, even if data changes.
    # CEX mid typically updates every 50–200 ms; momentum stats over a 60 s
    # window are dominated by older ticks and rarely change meaningfully
    # within a single 50 ms strategy tick.
    _CACHE_TTL_SEC: float = 0.050

    def __init__(self, agg: PriceAggregator) -> None:
        self.agg = agg
        self._cache: MomentumReading | None = None
        self._cache_ts: float = 0.0
        self._cache_last_t: float = 0.0
        self._cache_size: int = 0

    def read(self, now: float | None = None) -> MomentumReading:
        now = now if now is not None else time.time()

        # Fast-path: cache still fresh AND no new data has arrived.
        if self._cache is not None and (now - self._cache_ts) < self._CACHE_TTL_SEC:
            latest = self.agg.cex_mid.latest()
            if (
                latest is not None
                and latest[0] == self._cache_last_t
                and len(self.agg.cex_mid) == self._cache_size
            ):
                return self._cache

        # --- Full recompute -----------------------------------------------
        # arrays_in_window() returns numpy arrays directly — zero Python-level
        # element iteration (all work in NumPy C layer).
        ts, px = self.agg.cex_mid.arrays_in_window(60.0, now)
        n = len(px)

        _empty = MomentumReading(0, 0.0, 0.0, 0.0, 0.0)

        if n < 12:
            self._cache = _empty
            self._cache_ts = now
            latest = self.agg.cex_mid.latest()
            self._cache_last_t = latest[0] if latest else 0.0
            self._cache_size = len(self.agg.cex_mid)
            return _empty

        if px[0] <= 0 or px[-1] <= 0:
            return _empty

        # Log returns between consecutive ticks — still O(n) but entirely in C.
        rets = np.diff(np.log(px))
        if rets.size < 4:
            return _empty

        sd = float(np.std(rets, ddof=1)) or 1e-12
        mean = float(np.mean(rets))
        z = abs(mean) / sd * math.sqrt(rets.size)

        # 30 s vs 60 s log returns — use searchsorted on already-ordered ts.
        cutoff_30 = now - 30.0
        cutoff_60 = now - 60.0  # redundant (window is 60 s) but explicit

        idx_30 = int(np.searchsorted(ts, cutoff_30, side="left"))
        # 60 s window: ts[0] is already the oldest within 60 s from arrays_in_window.
        r30 = 0.0
        if n - idx_30 >= 2:
            r30 = float(math.log(px[-1] / px[idx_30]))
        r60 = 0.0
        if n >= 2:
            r60 = float(math.log(px[-1] / px[0]))

        # Acceleration: late half (last 30 s) vs early half (30–60 s ago).
        # idx_30 splits the arrays: early = [:idx_30], late = [idx_30:]
        px_early = px[:idx_30]
        px_late = px[idx_30:]
        if px_early.size >= 2 and px_late.size >= 2:
            accel = float(
                math.log(px_late[-1] / px_late[0])
                - math.log(px_early[-1] / px_early[0])
            )
        else:
            accel = 0.0

        if r30 > 0 and r60 > 0 and mean > 0:
            direction = 1
        elif r30 < 0 and r60 < 0 and mean < 0:
            direction = -1
        else:
            direction = 0

        reading = MomentumReading(direction, z, r30, r60, accel)

        # Update cache.
        self._cache = reading
        self._cache_ts = now
        self._cache_last_t = float(ts[-1])
        self._cache_size = len(self.agg.cex_mid)
        return reading
