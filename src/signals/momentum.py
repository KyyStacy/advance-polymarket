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
    """

    def __init__(self, agg: PriceAggregator):
        self.agg = agg

    def read(self, now: float | None = None) -> MomentumReading:
        now = now if now is not None else time.time()

        items = list(self.agg.cex_mid.items_in_window(60.0, now))
        if len(items) < 12:
            return MomentumReading(0, 0.0, 0.0, 0.0, 0.0)

        ts = np.array([t for t, _ in items])
        px = np.array([p for _, p in items])

        if px[0] <= 0 or px[-1] <= 0:
            return MomentumReading(0, 0.0, 0.0, 0.0, 0.0)

        # log returns between consecutive ticks
        rets = np.diff(np.log(px))
        if rets.size < 4:
            return MomentumReading(0, 0.0, 0.0, 0.0, 0.0)
        sd = float(np.std(rets, ddof=1)) or 1e-12
        mean = float(np.mean(rets))
        z = abs(mean) / sd * math.sqrt(rets.size)

        # 30s vs 60s log returns
        def ret_over(secs: float) -> float:
            cutoff = now - secs
            mask = ts >= cutoff
            if mask.sum() < 2:
                return 0.0
            return float(math.log(px[mask][-1] / px[mask][0]))

        r30 = ret_over(30.0)
        r60 = ret_over(60.0)

        # Acceleration: late half (last 30s) vs early half (30-60s ago)
        cutoff_mid = now - 30.0
        cutoff_old = now - 60.0
        early = px[(ts >= cutoff_old) & (ts < cutoff_mid)]
        late = px[ts >= cutoff_mid]
        if early.size >= 2 and late.size >= 2:
            r_early = math.log(early[-1] / early[0])
            r_late = math.log(late[-1] / late[0])
            accel = r_late - r_early
        else:
            accel = 0.0

        if r30 > 0 and r60 > 0 and mean > 0:
            direction = 1
        elif r30 < 0 and r60 < 0 and mean < 0:
            direction = -1
        else:
            direction = 0

        return MomentumReading(direction, z, r30, r60, accel)
