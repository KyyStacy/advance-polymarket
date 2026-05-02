from __future__ import annotations

import time
from dataclasses import dataclass

from ..feeds.price_aggregator import PriceAggregator


@dataclass
class LeadLagReading:
    deviation_bps: float      # signed: (cex - oracle) / oracle * 10000
    cex_price: float
    oracle_price: float
    oracle_age_s: float       # how stale the oracle reading is


class LeadLagDetector:
    """Measures price deviation between fast CEX leg and slow oracle leg.

    A positive deviation means CEX has moved up faster than the oracle has caught
    up — bullish for "Up" if it persists into resolution.
    """

    def __init__(self, agg: PriceAggregator):
        self.agg = agg

    def read(self, now: float | None = None) -> LeadLagReading | None:
        now = now if now is not None else time.time()
        cex = self.agg.latest_cex()
        ora = self.agg.latest_oracle()
        if not cex or not ora:
            return None
        cex_ts, cex_p = cex
        ora_ts, ora_p = ora
        if cex_p <= 0 or ora_p <= 0:
            return None
        bps = (cex_p - ora_p) / ora_p * 1e4
        return LeadLagReading(
            deviation_bps=bps,
            cex_price=cex_p,
            oracle_price=ora_p,
            oracle_age_s=max(0.0, now - ora_ts),
        )
