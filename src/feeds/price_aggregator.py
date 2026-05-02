from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Optional

from ..utils.ringbuffer import TimedSeries


@dataclass
class PriceAggregator:
    """Maintains synchronized BTC views from CEX leg and oracle leg.

    Role isolation:
      - cex_mid: blended CEX mid (avg of binance + coinbase if both fresh;
        else freshest).  Used ONLY as a secondary comparison indicator for
        momentum analysis and the lead-lag algorithm (cex_p - ora_p).
        Never used as anchor or oracle substitute.
      - oracle: Chainlink RTDS price reference — single source of truth for
        resolution pricing.  Stored here for lead-lag comparison; the
        authoritative candle-open anchor is managed by PolymarketRtdsFeed
        and locked by TradingBot._try_lock_anchor().

    Provides time-series for momentum + lead-lag analyzers.
    """

    fresh_seconds: float = 5.0  # how recent a tick must be to be "fresh"
    binance: TimedSeries = field(default_factory=lambda: TimedSeries(maxlen=900))
    coinbase: TimedSeries = field(default_factory=lambda: TimedSeries(maxlen=900))
    cex_mid: TimedSeries = field(default_factory=lambda: TimedSeries(maxlen=900))
    oracle: TimedSeries = field(default_factory=lambda: TimedSeries(maxlen=300))

    def on_cex(self, source: str, ts: float, mid: float, last_trade: float | None) -> None:
        if source == "binance":
            self.binance.push(ts, mid)
        elif source == "coinbase":
            self.coinbase.push(ts, mid)
        else:
            return

        b = self.binance.latest()
        c = self.coinbase.latest()
        now = ts
        bf = b is not None and (now - b[0]) <= self.fresh_seconds
        cf = c is not None and (now - c[0]) <= self.fresh_seconds
        if bf and cf:
            blended = (b[1] + c[1]) / 2.0
        elif bf:
            blended = b[1]
        elif cf:
            blended = c[1]
        else:
            return
        self.cex_mid.push(now, blended)

    def on_oracle(self, ts: float, price: float, _updated_at: int) -> None:
        # Use wall-clock receive time, not Chainlink's on-chain updatedAt.
        # updatedAt can be 10–30s old, making time-window freshness checks
        # unreliable.  Wall-clock accurately reflects when we received the data.
        self.oracle.push(time.time(), price)

    def latest_cex(self) -> Optional[tuple[float, float]]:
        return self.cex_mid.latest()

    def latest_oracle(self) -> Optional[tuple[float, float]]:
        return self.oracle.latest()
