from __future__ import annotations
import asyncio
import time
import math
import aiohttp
import numpy as np
from typing import List, Optional
from ..utils.logger import get_logger
from ..config.settings import Settings

log = get_logger()

BINANCE_KLINES_URL = "https://api.binance.com/api/v3/klines"

class VolatilityMonitor:
    """Calculates BTC Volatility Score (1-10) using 1m candle log returns.
    
    Logic mirrors the Pine Script 'BTC Volatility Index (1-10) V6':
    1. Calculate Log Returns of 1m Close prices.
    2. Calculate Standard Deviation of log returns (Lookback=20).
    3. Rank the current StDev against a historical window (Length=252) using Percentile Rank.
    4. Convert to 1-10 scale.
    """

    def __init__(self, aggregator, settings: Settings):
        self.agg = aggregator
        self.s = settings
        
        # Parameters matching volatile-meter.txt
        self.stdev_length = settings.vol_stdev_length
        self.hist_length = settings.vol_hist_length
        
        # Internal state
        self.price_history: List[float] = [] 
        self.log_returns: List[float] = []
        self.stdev_history: List[float] = [] 
        
        self.current_score: float = 5.0
        self.regime: str = "CHOPPY"
        
        self._stop = asyncio.Event()
        self._task: Optional[asyncio.Task] = None

    def start(self):
        if self._task is None:
            self._task = asyncio.create_task(self._run())

    def stop(self):
        self._stop.set()
        if self._task:
            self._task.cancel()

    async def _run(self):
        log.info("[vol-monitor] background task started")
        
        # Seed history from Binance API to avoid warmup time
        await self._seed_history()
        
        while not self._stop.is_set():
            try:
                # Wait until the start of the next minute (XX:XX:00)
                now = time.time()
                sleep_sec = 60 - (now % 60)
                # Add a tiny 100ms buffer to ensure the candle has definitely closed
                await asyncio.sleep(sleep_sec + 0.1)
                
                self._update()
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"[vol-monitor] error: {e}")
                await asyncio.sleep(5)

    async def _seed_history(self):
        """Fetch historical 1m klines from Binance to seed the monitor."""
        log.info("[vol-monitor] seeding history from Binance API (BTCUSDT)...")
        # We need enough candles to fill both hist_length and stdev_length
        limit = self.hist_length + self.stdev_length + 5
        url = f"{BINANCE_KLINES_URL}?symbol=BTCUSDT&interval=1m&limit={limit}"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as resp:
                    if resp.status != 200:
                        log.warning(f"[vol-monitor] failed to seed: HTTP {resp.status}")
                        return
                    data = await resp.json()
            
            # data format: [ [ts, open, high, low, close, ...], ... ]
            closes = [float(k[4]) for k in data]
            if len(closes) < self.stdev_length + 2:
                log.warning("[vol-monitor] not enough historical data from Binance")
                return
            
            # Populate price_history (last prices for rolling calculation)
            self.price_history = closes[-(self.stdev_length + 1):]
            
            # Clear and populate from scratch to ensure consistency
            self.log_returns = []
            self.stdev_history = []
            
            # Temp buffer for log returns to compute rolling StDev
            temp_lrs = []
            for i in range(1, len(closes)):
                lr = math.log(closes[i] / closes[i-1])
                temp_lrs.append(lr)
                if len(temp_lrs) > self.stdev_length:
                    temp_lrs.pop(0)
                
                # If we have a full window, compute a StDev sample
                if len(temp_lrs) == self.stdev_length:
                    self.stdev_history.append(float(np.std(temp_lrs)))
                    if len(self.stdev_history) > self.hist_length:
                        self.stdev_history.pop(0)
            
            # Initialize log_returns with the most recent window
            self.log_returns = temp_lrs
            
            if self.stdev_history:
                log.info(f"[vol-monitor] seeded with {len(self.stdev_history)} historical StDev samples")
                self._calculate_score()
            
        except Exception as e:
            log.warning(f"[vol-monitor] seeding error: {e}")

    def _update(self):
        latest = self.agg.latest_cex()
        if latest is None:
            return
        
        price = latest[1]
        self.price_history.append(price)
        if len(self.price_history) > self.stdev_length + 1:
            self.price_history.pop(0)
            
        if len(self.price_history) < 2:
            return
            
        lr = math.log(self.price_history[-1] / self.price_history[-2])
        self.log_returns.append(lr)
        if len(self.log_returns) > self.stdev_length:
            self.log_returns.pop(0)
            
        if len(self.log_returns) < self.stdev_length:
            return
            
        current_stdev = float(np.std(self.log_returns))
        self.stdev_history.append(current_stdev)
        if len(self.stdev_history) > self.hist_length:
            self.stdev_history.pop(0)
            
        self._calculate_score()

    def _calculate_score(self):
        if not self.stdev_history:
            return
            
        current_stdev = self.stdev_history[-1]
        
        # Calculate Percentile Rank (Percent of history that is lower than current_stdev)
        if len(self.stdev_history) < 2:
            percentile = 50.0 # Default to middle
        else:
            count_lower = sum(1 for v in self.stdev_history[:-1] if current_stdev > v)
            percentile = (count_lower / (len(self.stdev_history) - 1)) * 100.0
        
        # Scale 1-10
        score = 1.0 + (9.0 * percentile / 100.0)
        self.current_score = round(score, 1)
        
        # Determine Regime
        old_regime = self.regime
        if self.current_score >= self.s.vol_turbo_threshold:
            self.regime = "TURBO"
        elif self.current_score >= self.s.vol_choppy_threshold:
            self.regime = "CHOPPY"
        else:
            self.regime = "STEADY"
            
        if self.regime != old_regime:
            log.info(f"[vol-monitor] REGIME CHANGE: {old_regime} -> {self.regime} (score={self.current_score})")
        else:
            log.info(f"[vol-monitor] score={self.current_score} regime={self.regime}")

    def get_regime_params(self) -> dict:
        """Returns the strategy parameters for the current active regime."""
        if self.regime == "STEADY":
            return {
                "min_edge": self.s.steady_min_edge,
                "min_momentum_z": self.s.steady_min_momentum_z,
                "min_leadlag_bps": self.s.steady_min_leadlag_bps,
                "sigma": self.s.steady_sigma
            }
        elif self.regime == "TURBO":
            return {
                "min_edge": self.s.turbo_min_edge,
                "min_momentum_z": self.s.turbo_min_momentum_z,
                "min_leadlag_bps": self.s.turbo_min_leadlag_bps,
                "sigma": self.s.turbo_sigma
            }
        else: # CHOPPY (Default)
            return {
                "min_edge": self.s.choppy_min_edge,
                "min_momentum_z": self.s.choppy_min_momentum_z,
                "min_leadlag_bps": self.s.choppy_min_leadlag_bps,
                "sigma": self.s.choppy_sigma
            }
