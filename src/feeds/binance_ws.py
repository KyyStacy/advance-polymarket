from __future__ import annotations

import asyncio
import random
import time
from typing import Callable

import orjson
import websockets

from ..utils.logger import get_logger

log = get_logger()

# Combined stream: bookTicker (best bid/ask) + aggTrade (taker prints)
BINANCE_URL = (
    "wss://stream.binance.com:9443/stream?streams=btcusdt@bookTicker/btcusdt@aggTrade"
)


class BinanceFeed:
    """Streams BTCUSDT mid + last trade from Binance.

    Fires `on_tick(source, ts_seconds, mid, last_trade)` with `last_trade` possibly None.
    """

    def __init__(self, on_tick: Callable[[str, float, float, float | None], None]):
        self.on_tick = on_tick
        self._task: asyncio.Task | None = None
        self._stop = asyncio.Event()
        self.last_mid: float | None = None
        self.last_trade: float | None = None

    async def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name="binance-ws")

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass

    async def _run(self) -> None:
        backoff = 1.0
        while not self._stop.is_set():
            try:
                async with websockets.connect(
                    BINANCE_URL, ping_interval=20, ping_timeout=20, max_queue=512
                ) as ws:
                    log.info("[binance] connected")
                    backoff = 1.0
                    async for raw in ws:
                        msg = orjson.loads(raw)
                        data = msg.get("data") or {}
                        stream = msg.get("stream", "")
                        ts = time.time()
                        if stream.endswith("@bookTicker"):
                            bid = float(data.get("b", 0))
                            ask = float(data.get("a", 0))
                            if bid > 0 and ask > 0:
                                self.last_mid = (bid + ask) / 2.0
                                self.on_tick("binance", ts, self.last_mid, self.last_trade)
                        elif stream.endswith("@aggTrade"):
                            p = float(data.get("p", 0))
                            if p > 0:
                                self.last_trade = p
                                if self.last_mid is not None:
                                    self.on_tick(
                                        "binance", ts, self.last_mid, self.last_trade
                                    )
            except asyncio.CancelledError:
                raise
            except Exception as e:
                # Jitter prevents synchronized reconnect storms when multiple
                # bots / clients drop at once. Floor at 0.2s for fast recovery.
                wait = max(0.2, backoff + random.uniform(0.0, 0.5))
                log.warning(f"[binance] disconnect: {e}; retry in {wait:.2f}s")
                await asyncio.sleep(wait)
                backoff = min(backoff * 2, 30.0)
