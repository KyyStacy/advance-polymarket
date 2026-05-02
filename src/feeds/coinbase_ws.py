from __future__ import annotations

import asyncio
import time
from typing import Callable

import orjson
import websockets

from ..utils.logger import get_logger

log = get_logger()

COINBASE_URL = "wss://ws-feed.exchange.coinbase.com"


class CoinbaseFeed:
    """Streams BTC-USD ticker + matches via the public Coinbase Exchange WS.

    Fires `on_tick(source, ts_seconds, mid, last_trade)`.
    """

    def __init__(self, on_tick: Callable[[str, float, float, float | None], None]):
        self.on_tick = on_tick
        self._task: asyncio.Task | None = None
        self._stop = asyncio.Event()
        self.last_mid: float | None = None
        self.last_trade: float | None = None

    async def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name="coinbase-ws")

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
        sub = orjson.dumps(
            {
                "type": "subscribe",
                "product_ids": ["BTC-USD"],
                "channels": ["ticker", "matches"],
            }
        ).decode()
        while not self._stop.is_set():
            try:
                async with websockets.connect(
                    COINBASE_URL, ping_interval=15, ping_timeout=15, max_queue=512
                ) as ws:
                    await ws.send(sub)
                    log.info("[coinbase] connected + subscribed")
                    backoff = 1.0
                    async for raw in ws:
                        msg = orjson.loads(raw)
                        mtype = msg.get("type")
                        ts = time.time()
                        if mtype == "ticker":
                            bid = float(msg.get("best_bid") or 0)
                            ask = float(msg.get("best_ask") or 0)
                            if bid > 0 and ask > 0:
                                self.last_mid = (bid + ask) / 2.0
                                price = msg.get("price")
                                if price:
                                    self.last_trade = float(price)
                                self.on_tick(
                                    "coinbase", ts, self.last_mid, self.last_trade
                                )
                        elif mtype == "match" or mtype == "last_match":
                            p = float(msg.get("price") or 0)
                            if p > 0:
                                self.last_trade = p
                                if self.last_mid is not None:
                                    self.on_tick(
                                        "coinbase", ts, self.last_mid, self.last_trade
                                    )
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning(f"[coinbase] disconnect: {e}; retry in {backoff:.1f}s")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)
