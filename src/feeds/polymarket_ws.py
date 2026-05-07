from __future__ import annotations

import asyncio
import time
from typing import Callable, Iterable

import orjson
import websockets

from ..utils.logger import get_logger

log = get_logger()


class PolymarketBookFeed:
    """Subscribes to the public CLOB market WS for an asset (token) id list.

    Maintains best_bid/best_ask per token. Fires `on_book(token_id, ts, bid, ask)`
    whenever top of book changes.
    """

    def __init__(
        self,
        url: str,
        token_ids: Iterable[str],
        on_book: Callable[[str, float, float | None, float | None], None],
    ):
        self.url = url
        self.token_ids = list(token_ids)
        self.on_book = on_book
        self._task: asyncio.Task | None = None
        self._stop = asyncio.Event()
        self.books: dict[str, dict] = {tid: {"bid": None, "ask": None} for tid in self.token_ids}
        self._ws: object | None = None  # live WS connection, None when disconnected

    async def update_tokens(self, token_ids: Iterable[str]) -> None:
        self.token_ids = list(token_ids)
        for tid in self.token_ids:
            self.books.setdefault(tid, {"bid": None, "ask": None})
        # Re-subscribe on the live connection so new tokens stream immediately.
        # Without this, the server keeps sending old-token updates only and the
        # new market window gets no bid/ask data until the next reconnect.
        if self._ws is not None:
            try:
                sub = {"assets_ids": self.token_ids, "type": "market"}
                await self._ws.send(orjson.dumps(sub).decode())  # type: ignore[union-attr]
                log.info(f"[polymarket-ws] re-subscribed to {len(self.token_ids)} assets")
            except Exception as e:
                log.warning(f"[polymarket-ws] re-subscribe failed: {e}")

    async def restart(self, token_ids: Iterable[str] | None = None) -> None:
        """Hard restart: stop, clear state, re-subscribe with new tokens.

        Mirrors best_ask.py pattern — every new window gets a fresh WS
        connection so the server pushes an initial snapshot immediately.
        """
        await self.stop()
        self._stop.clear()
        if token_ids is not None:
            self.token_ids = list(token_ids)
        self.books = {tid: {"bid": None, "ask": None} for tid in self.token_ids}
        self._ws = None
        await self.start()

    async def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name="polymarket-ws")

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
            if not self.token_ids:
                await asyncio.sleep(0.5)
                continue
            try:
                async with websockets.connect(
                    self.url, ping_interval=10, ping_timeout=10, max_queue=512
                ) as ws:
                    self._ws = ws
                    sub = {"assets_ids": self.token_ids, "type": "market"}
                    await ws.send(orjson.dumps(sub).decode())
                    log.info(
                        f"[polymarket-ws] subscribed to {len(self.token_ids)} assets"
                    )
                    backoff = 1.0

                    async def pinger():
                        try:
                            while not self._stop.is_set():
                                await asyncio.sleep(9)
                                await ws.send("PING")
                        except (asyncio.CancelledError, websockets.ConnectionClosed):
                            return

                    p_task = asyncio.create_task(pinger())
                    try:
                        async for raw in ws:
                            if raw == "PONG":
                                continue
                            try:
                                payload = orjson.loads(raw)
                            except Exception:
                                continue
                            msgs = payload if isinstance(payload, list) else [payload]
                            for m in msgs:
                                self._handle(m)
                    finally:
                        p_task.cancel()

            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning(f"[polymarket-ws] disconnect: {e}; retry {backoff:.1f}s")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)
            finally:
                self._ws = None

    def _handle(self, m: dict) -> None:
        et = m.get("event_type") or m.get("type")
        tid = m.get("asset_id") or m.get("token_id")
        if not tid:
            return
        book = self.books.get(tid)
        if book is None:
            self.books[tid] = {"bid": None, "ask": None}
            book = self.books[tid]
        ts = time.time()
        changed = False
        if et == "book":
            bids = m.get("bids") or []
            asks = m.get("asks") or []
            best_bid = max((float(b["price"]) for b in bids), default=None)
            best_ask = min((float(a["price"]) for a in asks), default=None)
            if best_bid != book["bid"] or best_ask != book["ask"]:
                book["bid"] = best_bid
                book["ask"] = best_ask
                changed = True
        elif et == "price_change":
            for ch in m.get("changes") or []:
                side = (ch.get("side") or "").upper()
                price = float(ch.get("price"))
                size = float(ch.get("size") or 0)
                if side == "BUY":
                    if size == 0 and book["bid"] == price:
                        book["bid"] = None
                    elif book["bid"] is None or price > book["bid"]:
                        book["bid"] = price
                elif side == "SELL":
                    if size == 0 and book["ask"] == price:
                        book["ask"] = None
                    elif book["ask"] is None or price < book["ask"]:
                        book["ask"] = price
            changed = True
        if changed:
            self.on_book(tid, ts, book["bid"], book["ask"])
