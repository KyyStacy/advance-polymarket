from __future__ import annotations

import asyncio
import json
import time
from collections import deque
from typing import Callable

import websockets

from ..utils.logger import get_logger

log = get_logger()

# Polymarket RTDS WebSocket — streams the same Chainlink BTC/USD oracle
# Polymarket uses for resolution. Pushes only when Chainlink updates on-chain
# (~0.5% deviation or 1-hour heartbeat). Between windows no new ticks = normal.
RTDS_URL = "wss://ws-live-data.polymarket.com"


class PriceVelocityTracker:
    """Rolling price velocity over a time window.

    velocity(window=30) = (latest - oldest_in_window) / oldest_in_window
    """

    def __init__(self, maxlen: int = 600):
        self._history: deque[tuple[float, float]] = deque(maxlen=maxlen)

    def update(self, price: float) -> None:
        if price > 0:
            self._history.append((time.time(), price))

    def velocity(self, window: float = 30.0) -> float:
        if len(self._history) < 2:
            return 0.0
        now = time.time()
        cutoff = now - window
        old_price: float | None = None
        for ts, px in self._history:
            if ts >= cutoff:
                old_price = px
                break
        if old_price is None:
            return 0.0
        current = self._history[-1][1]
        return (current - old_price) / old_price


class PolymarketRtdsFeed:
    """Streams BTC/USD Chainlink oracle price from Polymarket's RTDS WebSocket.

    Connection strategy mirrors rtds_example.py exactly:
      - websockets built-in ping_interval=10 keeps the TCP connection alive
        at the protocol level without any custom watchdog.
      - Additional manual text PING every 20s keeps the RTDS application layer
        alive (Polymarket server expects text PING, replies with text PONG).
      - Auto-reconnects on disconnect with exponential backoff.
      - NO custom watchdog — the built-in ping handles liveness detection.

    Key properties:
      latest_price         — most recent Chainlink tick received
      current_candle_open  — first price of the current 5-min bucket
      status               — "WAITING_FIRST_CANDLE" → "LIVE"
      candle_ready         — True once we have a valid candle open
      tick_count           — ticks in the current bucket
    """

    def __init__(
        self,
        on_update: Callable[[float, float, int], None],
        symbol: str = "btc/usd",
        url: str = RTDS_URL,
        interval_sec: int = 300,
    ):
        self.on_update = on_update
        self.symbol = symbol.lower().replace(" ", "")
        self.url = url
        self.interval_ms = interval_sec * 1000

        self._task: asyncio.Task | None = None
        self._stop = asyncio.Event()

        # State exposed to callers
        self.latest_price: float = 0.0
        self.current_candle_open: float = 0.0
        # Initialise _bucket to current wall-clock bucket (MonitorState pattern).
        # Any tick that arrives in a LATER bucket will trigger candle transition.
        now_ms = int(time.time() * 1000)
        self._bucket: int = (now_ms // self.interval_ms) * self.interval_ms
        self.tick_count: int = 0
        self.status: str = "WAITING_FIRST_CANDLE"

        self.velocity = PriceVelocityTracker()

        # Wall-clock time of last received message (data or PONG).
        self.last_message_at: float = 0.0
        # Wall-clock time of last actual price tick (not PONG).
        self.last_tick_at: float = 0.0

    # ------------------------------------------------------------------ state

    def is_connected(self, max_idle_sec: float = 60.0) -> bool:
        """True if we've received any RTDS traffic (data or PONG) recently."""
        if self.last_message_at <= 0:
            return False
        return (time.time() - self.last_message_at) < max_idle_sec

    @property
    def candle_ready(self) -> bool:
        """True when we have a valid candle open for the current bucket.

        Mirrors MonitorState from rtds_example.py: transitions to LIVE on
        the first price tick received after a new bucket boundary.
        """
        return self.status == "LIVE" and self.current_candle_open > 0

    @property
    def bucket_start_ms(self) -> int:
        """Epoch-ms of the start of the current tracked bucket (wall-clock)."""
        return self._bucket

    def seconds_until_next_bucket(self) -> float:
        """Countdown in seconds until the next candle boundary."""
        now_ms = int(time.time() * 1000)
        current_bucket = (now_ms // self.interval_ms) * self.interval_ms
        return max(0.0, (current_bucket + self.interval_ms - now_ms) / 1000.0)

    # ------------------------------------------------------------------ lifecycle

    async def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name="poly-rtds")

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass

    # ------------------------------------------------------------------ tick logic

    def _apply_tick(self, ts_ms: int, price: float) -> None:
        """Core tick processor — updates price, candle open, and velocity.

        Mirrors MonitorState.apply_tick from rtds_example.py:
        - On bucket transition: lock candle open, reset tick_count, set LIVE.
        - Within same bucket: update latest_price and increment tick_count.
        """
        if price <= 0:
            return

        self.latest_price = price
        self.velocity.update(price)
        self.last_tick_at = time.time()

        # Gunakan timestamp dari Chainlink payload persis seperti rtds_example.py
        bucket = (ts_ms // self.interval_ms) * self.interval_ms

        if bucket > self._bucket:
            self._bucket = bucket
            self.current_candle_open = price
            self.tick_count = 0
            self.status = "LIVE"

        self.tick_count += 1

        self.on_update(ts_ms / 1000.0, price, ts_ms)

    # ------------------------------------------------------------------ message handling

    def _sub_msg(self) -> str:
        filt = json.dumps({"symbol": self.symbol}, separators=(",", ":"))
        return json.dumps({
            "action": "subscribe",
            "subscriptions": [
                {
                    "topic": "crypto_prices_chainlink",
                    "type": "*",
                    "filters": filt,
                }
            ],
        })

    def _handle(self, raw) -> None:
        """Process a single raw WebSocket message."""
        self.last_message_at = time.time()

        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", "ignore")
        # PONG reply from our manual text PING — keep-alive confirmed, no data.
        if not raw or raw == "PONG":
            return

        try:
            data = json.loads(raw)
        except (TypeError, ValueError):
            return

        # Normalise to list — Polymarket sometimes sends array, sometimes object.
        items = data if isinstance(data, list) else [data]

        for msg in items:
            topic = msg.get("topic", "")
            if topic not in ("crypto_prices", "crypto_prices_chainlink"):
                continue

            payload = msg.get("payload") or {}
            msg_type = msg.get("type", "")

            # Skip subscription acks and other non-update messages.
            if msg_type and msg_type not in ("update", ""):
                continue

            sym = str(payload.get("symbol", "")).lower().replace(" ", "")
            if sym and sym != self.symbol:
                continue

            try:
                price = float(payload.get("value") or 0)
            except (TypeError, ValueError):
                continue

            ts_ms = int(
                payload.get("timestamp")
                or msg.get("timestamp")
                or time.time() * 1000
            )
            self._apply_tick(ts_ms, price)

    # ------------------------------------------------------------------ main loop

    async def _run(self) -> None:
        """Main WebSocket loop — mirrors stream_rtds() from rtds_example.py.

        Uses websockets built-in ping_interval=10 for protocol-level keepalive
        (same as rtds_example). Also sends a manual text PING every 20s so the
        Polymarket RTDS application layer stays alive. NO custom watchdog —
        the built-in ping timeout handles dead-connection detection.
        """
        backoff = 1.0

        while not self._stop.is_set():
            try:
                async with websockets.connect(
                    self.url,
                    ping_interval=10,   # built-in WS ping every 10s (rtds_example uses 10)
                    ping_timeout=20,    # consider dead if no PONG within 20s → reconnect
                    close_timeout=2,
                ) as ws:
                    await ws.send(self._sub_msg())
                    log.debug(f"[rtds] subscribed Chainlink {self.symbol}")
                    backoff = 1.0
                    self.last_message_at = time.time()

                    # Manual text PING every 20s — same interval as rtds_example.py.
                    # Keeps the Polymarket RTDS application layer alive in addition
                    # to the protocol-level websockets ping above.
                    async def pinger():
                        try:
                            while not self._stop.is_set():
                                await asyncio.sleep(20)
                                await ws.send("PING")
                        except (asyncio.CancelledError, websockets.ConnectionClosed):
                            return

                    p_task = asyncio.create_task(pinger())
                    try:
                        async for raw in ws:
                            self._handle(raw)
                    finally:
                        p_task.cancel()

            except asyncio.CancelledError:
                raise

            except Exception as e:
                log.debug(f"[rtds] disconnect ({type(e).__name__}): {e}; retry in {backoff:.1f}s")
                self.status = "RECONNECTING"
                await asyncio.sleep(backoff)
                backoff = min(backoff * 1.5, 30.0)
