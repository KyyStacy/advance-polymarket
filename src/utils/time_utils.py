from __future__ import annotations

import time

WINDOW_SEC = 300  # 5-minute markets


def now_ms() -> int:
    return int(time.time() * 1000)


def now_s() -> float:
    return time.time()


def current_window_start(ts: float | None = None) -> int:
    """Unix timestamp (seconds) of the start of the current 5-minute window."""
    t = int(ts if ts is not None else time.time())
    return t - (t % WINDOW_SEC)


def next_window_start(ts: float | None = None) -> int:
    return current_window_start(ts) + WINDOW_SEC


def seconds_into_window(ts: float | None = None) -> float:
    t = ts if ts is not None else time.time()
    return t - current_window_start(t)


def seconds_left_in_window(ts: float | None = None) -> float:
    t = ts if ts is not None else time.time()
    return next_window_start(t) - t


def window_slug(window_ts: int) -> str:
    return f"btc-updown-5m-{window_ts}"
