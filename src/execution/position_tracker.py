from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field


@dataclass
class Position:
    token_id: str
    side: str            # "UP" / "DOWN"
    entry_price: float   # avg fill price (0..1)
    size_shares: float
    cost_usd: float
    opened_at: float = field(default_factory=time.time)
    market_window_ts: int = 0
    closed: bool = False
    realized_pnl: float = 0.0
    exit_price: float | None = None
    reconciled: bool = False  # True once on-chain balance confirms entry size/price
    # Market-specific signing context — must match between buy and sell, else
    # SELL signs against wrong exchange contract and gets 400 "signer address".
    neg_risk: bool = False
    tick_size: float = 0.01
    
    # Latch to remember that an exit was decided. Prevents the bot from holding
    # a losing position if the sell fails and the exit signal briefly disappears.
    exit_intent_latched: bool = False

    def unrealized_pnl(self, mark_price: float) -> float:
        return (mark_price - self.entry_price) * self.size_shares


@dataclass
class PositionTracker:
    open: dict[str, Position] = field(default_factory=dict)
    closed: list[Position] = field(default_factory=list)
    # Guards compound mutations + iteration vs background reconcile tasks.
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False, compare=False)

    def add(self, p: Position) -> None:
        with self._lock:
            self.open[p.token_id] = p

    def close(self, token_id: str, exit_price: float) -> Position | None:
        with self._lock:
            p = self.open.pop(token_id, None)
            if not p:
                return None
            p.closed = True
            p.exit_price = exit_price
            p.realized_pnl = (exit_price - p.entry_price) * p.size_shares
            self.closed.append(p)
            return p

    def pop_open(self, token_id: str) -> Position | None:
        """Safe alternative to `tracker.open.pop(token_id, None)`."""
        with self._lock:
            return self.open.pop(token_id, None)

    def snapshot_open(self) -> list[Position]:
        """Return a list copy of open positions safe to iterate across awaits."""
        with self._lock:
            return list(self.open.values())

    def has_open_for_window(self, window_ts: int) -> bool:
        with self._lock:
            return any(p.market_window_ts == window_ts for p in self.open.values())

    def get_for_window(self, window_ts: int) -> Position | None:
        with self._lock:
            for p in self.open.values():
                if p.market_window_ts == window_ts:
                    return p
            return None

    def remove(self, token_id: str) -> Position | None:
        """Alias for pop_open — used by ghost-fill recovery paths."""
        return self.pop_open(token_id)
