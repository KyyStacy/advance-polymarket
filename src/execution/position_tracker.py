from __future__ import annotations

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

    def unrealized_pnl(self, mark_price: float) -> float:
        return (mark_price - self.entry_price) * self.size_shares


@dataclass
class PositionTracker:
    open: dict[str, Position] = field(default_factory=dict)
    closed: list[Position] = field(default_factory=list)

    def add(self, p: Position) -> None:
        self.open[p.token_id] = p

    def close(self, token_id: str, exit_price: float) -> Position | None:
        p = self.open.pop(token_id, None)
        if not p:
            return None
        p.closed = True
        p.exit_price = exit_price
        p.realized_pnl = (exit_price - p.entry_price) * p.size_shares
        self.closed.append(p)
        return p

    def has_open_for_window(self, window_ts: int) -> bool:
        return any(p.market_window_ts == window_ts for p in self.open.values())

    def get_for_window(self, window_ts: int) -> Position | None:
        for p in self.open.values():
            if p.market_window_ts == window_ts:
                return p
        return None
