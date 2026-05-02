from __future__ import annotations

import time
from dataclasses import dataclass, field

from ..config.settings import Settings
from ..utils.logger import get_logger

log = get_logger()


@dataclass
class DailyPnl:
    day_key: str
    realized: float = 0.0
    n_trades: int = 0


@dataclass
class RiskManager:
    settings: Settings
    bankroll_usd: float = 0.0
    pnl: DailyPnl = field(default_factory=lambda: DailyPnl(day_key=""))

    @staticmethod
    def _today_key() -> str:
        return time.strftime("%Y-%m-%d", time.gmtime())

    def _ensure_day(self) -> None:
        today = self._today_key()
        if self.pnl.day_key != today:
            self.pnl = DailyPnl(day_key=today)

    def update_bankroll(self, usd: float) -> None:
        self.bankroll_usd = max(0.0, float(usd))

    def record_trade_pnl(self, pnl: float) -> None:
        self._ensure_day()
        self.pnl.realized += pnl
        self.pnl.n_trades += 1

    def can_trade(self) -> tuple[bool, str]:
        self._ensure_day()
        if self.pnl.realized <= -abs(self.settings.max_daily_loss_usd):
            return False, (
                f"daily-loss-cap hit ({self.pnl.realized:.2f} <= "
                f"-{self.settings.max_daily_loss_usd:.2f})"
            )
        return True, "ok"

    def position_size_usd(self, edge: float, implied_p: float) -> float:
        """Choose USD size given edge and odds. Bounded by min/max bet config.

        Sizing rule:
          - base = max_bet according to mode (pct of bankroll OR fixed USD)
          - apply Kelly-fraction throttle: f = edge / (1 - implied_p)  (capped 25%)
          - final = clamp(base * f_scaled, MIN_BET, base)
        """
        s = self.settings
        if s.max_bet_mode == "pct":
            base = self.bankroll_usd * (s.max_bet_pct / 100.0)
        else:
            base = s.max_bet_usd

        if base <= 0:
            return 0.0
        if implied_p <= 0 or implied_p >= 1:
            f = 0.0
        else:
            f = max(0.0, edge / max(1e-6, 1.0 - implied_p))
            f = min(f, 0.25)  # never bet more than 25% Kelly
        size = base * (f / 0.25) if base > 0 else 0.0  # scale to base when full Kelly
        size = max(min(size, base), 0.0)
        if size > 0 and size < s.min_bet_usd:
            size = s.min_bet_usd if base >= s.min_bet_usd else 0.0
        return round(size, 2)
