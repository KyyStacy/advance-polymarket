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
    total_pnl: float = 0.0  # cumulative trade-based P&L (kept for dry-run sizing)

    # Balance-based baselines for display P&L
    _start_balance: float = 0.0          # balance when bot first started
    _daily_start_balance: float = 0.0     # balance at WIB midnight (or first read)
    _last_day_key: str = ""              # for midnight detection

    @staticmethod
    def _today_key() -> str:
        # Reset daily P&L at WIB (UTC+7) midnight, not UTC midnight
        from datetime import datetime, timedelta, timezone
        utc_now = datetime.now(timezone.utc)
        wib_tz = timezone(timedelta(hours=7))
        wib_now = utc_now.astimezone(wib_tz)
        return wib_now.strftime("%Y-%m-%d")

    def _ensure_day(self) -> None:
        today = self._today_key()
        if self.pnl.day_key != today:
            self.pnl = DailyPnl(day_key=today)

    @property
    def balance_total_pnl(self) -> float:
        """Total P&L since bot start, measured from balance change."""
        if self._start_balance <= 0:
            return 0.0
        return self.bankroll_usd - self._start_balance

    @property
    def balance_daily_pnl(self) -> float:
        """Daily P&L since last WIB midnight, measured from balance change."""
        if self._daily_start_balance <= 0:
            return 0.0
        return self.bankroll_usd - self._daily_start_balance

    def init_start_balance(self, usd: float) -> None:
        """Set starting balance (once, at bot startup)."""
        if self._start_balance <= 0:
            self._start_balance = max(0.0, float(usd))
            log.info(f"[risk] start balance locked: ${self._start_balance:.2f}")

    def reset_daily_balance(self, usd: float) -> None:
        """Reset daily baseline (called at WIB midnight or on first balance read)."""
        self._daily_start_balance = max(0.0, float(usd))
        log.info(f"[risk] daily balance reset: ${self._daily_start_balance:.2f}")

    def maybe_reset_daily(self, usd: float) -> None:
        """Auto-detect WIB day change and reset daily baseline."""
        today = self._today_key()
        if today != self._last_day_key:
            self._last_day_key = today
            self.reset_daily_balance(usd)
            # Also clear trade-based daily tracker so it stays consistent
            self.pnl = DailyPnl(day_key=today)

    def update_bankroll(self, usd: float) -> None:
        self.bankroll_usd = max(0.0, float(usd))

    def record_trade_pnl(self, pnl: float) -> None:
        self._ensure_day()
        self.pnl.realized += pnl
        self.pnl.n_trades += 1
        self.total_pnl += pnl

    def can_trade(self) -> tuple[bool, str]:
        self._ensure_day()
        if self.pnl.realized <= -abs(self.settings.max_daily_loss_usd):
            return False, (
                f"daily-loss-cap hit ({self.pnl.realized:.2f} <= "
                f"-{self.settings.max_daily_loss_usd:.2f})"
            )
        return True, "ok"

    def position_size_usd(self, edge: float, implied_p: float, win_prob: float = 0.5) -> float:
        """Choose USD size given edge, odds, and win probability.

        Sizing rule:
          - base = max_bet according to mode (pct of bankroll OR fixed USD)
          - bet ceiling scales with win_prob linearly from min_bet (50% confidence)
            to base (100% confidence) — higher probability → larger ceiling
          - Kelly fraction f = edge / (1 - implied_p) (capped 25%) applied within ceiling
          - final = clamp(ceil * f_scaled, MIN_BET, ceil)
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

        # Probability-scaled ceiling: win_prob [0.5, 1.0] → ceiling [min_bet, base].
        # At 50% (coin flip) ceiling = min_bet; at 100% (certain) ceiling = base.
        prob_scale = max(0.0, min(1.0, (win_prob - 0.5) * 2.0))
        ceil_usd = s.min_bet_usd + prob_scale * max(0.0, base - s.min_bet_usd)
        ceil_usd = max(s.min_bet_usd, min(ceil_usd, base))

        # Kelly fraction scales within the probability-determined ceiling
        size = ceil_usd * (f / 0.25) if f > 0 else 0.0
        size = max(0.0, min(size, ceil_usd))

        if 0 < size < s.min_bet_usd:
            size = s.min_bet_usd if ceil_usd >= s.min_bet_usd else 0.0
        return round(size, 2)
