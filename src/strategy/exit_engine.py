from __future__ import annotations

import time
from dataclasses import dataclass

from ..config.settings import Settings
from ..signals.lead_lag import LeadLagReading
from ..signals.mispricing import MispricingReading
from ..signals.momentum import MomentumReading


@dataclass
class ExitDecision:
    exit: bool
    reason: str


class ExitEngine:
    def __init__(self, settings: Settings):
        self.s = settings
        self._last_mispricing_ts: float = time.time()  # init to now so first ticks get a grace period
        
        # Current active thresholds (updated by regime)
        self.active_min_edge: float = settings.steady_min_edge
        self.active_min_leadlag_bps: float = settings.steady_min_leadlag_bps

    def update_thresholds(self, min_edge: float, min_leadlag_bps: float):
        """Update active thresholds based on volatility regime."""
        self.active_min_edge = min_edge
        self.active_min_leadlag_bps = min_leadlag_bps

    def decide(
        self,
        side: str,
        seconds_left: float,
        momentum: MomentumReading,
        leadlag: LeadLagReading | None,
        mispricing: MispricingReading | None,
        unrealized_pct: float,
        sigma: float,
        seconds_held: float = 0.0,
        peak_unreal: float = 0.0,
    ) -> ExitDecision:
        # ---- VICTORY LOCK (Fix 6 - Paranoid Edition) ----
        # We only lock if:
        # 1. We are in the final stretch (last exit_victory_lock_sec).
        # 2. Mathematical safety is extreme (Z >= victory_lock_z).
        # 3. Momentum is NOT strongly against us (Adverse Z < exit_victory_lock_max_adverse_z).
        if mispricing is not None and mispricing.fair_basis.get('anchor_price', 0) > 0 and seconds_left <= self.s.exit_victory_lock_sec:
            import math
            anchor = mispricing.fair_basis['anchor_price']
            current = mispricing.fair_basis['current_cex']
            log_dist = math.log(current / anchor)
            if side == "DOWN":
                log_dist = -log_dist
            
            sigma_left = sigma * math.sqrt(max(seconds_left, 1.0))
            safety_z = log_dist / max(sigma_left, 1e-9)
            
            if safety_z >= self.s.exit_victory_lock_z:
                # Check for "Black Swan" momentum reversal
                is_adverse_mom = (side == "UP" and momentum.direction == -1) or (side == "DOWN" and momentum.direction == 1)
                if not (is_adverse_mom and momentum.z_score > self.s.exit_victory_lock_max_adverse_z):
                    return ExitDecision(False, f"victory-lock[protected] z={safety_z:.1f} mom-z={momentum.z_score:.1f}")
                # If adverse momentum is too strong, fall through to normal exit logic!

        # Hard time floor — always honor before resolution lag eats us
        if seconds_left <= self.s.exit_hard_floor_sec:
            return ExitDecision(True, f"time-floor<={self.s.exit_hard_floor_sec}s")

        # Hard stop-loss — unconditionally cut deep losers
        if unrealized_pct <= self.s.exit_hard_stop_loss_pct:
            return ExitDecision(True, f"hard-stop-loss unreal={unrealized_pct:+.1%}<={self.s.exit_hard_stop_loss_pct:+.1%}")

        now = time.time()
        if mispricing is None:
            stale_for = now - self._last_mispricing_ts if self._last_mispricing_ts else 9999.0
            if stale_for > self.s.exit_stale_seconds:
                return ExitDecision(True, f"book-stale>{self.s.exit_stale_seconds:.0f}s")
            return ExitDecision(False, "no-mispricing-data-yet")
        self._last_mispricing_ts = now

        # Own-fill grace: block all signal-driven exits in the first 1s after
        # entry. Only time-floor (above) bypasses this — book-stale and signal
        # exits all wait for the book to settle. Prevents adverse selection
        # from our own market buy snapping fair_p_up against us instantly.
        if seconds_held < self.s.exit_own_fill_grace_seconds:
            return ExitDecision(
                False,
                f"hold[own-fill-grace] {seconds_held:.2f}s<{self.s.exit_own_fill_grace_seconds:.1f}s"
            )

        in_profit = unrealized_pct >= self.s.exit_profit_hold_pct

        # Take-profit: lock in large gains immediately (bypasses min hold)
        if unrealized_pct >= self.s.exit_take_profit_pct:
            return ExitDecision(True, f"take-profit unreal={unrealized_pct:+.1%}")

        # Trailing stop: once peak >= 40%, exit if dropped 10pp from peak (bypasses min hold)
        if peak_unreal >= self.s.exit_trailing_stop_min_peak:
            drawdown = peak_unreal - unrealized_pct
            if drawdown >= self.s.exit_trailing_stop_dist:
                return ExitDecision(
                    True,
                    f"trailing-stop peak={peak_unreal:+.1%} drop={drawdown:.1%}>={self.s.exit_trailing_stop_dist:.1%}"
                )

        # Side flip — model fully disagrees. Always exit (bypasses min hold)
        # unless Momentum Shield is active.
        if mispricing.side is not None and mispricing.side != side:
            # FIX 5: Momentum Shield
            # If momentum is strongly supporting our current side, ignore the side-flip.
            # This protects against 'Anchor Gravity' at the end of windows.
            mom_supports = (side == "UP" and momentum.direction == 1) or (side == "DOWN" and momentum.direction == -1)
            if mom_supports and momentum.z_score >= self.s.exit_momentum_shield_z:
                 return ExitDecision(False, f"shield[active] mom-z={momentum.z_score:.2f}>={self.s.exit_momentum_shield_z}")

            return ExitDecision(True, f"side-flipped fair_p_up={mispricing.fair_p_up:.3f}")

        # Min hold time — block signal-based exits during initial settle period.
        # Time-floor, book-stale, and side-flip above already bypassed this.
        if seconds_held < self.s.exit_min_hold_seconds:
            return ExitDecision(
                False,
                f"hold[settling] {seconds_held:.0f}s<{self.s.exit_min_hold_seconds:.0f}s "
                f"unreal={unrealized_pct:+.1%}"
            )

        # Adverse momentum
        if side == "UP":
            adverse = momentum.direction == -1
        else:
            adverse = momentum.direction == 1
        if adverse:
            z_threshold = self.s.exit_z_in_profit if in_profit else self.s.exit_loss_exit_z
            if momentum.z_score >= z_threshold:
                regime = "profit" if in_profit else "loss"
                return ExitDecision(
                    True,
                    f"momentum-flip[{regime}] dir={momentum.direction} "
                    f"z={momentum.z_score:.2f}>={z_threshold:.2f} unreal={unrealized_pct:+.1%}"
                )

        # Edge collapse — different tolerance based on P&L
        edge_floor = (
            self.active_min_edge * self.s.exit_edge_collapse_profit_frac if in_profit
            else self.active_min_edge * self.s.exit_edge_collapse_loss_frac
        )
        if mispricing.edge < edge_floor:
            regime = "profit" if in_profit else "loss"
            return ExitDecision(
                True,
                f"edge-collapsed[{regime}] {mispricing.edge:.3f}<{edge_floor:.3f}"
            )

        # Lead-lag flip exit — require ALL of:
        #  1. Lead-lag has flipped against our side (signed below floor that
        #     puts it on the wrong side of zero, not merely "converged").
        #  2. Adverse momentum confirms (z >= LEADLAG_EXIT_MIN_ADVERSE_Z) so a
        #     single weak signal can't close the trade.
        #  3. If we're in loss, loss must be material (LEADLAG_EXIT_MIN_LOSS_PCT)
        #     — small drawdowns under noise shouldn't trigger exit while plenty
        #     of window time remains and the entry thesis still has runway.
        if leadlag is not None:
            signed = leadlag.deviation_bps if side == "UP" else -leadlag.deviation_bps
            ll_floor = (
                self.active_min_leadlag_bps * self.s.exit_ll_converge_profit_frac if in_profit
                else self.active_min_leadlag_bps * self.s.exit_ll_converge_loss_frac
            )
            if signed < ll_floor:
                # Confirmation gates
                adverse_dir = (
                    (side == "UP" and momentum.direction == -1)
                    or (side == "DOWN" and momentum.direction == 1)
                )
                mom_confirms = adverse_dir and momentum.z_score >= self.s.exit_ll_min_adverse_z
                loss_material = in_profit or unrealized_pct <= self.s.exit_ll_min_loss_pct
                if mom_confirms and loss_material:
                    regime = "profit" if in_profit else "loss"
                    return ExitDecision(
                        True,
                        f"leadlag-flipped[{regime}] {signed:.1f}bps<{ll_floor:.1f} "
                        f"+mom z={momentum.z_score:.2f} unreal={unrealized_pct:+.1%}"
                    )

        return ExitDecision(
            False,
            f"hold[{('profit' if in_profit else 'loss')}] "
            f"edge={mispricing.edge:.3f} z={momentum.z_score:.2f} unreal={unrealized_pct:+.1%}"
        )
