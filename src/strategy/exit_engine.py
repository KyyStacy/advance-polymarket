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
    """Asymmetric signal-based exit. Hold winners; cut losers fast.

    Philosophy: maximize EV, not quick wins. A reversal in our direction is
    cheap to ride (we already have profit cushion); a reversal against us
    compounds losses every tick. So thresholds differ by P&L state.

    Exit triggers (any one):
      - Time floor before resolution (always — bypasses min hold)
      - Book stale > STALE_EXIT_SECONDS (Polymarket WS dead — bypasses min hold)
      - Side flip (model fully disagrees — bypasses min hold)
      - After MIN_HOLD_SECONDS only:
        - In-loss + adverse momentum z >= LOSS_EXIT_Z
        - In-profit + EXTREME adverse signal (only confirmed reversal)
        - Edge collapse / lead-lag convergence
    """

    STALE_EXIT_SECONDS = 30.0
    # Minimum hold time after entry before signal-based exits can fire.
    # Without this, z dropping from 2.94→1.83 in 12s (noise reversion) was
    # triggering exits seconds after entry on strong signal.
    MIN_HOLD_SECONDS = 45.0
    # P&L threshold separating "cut fast" vs "let it run" regimes
    PROFIT_HOLD_PCT = 0.10
    # Loss-regime adverse z floor. Must be ABOVE entry threshold
    # (min_momentum_z=1.5) so noise reversion doesn't trigger exit.
    LOSS_EXIT_Z = 2.0
    # Higher z required to exit while sitting in profit
    Z_EXIT_IN_PROFIT = 2.5
    # Edge collapse: in profit, allow much more decay before exit
    EDGE_COLLAPSE_LOSS_FRAC = 1.0 / 3.0      # cut in loss when edge < min/3
    EDGE_COLLAPSE_PROFIT_FRAC = 0.0          # in profit, only exit when edge actually negative
    # Lead-lag convergence: in profit allow more, in loss exit fast
    LEADLAG_CONVERGE_LOSS_FRAC = 0.25
    LEADLAG_CONVERGE_PROFIT_FRAC = -0.5      # only exit if leadlag actively flipped

    def __init__(self, settings: Settings):
        self.s = settings
        self._last_mispricing_ts: float = 0.0

    def decide(
        self,
        side: str,
        seconds_left: float,
        momentum: MomentumReading,
        leadlag: LeadLagReading | None,
        mispricing: MispricingReading | None,
        unrealized_pct: float,
        seconds_held: float = 0.0,
    ) -> ExitDecision:
        # Hard time floor — always honor before resolution lag eats us
        if seconds_left <= self.s.exit_hard_floor_sec:
            return ExitDecision(True, f"time-floor<={self.s.exit_hard_floor_sec}s")

        now = time.time()
        if mispricing is None:
            stale_for = now - self._last_mispricing_ts if self._last_mispricing_ts else 9999.0
            if stale_for > self.STALE_EXIT_SECONDS:
                return ExitDecision(True, f"book-stale>{self.STALE_EXIT_SECONDS:.0f}s")
            return ExitDecision(False, "no-mispricing-data-yet")
        self._last_mispricing_ts = now

        in_profit = unrealized_pct >= self.PROFIT_HOLD_PCT

        # Side flip — model fully disagrees. Always exit (bypasses min hold).
        if mispricing.side is not None and mispricing.side != side:
            return ExitDecision(True, f"side-flipped fair_p_up={mispricing.fair_p_up:.3f}")

        # Min hold time — block signal-based exits during initial settle period.
        # Time-floor, book-stale, and side-flip above already bypassed this.
        if seconds_held < self.MIN_HOLD_SECONDS:
            return ExitDecision(
                False,
                f"hold[settling] {seconds_held:.0f}s<{self.MIN_HOLD_SECONDS:.0f}s "
                f"unreal={unrealized_pct:+.1%}"
            )

        # Adverse momentum
        if side == "UP":
            adverse = momentum.direction == -1
        else:
            adverse = momentum.direction == 1
        if adverse:
            z_threshold = self.Z_EXIT_IN_PROFIT if in_profit else self.LOSS_EXIT_Z
            if momentum.z_score >= z_threshold:
                regime = "profit" if in_profit else "loss"
                return ExitDecision(
                    True,
                    f"momentum-flip[{regime}] dir={momentum.direction} "
                    f"z={momentum.z_score:.2f}>={z_threshold:.2f} unreal={unrealized_pct:+.1%}"
                )

        # Edge collapse — different tolerance based on P&L
        edge_floor = (
            self.s.min_edge * self.EDGE_COLLAPSE_PROFIT_FRAC if in_profit
            else self.s.min_edge * self.EDGE_COLLAPSE_LOSS_FRAC
        )
        if mispricing.edge < edge_floor:
            regime = "profit" if in_profit else "loss"
            return ExitDecision(
                True,
                f"edge-collapsed[{regime}] {mispricing.edge:.3f}<{edge_floor:.3f}"
            )

        # Lead-lag convergence — in profit, only exit if signal flipped sign
        if leadlag is not None:
            signed = leadlag.deviation_bps if side == "UP" else -leadlag.deviation_bps
            ll_floor = (
                self.s.min_leadlag_bps * self.LEADLAG_CONVERGE_PROFIT_FRAC if in_profit
                else self.s.min_leadlag_bps * self.LEADLAG_CONVERGE_LOSS_FRAC
            )
            if signed < ll_floor:
                regime = "profit" if in_profit else "loss"
                return ExitDecision(
                    True,
                    f"leadlag-converged[{regime}] {signed:.1f}bps<{ll_floor:.1f}"
                )

        return ExitDecision(
            False,
            f"hold[{('profit' if in_profit else 'loss')}] "
            f"edge={mispricing.edge:.3f} z={momentum.z_score:.2f} unreal={unrealized_pct:+.1%}"
        )
