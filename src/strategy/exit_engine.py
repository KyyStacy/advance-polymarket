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
      - Take-profit >= TAKE_PROFIT_PCT (bypasses min hold — lock in big wins)
      - Trailing stop: peak >= TRAILING_STOP_MIN_PEAK and drop >= TRAILING_STOP_DIST
      - Side flip (model fully disagrees — bypasses min hold)
      - After MIN_HOLD_SECONDS only:
        - In-loss + adverse momentum z >= LOSS_EXIT_Z
        - In-profit + EXTREME adverse signal (only confirmed reversal)
        - Edge collapse / lead-lag convergence
    """

    # 30s was too permissive — Polymarket book heartbeats every 2–5s, so a
    # 10s silence almost always means a real disconnect, not a normal pause.
    # Tighter threshold triggers stale-exit 3× faster on actual feed loss.
    STALE_EXIT_SECONDS = 10.0
    # Minimum hold time after entry before signal-based exits can fire.
    # Without this, z dropping from 2.94→1.83 in 12s (noise reversion) was
    # triggering exits seconds after entry on strong signal.
    MIN_HOLD_SECONDS = 45.0
    # Hard floor against own-fill adverse selection: own market buy moves the
    # ask and immediately tilts the fair-probability estimate against us. Side
    # flip / take-profit can otherwise fire on the same tick as the fill and
    # lock in 50–200bps loss before the market has even acked our trade.
    # This window is tiny (1s) but enough for the book to settle post-fill.
    OWN_FILL_GRACE_SECONDS = 1.0
    # Hard take-profit: only at extreme gains (300%) as a safety ceiling.
    # Primary profit-protection is the trailing stop below — this hard cap
    # is intentionally high so 150-200% runs are not cut prematurely.
    TAKE_PROFIT_PCT = 3.00
    # Trailing stop: once peak unrealized >= TRAILING_STOP_MIN_PEAK,
    # exit if current unrealized drops TRAILING_STOP_DIST from the peak.
    # Activates at 30% peak; 25pp drawdown from peak triggers exit.
    # Example: peak=150% → exit if mark falls back to 125%.
    TRAILING_STOP_MIN_PEAK = 0.30
    TRAILING_STOP_DIST = 0.25
    # P&L threshold separating "cut fast" vs "let it run" regimes
    PROFIT_HOLD_PCT = 0.10
    # Loss-regime adverse z floor. Should be >= entry threshold
    # (MIN_MOMENTUM_Z, default 2.0) so noise reversion doesn't trigger exit.
    LOSS_EXIT_Z = 2.0
    # Higher z required to exit while sitting in profit
    Z_EXIT_IN_PROFIT = 2.5
    # Edge collapse: in profit, allow much more decay before exit
    EDGE_COLLAPSE_LOSS_FRAC = 1.0 / 3.0      # cut in loss when edge < min/3
    EDGE_COLLAPSE_PROFIT_FRAC = 0.0          # in profit, only exit when edge actually negative
    # Lead-lag exit only when signal has clearly REVERSED, not merely converged.
    # Earlier setting (0.25 in loss) cut profitable trades on minor positive
    # signal weakening (e.g. 0.4bps<0.5 floor) while plenty of window remained.
    # Now require leadlag to be at least min_leadlag * frac BELOW zero (negative
    # for our side), and additionally require adverse momentum confirmation so a
    # single noisy signal alone never triggers exit.
    LEADLAG_CONVERGE_LOSS_FRAC = -0.25       # ~ -0.5bps before considered "flipped" in loss
    LEADLAG_CONVERGE_PROFIT_FRAC = -0.5      # ~ -1.0bps before considered "flipped" in profit
    # Minimum momentum z (in adverse direction) required to confirm leadlag-flip exit.
    # Without this, a single weak signal could close a winning thesis prematurely.
    LEADLAG_EXIT_MIN_ADVERSE_Z = 1.0
    # When loss is mild and time remains, hold even on signal weakness.
    # Prevents cutting on noise reversion that hasn't reached real conviction.
    LEADLAG_EXIT_MIN_LOSS_PCT = -0.15        # only consider leadlag exit if loss > 15%

    def __init__(self, settings: Settings):
        self.s = settings
        self._last_mispricing_ts: float = time.time()  # init to now so first ticks get a grace period

    def decide(
        self,
        side: str,
        seconds_left: float,
        momentum: MomentumReading,
        leadlag: LeadLagReading | None,
        mispricing: MispricingReading | None,
        unrealized_pct: float,
        seconds_held: float = 0.0,
        peak_unreal: float = 0.0,
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

        # Own-fill grace: block all signal-driven exits in the first 1s after
        # entry. Only time-floor (above) bypasses this — book-stale and signal
        # exits all wait for the book to settle. Prevents adverse selection
        # from our own market buy snapping fair_p_up against us instantly.
        if seconds_held < self.OWN_FILL_GRACE_SECONDS:
            return ExitDecision(
                False,
                f"hold[own-fill-grace] {seconds_held:.2f}s<{self.OWN_FILL_GRACE_SECONDS:.1f}s"
            )

        in_profit = unrealized_pct >= self.PROFIT_HOLD_PCT

        # Take-profit: lock in large gains immediately (bypasses min hold)
        if unrealized_pct >= self.TAKE_PROFIT_PCT:
            return ExitDecision(True, f"take-profit unreal={unrealized_pct:+.1%}")

        # Trailing stop: once peak >= 40%, exit if dropped 30pp from peak (bypasses min hold)
        if peak_unreal >= self.TRAILING_STOP_MIN_PEAK:
            drawdown = peak_unreal - unrealized_pct
            if drawdown >= self.TRAILING_STOP_DIST:
                return ExitDecision(
                    True,
                    f"trailing-stop peak={peak_unreal:+.1%} drop={drawdown:.1%}>={self.TRAILING_STOP_DIST:.1%}"
                )

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
                self.s.min_leadlag_bps * self.LEADLAG_CONVERGE_PROFIT_FRAC if in_profit
                else self.s.min_leadlag_bps * self.LEADLAG_CONVERGE_LOSS_FRAC
            )
            if signed < ll_floor:
                # Confirmation gates
                adverse_dir = (
                    (side == "UP" and momentum.direction == -1)
                    or (side == "DOWN" and momentum.direction == 1)
                )
                mom_confirms = adverse_dir and momentum.z_score >= self.LEADLAG_EXIT_MIN_ADVERSE_Z
                loss_material = in_profit or unrealized_pct <= self.LEADLAG_EXIT_MIN_LOSS_PCT
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
