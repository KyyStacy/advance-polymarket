from __future__ import annotations

from dataclasses import dataclass

from ..config.settings import Settings
from ..signals.lead_lag import LeadLagReading
from ..signals.mispricing import MispricingReading
from ..signals.momentum import MomentumReading


@dataclass
class EntryDecision:
    enter: bool
    side: str | None       # "UP" / "DOWN"
    token_id: str | None
    implied_ask: float | None
    edge: float
    reason: str


class EntryEngine:
    def __init__(self, settings: Settings):
        self.s = settings
        self._persistence_counts: dict[tuple[str, str], int] = {}
        self._last_tick_valid: set[tuple[str, str]] = set()
        
        # Current active thresholds (initialized with steady defaults)
        self.active_min_edge: float = settings.steady_min_edge
        self.active_min_momentum_z: float = settings.steady_min_momentum_z
        self.active_min_leadlag_bps: float = settings.steady_min_leadlag_bps
        self.active_regime: str = "STEADY"

    def update_thresholds(self, min_edge: float, min_momentum_z: float, min_leadlag_bps: float, regime: str = "CHOPPY"):
        """Update active thresholds based on volatility regime."""
        self.active_min_edge = min_edge
        self.active_min_momentum_z = min_momentum_z
        self.active_min_leadlag_bps = min_leadlag_bps
        self.active_regime = regime

    def decide(
        self,
        seconds_into_window: float,
        seconds_left: float,
        momentum: MomentumReading,
        leadlag: LeadLagReading | None,
        mispricing: MispricingReading | None,
        up_ask: float | None,
        down_ask: float | None,
        up_bid: float | None,
        down_bid: float | None,
        up_token_id: str,
        down_token_id: str,
    ) -> EntryDecision:
        # Window timing gate
        if seconds_into_window < self.s.entry_window_start_sec:
            self._persistence_counts.clear()
            return EntryDecision(False, None, None, None, 0.0, "before-entry-window")
        if seconds_into_window > self.s.entry_window_end_sec:
            self._persistence_counts.clear()
            return EntryDecision(False, None, None, None, 0.0, "after-entry-window")
        if seconds_left < max(self.s.exit_hard_floor_sec * 2, 30):
            self._persistence_counts.clear()
            return EntryDecision(False, None, None, None, 0.0, "too-late")

        if mispricing is None or mispricing.side is None:
            self._persistence_counts.clear()
            return EntryDecision(False, None, None, None, 0.0, "no-mispricing")

        side = mispricing.side
        if side == "UP":
            implied_ask = up_ask
            implied_bid = up_bid
            token_id = up_token_id
        else:
            implied_ask = down_ask
            implied_bid = down_bid
            token_id = down_token_id
        
        # Identity for persistence tracking
        signal_id = (side, token_id)

        if implied_ask is None or implied_ask <= 0 or implied_ask >= 1:
            self._persistence_counts.pop(signal_id, None)
            return EntryDecision(False, side, token_id, implied_ask, mispricing.edge, "bad-ask")

        # Liquidity gate: must have a real exit. No bid = no buyer = stuck.
        if implied_bid is None or implied_bid <= 0:
            self._persistence_counts.pop(signal_id, None)
            return EntryDecision(False, side, token_id, implied_ask, mispricing.edge,
                                 "no-exit-bid")
        if implied_bid < self.s.entry_min_exit_bid:
            self._persistence_counts.pop(signal_id, None)
            return EntryDecision(False, side, token_id, implied_ask, mispricing.edge,
                                 f"thin-bid={implied_bid:.3f}<{self.s.entry_min_exit_bid}")
        spread = implied_ask - implied_bid
        if spread > self.s.entry_max_spread:
            self._persistence_counts.pop(signal_id, None)
            return EntryDecision(False, side, token_id, implied_ask, mispricing.edge,
                                 f"spread-too-wide={spread:.3f}>{self.s.entry_max_spread}")

        # Direction confirmation: momentum must agree with chosen side (UP needs +, DOWN -)
        if (side == "UP" and momentum.direction != 1) or (side == "DOWN" and momentum.direction != -1):
            self._persistence_counts.pop(signal_id, None)
            return EntryDecision(False, side, token_id, implied_ask, mispricing.edge, "mom-disagree")

        # Strength gates
        if momentum.z_score < self.active_min_momentum_z:
            self._persistence_counts.pop(signal_id, None)
            return EntryDecision(False, side, token_id, implied_ask, mispricing.edge,
                                 f"momentum-weak z={momentum.z_score:.2f}")
        if momentum.z_score > self.s.entry_max_momentum_z:
            self._persistence_counts.pop(signal_id, None)
            return EntryDecision(False, side, token_id, implied_ask, mispricing.edge,
                                 f"momentum-exhausted z={momentum.z_score:.2f}")

        if leadlag is not None:
            # FIX 2: Symmetric Lead-Lag Gate
            # We want to ensure CEX is leading in the direction of our trade.
            # UP: CEX must be > Oracle (deviation > 0)
            # DOWN: CEX must be < Oracle (deviation < 0)
            ll_deviation = leadlag.deviation_bps
            is_correct_direction = (side == "UP" and ll_deviation > 0) or (side == "DOWN" and ll_deviation < 0)
            ll_strength = abs(ll_deviation)

            if not is_correct_direction:
                self._persistence_counts.pop(signal_id, None)
                return EntryDecision(False, side, token_id, implied_ask, mispricing.edge,
                                     f"leadlag-wrong-dir ({ll_deviation:.1f}bps)")

            if ll_strength < self.active_min_leadlag_bps:
                self._persistence_counts.pop(signal_id, None)
                return EntryDecision(False, side, token_id, implied_ask, mispricing.edge,
                                     f"leadlag-weak {ll_strength:.1f}bps < {self.active_min_leadlag_bps}")

        # Odds bounds
        if implied_ask > self.s.max_odds:
            self._persistence_counts.pop(signal_id, None)
            return EntryDecision(False, side, token_id, implied_ask, mispricing.edge,
                                 f"above-max-odds ask={implied_ask:.3f}>{self.s.max_odds}")

        if implied_ask < self.s.min_odds:
            if not self.s.allow_out_of_bound_ev:
                self._persistence_counts.pop(signal_id, None)
                return EntryDecision(False, side, token_id, implied_ask, mispricing.edge,
                                     f"below-min-odds ask={implied_ask:.3f}<{self.s.min_odds} (no-ev-override)")
            if mispricing.edge < self.s.ev_override_min_edge:
                self._persistence_counts.pop(signal_id, None)
                return EntryDecision(False, side, token_id, implied_ask, mispricing.edge,
                                     f"below-min-odds edge={mispricing.edge:.3f}<{self.s.ev_override_min_edge:.2f}")

        # Stricter Edge logic: Edge must comfortably exceed the current spread.
        # Quality Filter: Required Edge = max(active_min_edge, spread * 1.25)
        required_edge = max(self.active_min_edge, spread * 1.25)
        if mispricing.edge < required_edge:
            self._persistence_counts.pop(signal_id, None)
            return EntryDecision(False, side, token_id, implied_ask, mispricing.edge,
                                 f"edge-vs-spread {mispricing.edge:.3f}<{required_edge:.3f}")

        # Momentum Velocity Filter
        ret30 = momentum.return_30s if side == "UP" else -momentum.return_30s
        ret60 = momentum.return_60s if side == "UP" else -momentum.return_60s
        if ret30 < ret60 * 0.40:
             self._persistence_counts.pop(signal_id, None)
             return EntryDecision(False, side, token_id, implied_ask, mispricing.edge,
                                  f"momentum-velocity-low 30s={ret30:.6f} vs 60s={ret60:.6f}")

        # Fix 4: Block UP in Low-Ask Zone during TURBO regime
        # Underdog bets at < 0.20 are too risky when volatility is extreme.
        if self.active_regime == "TURBO" and side == "UP" and implied_ask < self.s.entry_turbo_low_ask_limit:
             self._persistence_counts.pop(signal_id, None)
             return EntryDecision(False, side, token_id, implied_ask, mispricing.edge,
                                  f"turbo-low-ask-risk ({implied_ask:.3f})")

        # High-odds quality gate
        if implied_ask > 0.70 and leadlag is not None:
             ll_strength = abs(leadlag.deviation_bps)
             if ll_strength < self.active_min_leadlag_bps * 1.5:
                  self._persistence_counts.pop(signal_id, None)
                  return EntryDecision(False, side, token_id, implied_ask, mispricing.edge,
                                       f"high-odds-ll-weak ask={implied_ask:.2f} ll={ll_strength:.1f}bps")

        # --- SIGNAL IS VALID ---
        # Now check persistence (must be valid for N consecutive ticks)
        count = self._persistence_counts.get(signal_id, 0) + 1
        self._persistence_counts[signal_id] = count
        
        # Clear other sides/tokens that are no longer valid in this tick
        self._last_tick_valid = {signal_id}
        for k in list(self._persistence_counts.keys()):
            if k != signal_id:
                self._persistence_counts.pop(k, None)

        if count < self.s.entry_persistence_ticks:
            return EntryDecision(False, side, token_id, implied_ask, mispricing.edge,
                                 f"signal-warming-up ({count}/{self.s.entry_persistence_ticks})")

        return EntryDecision(True, side, token_id, implied_ask, mispricing.edge, "ok")
