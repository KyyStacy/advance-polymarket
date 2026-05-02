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
    # Liquidity gates — a position with no viable exit is a guaranteed loss.
    # MIN_EXIT_BID: must be possible to sell at >= this price to hand over the
    # position. 0.05 = if no bidder is even at 5¢, the market is too thin.
    # MAX_SPREAD: ask - bid must be reasonable. >15¢ implies one-sided book.
    MIN_EXIT_BID = 0.05
    MAX_SPREAD = 0.15

    def __init__(self, settings: Settings):
        self.s = settings

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
            return EntryDecision(False, None, None, None, 0.0, "before-entry-window")
        if seconds_into_window > self.s.entry_window_end_sec:
            return EntryDecision(False, None, None, None, 0.0, "after-entry-window")
        if seconds_left < max(self.s.exit_hard_floor_sec * 2, 30):
            return EntryDecision(False, None, None, None, 0.0, "too-late")

        if mispricing is None or mispricing.side is None:
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
        if implied_ask is None or implied_ask <= 0 or implied_ask >= 1:
            return EntryDecision(False, side, token_id, implied_ask, mispricing.edge, "bad-ask")

        # Liquidity gate: must have a real exit. No bid = no buyer = stuck.
        if implied_bid is None or implied_bid <= 0:
            return EntryDecision(False, side, token_id, implied_ask, mispricing.edge,
                                 "no-exit-bid")
        if implied_bid < self.MIN_EXIT_BID:
            return EntryDecision(False, side, token_id, implied_ask, mispricing.edge,
                                 f"thin-bid={implied_bid:.3f}<{self.MIN_EXIT_BID}")
        spread = implied_ask - implied_bid
        if spread > self.MAX_SPREAD:
            return EntryDecision(False, side, token_id, implied_ask, mispricing.edge,
                                 f"spread-too-wide={spread:.3f}>{self.MAX_SPREAD}")

        # Direction confirmation: momentum must agree with chosen side (UP needs +, DOWN -)
        if side == "UP" and momentum.direction != 1:
            return EntryDecision(False, side, token_id, implied_ask, mispricing.edge, "mom-disagree")
        if side == "DOWN" and momentum.direction != -1:
            return EntryDecision(False, side, token_id, implied_ask, mispricing.edge, "mom-disagree")

        # Strength gates
        if momentum.z_score < self.s.min_momentum_z:
            return EntryDecision(False, side, token_id, implied_ask, mispricing.edge,
                                 f"momentum-weak z={momentum.z_score:.2f}")

        if leadlag is not None:
            ll_signed = leadlag.deviation_bps if side == "UP" else -leadlag.deviation_bps
            if ll_signed < self.s.min_leadlag_bps:
                return EntryDecision(False, side, token_id, implied_ask, mispricing.edge,
                                     f"leadlag-weak {ll_signed:.1f}bps")

        # Odds bounds
        # Above MAX_ODDS (>0.65): margin terlalu tipis (<35 sen/share) — blok penuh, tanpa exception.
        if implied_ask > self.s.max_odds:
            return EntryDecision(False, side, token_id, implied_ask, mispricing.edge,
                                 f"above-max-odds ask={implied_ask:.3f}>{self.s.max_odds}")

        # Below MIN_ODDS (<0.40): posisi contrarian dengan profit potensial >60 sen/share.
        # Diizinkan jika ALLOW_OUT_OF_BOUND_EV=true dan edge cukup kuat (mispricing ekstrem).
        if implied_ask < self.s.min_odds:
            if not self.s.allow_out_of_bound_ev:
                return EntryDecision(False, side, token_id, implied_ask, mispricing.edge,
                                     f"below-min-odds ask={implied_ask:.3f}<{self.s.min_odds} (no-ev-override)")
            if mispricing.edge < self.s.ev_override_min_edge:
                return EntryDecision(False, side, token_id, implied_ask, mispricing.edge,
                                     f"below-min-odds edge={mispricing.edge:.3f}<{self.s.ev_override_min_edge:.2f}")

        # Edge gate
        if mispricing.edge < self.s.min_edge:
            return EntryDecision(False, side, token_id, implied_ask, mispricing.edge,
                                 f"edge<{self.s.min_edge:.2f}")

        return EntryDecision(True, side, token_id, implied_ask, mispricing.edge, "ok")
