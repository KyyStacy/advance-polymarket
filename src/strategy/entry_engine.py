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
        if implied_bid < self.s.entry_min_exit_bid:
            return EntryDecision(False, side, token_id, implied_ask, mispricing.edge,
                                 f"thin-bid={implied_bid:.3f}<{self.s.entry_min_exit_bid}")
        spread = implied_ask - implied_bid
        if spread > self.s.entry_max_spread:
            return EntryDecision(False, side, token_id, implied_ask, mispricing.edge,
                                 f"spread-too-wide={spread:.3f}>{self.s.entry_max_spread}")

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

        # Extreme-price filters — two tiers based on how close to resolution:
        #
        # Tier 1 (ask < 0.10): market is ~90%+ sure we lose. Near resolution,
        # books are very thin and the outcome is nearly binary. Live data shows:
        #   - Wins at ask<0.10 ALL had ll >= 2.5bps
        #   - Losses at ask<0.10 ALL had ll = 2.1-2.2bps (barely above 2.0 floor)
        # Require ll >= 2.5bps as an additional confirmation that lead-lag
        # genuinely supports the trade, not just hitting the minimum threshold.
        if implied_ask < 0.10 and leadlag is not None:
            ll_signed = leadlag.deviation_bps if side == "UP" else -leadlag.deviation_bps
            if ll_signed < 2.5:
                return EntryDecision(
                    False, side, token_id, implied_ask, mispricing.edge,
                    f"extreme-price-ll-weak ask={implied_ask:.3f}<0.10 ll={ll_signed:.1f}bps<2.5"
                )
        #
        # Tier 2 (ask < 0.15): market prices us as 85%+ loser.
        #   1. Book is thin → market order fills 15-25% above signal ask
        #   2. Momentum half-life ~2s; Gnosis Safe fills take 0.5-3s → stale by fill
        # Require z >= 2.5 (vs normal 2.0) for real directional conviction.
        if implied_ask < 0.15 and momentum.z_score < 2.5:
            return EntryDecision(
                False, side, token_id, implied_ask, mispricing.edge,
                f"extreme-price-z-weak ask={implied_ask:.3f}<0.15 z={momentum.z_score:.2f}<2.5"
            )

        # Edge gate
        if mispricing.edge < self.s.min_edge:
            return EntryDecision(False, side, token_id, implied_ask, mispricing.edge,
                                 f"edge<{self.s.min_edge:.2f}")

        # Momentum Velocity: ensure short-term (30s) momentum hasn't completely
        # died out compared to the 60s average. This is more lenient than
        # acceleration but still catches late entries.
        ret30 = momentum.return_30s if side == "UP" else -momentum.return_30s
        ret60 = momentum.return_60s if side == "UP" else -momentum.return_60s
        if ret30 < ret60 * 0.40:
             return EntryDecision(False, side, token_id, implied_ask, mispricing.edge,
                                  f"momentum-velocity-low 30s={ret30:.6f} vs 60s={ret60:.6f}")


        # High-odds quality gate: for trades at ask > 0.70, upside is capped.
        # Require lead-lag to be stronger than baseline.
        if implied_ask > 0.70 and leadlag is not None:
             ll_signed = leadlag.deviation_bps if side == "UP" else -leadlag.deviation_bps
             if ll_signed < self.s.min_leadlag_bps * 1.5:
                  return EntryDecision(False, side, token_id, implied_ask, mispricing.edge,
                                       f"high-odds-ll-weak ask={implied_ask:.2f} ll={ll_signed:.1f}bps")

        return EntryDecision(True, side, token_id, implied_ask, mispricing.edge, "ok")
