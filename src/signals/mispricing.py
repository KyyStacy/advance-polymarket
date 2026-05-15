from __future__ import annotations

import math
from dataclasses import dataclass

from ..signals.lead_lag import LeadLagReading
from ..signals.momentum import MomentumReading


@dataclass
class MispricingReading:
    fair_p_up: float          # model prob that BTC closes >= start
    implied_p_up: float       # from polymarket "Up" ask
    edge_up: float            # fair_p_up - implied_p_up
    edge_down: float          # (1 - fair_p_up) - implied_p_down
    side: str | None          # "UP" / "DOWN" / None
    edge: float               # max(edge_up, edge_down) on chosen side
    fair_basis: dict


class MispricingEvaluator:
    """Estimates fair P(Up) from current CEX-vs-anchor delta + momentum drift,
    then compares to Polymarket's implied probability.

    Data flow architecture:
      - anchor_price: MUST be Chainlink RTDS candle-open price (single source
        of truth).  Polymarket resolves Up/Down against Chainlink, so the
        anchor must come exclusively from the oracle — no CEX fallback.
      - current_cex: blended CEX mid (Binance + Coinbase).  Used as the "fast
        leg" because CEX price leads Chainlink by several seconds, giving a
        predictive edge on where the oracle will settle at resolution.

    The CEX leg is an *input signal* for forecasting, NOT a substitute for
    the anchor.  Lead-lag (cex_p - ora_p) runs separately in LeadLagDetector.
    """

    def __init__(
        self,
        # log-return per second volatility (annualized BTC ~ 60% / sqrt(yr seconds))
        sigma_per_sec: float = 0.000_06,
        momentum_drift_weight: float = 0.0008,
        leadlag_drift_weight_per_bp: float = 0.0002,
        log_now_decay: float = 0.5,
    ):
        self.sigma_per_sec = sigma_per_sec
        self.mom_w = momentum_drift_weight
        self.ll_w = leadlag_drift_weight_per_bp
        self.log_now_decay = log_now_decay

    def update_sigma(self, sigma: float):
        """Update the underlying volatility parameter for fair price calculation."""
        self.sigma_per_sec = sigma

    def estimate_fair_p_up(
        self,
        anchor_price: float,
        current_cex: float,
        seconds_left: float,
        momentum: MomentumReading,
        leadlag: LeadLagReading | None,
    ) -> tuple[float, dict]:
        if anchor_price <= 0 or current_cex <= 0 or seconds_left <= 0:
            return 0.5, {"reason": "no-signal"}

        # 1. State: Move that has already happened relative to anchor.
        log_now = math.log(current_cex / anchor_price)
        
        # FIX 1: Apply time-based decay to log_now. 
        # At the start of a candle (seconds_left is large), log_now is "noisy".
        # We shouldn't bet solely on the fact that price is already up/down.
        # As we approach resolution, log_now becomes "realized profit".
        # window_total is usually 300s (5m) or 60s (1m). 
        # We'll use a conservative 300s baseline or the current window's max.
        window_total = max(300.0, seconds_left)
        progress = 1.0 - (seconds_left / window_total)
        # log_now_decayed = log_now * (0.2 + 0.8 * progress) # Start with 20% weight, end at 100%
        log_now_decayed = log_now * self.log_now_decay 

        # 2. Prediction: Forward drift from momentum + lead-lag.
        mu = self.mom_w * momentum.return_30s + self.ll_w * (
            (leadlag.deviation_bps if leadlag else 0.0)
        )
        drift_forward = mu * seconds_left
        
        # log_total is our 'conviction' level for the resolution price.
        log_total = log_now_decayed + drift_forward

        sigma = self.sigma_per_sec * math.sqrt(max(seconds_left, 1.0))
        if sigma <= 1e-12:
            p_up = 1.0 if log_total >= 0 else 0.0
        else:
            # P(Z >= -log_total/sigma) = Phi(log_total/sigma)
            p_up = 0.5 * (1 + math.erf((log_total / sigma) / math.sqrt(2)))
        p_up = min(0.999, max(0.001, p_up))
        
        basis = {
            "log_now": log_now,
            "log_now_decayed": log_now_decayed,
            "mu_per_sec": mu,
            "drift_forward": drift_forward,
            "sigma": sigma,
            "seconds_left": seconds_left,
        }
        return p_up, basis

    def evaluate(
        self,
        anchor_price: float,
        current_cex: float,
        seconds_left: float,
        momentum: MomentumReading,
        leadlag: LeadLagReading | None,
        implied_p_up_ask: float | None,
        implied_p_down_ask: float | None,
        fee_rate: float = 0.0,
    ) -> MispricingReading | None:
        """Compute mispricing edge, net of round-trip taker fees.

        fee_rate: fractional taker fee (e.g. 0.02 for 2%). We subtract fees on
        BOTH legs — entry and the eventual exit — so the edge represents true
        net expected profit per share. Hold-to-resolution wins skip the exit
        fee, but most positions are closed via signal exit so we cost in both.
        """
        if implied_p_up_ask is None and implied_p_down_ask is None:
            return None
        fair, basis = self.estimate_fair_p_up(
            anchor_price, current_cex, seconds_left, momentum, leadlag
        )
        # Net edge = (fair_prob * (1 - fee_rate)) - (ask * (1 + fee_rate))
        # Entry: pay ask * (1 + fee). Exit: receive fair_p * (1 - fee).
        # The (1 - fee) on proceeds models early-exit cost, which is the typical
        # outcome — settling at resolution skips this fee but is rare.
        sell_factor = 1.0 - fee_rate
        buy_factor = 1.0 + fee_rate
        edge_up = (
            (fair * sell_factor - implied_p_up_ask * buy_factor)
            if implied_p_up_ask is not None
            else -1.0
        )
        edge_dn = (
            ((1.0 - fair) * sell_factor - implied_p_down_ask * buy_factor)
            if implied_p_down_ask is not None
            else -1.0
        )
        if edge_up >= edge_dn:
            side = "UP" if edge_up > 0 else None
            edge = edge_up
        else:
            side = "DOWN" if edge_dn > 0 else None
            edge = edge_dn
        return MispricingReading(
            fair_p_up=fair,
            implied_p_up=implied_p_up_ask if implied_p_up_ask is not None else float("nan"),
            edge_up=edge_up,
            edge_down=edge_dn,
            side=side,
            edge=edge,
            fair_basis=basis,
        )
