from __future__ import annotations

import asyncio
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass

from ..config.settings import Settings
from ..market.clob_client import PolymarketClobClient
from ..utils.logger import get_logger
from .position_tracker import Position, PositionTracker

log = get_logger()

# Cooldown after a real (non-transient) order failure
_ORDER_FAIL_COOLDOWN_SEC = 5.0
# How long to poll Polymarket balances waiting for fill settlement.
# POLY_1271 deposit wallet settlement is typically 1–4s on Polygon; 10s gives
# generous headroom for relayer congestion without prematurely falling back to
# response-parsed estimates that may mask the true fill price.
_FILL_POLL_DEADLINE_SEC = 10.0
_FILL_POLL_INTERVAL_SEC = 0.25
# Fast polling: Gnosis Safe relay can land a fill in 200–600ms, so the first
# checks should be aggressive. Tighter initial poll (50ms) + exponential
# backoff catches quick fills 50–100ms faster than the previous flat 100ms.
# Tighter initial poll: Gnosis Safe relay typically responds within 200–600ms.
# 25ms first check catches quick fills ~25ms faster than previous 50ms.
_FILL_POLL_FAST_SEC = 0.025
_FILL_POLL_FAST_WINDOW_SEC = 2.0
_FILL_POLL_BACKOFF_MULT = 1.5
_FILL_POLL_MAX_SEC = 0.25
# Minimum balance change considered a real fill (filters out dust noise)
_BALANCE_DELTA_EPSILON = 0.005
# Max ask drift (absolute USD) validated in main.py before calling buy_to_open.
# Kept here as a named constant so entry_engine / main.py can import it as
# the single source of truth for the drift gate threshold.
ENTRY_MAX_DRIFT = 0.04
# Hard cap on how far the ask can drift FROM THE ORIGINAL SIGNAL across an
# entire entry attempt sequence (across multiple buy_to_open calls within the
# same window+side). Must equal ENTRY_MAX_DRIFT so the cascade gate and the
# per-call gate enforce the same absolute ceiling (first_ask + ENTRY_MAX_DRIFT).
# If this were larger than ENTRY_MAX_DRIFT, repeated FAK kills could let the
# strategy chase a runaway book: per-call gate resets each new call while
# cascade gate lets it through — bot chased 0.06→0.21 in case 2.
ENTRY_MAX_CASCADE_DRIFT = ENTRY_MAX_DRIFT
# Forget the signal anchor after this many seconds of inactivity (allows fresh
# entries after the book settles or moves back toward the original signal).
ENTRY_SIGNAL_ANCHOR_TTL_SEC = 30.0
# Reduced 2.0→1.5s: momentum signals in 5-min windows last ~0.5–1s; fills
# landing after 1.5s are stale and almost always lose to immediate exit signals.
# Tighter deadline means faster abort on slow books — better to skip and
# re-evaluate than to sit in a dead order for 2 full seconds.
_BUY_TOTAL_DEADLINE_SEC = 2.0
_SELL_MAX_ATTEMPTS = 3
_SELL_RETRY_DELAY_SEC = 0.020  # 20 ms — let book briefly re-quote between attempts
# Reduced from 6→2 attempts: when attempt #1 FAK kills at worst_price, an instant
# retry hits the same empty book and kills too. Better to give up fast and let
# exit_engine fire again ~50ms later with a fresh mark_price (and possibly fresh
# book depth). Total sell budget: ~3s worst-case vs 12s previously.
# Polymarket POST P50 ~150–250ms, P99 ~500ms. Cutting from 1.5s → 0.6s
# saves up to ~1s per dead attempt while keeping a 2× safety margin over P99.
_SELL_API_TIMEOUT_SEC = 1.2
# Sell reconciliation deadline. Kept at 20s as a conservative cap covering
# slow-relayer settlement on POLY_1271 deposit wallets. If actual settlement
# proves consistently faster (1–4s typical), this can be tightened later.
_SELL_FILL_POLL_DEADLINE_SEC = 20.0
# Reduced 1.7→1.1s to match the tighter 1.5s total budget.
# Leaves ~400ms for ghost-fill check (25ms wait + balance read + headroom).
# Polymarket POST P50 ~150–250ms, P99 ~500ms — 1.1s is 2.2× P99.
# A fill that takes >1.1s almost certainly won't arrive before the signal is stale.
_BUY_API_TIMEOUT_SEC = 1.5
# Minimum USD value for a sell order to be worth attempting.
# Polymarket rejects orders where maker or taker amount is 0; small shares
# at low prices (e.g. 0.006sh × 0.09 = $0.0005) always fail. Use 5¢ as
# a safe floor — anything below this is a ghost position and should be cleared.
_MIN_SELL_USD = 0.05
# How long to block re-buy on the same token after a sell.
# From bot.log analysis: Gnosis Safe on-chain confirmation arrives in ~1.5-2s
# (not 15-20s — that was the reconciliation *timeout*, not actual settlement).
# 5s provides a 2.5x safety margin over the observed maximum. Re-buying faster
# creates a double-position: bg_reconcile captures pre_token=0, then sees
# old+new tokens in delta and inflates size.
_POST_SELL_GUARD_SEC = 5.0


@dataclass
class FillReport:
    success: bool
    avg_price: float
    size_shares: float
    cost_usd: float
    raw: dict
    error: str | None = None
    reconciled: bool = True  # False = avg_price/size are estimates not on-chain truth


def _parse_amount(raw: object, sane_max: float) -> float:
    """Parse a numeric amount from an order response field.

    Polymarket sometimes returns string values, and may return values in
    micro-units (1e6 scale) instead of whole units. We normalize using a
    `sane_max` heuristic — anything more than 100x what we'd reasonably expect
    is treated as scaled and divided by 1e6.
    """
    if raw is None:
        return 0.0
    try:
        val = float(raw)
    except (TypeError, ValueError):
        return 0.0
    if val <= 0:
        return 0.0
    if val > max(sane_max, 1.0) * 100:
        val /= 1e6
    return val


def _parse_fill_from_response(
    resp: dict,
    side: str,                # "BUY" or "SELL"
    expected_size: float,
    expected_cost: float,
) -> tuple[float, float, float] | None:
    """Extract (size_shares, cost_or_proceeds_usd, avg_price) from an order
    response.

    Polymarket response semantics for FAK market orders:
      - BUY:  makingAmount = USDC paid,        takingAmount = shares received
      - SELL: makingAmount = shares delivered, takingAmount = USDC received

    Returns None if the response lacks the fill amounts (caller should fall
    back to balance reconciliation).
    """
    making = _parse_amount(resp.get("makingAmount"), expected_cost if side == "BUY" else expected_size)
    taking = _parse_amount(resp.get("takingAmount"), expected_size if side == "BUY" else expected_cost)

    if side == "BUY":
        size = taking
        cost = making
    else:
        size = making
        cost = taking

    if size <= 0 or cost <= 0:
        return None
    return size, cost, cost / size


class OrderExecutor:
    def __init__(
        self,
        settings: Settings,
        clob: PolymarketClobClient,
        positions: PositionTracker,
    ):
        self.s = settings
        self.clob = clob
        self.positions = positions
        self._last_fail_ts: float = 0.0
        # Signal anchor per (window_ts, side): (ask_at_first_signal, ts).
        # Refreshed on first attempt for a (window, side); subsequent attempts
        # are gated against this anchor's drift, NOT against the current
        # decision_ask. Prevents the bot from chasing a runaway book up after
        # repeated FAK kills.
        self._entry_signal_anchor: dict[tuple[int, str], tuple[float, float]] = {}
        # Per-token timestamp of last confirmed sell. Guards against re-buying
        # within the Gnosis Safe settlement window (_POST_SELL_GUARD_SEC).
        self._last_sell_ts: dict[str, float] = {}
        # Per-token "known zero balance until" timestamp. Set by sell reconciler
        # when on-chain confirms tokens are gone. While now < this value, the
        # buy hot path SKIPS the ~100ms balance pre-check (saves latency for
        # re-entries in the same window).
        self._known_zero_until: dict[str, float] = {}
        # Tokens known to have slow Gnosis Safe settlement (< expected on first poll).
        # Subsequent sells do a quick 2s check instead of full 20s poll to avoid
        # blocking the main tick loop on every retry.
        self._slow_settlement_tokens: set[str] = set()
        # Tokens whose sell reconcile is still in flight. Buy guard blocks re-entry
        self._reconciling_sell: set[str] = set()
        # Tracks tokens currently in the middle of a BUY call to prevent double-entry
        self._pending_buys: set[str] = set()
        self._counters: dict[str, int] = {
            "buy_filled": 0,
            "buy_fak_killed": 0,
            "buy_timeout": 0,
            "buy_ghost": 0,
            "buy_rejected": 0,
            "sell_filled": 0,
            "sell_fak_killed": 0,
            "sell_failed": 0,
        }

    def counters_snapshot(self) -> dict[str, int]:
        return dict(self._counters)

    def in_cooldown(self) -> bool:
        return (time.time() - self._last_fail_ts) < _ORDER_FAIL_COOLDOWN_SEC

    # ------------------------------------------------------------ signal anchor
    def check_signal_anchor(
        self,
        window_ts: int,
        side: str,
        current_ask: float,
    ) -> tuple[bool, str, float | None]:
        """Validate current ask against the recorded first-signal ask for this
        (window, side). Returns (allowed, reason, anchor_ask).

        - First call for a (window, side): records anchor, returns (True, ...).
        - Subsequent calls: rejects if drift > ENTRY_MAX_CASCADE_DRIFT.
        - Anchors older than ENTRY_SIGNAL_ANCHOR_TTL_SEC are reset (book has
          had time to settle; a fresh signal is genuinely new).
        """
        key = (window_ts, side)
        now = time.time()
        anchor = self._entry_signal_anchor.get(key)
        if anchor is not None:
            anchor_ask, anchor_ts = anchor
            if (now - anchor_ts) > ENTRY_SIGNAL_ANCHOR_TTL_SEC:
                anchor = None
                self._entry_signal_anchor.pop(key, None)
        if anchor is None:
            self._entry_signal_anchor[key] = (current_ask, now)
            return True, "anchor-set", current_ask
        anchor_ask, _ = anchor
        drift = current_ask - anchor_ask
        if drift > ENTRY_MAX_CASCADE_DRIFT:
            return False, (
                f"cascade-drift ${drift:+.4f} from anchor ${anchor_ask:.3f} "
                f"exceeds ${ENTRY_MAX_CASCADE_DRIFT:.4f}"
            ), anchor_ask
        return True, "within-anchor-drift", anchor_ask

    def clear_signal_anchors(self, window_ts: int | None = None) -> None:
        """Clear signal anchors. If `window_ts` given, only that window's keys
        are cleared (called on window rollover); otherwise wipe all."""
        if window_ts is None:
            self._entry_signal_anchor.clear()
        else:
            for k in list(self._entry_signal_anchor.keys()):
                if k[0] != window_ts:
                    self._entry_signal_anchor.pop(k, None)

    # ------------------------------------------------------------ reconcile
    async def _get_token_balance_with_retry(self, token_id: str, retries: int = 2) -> float | None:
        """Fetch balance with explicit retries for network resilience."""
        for i in range(retries + 1):
            try:
                bal = await self.clob.get_token_balance(token_id)
                if bal is not None:
                    return bal
            except Exception as e:
                if i == retries:
                    log.warning(f"[exec] balance fetch failed after {retries} retries: {e}")
                await asyncio.sleep(0.1)
        return None

    async def _reconcile_buy(
        self,
        token_id: str,
        pre_token: float,
        pre_pusd: float,
        fallback_size: float,
        fallback_cost: float,
    ) -> tuple[float, float, float, bool]:
        """Wait for token balance to increase, then compute exact fill from delta.

        Returns (size_shares, cost_usd, avg_price, reconciled). `reconciled=True`
        means values come from on-chain balance delta (ground truth). `False`
        means we timed out and the values are estimates — caller should treat
        them as suspicious.
        """
        deadline = time.monotonic() + _FILL_POLL_DEADLINE_SEC
        interval = _FILL_POLL_FAST_SEC
        while time.monotonic() < deadline:
            cur_token = await self._get_token_balance_with_retry(token_id)
            if (
                cur_token is not None
                and cur_token > pre_token + _BALANCE_DELTA_EPSILON
            ):
                size = cur_token - pre_token
                cur_pusd = await self.clob.get_usdc_balance()
                if cur_pusd is not None:
                    cost = max(pre_pusd - cur_pusd, 0.0)
                else:
                    cost = fallback_cost
                avg = cost / size if size > 0 else fallback_cost / max(fallback_size, 1e-6)
                return size, cost, avg, True
            await asyncio.sleep(interval)
            interval = min(interval * _FILL_POLL_BACKOFF_MULT, _FILL_POLL_MAX_SEC)
        # Final read — sometimes the delta lands right after the deadline
        cur_token = await self._get_token_balance_with_retry(token_id)
        cur_pusd = await self.clob.get_usdc_balance()
        if (
            cur_token is not None
            and cur_token > pre_token + _BALANCE_DELTA_EPSILON
            and cur_pusd is not None
        ):
            size = cur_token - pre_token
            cost = max(pre_pusd - cur_pusd, 0.0)
            avg = cost / size if size > 0 else fallback_cost / max(fallback_size, 1e-6)
            log.info(f"[exec] buy reconciled on final read after {_FILL_POLL_DEADLINE_SEC}s")
            return size, cost, avg, True
        log.warning(
            f"[exec] BUY NOT RECONCILED within {_FILL_POLL_DEADLINE_SEC}s — "
            f"logging estimate size={fallback_size:.4f} cost=${fallback_cost:.2f} "
            f"(actual fill may differ; check Polymarket directly)"
        )
        avg = fallback_cost / max(fallback_size, 1e-6)
        return fallback_size, fallback_cost, avg, False

    async def _reconcile_sell(
        self,
        token_id: str,
        pre_token: float,
        pre_pusd: float,
        sold_shares: float,
        fallback_avg: float,
    ) -> tuple[float, float, bool]:
        """Wait for both token-balance decrease and USDC-balance increase, then
        compute realized avg from the on-chain pUSD delta.

        Returns (avg_price_received_per_share, proceeds_usd, reconciled).
        `reconciled=False` means values are estimates — actual fill may differ.

        Uses _SELL_FILL_POLL_DEADLINE_SEC (20 s) instead of the buy deadline
        (10 s) as a safety margin for slow relayer settlement on POLY_1271
        deposit wallets. We require BOTH deltas before trusting the result:
        if token balance drops but USDC hasn't settled yet, proceeds = 0,
        which would produce a spurious fill_avg of 0.
        """
        deadline = time.monotonic() + _SELL_FILL_POLL_DEADLINE_SEC
        interval = _FILL_POLL_FAST_SEC
        while time.monotonic() < deadline:
            cur_token = await self._get_token_balance_with_retry(token_id)
            if (
                cur_token is not None
                and cur_token < pre_token - _BALANCE_DELTA_EPSILON
            ):
                cur_pusd = await self.clob.get_usdc_balance()
                # Require USDC increase too — token can drop before USDC credits
                # (Gnosis Safe relay lag). Returning with proceeds=0 would produce
                # fill_avg=0 and corrupt PnL.
                if cur_pusd is not None and cur_pusd > pre_pusd + _BALANCE_DELTA_EPSILON:
                    proceeds = cur_pusd - pre_pusd
                    actual_sold = pre_token - cur_token
                    if actual_sold > 0:
                        return proceeds / actual_sold, proceeds, True
                # else: token dropped but USDC pending — keep polling
            await asyncio.sleep(interval)
            interval = min(interval * _FILL_POLL_BACKOFF_MULT, _FILL_POLL_MAX_SEC)
        # Final read — sometimes both deltas land right after the deadline
        cur_token = await self._get_token_balance_with_retry(token_id)
        cur_pusd = await self.clob.get_usdc_balance()
        if (
            cur_token is not None
            and cur_token < pre_token - _BALANCE_DELTA_EPSILON
            and cur_pusd is not None
            and cur_pusd > pre_pusd + _BALANCE_DELTA_EPSILON
        ):
            proceeds = cur_pusd - pre_pusd
            actual_sold = pre_token - cur_token
            if actual_sold > 0:
                log.info(f"[exec] sell reconciled on final read after {_SELL_FILL_POLL_DEADLINE_SEC}s")
                return proceeds / actual_sold, proceeds, True
        log.warning(
            f"[exec] SELL NOT RECONCILED within {_SELL_FILL_POLL_DEADLINE_SEC}s — "
            f"logging estimate avg={fallback_avg:.4f} (actual fill may differ; "
            "check Polymarket directly)"
        )
        return fallback_avg, fallback_avg * sold_shares, False

    # ------------------------------------------------------------ background reconcile
    async def _bg_reconcile_buy(
        self,
        token_id: str,
        pre_token: float,
        pre_pusd: float,
        fallback_size: float,
        fallback_cost: float,
        pos_opened_at: float = 0.0,
    ) -> None:
        """Background task: wait for fill settlement, update position with actual values.

        pos_opened_at: timestamp of the position this task was launched for.
        Guards against stale reconcile tasks overwriting a newer position created
        after the original position was closed and re-entered within the same window.
        """
        try:
            size, cost, avg, reconciled = await self._reconcile_buy(
                token_id, pre_token, pre_pusd, fallback_size, fallback_cost
            )
            pos = self.positions.open.get(token_id)
            if pos is not None and reconciled:
                # Guard 1: position was closed and re-opened since this task launched.
                # The current pos belongs to a different trade — do NOT overwrite it.
                if pos_opened_at > 0 and abs(pos.opened_at - pos_opened_at) > 0.5:
                    log.warning(
                        f"[exec] reconcile skipped — position replaced since task start "
                        f"(task for t={pos_opened_at:.1f}, current t={pos.opened_at:.1f})"
                    )
                    return
                # Guard 2: sanity-check delta against expected size. A corrupted delta
                # (e.g., caught mid-settlement when the previous sell just cleared and
                # the new buy hasn't landed yet) produces a near-zero or absurd size.
                if size < fallback_size * 0.4 or size > fallback_size * 2.5:
                    log.warning(
                        f"[exec] reconcile sanity-check: delta={size:.4f}sh deviates "
                        f">60%% from response estimate={fallback_size:.4f}sh — "
                        "keeping response estimate (balance mid-settlement)"
                    )
                    return
                pos.entry_price = avg
                pos.size_shares = size
                pos.cost_usd = cost
                pos.reconciled = True
            marker = "FILL" if reconciled else "FILL?"
            log.info(
                f"[exec] {marker} BUY reconciled: {size:.4f}sh @{avg:.4f} cost=${cost:.4f}"
                f"{'' if reconciled else ' (ESTIMATE — settlement timed out)'}"
            )
        except Exception as e:
            log.error(f"[exec] _bg_reconcile_buy error for {token_id[:8]}: {e}")

    async def _bg_reconcile_sell(
        self,
        token_id: str,
        pre_token: float,
        pre_pusd: float,
        sold_shares: float,
        response_avg: float,
        pos_side: str = "",
        pos_window_ts: int = 0,
        pos_neg_risk: bool = False,
        pos_tick_size: float = 0.01,
    ) -> None:
        """Background task: verify response-parsed sell fill against on-chain data.

        Called only when _parse_fill_from_response succeeded (response_avg is the
        response-reported price). Logs a WARNING if on-chain data significantly
        contradicts the response.

        CRITICAL: If reconciliation times out AND tokens are still present on-chain,
        the CLOB response was optimistic (Gnosis Safe relay may have dropped the tx).
        Re-open the position so the exit loop can attempt another sell.
        pos_side / pos_window_ts are needed to reconstruct the position if that happens.
        """
        try:
            avg, proceeds, reconciled = await self._reconcile_sell(
                token_id, pre_token, pre_pusd, sold_shares, response_avg
            )
            if not reconciled:
                # Reconciliation timed out — check if tokens are still on Polymarket.
                # A successful CLOB response + no on-chain confirmation = Gnosis Safe
                # relay may have dropped the transaction. If tokens are still present,
                # re-open the position so the strategy can retry the sell.
                cur_token = await self._get_token_balance_with_retry(token_id)
                if cur_token is not None and cur_token > _BALANCE_DELTA_EPSILON:
                    log.error(
                        f"[exec] CRITICAL — SELL NOT CONFIRMED: {cur_token:.4f}sh still on-chain "
                        f"after {_SELL_FILL_POLL_DEADLINE_SEC}s (Gnosis Safe relay may have dropped tx). "
                        "Re-opening position for retry."
                    )
                    if pos_side and pos_window_ts:
                        from .position_tracker import Position
                        self.positions.add(Position(
                            token_id=token_id,
                            side=pos_side,
                            entry_price=response_avg,
                            size_shares=sold_shares,
                            cost_usd=response_avg * sold_shares,
                            market_window_ts=pos_window_ts,
                            neg_risk=pos_neg_risk,
                            tick_size=pos_tick_size,
                        ))
                else:
                    log.warning(
                        f"[exec] SELL NOT RECONCILED within {_SELL_FILL_POLL_DEADLINE_SEC}s — "
                        f"estimate avg={response_avg:.4f} (balance shows 0 — assume filled)"
                    )
                return
            # Sell confirmed on-chain → mark token balance as "known zero" for
            # 120s so the buy hot path can skip the ~100ms balance pre-check on
            # any re-entry within this window (extended from 30s: HFT re-entries
            # happen within seconds of a close; 30s TTL caused unnecessary cold
            # fetches on the 2nd re-entry in the same window).
            self._known_zero_until[token_id] = time.time() + 120.0
            diff = abs(avg - response_avg)
            if diff > 0.005:
                log.warning(
                    f"[exec] SELL VERIFY mismatch: on-chain @{avg:.4f} vs "
                    f"response @{response_avg:.4f} (diff ${diff:.4f}) — "
                    "response-parsed price used for PnL; check Polymarket"
                )
            else:
                log.info(
                    f"[exec] SELL VERIFIED on-chain: {sold_shares:.4f}sh @{avg:.4f} "
                    f"proceeds=${proceeds:.4f}"
                )
        except Exception as e:
            log.error(f"[exec] _bg_reconcile_sell error for {token_id[:8]}: {e}")
        finally:
            self._reconciling_sell.discard(token_id)

    async def _wait_for_token_balance(
        self,
        token_id: str,
        timeout_sec: float = 20.0,
        min_expected: float = 0.0,
    ) -> float:
        """Poll token balance until substantial or timeout.

        If min_expected > 0, only returns early when balance >= min_expected * 0.50.
        This prevents returning on dust (e.g. 0.0086sh residual) when the real
        batch (e.g. 1.4286sh) is still in transit through Gnosis Safe.
        """
        deadline = time.monotonic() + timeout_sec
        threshold = max(_BALANCE_DELTA_EPSILON, min_expected * 0.50)
        while time.monotonic() < deadline:
            await asyncio.sleep(0.5)
            cur = await self._get_token_balance_with_retry(token_id)
            if cur is not None and cur >= threshold:
                return cur
        return 0.0

    # ------------------------------------------------------------ buy
    @staticmethod
    def _is_fak_kill(err: str) -> bool:
        s = err.lower()
        # "no orders found" and "fak order" are the SDK's standard FAK-kill strings.
        # "no match" is a shorter variant some API versions return for the same condition.
        return "no orders found" in s or "fak order" in s or "no match" in s

    async def buy_to_open(
        self,
        token_id: str,
        side_label: str,
        usd_amount: float,
        expected_ask: float,
        market_window_ts: int,
        *,
        neg_risk: bool,
        tick_size: float,
        get_fresh_ask: Callable[[], float | None] | None = None,
        signal_ts_monotonic: float | None = None,
    ) -> FillReport:
        """Single-shot FAK market buy with worst-price slippage protection.

        worst_price = expected_ask + ENTRY_MAX_DRIFT is embedded in the order so
        the exchange enforces the ceiling — no client-side retry needed after a
        FAK kill. Ghost-fill detection still runs on timeout to recover positions
        that landed on-chain before our asyncio timeout fired.
        `get_fresh_ask` callback aborts before the API call if the book has
        already blown past worst_price, saving the 1.7s timeout budget.
        """
        if self.in_cooldown():
            log.info(f"[exec] buy skipped — executor in cooldown ({_ORDER_FAIL_COOLDOWN_SEC:.0f}s)")
            return FillReport(False, 0.0, 0.0, 0.0, {}, error="cooldown")
        
        if token_id in self._pending_buys:
            return FillReport(False, 0.0, 0.0, 0.0, {}, error="buy-in-flight")

        if self.s.dry_run:
            shares = usd_amount / max(expected_ask, 1e-3)
            log.info(
                f"[DRY] BUY {side_label} ${usd_amount:.2f} @≈{expected_ask:.3f} "
                f"={shares:.2f} shares token={token_id[:8]}…"
            )
            self.positions.add(
                Position(
                    token_id=token_id,
                    side=side_label,
                    entry_price=expected_ask,
                    size_shares=shares,
                    cost_usd=usd_amount,
                    market_window_ts=market_window_ts,
                    neg_risk=neg_risk,
                    tick_size=tick_size,
                )
            )
            self._entry_signal_anchor.pop((market_window_ts, side_label), None)
            return FillReport(True, expected_ask, shares, usd_amount, {"dry": True})

        # Post-sell guard: block re-buy for _POST_SELL_GUARD_SEC (5s) after any sell.
        # From bot.log: Gnosis Safe on-chain confirmation arrives in ~1.5-2s, but the
        # CLOB balance shows 0 immediately on acknowledgment. Re-buying in that window
        # causes bg_reconcile to capture pre_token=0, then see old+new tokens in the
        # delta → inflated size and wrong entry price.
        last_sell = self._last_sell_ts.get(token_id, 0.0)
        elapsed_since_sell = time.time() - last_sell
        if elapsed_since_sell < _POST_SELL_GUARD_SEC:
            log.warning(
                f"[exec] buy skipped — sold {elapsed_since_sell:.1f}s ago "
                f"(Gnosis Safe guard: wait {_POST_SELL_GUARD_SEC:.0f}s)"
            )
            return FillReport(False, 0.0, 0.0, 0.0, {}, error="post-sell-cooldown")

        # Block re-buy while a previous sell's reconcile is still in flight.
        # The post-sell guard above (5s) is shorter than the reconcile deadline
        # (20s); without this check, the reconcile could time out and re-open
        # the OLD position while a NEW position is also open → double-position
        # corruption (see Bug 1 / Bug 2 in project memory).
        if token_id in self._reconciling_sell:
            log.warning(
                f"[exec] buy skipped — sell reconcile still in flight for "
                f"{token_id[:8]}… (would risk double-position on timeout)"
            )
            return FillReport(False, 0.0, 0.0, 0.0, {}, error="sell-reconciling")

        # Token balance pre-check.
        # Hot-path optimisation: skip the ~100ms HTTP fetch when we KNOW the
        # balance is zero. Two safe sources of "known zero":
        #   (a) we never sold this token this session (so no residual possible),
        #   (b) sell reconciler confirmed zero within the last 30s.
        # On cache miss (cold token / unknown state), fall back to the sync
        # fetch to keep the orphan safety net intact.
        pre_pusd = self.clob._bankroll_cache
        ever_traded = token_id in self._last_sell_ts
        cached_zero_ok = time.time() < self._known_zero_until.get(token_id, 0.0)
        if not ever_traded or cached_zero_ok:
            pre_token = 0.0
        else:
            pre_token = (await self._get_token_balance_with_retry(token_id)) or 0.0
            if pre_token > 0.01:
                log.warning(
                    f"[exec] buy skipped — token {token_id[:8]}… has {pre_token:.4f}sh balance "
                    "(previous sell still settling or orphaned position)"
                )
                return FillReport(False, 0.0, 0.0, 0.0, {}, error="existing-balance")

        worst_price = round(expected_ask + ENTRY_MAX_DRIFT, 3)
        log.info(
            f"[exec] placing market buy {side_label} ${usd_amount:.2f} "
            f"@≈{expected_ask:.3f} worst={worst_price:.3f} "
            f"token={token_id[:8]}…"
        )

        # Pre-flight ask check (O(1) WS lookup) — abort before the API call if the
        # book has already blown past worst_price. Saves the 1.7s timeout budget.
        if get_fresh_ask is not None:
            fresh = get_fresh_ask()
            if fresh is None or fresh <= 0:
                log.info("[exec] buy aborted — ask unavailable (WS gap)")
                return FillReport(False, 0.0, 0.0, 0.0, {}, error="ask-gone")
            drift = fresh - expected_ask
            if drift > ENTRY_MAX_DRIFT:
                log.info(
                    f"[exec] buy aborted — fresh ask {fresh:.3f} > worst {worst_price:.3f} "
                    f"(drift ${drift:+.4f})"
                )
                if signal_ts_monotonic is not None:
                    total_ms = (time.monotonic() - signal_ts_monotonic) * 1000
                    log.info(
                        f"[lat] signal_to_result={total_ms:.0f}ms api=0ms "
                        f"outcome=drift token={token_id[:8]}"
                    )
                return FillReport(False, 0.0, 0.0, 0.0, {}, error=f"drift({drift:+.4f})")

        # Signal-age budget enforcement. The signal that triggered this buy is
        # only valid for ~1-2s before the book moves; respect _BUY_TOTAL_DEADLINE_SEC
        # measured from the moment the entry decision fired in main.py.
        api_timeout = _BUY_API_TIMEOUT_SEC
        if signal_ts_monotonic is not None:
            elapsed_since_signal = time.monotonic() - signal_ts_monotonic
            budget = _BUY_TOTAL_DEADLINE_SEC - elapsed_since_signal
            if budget <= 0.20:  # need ≥200ms to even attempt the POST
                log.info(
                    f"[exec] buy aborted — signal stale "
                    f"({elapsed_since_signal*1000:.0f}ms > {_BUY_TOTAL_DEADLINE_SEC*1000:.0f}ms budget)"
                )
                return FillReport(
                    False, 0.0, 0.0, 0.0, {},
                    error=f"signal-stale({elapsed_since_signal*1000:.0f}ms)",
                )
            # Leave ~200ms headroom for ghost-fill check + log work.
            api_timeout = min(_BUY_API_TIMEOUT_SEC, max(0.30, budget - 0.20))

        # Single attempt with full budget. worst_price is embedded in the order so
        # the exchange kills any unfilled portion above that price automatically.
        self._pending_buys.add(token_id)
        resp: dict | None = None
        last_err: str = ""
        ghost_fill_delta: float = 0.0
        t0 = time.monotonic()
        cancel_flag = threading.Event()
        try:
            try:
                resp = await asyncio.wait_for(
                    self.clob.place_market_buy(
                        token_id, usd_amount, worst_price,
                        neg_risk=neg_risk,
                        tick_size=f"{tick_size:g}",
                        cancel_flag=cancel_flag,
                    ),
                    timeout=api_timeout,
                )
            except asyncio.TimeoutError:
                cancel_flag.set()
                elapsed_so_far = (time.monotonic() - t0) * 1000
                log.warning(
                    f"[exec] buy TIMEOUT >{api_timeout*1000:.0f}ms "
                    f"(total {elapsed_so_far:.0f}ms) — checking for ghost fill (polling)"
                )
                # Settlement on Polygon is slow (1-4s). Poll for up to 4s to be sure.
                poll_deadline = time.monotonic() + 4.0
                while time.monotonic() < poll_deadline:
                    await asyncio.sleep(0.75)
                    post_bal = await self._get_token_balance_with_retry(token_id)
                    if post_bal is not None:
                        expected_sz = usd_amount / max(expected_ask, 1e-3)
                        delta = (post_bal or 0.0) - pre_token
                        if delta >= expected_sz * 0.40:
                            log.warning(f"[exec] GHOST BUY recovered after timeout (polled): {delta:.4f}sh")
                            ghost_fill_delta = delta
                            last_err = "timeout-ghost-fill"
                            break
                if ghost_fill_delta <= 0:
                    self._counters["buy_timeout"] += 1
                    return FillReport(False, 0.0, 0.0, 0.0, {}, error="timeout")

            except Exception as e:
                last_err = str(e)
                # Check for ghost fill on network errors OR duplicated order errors
                # (Duplicated means a previous attempt actually landed but we didn't get the resp).
                is_net = any(x in last_err.lower() for x in ["error sending request", "connection", "pool error"])
                is_dup = "duplicated" in last_err.lower()
                
                if is_net or is_dup:
                    log.warning(f"[exec] network/dup error during buy — checking for ghost fill (polling): {last_err}")
                    # Settlement on Polygon is slow (1-4s). Poll for up to 4s to be sure.
                    poll_deadline = time.monotonic() + 4.0
                    while time.monotonic() < poll_deadline:
                        await asyncio.sleep(0.75)
                        post_bal = await self._get_token_balance_with_retry(token_id)
                        if post_bal is not None:
                            expected_sz = usd_amount / max(expected_ask, 1e-3)
                            delta = (post_bal or 0.0) - pre_token
                            if delta >= expected_sz * 0.40:
                                log.warning(f"[exec] GHOST BUY recovered after net/dup error (polled): {delta:.4f}sh")
                                ghost_fill_delta = delta
                                break
                    if ghost_fill_delta <= 0:
                        return FillReport(False, 0.0, 0.0, 0.0, {}, error=f"error:{last_err}")
                else:
                    if self._is_fak_kill(last_err):
                        self._counters["buy_fak_killed"] += 1
                        log.info("[exec] buy killed (FAK no match) — book empty below worst_price")
                    else:
                        self._counters["buy_rejected"] += 1
                        log.error(f"[exec] buy_to_open exception: {last_err}")
                        self._last_fail_ts = time.time()
                    return FillReport(False, 0.0, 0.0, 0.0, {}, error=last_err)

            elapsed_ms = (time.monotonic() - t0) * 1000

            # Handle ghost fill detected in timeout handler.
            if ghost_fill_delta > 0:
                new_pos = Position(
                    token_id=token_id,
                    side=side_label,
                    entry_price=expected_ask,
                    size_shares=ghost_fill_delta,
                    cost_usd=ghost_fill_delta * expected_ask,
                    market_window_ts=market_window_ts,
                    reconciled=False,
                    neg_risk=neg_risk,
                    tick_size=tick_size,
                )
                self.positions.add(new_pos)
                self._counters["buy_ghost"] += 1
                log.warning(
                    f"[exec] GHOST BUY recovered {side_label} {ghost_fill_delta:.4f}sh "
                    f"@~{expected_ask:.4f} in {elapsed_ms:.0f}ms"
                )
                return FillReport(
                    True, expected_ask, ghost_fill_delta,
                    ghost_fill_delta * expected_ask, {}, error="timeout-recovered",
                )

            if resp is None:
                self._counters["buy_timeout"] += 1
                log.warning(
                    f"[exec] BUY UNFILLED in {elapsed_ms:.0f}ms — last={last_err[:120]}"
                )
                if signal_ts_monotonic is not None:
                    total_ms = (time.monotonic() - signal_ts_monotonic) * 1000
                    log.info(
                        f"[lat] signal_to_result={total_ms:.0f}ms api={elapsed_ms:.0f}ms "
                        f"outcome=fak-killed token={token_id[:8]}"
                    )
                return FillReport(False, 0.0, 0.0, 0.0, {}, error=f"fak-killed:{last_err}")

            ok = bool(resp.get("success", True)) and not resp.get("errorMsg") and not resp.get("message") and not resp.get("error")
            if not ok:
                err_msg = str(resp.get("errorMsg") or resp.get("error") or resp.get("message") or "")
                if self._is_fak_kill(err_msg):
                    self._counters["buy_fak_killed"] += 1
                    log.info("[exec] BUY killed (FAK no match) — book empty below worst_price")
                else:
                    self._counters["buy_rejected"] += 1
                    log.warning(f"[exec] BUY rejected (non-retryable): {err_msg or resp}")
                    self._last_fail_ts = time.time()
                return FillReport(False, 0.0, 0.0, 0.0, {}, error=err_msg or "rejected")

            # Parse actual fill from response. Polymarket returns makingAmount /
            # takingAmount on filled orders — using these gives accurate entry
            # price/size immediately, no balance polling required for PnL.
            expected_size = usd_amount / max(expected_ask, 1e-3)
            parsed = _parse_fill_from_response(resp, "BUY", expected_size, usd_amount)
            if parsed is not None:
                est_size, est_cost, est_avg = parsed
                from_response = True
            else:
                est_size = expected_size
                est_cost = usd_amount
                est_avg = expected_ask
                from_response = False

            # Register position immediately so the strategy loop can manage it on the
            # next tick. Background task verifies via balance delta as a safety net.
            new_pos = Position(
                token_id=token_id,
                side=side_label,
                entry_price=est_avg,
                size_shares=est_size,
                cost_usd=est_cost,
                market_window_ts=market_window_ts,
                reconciled=from_response,
                neg_risk=neg_risk,
                tick_size=tick_size,
            )
            self.positions.add(new_pos)
            self._counters["buy_filled"] += 1
            # Capture opened_at now so the bg_reconcile guard can verify it hasn't
            # been replaced by a newer position by the time the task runs.
            pos_opened_at = new_pos.opened_at
            # Successful entry — drop the cascade anchor so a future SELL+RE-ENTRY
            # within the same window starts fresh.
            self._entry_signal_anchor.pop((market_window_ts, side_label), None)
            post_ms = resp.get("_post_ms", 0.0) if isinstance(resp, dict) else 0.0
            sign_ms = resp.get("_sign_ms", 0.0) if isinstance(resp, dict) else 0.0
            log.info(
                f"[exec] BUY SENT {side_label} ${est_cost:.2f} @≈{est_avg:.4f} "
                f"~{est_size:.4f}sh in {elapsed_ms:.0f}ms (sign={sign_ms:.0f}ms post={post_ms:.0f}ms)"
                f"{' (from response)' if from_response else ' — reconciling'}"
            )
            if signal_ts_monotonic is not None:
                total_ms = (time.monotonic() - signal_ts_monotonic) * 1000
                log.info(
                    f"[lat] signal_to_result={total_ms:.0f}ms api={elapsed_ms:.0f}ms "
                    f"outcome=filled token={token_id[:8]}"
                )
            asyncio.ensure_future(
                self._bg_reconcile_buy(token_id, pre_token, pre_pusd, est_size, est_cost, pos_opened_at)
            )
            return FillReport(True, est_avg, est_size, est_cost, resp, reconciled=from_response)
        finally:
            self._pending_buys.discard(token_id)

    # ------------------------------------------------------------ sell
    async def sell_to_close(
        self, token_id: str, mark_price: float
    ) -> FillReport:
        pos = self.positions.open.get(token_id)
        if pos is None:
            return FillReport(False, 0.0, 0.0, 0.0, {}, error="no-position")
        if self.s.dry_run:
            log.info(
                f"[DRY] SELL {pos.side} {pos.size_shares:.4f}sh @≈{mark_price:.3f} "
                f"token={token_id[:8]}…"
            )
            self.positions.close(token_id, mark_price)
            return FillReport(
                True, mark_price, pos.size_shares, mark_price * pos.size_shares, {"dry": True}
            )

        # Floor mark_price to Polymarket minimum (0.01). Sending a sell at
        # implied_price < 0.01 causes "invalid price (0.001)" rejection. This
        # happens when the market is near-dead (UP token → 0 in a DOWN resolution).
        mark_price = max(mark_price, 0.01)

        # Snapshot pre-trade state for background reconciliation.
        # Note: _bankroll_cache may be stale up to ~40s; the response-parsed
        # fill is the primary source of truth and balance polling is a safety
        # net only.
        actual_bal = pos.size_shares
        pre_pusd = self.clob._bankroll_cache
        sold_shares = pos.size_shares
        pos_side = pos.side
        pos_window_ts = pos.market_window_ts
        pos_neg_risk = pos.neg_risk
        pos_tick_size = pos.tick_size

        # Guard: reject sells too small for Polymarket's minimum order threshold.
        # 0.0067sh × 0.09 = $0.0006 → always rejected with "invalid amounts".
        # These ghost positions arise from reconciliation corruption; clear them.
        if sold_shares * mark_price < _MIN_SELL_USD:
            log.warning(
                f"[exec] position too small to sell: {sold_shares:.6f}sh × "
                f"{mark_price:.3f} = ${sold_shares * mark_price:.4f} < "
                f"${_MIN_SELL_USD} minimum — clearing ghost position"
            )
            self.positions.pop_open(token_id)
            return FillReport(False, 0.0, 0.0, 0.0, {}, error="too-small-to-sell")

        # Retry loop on FAK kills — same rationale as buy: the book sometimes
        # briefly clears at our implied price and a short pause refills it.
        # Without this, a single FAK kill stranded the position until the next
        # tick's exit signal fired ~50-200ms later, missing favorable fills.
        resp: dict | None = None
        last_err: str = ""
        t0 = time.monotonic()
        # One-shot flag: the settlement-wait poll runs at most ONCE per sell_to_close
        # call, on the first "balance: 0" error. A second "balance: 0" after the wait
        # means tokens were consumed by a phantom previous sell — handle differently.
        _settlement_waited = False
        for attempt in range(1, _SELL_MAX_ATTEMPTS + 1):
            if attempt > 1:
                await asyncio.sleep(_SELL_RETRY_DELAY_SEC)

            try:
                # 15% slippage protection for market sell
                wp = max(round(mark_price * 0.85, 2), 0.01)
                resp = await asyncio.wait_for(
                    self.clob.place_market_sell(
                        token_id, sold_shares, worst_price=wp,
                        neg_risk=pos_neg_risk,
                        tick_size=f"{pos_tick_size:g}",
                    ),
                    timeout=_SELL_API_TIMEOUT_SEC,
                )
            except asyncio.TimeoutError:
                elapsed_so_far = (time.monotonic() - t0) * 1000
                log.warning(
                    f"[exec] sell attempt {attempt}/{_SELL_MAX_ATTEMPTS} "
                    f"TIMEOUT >{_SELL_API_TIMEOUT_SEC*1000:.0f}ms — retrying "
                    f"(total {elapsed_so_far:.0f}ms so far)"
                )
                resp = None
                continue
            except Exception as e:
                last_err = str(e)
                # Check for ghost sell fill on network errors!
                if "error sending request" in last_err.lower() or "connection" in last_err.lower() or "pool error" in last_err.lower():
                    log.warning(f"[exec] network error during sell — checking for ghost fill (polling): {last_err}")
                    # Settlement on Polygon is slow (1-4s). Poll for up to 5s to be sure.
                    poll_deadline = time.monotonic() + 5.0
                    while time.monotonic() < poll_deadline:
                        await asyncio.sleep(0.75)
                        cur_bal = await self._get_token_balance_with_retry(token_id)
                        # Sell confirmed if: (a) no tokens remain, OR (b) only dust remains.
                        if cur_bal is None or cur_bal < _BALANCE_DELTA_EPSILON or cur_bal < sold_shares * 0.10:
                            log.info(f"[exec] SELL ghost fill detected: {cur_bal:.4f}sh remaining — assuming filled")
                            self.positions.remove(token_id)
                            self._counters["sell_filled"] += 1
                            self._last_sell_ts[token_id] = time.time()
                            return FillReport(True, mark_price, sold_shares, mark_price * sold_shares, {}, error="ghost-recovered")
                    log.error(f"[exec] sell_to_close failed after network error & poll: {last_err}")
                else:
                    log.error(f"[exec] sell_to_close failed: {last_err}")
                
                is_fak = self._is_fak_kill(last_err)
                if is_fak and attempt < _SELL_MAX_ATTEMPTS:
                    log.info(
                        f"[exec] sell attempt {attempt}/{_SELL_MAX_ATTEMPTS} "
                        f"killed (FAK no match) — retrying"
                    )
                    resp = None
                    continue

                if "not enough balance" in last_err.lower():
                    # "sum of matched orders" = a previous sell attempt got queued by
                    # Polymarket and is consuming all token balance. This means the
                    # sell may have already matched at the CLOB level. Wait 3s and
                    # check if tokens are gone (= sell confirmed) or still there (= retry).
                    if "sum of matched orders" in last_err:
                        log.warning(
                            f"[exec] sell: existing order consuming all tokens "
                            f"— polling up to 1.5s for confirmation..."
                        )
                        # Poll-don't-sleep: bail out as soon as balance clears so
                        # the main loop isn't frozen for the full 1.5s. The previous
                        # 3.0s sleep blocked ~60 strategy ticks for no reason.
                        cur_bal = None
                        _phantom_deadline = time.monotonic() + 1.5
                        while time.monotonic() < _phantom_deadline:
                            await asyncio.sleep(0.15)
                            cur_bal = await self._get_token_balance_with_retry(token_id)
                            if cur_bal is None or cur_bal < sold_shares * 0.50:
                                break
                        # Sell confirmed if: (a) no tokens remain, OR (b) only dust remains.
                        # Dust (e.g. 0.0086sh << 1.43sh) = old residual from prior sessions;
                        # the current batch was cleared by the matched sell order.
                        if cur_bal is None or cur_bal < sold_shares * 0.50:
                            log.info(
                                f"[exec] sell confirmed — {sold_shares:.4f}sh cleared "
                                f"(residual: {cur_bal or 0:.4f}sh). "
                                f"Closing at mark={mark_price:.3f} (verify fill on Polymarket)"
                            )
                            self.positions.close(token_id, mark_price)
                            self._last_sell_ts[token_id] = time.time()
                            self._slow_settlement_tokens.discard(token_id)
                            return FillReport(
                                True, mark_price, sold_shares,
                                mark_price * sold_shares, {},
                            )
                        # Substantial tokens still there → FAK order was actually cancelled
                        sold_shares = min(cur_bal, sold_shares)
                        log.info(
                            f"[exec] tokens still present ({cur_bal:.4f}sh) "
                            "— pending order cancelled, retrying sell"
                        )
                        resp = None
                        continue

                    # "balance: 0" = Gnosis Safe buy not yet confirmed on-chain.
                    # Poll inline rather than bouncing to main.py 80 times.
                    # If token is known slow-settling, use a short 2s check instead
                    # of the full 20s to avoid blocking the tick loop repeatedly.
                    if not _settlement_waited:
                        _settlement_waited = True
                        is_slow = token_id in self._slow_settlement_tokens
                        poll_sec = 2.0 if is_slow else 20.0
                        log.info(
                            f"[exec] sell stalled — balance=0 (Gnosis Safe buy not yet settled). "
                            f"Polling up to {poll_sec:.0f}s for on-chain arrival..."
                        )
                        cur_bal = await self._wait_for_token_balance(
                            token_id, timeout_sec=poll_sec, min_expected=sold_shares
                        )
                        if cur_bal >= sold_shares * 0.50:
                            self._slow_settlement_tokens.discard(token_id)
                            sold_shares = min(cur_bal, sold_shares)
                            log.info(
                                f"[exec] buy settled: {cur_bal:.4f}sh arrived — retrying sell"
                            )
                            resp = None
                            continue
                        elif cur_bal > _BALANCE_DELTA_EPSILON:
                            # Dust only — main batch still in transit through Gnosis Safe
                            self._slow_settlement_tokens.add(token_id)
                            log.warning(
                                f"[exec] settlement partial: {cur_bal:.4f}sh dust "
                                f"<< {sold_shares:.4f}sh expected — will retry next tick"
                            )
                            return FillReport(False, 0.0, 0.0, 0.0, {}, error="settlement-dust")
                        self._slow_settlement_tokens.add(token_id)
                        log.warning(f"[exec] balance still 0 after {poll_sec:.0f}s — buy may have failed")
                    # Already waited or balance still 0 → check if phantom sell happened.
                    # Short poll loop (max 300ms) instead of 1.0s blocking sleep.
                    cur_bal = None
                    _phantom2_deadline = time.monotonic() + 0.3
                    while time.monotonic() < _phantom2_deadline:
                        await asyncio.sleep(0.1)
                        cur_bal = await self._get_token_balance_with_retry(token_id)
                        if cur_bal is None or cur_bal < _BALANCE_DELTA_EPSILON:
                            break
                    if cur_bal is None or cur_bal < _BALANCE_DELTA_EPSILON:
                        log.warning(
                            "[exec] balance=0 and buy not settling — "
                            "tokens may have been sold by phantom order; closing position"
                        )
                        self.positions.close(token_id, mark_price)
                        self._last_sell_ts[token_id] = time.time()
                        return FillReport(
                            True, mark_price, sold_shares,
                            mark_price * sold_shares, {},
                            error="phantom-sell-assumed",
                        )
                    # Tokens appeared — sell was just very slow; retry once more
                    sold_shares = min(cur_bal, sold_shares)
                    resp = None
                    continue

                if "invalid amounts" in last_err.lower():
                    log.warning(f"[exec] sell_to_close transient error: {last_err}")
                    return FillReport(False, 0.0, 0.0, 0.0, {}, error=last_err)

                # Last attempt FAK kill or non-retryable error
                if is_fak:
                    self._counters["sell_fak_killed"] += 1
                else:
                    self._counters["sell_failed"] += 1
                    self._last_fail_ts = time.time()
                log.warning(f"[exec] sell_to_close failed: {last_err}")
                return FillReport(False, 0.0, 0.0, 0.0, {}, error=last_err)

            ok = bool(resp.get("success", True)) and not resp.get("errorMsg") and not resp.get("message") and not resp.get("error")
            if ok:
                break

            err_msg = str(resp.get("errorMsg") or resp.get("error") or resp.get("message") or "")
            last_err = err_msg
            is_fak = self._is_fak_kill(err_msg)
            
            if is_fak and attempt < _SELL_MAX_ATTEMPTS:
                log.info(
                    f"[exec] sell attempt {attempt}/{_SELL_MAX_ATTEMPTS} "
                    f"rejected (no match) — retrying"
                )
                resp = None
                continue

            # Handle "not enough balance" appearing in response body
            if "not enough balance" in err_msg.lower():
                cur_bal = await self._get_token_balance_with_retry(token_id)
                if cur_bal is not None and cur_bal < sold_shares:
                    log.warning(
                        f"[exec] SELL balance mismatch: tracker={sold_shares:.4f}sh vs "
                        f"on-chain={cur_bal:.4f}sh — syncing and retrying"
                    )
                    sold_shares = cur_bal
                    pos.size_shares = cur_bal
                    if sold_shares * mark_price < _MIN_SELL_USD:
                        log.info(f"[exec] synced position for {token_id[:8]} is dust — closing")
                        self.positions.close(token_id, mark_price)
                        return FillReport(True, mark_price, 0.0, 0.0, {}, error="dust-sync")
                    resp = None
                    continue

                if "sum of matched orders" in err_msg:
                    log.warning("[exec] sell response: existing order consuming all tokens — checking...")
                    await asyncio.sleep(3.0)
                    cur_bal = await self._get_token_balance_with_retry(token_id)
                    # Sell confirmed if no tokens or only dust (old residual) remain
                    if cur_bal is None or cur_bal < sold_shares * 0.50:
                        log.info(
                            f"[exec] sell confirmed via response path — {sold_shares:.4f}sh cleared "
                            f"(residual: {cur_bal or 0:.4f}sh)"
                        )
                        self.positions.close(token_id, mark_price)
                        self._last_sell_ts[token_id] = time.time()
                        self._slow_settlement_tokens.discard(token_id)
                        return FillReport(True, mark_price, sold_shares, mark_price * sold_shares, {})
                    # Substantial tokens still there → retry
                    sold_shares = min(cur_bal, sold_shares)
                    resp = None
                    continue
            # Non-retryable response error (or last FAK attempt via response path)
            if is_fak:
                self._counters["sell_fak_killed"] += 1
            else:
                self._counters["sell_failed"] += 1
                self._last_fail_ts = time.time()
            log.warning(f"[exec] SELL rejected (non-retryable): {err_msg or resp}")
            return FillReport(False, 0.0, 0.0, 0.0, resp, error="rejected")

        elapsed_ms = (time.monotonic() - t0) * 1000
        if resp is None:
            self._counters["sell_fak_killed"] += 1
            log.warning(
                f"[exec] SELL UNFILLED after {_SELL_MAX_ATTEMPTS} attempts "
                f"in {elapsed_ms:.0f}ms — last={last_err[:120]}"
            )
            return FillReport(False, 0.0, 0.0, 0.0, {}, error=f"fak-killed:{last_err}")

        # Parse actual fill from response. For SELL: makingAmount = shares
        # delivered, takingAmount = USDC received. Use this as the close price
        # so realized_pnl is accurate, NOT mark_price (which is just our local
        # bid snapshot and can be wildly stale — case 2: mark=0.05, actual=0.16).
        expected_proceeds = mark_price * sold_shares
        parsed = _parse_fill_from_response(resp, "SELL", sold_shares, expected_proceeds)
        self._counters["sell_filled"] += 1
        post_ms = resp.get("_post_ms", 0.0) if isinstance(resp, dict) else 0.0
        sign_ms = resp.get("_sign_ms", 0.0) if isinstance(resp, dict) else 0.0
        if parsed is not None:
            filled_size, proceeds_usd, fill_avg = parsed
            reconciled_flag = True
            log.info(
                f"[exec] SELL SENT {pos.side} {filled_size:.4f}sh @≈{fill_avg:.4f} "
                f"proceeds=${proceeds_usd:.4f} in {elapsed_ms:.0f}ms "
                f"(sign={sign_ms:.0f}ms post={post_ms:.0f}ms from response)"
            )
            self.positions.close(token_id, fill_avg)
            self._last_sell_ts[token_id] = time.time()
            self._slow_settlement_tokens.discard(token_id)
            # Background verify: warn if on-chain data contradicts response.
            # Passes pos_side/pos_window_ts so the task can re-open the position
            # if on-chain confirmation times out (Gnosis Safe relay dropped the tx).
            self._reconciling_sell.add(token_id)
            asyncio.ensure_future(
                self._bg_reconcile_sell(
                    token_id, actual_bal, pre_pusd, sold_shares, fill_avg,
                    pos_side, pos_window_ts, pos_neg_risk, pos_tick_size,
                )
            )
        else:
            # Response didn't carry fill amounts (common with Gnosis Safe async
            # relay). Await on-chain settlement synchronously so PnL is locked
            # with the real fill price, not a stale mark snapshot. Uses the
            # extended 20 s timeout (_SELL_FILL_POLL_DEADLINE_SEC).
            log.info(
                f"[exec] SELL SENT {pos.side} {sold_shares:.4f}sh in {elapsed_ms:.0f}ms "
                "— response missing fill amounts, awaiting on-chain settlement..."
            )
            fill_avg, proceeds_usd, reconciled_flag = await self._reconcile_sell(
                token_id, actual_bal, pre_pusd, sold_shares, mark_price
            )
            filled_size = sold_shares
            settle_label = "on-chain reconciled" if reconciled_flag else "mark estimate — settlement timed out"
            log.info(
                f"[exec] SELL SETTLED {pos.side} {filled_size:.4f}sh @{fill_avg:.4f} "
                f"proceeds=${proceeds_usd:.4f} ({settle_label})"
            )
            self.positions.close(token_id, fill_avg)
            self._last_sell_ts[token_id] = time.time()

        return FillReport(True, fill_avg, filled_size, proceeds_usd, resp, reconciled=reconciled_flag)
