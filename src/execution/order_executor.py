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
# Gnosis Safe (sig_type=2) settlement can take 5–8s; 2.5s was too short and
# triggered fallback estimates that masked actual fill prices.
_FILL_POLL_DEADLINE_SEC = 10.0
_FILL_POLL_INTERVAL_SEC = 0.25
# Fast polling for first 2s: catches quick EOA fills without flooding later
_FILL_POLL_FAST_SEC = 0.10
_FILL_POLL_FAST_WINDOW_SEC = 2.0
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
# Wall-clock deadline for the entire buy sequence.
# Momentum signals in 5-minute Polymarket windows last ~1-2 seconds; fills landing
# after this deadline are "stale" and almost always lose to immediate exit signals.
_BUY_TOTAL_DEADLINE_SEC = 2.0
_SELL_MAX_ATTEMPTS = 6
_SELL_RETRY_DELAY_SEC = 0.010  # 10 ms — fast retry to reduce total sell latency during
                                # volatile moves; 6 attempts × ~250ms API ≈ 1.5s total
# Hard timeout per sell API call — mirrors buy timeout. Prevents sell from hanging
# during API slowdowns while position is open and market is moving against us.
_SELL_API_TIMEOUT_SEC = 2.0
# Sell reconciliation needs longer deadline than buys: Gnosis Safe (sig_type=2)
# transactions take 15–20 s on Polygon. 10 s was too short and caused the
# reconciler to time out, leaving the PnL locked at a stale mark_price estimate
# instead of the actual on-chain fill (case 2: estimate 0.05, actual 0.16).
_SELL_FILL_POLL_DEADLINE_SEC = 20.0
# Hard timeout per buy API call (sign + POST). Single attempt uses most of the
# 2s signal budget, leaving 0.3s for ghost-fill check (150ms wait + balance read).
# worst_price embedded in the order lets the exchange enforce the price ceiling —
# no retry needed after a FAK kill.
_BUY_API_TIMEOUT_SEC = 1.7
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
        # Tokens known to have slow Gnosis Safe settlement (< expected on first poll).
        # Subsequent sells do a quick 2s check instead of full 20s poll to avoid
        # blocking the main tick loop on every retry.
        self._slow_settlement_tokens: set[str] = set()

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
        fast_until = time.monotonic() + _FILL_POLL_FAST_WINDOW_SEC
        while time.monotonic() < deadline:
            cur_token = await self.clob.get_token_balance(token_id)
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
            interval = _FILL_POLL_FAST_SEC if time.monotonic() < fast_until else _FILL_POLL_INTERVAL_SEC
            await asyncio.sleep(interval)
        # Final read — sometimes the delta lands right after the deadline
        cur_token = await self.clob.get_token_balance(token_id)
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
        (10 s) because Gnosis Safe (sig_type=2) transactions take 15–20 s on
        Polygon. We require BOTH deltas before trusting the result: if token
        balance drops but USDC hasn't settled yet, proceeds = 0, which would
        produce a spurious fill_avg of 0.
        """
        deadline = time.monotonic() + _SELL_FILL_POLL_DEADLINE_SEC
        fast_until = time.monotonic() + _FILL_POLL_FAST_WINDOW_SEC
        while time.monotonic() < deadline:
            cur_token = await self.clob.get_token_balance(token_id)
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
            interval = _FILL_POLL_FAST_SEC if time.monotonic() < fast_until else _FILL_POLL_INTERVAL_SEC
            await asyncio.sleep(interval)
        # Final read — sometimes both deltas land right after the deadline
        cur_token = await self.clob.get_token_balance(token_id)
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
                cur_token = await self.clob.get_token_balance(token_id)
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
                        ))
                else:
                    log.warning(
                        f"[exec] SELL NOT RECONCILED within {_SELL_FILL_POLL_DEADLINE_SEC}s — "
                        f"estimate avg={response_avg:.4f} (balance shows 0 — assume filled)"
                    )
                return
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
            cur = await self.clob.get_token_balance(token_id)
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
        get_fresh_ask: Callable[[], float | None] | None = None,
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

        # Read actual token balance before placing the buy. This is a ~100ms
        # HTTP call but is essential for correct balance-delta reconciliation.
        # A pre_token > 0 here means the sell either didn't land or the 5s guard
        # above didn't catch a very recent sell. Block in either case.
        pre_pusd = self.clob._bankroll_cache
        pre_token = (await self.clob.get_token_balance(token_id)) or 0.0
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
                return FillReport(False, 0.0, 0.0, 0.0, {}, error=f"drift({drift:+.4f})")

        # Single attempt with full budget. worst_price is embedded in the order so
        # the exchange kills any unfilled portion above that price automatically.
        resp: dict | None = None
        last_err: str = ""
        ghost_fill_delta: float = 0.0
        t0 = time.monotonic()
        cancel_flag = threading.Event()
        try:
            resp = await asyncio.wait_for(
                self.clob.place_market_buy(
                    token_id, usd_amount, worst_price, cancel_flag=cancel_flag
                ),
                timeout=_BUY_API_TIMEOUT_SEC,
            )
        except asyncio.TimeoutError:
            cancel_flag.set()
            elapsed_so_far = (time.monotonic() - t0) * 1000
            log.warning(
                f"[exec] buy TIMEOUT >{_BUY_API_TIMEOUT_SEC*1000:.0f}ms "
                f"(total {elapsed_so_far:.0f}ms) — quick ghost-fill check"
            )
            # The POST may have reached Polymarket before the timeout fired.
            # Brief wait (150ms) for Gnosis Safe to begin on-chain settlement,
            # then check balance. If tokens arrived, recover as ghost position.
            await asyncio.sleep(0.15)
            post_bal = await self.clob.get_token_balance(token_id)
            if post_bal is not None:
                expected_sz = usd_amount / max(expected_ask, 1e-3)
                delta = (post_bal or 0.0) - pre_token
                if delta >= expected_sz * 0.40:
                    log.warning(
                        f"[exec] BUY ghost fill detected: {delta:.4f}sh arrived "
                        f"— recovering @~{expected_ask:.4f}"
                    )
                    ghost_fill_delta = delta
                    last_err = "timeout-ghost-fill"
        except Exception as e:
            last_err = str(e)
            if self._is_fak_kill(last_err):
                log.info("[exec] buy killed (FAK no match) — book empty below worst_price")
            else:
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
            )
            self.positions.add(new_pos)
            log.warning(
                f"[exec] GHOST BUY recovered {side_label} {ghost_fill_delta:.4f}sh "
                f"@~{expected_ask:.4f} in {elapsed_ms:.0f}ms"
            )
            return FillReport(
                True, expected_ask, ghost_fill_delta,
                ghost_fill_delta * expected_ask, {}, error="timeout-recovered",
            )

        if resp is None:
            log.warning(
                f"[exec] BUY UNFILLED in {elapsed_ms:.0f}ms — last={last_err[:120]}"
            )
            return FillReport(False, 0.0, 0.0, 0.0, {}, error=f"fak-killed:{last_err}")

        ok = bool(resp.get("success", True)) and not resp.get("errorMsg")
        if not ok:
            err_msg = str(resp.get("errorMsg") or resp.get("error") or "")
            if self._is_fak_kill(err_msg):
                log.info("[exec] BUY killed (FAK no match) — book empty below worst_price")
            else:
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
        )
        self.positions.add(new_pos)
        # Capture opened_at now so the bg_reconcile guard can verify it hasn't
        # been replaced by a newer position by the time the task runs.
        pos_opened_at = new_pos.opened_at
        # Successful entry — drop the cascade anchor so a future SELL+RE-ENTRY
        # within the same window starts fresh.
        self._entry_signal_anchor.pop((market_window_ts, side_label), None)
        log.info(
            f"[exec] BUY SENT {side_label} ${est_cost:.2f} @≈{est_avg:.4f} "
            f"~{est_size:.4f}sh in {elapsed_ms:.0f}ms"
            f"{' (from response)' if from_response else ' — reconciling'}"
        )
        asyncio.ensure_future(
            self._bg_reconcile_buy(token_id, pre_token, pre_pusd, est_size, est_cost, pos_opened_at)
        )
        return FillReport(True, est_avg, est_size, est_cost, resp, reconciled=from_response)

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

        # Guard: reject sells too small for Polymarket's minimum order threshold.
        # 0.0067sh × 0.09 = $0.0006 → always rejected with "invalid amounts".
        # These ghost positions arise from reconciliation corruption; clear them.
        if sold_shares * mark_price < _MIN_SELL_USD:
            log.warning(
                f"[exec] position too small to sell: {sold_shares:.6f}sh × "
                f"{mark_price:.3f} = ${sold_shares * mark_price:.4f} < "
                f"${_MIN_SELL_USD} minimum — clearing ghost position"
            )
            self.positions.open.pop(token_id, None)
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
                resp = await asyncio.wait_for(
                    self.clob.place_market_sell(token_id, sold_shares),
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
                            f"— waiting 3s to check outcome..."
                        )
                        await asyncio.sleep(3.0)
                        cur_bal = await self.clob.get_token_balance(token_id)
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
                    # Already waited or balance still 0 → check if phantom sell happened
                    await asyncio.sleep(1.0)
                    cur_bal = await self.clob.get_token_balance(token_id)
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
                log.warning(f"[exec] sell_to_close failed: {last_err}")
                if not is_fak:
                    self._last_fail_ts = time.time()
                return FillReport(False, 0.0, 0.0, 0.0, {}, error=last_err)

            ok = bool(resp.get("success", True)) and not resp.get("errorMsg")
            if ok:
                break

            err_msg = str(resp.get("errorMsg") or resp.get("error") or "")
            last_err = err_msg
            is_fak = self._is_fak_kill(err_msg)
            if is_fak and attempt < _SELL_MAX_ATTEMPTS:
                log.info(
                    f"[exec] sell attempt {attempt}/{_SELL_MAX_ATTEMPTS} "
                    f"rejected (no match) — retrying"
                )
                resp = None
                continue
            # Handle "not enough balance" appearing in response body (same logic as above)
            if "not enough balance" in err_msg.lower() and "sum of matched orders" in err_msg:
                log.warning("[exec] sell response: existing order consuming all tokens — checking...")
                await asyncio.sleep(3.0)
                cur_bal = await self.clob.get_token_balance(token_id)
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
            log.warning(f"[exec] SELL rejected (non-retryable): {err_msg or resp}")
            if not is_fak:
                self._last_fail_ts = time.time()
            return FillReport(False, 0.0, 0.0, 0.0, resp, error="rejected")

        elapsed_ms = (time.monotonic() - t0) * 1000
        if resp is None:
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
        if parsed is not None:
            filled_size, proceeds_usd, fill_avg = parsed
            reconciled_flag = True
            log.info(
                f"[exec] SELL SENT {pos.side} {filled_size:.4f}sh @≈{fill_avg:.4f} "
                f"proceeds=${proceeds_usd:.4f} in {elapsed_ms:.0f}ms (from response)"
            )
            self.positions.close(token_id, fill_avg)
            self._last_sell_ts[token_id] = time.time()
            self._slow_settlement_tokens.discard(token_id)
            # Background verify: warn if on-chain data contradicts response.
            # Passes pos_side/pos_window_ts so the task can re-open the position
            # if on-chain confirmation times out (Gnosis Safe relay dropped the tx).
            asyncio.ensure_future(
                self._bg_reconcile_sell(
                    token_id, actual_bal, pre_pusd, sold_shares, fill_avg,
                    pos_side, pos_window_ts,
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
