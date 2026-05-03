from __future__ import annotations

import asyncio
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
# Retry config for FAK kills — Polymarket sometimes briefly clears the book
# at our implied price. A short pause + re-attempt usually fills.
_BUY_MAX_ATTEMPTS = 3
_BUY_RETRY_DELAY_SEC = 0.06  # 60 ms — long enough for book refill, short enough to stay in edge


@dataclass
class FillReport:
    success: bool
    avg_price: float
    size_shares: float
    cost_usd: float
    raw: dict
    error: str | None = None
    reconciled: bool = True  # False = avg_price/size are estimates not on-chain truth


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

    def in_cooldown(self) -> bool:
        return (time.time() - self._last_fail_ts) < _ORDER_FAIL_COOLDOWN_SEC

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
        """Wait for token balance to decrease, compute realized avg from pUSD delta.

        Returns (avg_price_received_per_share, proceeds_usd, reconciled).
        `reconciled=False` means values are estimates and may not reflect the
        actual fill price — log accordingly.
        """
        deadline = time.monotonic() + _FILL_POLL_DEADLINE_SEC
        fast_until = time.monotonic() + _FILL_POLL_FAST_WINDOW_SEC
        while time.monotonic() < deadline:
            cur_token = await self.clob.get_token_balance(token_id)
            if (
                cur_token is not None
                and cur_token < pre_token - _BALANCE_DELTA_EPSILON
            ):
                cur_pusd = await self.clob.get_usdc_balance()
                if cur_pusd is not None:
                    proceeds = max(cur_pusd - pre_pusd, 0.0)
                    actual_sold = pre_token - cur_token
                    if actual_sold > 0:
                        return proceeds / actual_sold, proceeds, True
            interval = _FILL_POLL_FAST_SEC if time.monotonic() < fast_until else _FILL_POLL_INTERVAL_SEC
            await asyncio.sleep(interval)
        # Final read — sometimes the delta lands right after the deadline
        cur_token = await self.clob.get_token_balance(token_id)
        cur_pusd = await self.clob.get_usdc_balance()
        if (
            cur_token is not None
            and cur_token < pre_token - _BALANCE_DELTA_EPSILON
            and cur_pusd is not None
        ):
            proceeds = max(cur_pusd - pre_pusd, 0.0)
            actual_sold = pre_token - cur_token
            if actual_sold > 0:
                log.info(f"[exec] sell reconciled on final read after {_FILL_POLL_DEADLINE_SEC}s")
                return proceeds / actual_sold, proceeds, True
        log.warning(
            f"[exec] SELL NOT RECONCILED within {_FILL_POLL_DEADLINE_SEC}s — "
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
    ) -> None:
        """Background task: wait for fill settlement, update position with actual values."""
        try:
            size, cost, avg, reconciled = await self._reconcile_buy(
                token_id, pre_token, pre_pusd, fallback_size, fallback_cost
            )
            pos = self.positions.open.get(token_id)
            if pos is not None and reconciled:
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
        fallback_avg: float,
    ) -> None:
        """Background task: confirm sell fill price for logging."""
        try:
            avg, proceeds, reconciled = await self._reconcile_sell(
                token_id, pre_token, pre_pusd, sold_shares, fallback_avg
            )
            marker = "CLOSE" if reconciled else "CLOSE?"
            log.info(
                f"[exec] {marker} SELL reconciled: {sold_shares:.4f}sh @{avg:.4f} "
                f"proceeds=${proceeds:.4f}"
                f"{'' if reconciled else ' (ESTIMATE — settlement timed out)'}"
            )
        except Exception as e:
            log.error(f"[exec] _bg_reconcile_sell error for {token_id[:8]}: {e}")

    # ------------------------------------------------------------ buy
    @staticmethod
    def _is_fak_kill(err: str) -> bool:
        s = err.lower()
        return "no orders found" in s or "fak order" in s

    async def buy_to_open(
        self,
        token_id: str,
        side_label: str,
        usd_amount: float,
        expected_ask: float,
        market_window_ts: int,
        get_fresh_ask: Callable[[], float | None] | None = None,
    ) -> FillReport:
        """Place a market buy. Auto-retries on FAK kill — book sometimes briefly
        clears at the implied price; a short pause + retry usually fills.
        `get_fresh_ask` is an optional callback (returns WebSocket-cached ask)
        used to re-validate drift on retries; abort early if drift blows out.
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
            return FillReport(True, expected_ask, shares, usd_amount, {"dry": True})

        # Skip the pre-trade get_token_balance HTTP call: each market window
        # has unique CTF token_ids and we never re-enter within a window, so
        # pre_token is always 0. Reconciliation uses the absolute post-order
        # balance directly. pUSD baseline comes from the in-memory cache.
        pre_token = 0.0
        pre_pusd = self.clob._bankroll_cache

        log.info(
            f"[exec] placing market buy {side_label} ${usd_amount:.2f} "
            f"@≈{expected_ask:.3f} token={token_id[:8]}…"
        )

        resp: dict | None = None
        last_err: str = ""
        t0 = time.monotonic()
        for attempt in range(1, _BUY_MAX_ATTEMPTS + 1):
            # On retry: brief pause + drift re-check (instant via WebSocket cache).
            if attempt > 1:
                await asyncio.sleep(_BUY_RETRY_DELAY_SEC)
                if get_fresh_ask is not None:
                    fresh = get_fresh_ask()
                    if fresh is None or fresh <= 0:
                        log.info("[exec] retry abort — fresh ask unavailable")
                        last_err = "ask-gone"
                        break
                    drift = fresh - expected_ask
                    if drift > ENTRY_MAX_DRIFT:
                        log.info(
                            f"[exec] retry abort — drift ${drift:+.4f} exceeds "
                            f"max ${ENTRY_MAX_DRIFT:.4f} (now {fresh:.3f})"
                        )
                        last_err = f"drift-on-retry({drift:+.4f})"
                        break

            try:
                resp = await self.clob.place_market_buy(token_id, usd_amount)
            except Exception as e:
                last_err = str(e)
                if self._is_fak_kill(last_err):
                    log.info(
                        f"[exec] buy attempt {attempt}/{_BUY_MAX_ATTEMPTS} "
                        f"killed (FAK no match) — book may have moved"
                    )
                    resp = None
                    continue
                log.error(f"[exec] buy_to_open exception (non-retryable): {last_err}")
                self._last_fail_ts = time.time()
                return FillReport(False, 0.0, 0.0, 0.0, {}, error=last_err)

            ok = bool(resp.get("success", True)) and not resp.get("errorMsg")
            if ok:
                break  # filled

            err_msg = str(resp.get("errorMsg") or resp.get("error") or "")
            last_err = err_msg
            if self._is_fak_kill(err_msg):
                log.info(
                    f"[exec] buy attempt {attempt}/{_BUY_MAX_ATTEMPTS} "
                    f"rejected (no match) — retrying"
                )
                resp = None
                continue
            log.warning(f"[exec] BUY rejected (non-retryable): {err_msg or resp}")
            self._last_fail_ts = time.time()
            return FillReport(False, 0.0, 0.0, 0.0, {}, error="rejected")

        elapsed_ms = (time.monotonic() - t0) * 1000
        if resp is None:
            log.warning(
                f"[exec] BUY UNFILLED after {_BUY_MAX_ATTEMPTS} attempts "
                f"in {elapsed_ms:.0f}ms — last={last_err[:120]}"
            )
            return FillReport(False, 0.0, 0.0, 0.0, {}, error=f"fak-killed:{last_err}")

        # Best-effort size estimate — updated with exact value by background reconciler.
        raw_taking = resp.get("takingAmount") or resp.get("size")
        expected_size = usd_amount / max(expected_ask, 1e-3)
        if raw_taking:
            est_size = float(raw_taking)
            if est_size > expected_size * 100:
                est_size /= 1e6
        else:
            est_size = expected_size

        # Register position immediately so the strategy loop can manage it on the
        # next tick. Background task updates entry_price/size with reconciled values.
        self.positions.add(
            Position(
                token_id=token_id,
                side=side_label,
                entry_price=expected_ask,
                size_shares=est_size,
                cost_usd=usd_amount,
                market_window_ts=market_window_ts,
            )
        )
        log.info(
            f"[exec] BUY SENT {side_label} ${usd_amount:.2f} @≈{expected_ask:.3f} "
            f"~{est_size:.4f}sh in {elapsed_ms:.0f}ms — reconciling"
        )
        asyncio.ensure_future(
            self._bg_reconcile_buy(token_id, pre_token, pre_pusd, est_size, usd_amount)
        )
        return FillReport(True, expected_ask, est_size, usd_amount, resp, reconciled=False)

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

        # Sell immediately with local position size — no pre-sell validation.
        # If reconciled: size is on-chain verified. If not: use order-response
        # estimate. Don't block on settlement confirmation — momentum matters.
        # If size is wrong, Polymarket rejects with "insufficient balance" →
        # caught by exception handler, no cooldown triggered (transient error).
        actual_bal = pos.size_shares

        pre_pusd = self.clob._bankroll_cache

        try:
            resp = await self.clob.place_market_sell(token_id, pos.size_shares)
        except Exception as e:
            err_str = str(e)
            log.error(f"[exec] sell_to_close failed: {e}")
            transient = (
                "no orders found" in err_str
                or "not enough balance" in err_str
                or "invalid amounts" in err_str
            )
            if not transient:
                self._last_fail_ts = time.time()
            return FillReport(False, 0.0, 0.0, 0.0, {}, error=err_str)

        ok = bool(resp.get("success", True)) and not resp.get("errorMsg")
        if not ok:
            log.warning(f"[exec] SELL rejected: {resp}")
            self._last_fail_ts = time.time()
            return FillReport(False, 0.0, 0.0, 0.0, resp, error="rejected")

        # Close position immediately with mark_price so the strategy loop is
        # unblocked for the next tick. Background task logs the reconciled fill price.
        sold_shares = pos.size_shares
        self.positions.close(token_id, mark_price)
        log.info(
            f"[exec] SELL SENT {pos.side} {sold_shares:.4f}sh @≈{mark_price:.3f} "
            f"token={token_id[:8]}… — reconciling"
        )
        asyncio.ensure_future(
            self._bg_reconcile_sell(token_id, actual_bal, pre_pusd, sold_shares, mark_price)
        )
        return FillReport(True, mark_price, sold_shares, mark_price * sold_shares, resp, reconciled=False)
