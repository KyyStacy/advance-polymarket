from __future__ import annotations

import asyncio
import time

from .config.settings import Settings, load_settings
from .execution.order_executor import ENTRY_MAX_DRIFT, OrderExecutor
from .execution.position_tracker import Position, PositionTracker
from .feeds.binance_ws import BinanceFeed
from .feeds.coinbase_ws import CoinbaseFeed
from .feeds.polymarket_rtds import PolymarketRtdsFeed
from .feeds.polymarket_ws import PolymarketBookFeed
from .feeds.price_aggregator import PriceAggregator
from .market.clob_client import PolymarketClobClient
from .market.discovery import MarketDiscovery, MarketInfo
from .market.redeemer import PositionRedeemer
from .risk.manager import RiskManager
from .signals.lead_lag import LeadLagDetector
from .signals.mispricing import MispricingEvaluator
from .signals.momentum import MomentumAnalyzer
from .strategy.entry_engine import EntryEngine
from .strategy.exit_engine import ExitEngine
from .utils.logger import get_logger, setup_logger
from .utils.time_utils import (
    current_window_start,
    seconds_into_window,
    seconds_left_in_window,
)

# Refresh bankroll every N ticks (400ms/tick → ~40s refresh)
_BANKROLL_REFRESH_TICKS = 100


class TradingBot:
    def __init__(self, settings: Settings | None = None):
        self.s = settings or load_settings()
        setup_logger(self.s.log_level, self.s.log_file, console=False)
        self.log = get_logger()

        self._dry_run_initial_bankroll = 100.0  # initial capital for dry run simulation

        self.agg = PriceAggregator()

        self.binance = BinanceFeed(self.agg.on_cex)
        self.coinbase = CoinbaseFeed(self.agg.on_cex)
        self.oracle = PolymarketRtdsFeed(
            on_update=self.agg.on_oracle,
            symbol="btc/usd",
        )

        self.poly_book = PolymarketBookFeed(
            self.s.polymarket_ws, token_ids=[], on_book=self._on_poly_book
        )
        self._poly_books: dict[str, dict] = {}

        self.discovery = MarketDiscovery(self.s.gamma_host)
        self.clob = PolymarketClobClient(self.s)
        self.redeemer = PositionRedeemer(self.s.polygon_rpc, self.s.private_key)

        self.momentum = MomentumAnalyzer(self.agg)
        self.leadlag = LeadLagDetector(self.agg)
        self.mispricing = MispricingEvaluator()

        self.entry_engine = EntryEngine(self.s)
        self.exit_engine = ExitEngine(self.s)

        self.positions = PositionTracker()
        self.executor = OrderExecutor(self.s, self.clob, self.positions)
        self.risk = RiskManager(self.s)

        self._stop = asyncio.Event()
        self._current_market: MarketInfo | None = None
        # Anchor price is now derived exclusively from Chainlink RTDS
        # candle open — no fallback to CEX.  Updated each time a new bucket
        # boundary is detected by oracle.candle_ready.
        self._anchor_price: float | None = None
        self._anchor_bucket_ms: int = -1  # bucket_start_ms when anchor was locked
        self._tick_count: int = 0
        # Polymarket taker fee — fixed at 2% (matches their published rate for prediction markets)
        self._taker_fee_rate: float = 0.02

        # Track positions held through window end (pending redemption)
        self._pending_redeem: list[tuple[str, str, str]] = []  # (condition_id, side, token_id)
        # Dedup key for signal logging — only log when signal state changes
        self._last_signal_key: str = ""
        self._start_time: float = time.time()

    # ------------------------------------------------------------------ feeds
    def _on_poly_book(
        self, token_id: str, ts: float, bid: float | None, ask: float | None
    ) -> None:
        self._poly_books[token_id] = {"bid": bid, "ask": ask, "ts": ts}

    # ------------------------------------------------------------------ anchor / window
    def _try_lock_anchor(self) -> bool:
        """Lock anchor price from Chainlink RTDS candle open.

        Called each tick.  Returns True once the oracle has reported a valid
        candle open for the current wall-clock bucket (oracle.candle_ready).
        No CEX fallback — Chainlink is the single source of truth for
        Polymarket resolution pricing.

        Mirrors the MonitorState pattern from rtds_example.py: the strategy
        stays in "WAITING" state until the first Chainlink tick of the new
        bucket sets the candle open.
        """
        if not self.oracle.candle_ready:
            return False

        bucket_ms = self.oracle.bucket_start_ms
        # Already locked for this bucket — no update needed.
        if bucket_ms == self._anchor_bucket_ms:
            return True

        anchor = self.oracle.current_candle_open
        if anchor <= 0:
            return False

        self._anchor_price = anchor
        self._anchor_bucket_ms = bucket_ms
        self.log.info(
            f"[bot] anchor LOCKED (chainlink candle-open) "
            f"bucket={bucket_ms} price={anchor:.2f} "
            f"ticks={self.oracle.tick_count} status={self.oracle.status}"
        )
        return True

    async def _switch_to_window(self, window_ts: int) -> None:
        market = await self.discovery.get_current_market(window_ts + 1)
        if market is None:
            self.log.warning(f"[bot] no market for window {window_ts} — slug not found in Gamma")
            self._current_market = None
            return
        self._current_market = market
        await self.poly_book.update_tokens(market.tokens)
        self.log.info(
            f"[bot] tracking market {market.slug} cond={market.condition_id[:10]}…"
        )
        # Anchor will be locked by _try_lock_anchor() on the first
        # _tick() call once oracle.candle_ready becomes True.
        # No blocking wait, no CEX fallback.

    # ------------------------------------------------------------------ data helpers
    def _ask_for(self, token_id: str) -> float | None:
        b = self._poly_books.get(token_id)
        return b["ask"] if b else None

    def _bid_for(self, token_id: str) -> float | None:
        b = self._poly_books.get(token_id)
        return b["bid"] if b else None

    # ------------------------------------------------------------------ bankroll
    async def _refresh_bankroll(self) -> None:
        if self.s.dry_run:
            # Dry run: simulate realistic bankroll by tracking P&L growth/drawdown.
            # Starting capital + cumulative realized P&L = current simulated balance.
            # This makes bet sizing scale realistically as portfolio grows or shrinks.
            simulated_bal = max(1.0, self._dry_run_initial_bankroll + self.risk.total_pnl)
            self.risk.update_bankroll(simulated_bal)
            return
        bal = await self.clob.get_usdc_balance()
        if bal is not None and bal > 0:
            old = self.risk.bankroll_usd
            self.risk.update_bankroll(bal)
            if abs(bal - old) > 0.01:
                self.log.info(f"[bot] bankroll updated: ${bal:.2f}")
        elif self.risk.bankroll_usd == 0.0:
            # Balance unavailable — fall back to MAX_BET_USD so sizing still works
            if self.s.max_bet_mode == "pct":
                self.log.warning(
                    "[bot] USDC balance unavailable and MAX_BET_MODE=pct — "
                    f"falling back to MAX_BET_USD=${self.s.max_bet_usd:.2f} as bankroll estimate"
                )
                self.risk.update_bankroll(self.s.max_bet_usd * 20)  # assume 5% of bankroll
            else:
                self.risk.update_bankroll(self.s.max_bet_usd * 4)  # minimal sentinel

    # ------------------------------------------------------------------ auto-claim
    async def _process_pending_redeems(self) -> None:
        if not self.s.auto_claim or self.s.dry_run or not self._pending_redeem:
            return
        still_pending: list[tuple[str, str, str]] = []
        for cond_id, side, token_id in self._pending_redeem:
            ok = await self.redeemer.redeem(cond_id, side)
            if not ok:
                still_pending.append((cond_id, side, token_id))
        self._pending_redeem = still_pending

    # ------------------------------------------------------------------ window close
    async def _resolve_window(self, market: MarketInfo) -> None:
        """Handle end-of-window for positions that survived through resolution.

        Signal-based exits should have already closed the position. If one remains
        it means the exit signal never fired — the position will be settled on-chain
        by Polymarket. If AUTO_CLAIM=true, queue it for redemption.
        """
        leftover = self.positions.get_for_window(market.window_ts)
        if leftover is None:
            return

        mark = self._bid_for(leftover.token_id)
        # Only try to sell if we're still inside an active window and there's a live bid
        if mark and mark > 0.01:
            self.log.info(
                f"[bot] PRE-RESOLUTION SELL {leftover.side} {leftover.size_shares:.2f}sh "
                f"@bid={mark:.3f} window={market.window_ts}"
            )
            await self.executor.sell_to_close(leftover.token_id, mark)
            if self.positions.closed:
                self.risk.record_trade_pnl(self.positions.closed[-1].realized_pnl)
        else:
            # No liquid exit — position goes to on-chain resolution
            # Mark as pending redemption; update PnL after claim
            self.log.info(
                f"[bot] HOLDING TO RESOLUTION {leftover.side} "
                f"cond={market.condition_id[:10]}… — queued for auto-claim"
            )
            if self.s.auto_claim and market.condition_id:
                self._pending_redeem.append(
                    (market.condition_id, leftover.side, leftover.token_id)
                )
            self.positions.open.pop(leftover.token_id, None)

    # ------------------------------------------------------------------ main tick
    async def _tick(self) -> None:
        m = self._current_market
        if m is None:
            return

        # ---- Bucket-aware anchor management (MonitorState pattern) ----
        # Try to lock anchor on every tick.  If the oracle hasn't delivered
        # the first Chainlink tick of this bucket yet, skip strategy work.
        if not self._try_lock_anchor():
            return
        if self._anchor_price is None:
            return

        now = time.time()
        sec_in = seconds_into_window(now)
        sec_left = seconds_left_in_window(now)
        cex = self.agg.latest_cex()
        if cex is None:
            return

        # Periodically refresh bankroll
        self._tick_count += 1
        if self._tick_count % _BANKROLL_REFRESH_TICKS == 1:
            await self._refresh_bankroll()
            await self._process_pending_redeems()

        mom = self.momentum.read(now)
        ll = self.leadlag.read(now)
        up_ask = self._ask_for(m.up_token_id)
        down_ask = self._ask_for(m.down_token_id)
        up_bid = self._bid_for(m.up_token_id)
        down_bid = self._bid_for(m.down_token_id)

        misp = self.mispricing.evaluate(
            anchor_price=self._anchor_price,
            current_cex=cex[1],
            seconds_left=sec_left,
            momentum=mom,
            leadlag=ll,
            implied_p_up_ask=up_ask,
            implied_p_down_ask=down_ask,
            fee_rate=self._taker_fee_rate,
        )

        # ---- manage existing position ----
        existing = self.positions.get_for_window(m.window_ts)
        if existing is not None:
            mark = (up_bid if existing.side == "UP" else down_bid) or existing.entry_price
            unreal_pct = (mark - existing.entry_price) / max(existing.entry_price, 1e-3)
            seconds_held = now - existing.opened_at
            ed = self.exit_engine.decide(
                existing.side, sec_left, mom, ll, misp, unreal_pct, seconds_held
            )
            if ed.exit:
                self.log.info(
                    f"[bot] EXIT {existing.side} mark={mark:.3f} "
                    f"unreal={unreal_pct:+.1%} held={seconds_held:.0f}s reason={ed.reason}"
                )
                sell = await self.executor.sell_to_close(existing.token_id, mark)
                if sell.success:
                    if self.positions.closed:
                        self.risk.record_trade_pnl(self.positions.closed[-1].realized_pnl)
                else:
                    self.log.warning(f"[bot] SELL FAILED {existing.side} — {sell.error}")
            return

        # ---- no position: check entry ----
        ok, why = self.risk.can_trade()
        if not ok:
            self.log.debug(f"[bot] skip entry: {why}")
            return

        # Skip entry attempt entirely if executor is still cooling down after a failure.
        # The next tick will re-evaluate signals fresh — no log spam, no stale prices.
        if self.executor.in_cooldown():
            return

        decision = self.entry_engine.decide(
            seconds_into_window=sec_in,
            seconds_left=sec_left,
            momentum=mom,
            leadlag=ll,
            mispricing=misp,
            up_ask=up_ask,
            down_ask=down_ask,
            up_bid=up_bid,
            down_bid=down_bid,
            up_token_id=m.up_token_id,
            down_token_id=m.down_token_id,
        )

        # Log signal state — only when it changes to avoid spam.
        # Fires whenever mispricing finds a non-zero edge on either side.
        if misp is not None and misp.side is not None:
            sig_key = f"{misp.side}:{decision.enter}:{decision.reason}"
            if sig_key != self._last_signal_key:
                self._last_signal_key = sig_key
                ask_val = (up_ask if misp.side == "UP" else down_ask) or 0.0
                ll_part = f" ll={ll.deviation_bps:.1f}bps" if ll else ""
                result = "ACCEPTED" if decision.enter else f"REJECTED ({decision.reason})"
                self.log.info(
                    f"[signal] {misp.side:4s} ask={ask_val:.3f} edge={misp.edge:.3f}"
                    f" z={mom.z_score:.2f}{ll_part} → {result}"
                )
        else:
            self._last_signal_key = ""

        if not decision.enter:
            return

        win_prob = misp.fair_p_up if decision.side == "UP" else (1.0 - misp.fair_p_up)
        size_usd = self.risk.position_size_usd(decision.edge, decision.implied_ask or 0.5, win_prob)
        if size_usd < self.s.min_bet_usd:
            self.log.debug(
                f"[bot] size_usd={size_usd:.2f} < min_bet={self.s.min_bet_usd} — skip"
            )
            return

        # Pre-trade re-validation — re-fetch the ask just before sending the
        # order. The book may have moved since the entry decision was made
        # (especially when momentum is firing). If the ask has drifted beyond
        # tolerance, abort and let the next tick re-evaluate from fresh data.
        decision_ask = decision.implied_ask or 0.5
        fresh_ask = self._ask_for(decision.token_id or "")
        if fresh_ask is None or fresh_ask <= 0:
            self.log.info(f"[bot] entry aborted — fresh ask gone for {decision.side}")
            return
        ask_drift = fresh_ask - decision_ask
        if ask_drift > ENTRY_MAX_DRIFT:
            self.log.info(
                f"[bot] entry aborted — ask drifted ${ask_drift:+.4f} "
                f"({decision_ask:.3f} → {fresh_ask:.3f})"
            )
            return
        # Use the fresher ask for execution so the slippage cap reflects reality.
        execution_ask = fresh_ask

        ll_str = f" ll={ll.deviation_bps:.1f}bps" if ll else ""
        fee_str = f" fee={self._taker_fee_rate:.2%}" if self._taker_fee_rate else ""
        self.log.info(
            f"[bot] ENTRY {decision.side} ${size_usd:.2f} "
            f"ask={execution_ask:.3f} edge={decision.edge:.3f} "
            f"wp={win_prob:.2f} z={mom.z_score:.2f}{ll_str}{fee_str}"
        )
        target_token = decision.token_id or ""
        fill = await self.executor.buy_to_open(
            token_id=target_token,
            side_label=decision.side or "?",
            usd_amount=size_usd,
            expected_ask=execution_ask,
            market_window_ts=m.window_ts,
            get_fresh_ask=lambda: self._ask_for(target_token),
        )
        if not fill.success:
            self.log.warning(f"[bot] BUY FAILED {decision.side} — {fill.error}")

    # ------------------------------------------------------------------ loop
    async def _strategy_loop(self) -> None:
        # Wait for CEX feeds to warm up so momentum has data to work with.
        # RTDS runs in parallel — we don't block on it here because:
        #  1. _tick() already gates on candle_ready (oracle.status == LIVE)
        #  2. rtds_example.py doesn't wait either — it streams and reacts
        for i in range(40):
            if self.agg.latest_cex() is not None:
                break
            if i == 20:
                self.log.warning("[bot] warm-up: CEX feeds slow to connect")
            await asyncio.sleep(0.5)
        self.log.info(
            "[bot] feeds ready — RTDS status=%s candle_ready=%s — entering strategy loop",
            self.oracle.status, self.oracle.candle_ready,
        )

        await self._refresh_bankroll()

        last_window = -1
        while not self._stop.is_set():
            now = time.time()
            win = current_window_start(now)
            if win != last_window:
                if last_window > 0 and self._current_market is not None:
                    await self._resolve_window(self._current_market)
                # Reset anchor bucket so _try_lock_anchor() will re-lock
                # the candle open on the first Chainlink tick of the new window.
                self._anchor_bucket_ms = -1
                self._anchor_price = None
                await self._switch_to_window(win)
                last_window = win
            try:
                await self._tick()
            except Exception as e:
                self.log.exception(f"[bot] tick error: {e}")
            # Evaluasi strategi dijalankan dengan interval sangat rapat (50ms)
            # agar reaksi terhadap perubahan harga (mispricing/lead-lag) nyaris instan.
            await asyncio.sleep(0.05)

    async def _display_loop(self) -> None:
        """
        Loop asinkron untuk memperbarui tampilan terminal / antarmuka secara terus menerus.
        Berjalan independen tanpa memblokir WebSocket atau Strategy.
        """
        import re as _re
        import sys
        from datetime import datetime, timedelta

        def get_et_offset() -> int:
            """Auto-detect EDT (UTC-4) or EST (UTC-5) based on current date."""
            now = datetime.now()
            month, day = now.month, now.day
            weekday = now.weekday()  # 0=Mon, 6=Sun

            is_edt = False
            if month > 3 and month < 11:  # Apr-Oct: always EDT
                is_edt = True
            elif month == 3:  # March: EDT after 2nd Sunday (~8-14)
                is_edt = day > 14 or (day > 7 and weekday == 6)
            elif month == 11:  # Nov: EST after 1st Sunday (~1-7)
                is_edt = day < 7 and weekday != 6

            return -4 if is_edt else -5

        G, R, Y, B, C, X = "\033[92m", "\033[91m", "\033[93m", "\033[1m", "\033[96m", "\033[0m"

        sys.stdout.write("\033[?25l\033[2J\033[H")

        try:
            while not self._stop.is_set():
                sys.stdout.write("\033[H")

                now_ms = int(time.time() * 1000)
                remain = max(0, (self.oracle.bucket_start_ms + self.oracle.interval_ms - now_ms) // 1000)

                if self.oracle.bucket_start_ms > 0:
                    from datetime import timezone
                    _utc = timezone.utc
                    _wib = timezone(timedelta(hours=7))
                    start_utc = datetime.fromtimestamp(self.oracle.bucket_start_ms / 1000, tz=_utc)
                    end_utc = datetime.fromtimestamp((self.oracle.bucket_start_ms + self.oracle.interval_ms) / 1000, tz=_utc)
                    wib_range = (
                        f"{start_utc.astimezone(_wib).strftime('%H:%M')} - "
                        f"{end_utc.astimezone(_wib).strftime('%H:%M')} WIB"
                    )
                    try:
                        from zoneinfo import ZoneInfo
                        et_tz = ZoneInfo("America/New_York")
                        et_range = (
                            f"{start_utc.astimezone(et_tz).strftime('%H:%M')} - "
                            f"{end_utc.astimezone(et_tz).strftime('%H:%M')} ET"
                        )
                    except Exception:
                        et_offset = get_et_offset()
                        et_range = (
                            f"{(start_utc + timedelta(hours=et_offset)).strftime('%H:%M')} - "
                            f"{(end_utc + timedelta(hours=et_offset)).strftime('%H:%M')} ET"
                        )
                    time_range = f"{wib_range} | {et_range}"
                else:
                    time_range = "WAITING..."
                
                # Chainlink price
                if self.oracle.latest_price > 0:
                    chainlink_str = f"${self.oracle.latest_price:,.2f}"
                else:
                    chainlink_str = f"{Y}WAITING...{X}"

                # Binance & Coinbase — read directly from feed's last_mid (updated every WS event)
                binance_str = (
                    f"${self.binance.last_mid:,.2f}" if self.binance.last_mid
                    else f"{Y}WAITING...{X}"
                )
                coinbase_str = (
                    f"${self.coinbase.last_mid:,.2f}" if self.coinbase.last_mid
                    else f"{Y}WAITING...{X}"
                )

                diff = 0.0  # initialize for display section
                if self.oracle.current_candle_open > 0:
                    price_to_beat_str = f"${self.oracle.current_candle_open:,.2f}"
                    diff = self.oracle.latest_price - self.oracle.current_candle_open
                    color = G if diff > 0 else (R if diff < 0 else X)
                    dir_icon = "ABOVE ▲" if diff > 0 else ("BELOW ▼" if diff < 0 else "FLAT ■")
                    diff_str = f"{color}{diff:+.2f}{X}"
                    status_str = f"{color}{B}{dir_icon}{X}"
                else:
                    price_to_beat_str = f"{Y}WAITING...{X}"
                    diff_str = f"{Y}WAITING...{X}"
                    status_str = f"{Y}{B}WAITING NEXT CANDLE{X}"

                daily_pnl = self.risk.pnl.realized
                total_pnl = self.risk.total_pnl
                daily_str = f"{G if daily_pnl >= 0 else R}{daily_pnl:+.2f}{X}"
                total_str = f"{G if total_pnl >= 0 else R}{total_pnl:+.2f}{X}"
                interval_min = int(self.oracle.interval_ms / 60000)
                next_candle_val = f"{int(remain//60):02d}:{int(remain%60):02d} | {self.oracle.status}"

                bal = self.risk.bankroll_usd
                bal_str = f"${bal:.2f}" if bal > 0 else f"{Y}--{X}"
                elapsed = int(time.time() - self._start_time)
                run_str = f"{elapsed//3600:02d}:{(elapsed%3600)//60:02d}:{elapsed%60:02d}"

                total_trades = len(self.positions.closed)
                wins = sum(1 for p in self.positions.closed if p.realized_pnl > 0) if total_trades > 0 else 0
                win_rate_str = f"{wins/total_trades*100:.1f}%" if total_trades > 0 else "0.0%"

                from datetime import timezone
                wib_now = datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=7)))
                wib_time = wib_now.strftime('%H:%M:%S')
                try:
                    from zoneinfo import ZoneInfo
                    et_time = datetime.now(ZoneInfo("America/New_York")).strftime('%H:%M:%S')
                except Exception:
                    et_offset = get_et_offset()
                    et_time = (datetime.now(timezone.utc) + timedelta(hours=et_offset)).strftime('%H:%M:%S')

                _ansi_strip = _re.compile(r'\033\[[0-9;]*m')

                def vlen(s: str) -> int:
                    return len(_ansi_strip.sub('', s))

                def pad_center(s: str, width: int) -> str:
                    stripped = _ansi_strip.sub('', s)
                    if len(stripped) >= width:
                        return s
                    left_pad = (width - len(stripped)) // 2
                    right_pad = width - len(stripped) - left_pad
                    return ' ' * left_pad + s + ' ' * right_pad

                def bar(fill: float, width: int = 20, char: str = '█') -> str:
                    """Progress bar: fill is 0..1"""
                    f = max(0, min(1, fill))
                    filled = int(f * width)
                    empty = width - filled
                    col = G if f >= 0.5 else (Y if f >= 0.25 else R)
                    return f"{col}{char * filled}{X}{char * empty}"

                W = 68
                CLR = "\033[K"
                H = f"{C}{'═' * W}{X}"
                T = f"{C}{'─' * W}{X}"

                countdown = f"{int(remain//60):02d}:{int(remain%60):02d}"

                # Win rate visual (before market direction)
                wr_val = (wins / total_trades) if total_trades > 0 else 0
                wr_bar = bar(wr_val, 15)

                # Market direction indicator
                if diff != 0:
                    momentum_icon = "📈" if diff > 0 else "📉"
                else:
                    momentum_icon = "◼"

                table = [
                    H + CLR,
                    f" {B}⚡ POLYMARKET BTC-{interval_min}M TRADING BOT{X}" + CLR,
                    f" {Y}┃{X} {time_range}" + CLR,
                    T + CLR,
                ]

                # Price section with grid layout
                table += [
                    f" {B}📊 PRICE FEEDS{X}" + CLR,
                    f" ┃ CEX Mid    {C}Binance:{X} {binance_str:<20}  {C}Coinbase:{X} {coinbase_str}" + CLR,
                    f" ┃ Chainlink  {C}Price:{X}   {B}{chainlink_str:<20}{X}  {C}Open:{X}  {B}{price_to_beat_str}{X}" + CLR,
                    f" ┃ Diff       {diff_str:<20}  {status_str:<15} {momentum_icon}" + CLR,
                    T + CLR,
                ]

                # Market countdown
                candle_color = Y if remain < 60 else C
                table += [
                    f" {B}⏱ NEXT CANDLE{X}  {candle_color}{countdown}{X}  ({self.oracle.status})" + CLR,
                    T + CLR,
                ]

                # Open position section
                if self.positions.open:
                    table.append(f" {B}📍 ACTIVE POSITION{X}" + CLR)
                    for pos in self.positions.open.values():
                        mark = self._bid_for(pos.token_id) or pos.entry_price
                        unreal = (mark - pos.entry_price) * pos.size_shares
                        unreal_pct = (unreal / (pos.cost_usd or 0.01)) * 100 if pos.cost_usd else 0
                        ucol = G if unreal >= 0 else R

                        side_icon = "▲" if pos.side == "UP" else "▼"
                        side_col = G if pos.side == "UP" else R

                        table.append(
                            f" ┃ {side_col}{side_icon} {pos.side}{X}  "
                            f"Size: {pos.size_shares:.3f}sh  "
                            f"Entry: ${pos.entry_price:.3f}  "
                            f"Mark: ${mark:.3f}" + CLR
                        )
                        table.append(
                            f" ┃ Unrealized: {ucol}{unreal:+.4f} ({unreal_pct:+.1f}%){X}  "
                            f"Held: {int((time.time() - pos.opened_at))}s" + CLR
                        )
                    table.append(T + CLR)
                else:
                    # Show ready-to-trade status
                    table.append(f" {Y}⏸ WAITING FOR SIGNAL{X}" + CLR)
                    table.append(T + CLR)

                # Performance section
                total_profit_color = G if total_pnl >= 0 else R
                daily_profit_color = G if daily_pnl >= 0 else R

                table += [
                    f" {B}📈 PERFORMANCE{X}" + CLR,
                    f" ┃ Bankroll    ${bal:.2f}" + CLR,
                    f" ┃ Daily P&L   {daily_profit_color}{daily_pnl:+.2f}{X}  ┃  Total P&L  {total_profit_color}{total_pnl:+.2f}{X}" + CLR,
                    f" ┃ Trades      {total_trades:<6}  ┃  Win Rate   {wr_bar}  {wr_val*100:.1f}%" + CLR,
                    f" ┃ Uptime      {run_str}" + CLR,
                    H + CLR,
                    f" {Y}{wib_time} WIB{X}  ┃  {Y}{et_time} ET{X}" + CLR,
                    H + CLR,
                    CLR,
                ]
                
                sys.stdout.write("\n".join(table) + "\n")
                sys.stdout.flush()
                
                await asyncio.sleep(0.2)
        finally:
            import sys
            sys.stdout.write("\033[?25h\n")

    # ------------------------------------------------------------------ run
    async def run(self) -> None:
        self.log.info(
            f"[bot] starting | DRY_RUN={self.s.dry_run} "
            f"odds=[{self.s.min_odds},{self.s.max_odds}] "
            f"max_daily_loss=${self.s.max_daily_loss_usd}"
        )
        await asyncio.gather(
            self.binance.start(),
            self.coinbase.start(),
            self.oracle.start(),
            self.poly_book.start(),
        )
        if not self.s.dry_run:
            try:
                await self.clob.ensure_ready()
            except Exception as e:
                self.log.error(f"[bot] CLOB init failed: {e} — forcing dry-run")
                self.s.dry_run = True

        try:
            display_task = asyncio.create_task(self._display_loop())
            await self._strategy_loop()
        finally:
            self._stop.set()
            await asyncio.gather(
                self.binance.stop(),
                self.coinbase.stop(),
                self.oracle.stop(),
                self.poly_book.stop(),
                return_exceptions=True,
            )
            await self.discovery.close()
            self.log.info(
                f"[bot] stopped | trades={self.risk.pnl.n_trades} "
                f"realized_pnl={self.risk.pnl.realized:+.2f}"
            )
