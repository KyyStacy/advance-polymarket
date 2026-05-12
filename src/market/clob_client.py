from __future__ import annotations

import asyncio
import logging
import sys
import threading
import time
from typing import Any

# Suppress noisy per-request error output from the py_clob_client_v2 library.
# FAK "no orders found" (400) is a normal transient condition, not a real error.
logging.getLogger("py_clob_client_v2").setLevel(logging.CRITICAL)


class _LibOutputFilter:
    """Wraps stdout/stderr to drop [py_clob_client_v2] request-error lines."""

    _TAG = "[py_clob_client_v2] request error"

    def __init__(self, stream: object) -> None:
        self._stream = stream

    def write(self, text: str) -> int:
        if self._TAG in text:
            return len(text)
        return self._stream.write(text)  # type: ignore[union-attr]

    def flush(self) -> None:
        self._stream.flush()  # type: ignore[union-attr]

    def __getattr__(self, name: str) -> object:
        return getattr(self._stream, name)


if not isinstance(sys.stdout, _LibOutputFilter):
    sys.stdout = _LibOutputFilter(sys.stdout)  # type: ignore[assignment]
if not isinstance(sys.stderr, _LibOutputFilter):
    sys.stderr = _LibOutputFilter(sys.stderr)  # type: ignore[assignment]

from py_clob_client_v2.client import ClobClient, order_to_json_v2
from py_clob_client_v2.clob_types import (
    ApiCreds,
    AssetType,
    BalanceAllowanceParams,
    BuilderConfig,
    MarketOrderArgsV2,
    OpenOrderParams,
    OrderArgsV2,
    OrderType,
    PartialCreateOrderOptions,
)
from py_clob_client_v2.constants import BYTES32_ZERO
from py_clob_client_v2.order_builder.constants import BUY, SELL

from ..config.settings import Settings
from ..utils.logger import get_logger

log = get_logger()

# Optional Rust extension for HFT network path
try:
    import polymarket_hft
    _HAS_RUST_HFT = True
except ImportError:
    _HAS_RUST_HFT = False

# True when the Python logger level is DEBUG — used to gate f-string
# construction of diagnostic payloads on the hot path (saves ~0.1–0.5 ms
# per order when running at INFO level in production).
_DEBUG_ENABLED: bool = False


def _refresh_debug_flag() -> None:
    """Call once after logger is fully configured."""
    global _DEBUG_ENABLED
    try:
        import logging as _logging
        _DEBUG_ENABLED = _logging.getLogger().isEnabledFor(_logging.DEBUG)
    except Exception:
        _DEBUG_ENABLED = False


class PolymarketClobClient:
    """Async wrapper around py-clob-client-v2 (Polymarket CLOB V2, April 28 2026+).

    All blocking calls dispatched via asyncio.to_thread.

    Phase-1 optimisations applied:
      - `import time` moved to module level (eliminates repeated sys.modules
        lookup inside per-order thread functions).
      - Diagnostic log.info() in _post() replaced with log.debug() behind an
        explicit level-check guard so f-string construction is skipped entirely
        at INFO level (saves ~0.1–0.5 ms / order in production).
      - Frequently accessed Settings attributes cached as instance locals to
        avoid repeated attribute chain traversal inside thread callbacks.
    """

    def __init__(self, settings: Settings):
        self.s = settings
        self._client: ClobClient | None = None
        self._rust_pool = None
        self._rust_creds = None
        self._bankroll_cache: float = 0.0  # last known pUSD balance for market order fees
        # Cache hot-path Settings fields as plain attributes — avoids
        # `self.s.<attr>` chain lookups inside per-order thread callbacks.
        self._poly_addr: str = settings.polymarket_address
        self._clob_api_key: str = settings.clob_api_key

    def _build_client(self) -> ClobClient:
        """Initialize ClobClient for POLY_1271 (sig_type=3) — deposit wallets only.

        POLY_1271 order flow (confirmed working):
        - order.maker = order.signer = deposit wallet (set by OrderBuilder._v2_order_signer())
        - CLOB API key may be EOA-registered: server validates the full ERC-7739 chain
          (EOA signed the order → EOA controls deposit wallet → isValidSignature passes).
        - DO NOT call create_or_derive_api_key() at startup — unnecessary overhead and
          creates a new session key each run. Use the pre-configured CLOB API creds.
        """
        from eth_account import Account as _Acc

        sig_type = self.s.signature_type
        if sig_type != 3:
            raise RuntimeError(
                f"This bot is POLY_1271-only (SIGNATURE_TYPE=3). "
                f"Got SIGNATURE_TYPE={sig_type}. Update .env."
            )

        funder = self.s.polymarket_address
        if not funder:
            raise RuntimeError(
                "POLY_1271 requires POLYMARKET_ADDRESS (deposit wallet) in .env"
            )

        eoa_addr = _Acc.from_key(self.s.private_key).address
        if funder.lower() == eoa_addr.lower():
            raise RuntimeError(
                "POLY_1271: POLYMARKET_ADDRESS must be the deposit-wallet smart "
                "contract address, NOT the EOA derived from PRIVATE_KEY."
            )

        if not (self.s.clob_api_key and self.s.clob_secret and self.s.clob_passphrase):
            raise RuntimeError(
                "POLY_1271 requires CLOB_API_KEY / CLOB_SECRET / CLOB_PASSPHRASE "
                "in .env — create them via Polymarket portal while logged in as the deposit wallet."
            )

        creds = ApiCreds(
            api_key=self.s.clob_api_key,
            api_secret=self.s.clob_secret,
            api_passphrase=self.s.clob_passphrase,
        )
        # Builder code attribution: if set, library auto-attaches to every
        # OrderArgsV2/MarketOrderArgsV2 (see py_clob_client_v2/client.py:714-716,
        # 795-797). Empty → BYTES32_ZERO → no attribution.
        builder_code = self.s.builder_code or BYTES32_ZERO
        builder_config = BuilderConfig(builder_code=builder_code)
        c = ClobClient(
            host=self.s.clob_host,
            chain_id=self.s.chain_id,
            key=self.s.private_key,
            creds=creds,
            signature_type=sig_type,
            funder=funder,
            builder_config=builder_config,
        )

        # Hard assertion: verify the OrderBuilder actually produces signer=funder for POLY_1271.
        # Uses inspect to check the live bytecode, not just attribute values.
        import inspect as _inspect
        builder = c.builder
        actual_sig_type = int(builder.signature_type)
        actual_funder = builder.funder or ""
        if actual_sig_type != 3:
            raise RuntimeError(
                f"[clob] BUG: OrderBuilder.signature_type={actual_sig_type}, expected 3. "
                "Clear all __pycache__ and run: pip install --force-reinstall py_clob_client_v2==1.0.1"
            )
        if actual_funder.lower() != funder.lower():
            raise RuntimeError(
                f"[clob] BUG: OrderBuilder.funder={actual_funder!r}, expected {funder!r}. "
                "Library init bug — check ClobClient constructor."
            )
        # Verify at runtime that _v2_order_signer (or equivalent) returns funder.
        # Call the method if present; otherwise inspect build_market_order source.
        if hasattr(builder, "_v2_order_signer"):
            actual_order_signer = builder._v2_order_signer()
        else:
            src = _inspect.getsource(type(builder).build_market_order)
            if "_v2_order_signer" not in src and "self.funder" not in src:
                raise RuntimeError(
                    "[clob] BUG: build_market_order does not use funder/POLY_1271 signer path. "
                    "Stale .pyc or wrong library version. "
                    "Run: pip install --force-reinstall py_clob_client_v2==1.0.1"
                )
            raise RuntimeError(
                f"[clob] BUG: Library missing _v2_order_signer() — order.signer would be EOA not deposit_wallet. "
                f"Run: pip install --force-reinstall py_clob_client_v2==1.0.1 "
                f"Library loaded from: {_inspect.getfile(type(builder))}"
            )
        if actual_order_signer.lower() != funder.lower():
            raise RuntimeError(
                f"[clob] BUG: _v2_order_signer()={actual_order_signer!r} != funder={funder!r}. "
                "POLY_1271 signer must equal deposit wallet. "
                f"Library: {_inspect.getfile(type(builder))}"
            )

        # Verify HTTP/2 keep-alive is enabled (huge latency win — TLS reuse).
        # py_clob_client_v2 uses a module-level httpx.Client(http2=True) in
        # http_helpers/helpers.py:24. Confirm by reading that module attr.
        try:
            from py_clob_client_v2.http_helpers import helpers as _hh
            _http2_enabled = bool(getattr(_hh._http_client, "_transport", None))
            _http2_label = "http2=on" if _http2_enabled else "http2=off"
        except Exception:
            _http2_label = "http2=?"

        builder_label = (
            f"{builder_code[:10]}…"
            if builder_code != BYTES32_ZERO
            else "NONE(no attribution)"
        )
        log.info(
            f"[clob] POLY_1271 ready: deposit_wallet={funder} "
            f"signer_eoa={eoa_addr} clob_api_key={self.s.clob_api_key[:8]}… "
            f"builder.sig_type={actual_sig_type} order_signer={actual_order_signer[:10]}… "
            f"builder_code={builder_label} keep_alive={_http2_label} "
            f"lib={_inspect.getfile(type(builder))}"
        )
        # Initialize Rust HFT pool if available
        if _HAS_RUST_HFT:
            try:
                self._rust_creds = polymarket_hft.RustApiCreds(
                    self.s.clob_api_key,
                    self.s.clob_secret,
                    self.s.clob_passphrase,
                )
                self._rust_pool = polymarket_hft.RustHttpPool(self.s.clob_host, 1100)
                log.info(f"[clob] Rust HTTP/2 pool active ({self._rust_pool})")
            except Exception as e:
                log.error(f"[clob] Failed to init Rust HFT path: {e}")
                self._rust_pool = None
                self._rust_creds = None

        return c

    async def ensure_ready(self) -> None:
        if self._client is None:
            if not self.s.private_key:
                raise RuntimeError("PRIVATE_KEY is not set; cannot initialize CLOB client")
            self._client = await asyncio.to_thread(self._build_client)
            # Refresh cached attribute references after client is built.
            self._poly_addr = self.s.polymarket_address
            self._clob_api_key = self.s.clob_api_key
            # Refresh debug flag now that the logger is fully configured.
            _refresh_debug_flag()
            if self._rust_pool:
                try:
                    await asyncio.to_thread(self._rust_pool.warm_up)
                except Exception as e:
                    log.warning(f"[clob] Rust pool warmup failed: {e}")
            log.info("[clob] V2 client ready")

    @property
    def client(self) -> ClobClient:
        if self._client is None:
            raise RuntimeError("CLOB client not initialized; call ensure_ready() first")
        return self._client

    # ------------------------------------------------------------------ market data
    async def midpoint(self, token_id: str) -> float | None:
        await self.ensure_ready()
        try:
            r = await asyncio.to_thread(self.client.get_midpoint, token_id)
            return float(r["mid"]) if isinstance(r, dict) else float(r)
        except Exception as e:
            log.warning(f"[clob] midpoint err: {e}")
            return None

    async def price(self, token_id: str, side: str) -> float | None:
        await self.ensure_ready()
        try:
            r = await asyncio.to_thread(self.client.get_price, token_id, side)
            return float(r["price"]) if isinstance(r, dict) else float(r)
        except Exception as e:
            log.warning(f"[clob] price err: {e}")
            return None

    async def order_book(self, token_id: str) -> Any:
        await self.ensure_ready()
        return await asyncio.to_thread(self.client.get_order_book, token_id)

    # ------------------------------------------------------------------ account / balance
    async def get_token_balance(self, token_id: str) -> float | None:
        """Returns the outcome token balance (shares held) for a specific CLOB token.

        Uses AssetType.CONDITIONAL with token_id. After a buy fill, settlement
        may take a tick or two — callers should poll briefly before trusting 0.
        """
        await self.ensure_ready()
        try:
            params = BalanceAllowanceParams(
                asset_type=AssetType.CONDITIONAL,
                token_id=token_id,
            )
            r = await asyncio.to_thread(self.client.get_balance_allowance, params)
            if isinstance(r, dict):
                raw = float(r.get("balance") or r.get("balance_allowance") or 0)
                return raw / 1e6 if raw > 1000 else raw
            return None
        except Exception as e:
            log.warning(f"[clob] token_balance err: {e}")
            return None

    async def get_usdc_balance(self) -> float | None:
        await self.ensure_ready()
        return await self._fetch_balance()

    async def _fetch_balance(self) -> float | None:
        try:
            params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
            r = await asyncio.to_thread(self.client.get_balance_allowance, params)
            if isinstance(r, dict):
                raw = float(r.get("balance") or r.get("balance_allowance") or 0)
                # pUSD returned as plain decimal (e.g. 25.50), not micro-units
                amount = raw / 1e6 if raw > 1000 else raw
                self._bankroll_cache = amount
                return amount
            return None
        except Exception as e:
            log.warning(f"[clob] pUSD balance err: {e}")
            return None

    async def open_orders(self) -> list[dict]:
        await self.ensure_ready()
        try:
            r = await asyncio.to_thread(self.client.get_orders, OpenOrderParams())
            return list(r) if r else []
        except Exception as e:
            log.warning(f"[clob] open_orders err: {e}")
            return []

    async def cancel(self, order_id: str) -> Any:
        await self.ensure_ready()
        return await asyncio.to_thread(self.client.cancel, order_id)

    async def cancel_all(self) -> Any:
        await self.ensure_ready()
        return await asyncio.to_thread(self.client.cancel_all)

    # ------------------------------------------------------------------ order placement
    # HOT PATH — NO RPC. CLOB POST is HTTP only; adding any web3/RPC call here
    # adds 50-200ms per order and breaks the 2s signal-to-fill budget. Off-hot-path
    # uses (background reconcile, periodic bankroll refresh) may use RPC freely.
    async def place_market_buy(
        self,
        token_id: str,
        usd_amount: float,
        worst_price: float,
        *,
        neg_risk: bool,
        tick_size: str = "0.01",
        order_type: str = OrderType.FAK,
        cancel_flag: threading.Event | None = None,
    ) -> dict:
        """FAK market buy with worst-price slippage protection.

        `worst_price`: exchange enforces this as the maximum fill price.
        Any liquidity at <= worst_price fills; the remainder is killed (FAK).
        `neg_risk`: must match market's negRisk flag from Gamma — wrong value
        signs against wrong exchange contract (CTFExchange vs NegRiskExchange)
        and triggers misleading "signer address" 400 error.
        `cancel_flag`: if set before the POST fires, the thread aborts.
        """
        await self.ensure_ready()
        user_balance = max(self._bankroll_cache, usd_amount * 1.05)
        args = MarketOrderArgsV2(
            token_id=token_id,
            amount=usd_amount,
            side=BUY,
            price=worst_price,
            user_usdc_balance=user_balance,
        )
        options = PartialCreateOrderOptions(tick_size=tick_size, neg_risk=neg_risk)

        # Pre-sign: signing (~3-5ms with eth_account C-ext) is done BEFORE the
        # caller's wait_for() timeout fires for the POST, freeing the full
        # api_timeout budget for the actual network round-trip.
        # NOTE: `time` is imported at module level — no per-call sys.modules lookup.
        _t_sign = time.monotonic()
        signed = await asyncio.to_thread(
            self.client.create_market_order, order_args=args, options=options
        )
        sign_ms = (time.monotonic() - _t_sign) * 1000
        if cancel_flag is not None and cancel_flag.is_set():
            raise RuntimeError("buy cancelled before POST (timeout)")

        # Cache locals once to avoid repeated attribute lookups inside the
        # thread callback (each `self.xxx` is a hash-table lookup).
        _client = self.client
        _poly_addr = self._poly_addr
        _api_key = self._clob_api_key
        _debug = _DEBUG_ENABLED
        _rust_pool = self._rust_pool
        _rust_creds = self._rust_creds

        def _post() -> Any:
            # Diagnostic log is behind _debug guard: f-string construction is
            # skipped entirely at INFO level, saving ~0.1–0.5 ms per order.
            if _debug:
                log.debug(
                    f"[clob.diag] BUY post sig=3 funder={_poly_addr[:10]} "
                    f"signed.signer={getattr(signed, 'signer', '?')[:10]} "
                    f"signed.maker={getattr(signed, 'maker', '?')[:10]} "
                    f"signed.builder={str(getattr(signed, 'builder', BYTES32_ZERO))[:10]} "
                    f"api_key={_api_key[:8]} "
                    f"neg_risk={neg_risk} tick={tick_size} sign_ms={sign_ms:.1f}"
                )
            if _rust_pool and _rust_creds:
                import orjson
                payload = order_to_json_v2(signed, _api_key, order_type, False, False)
                body_str = orjson.dumps(payload).decode("utf-8")
                # Releases GIL during network I/O
                resp_str = _rust_pool.post_order(body_str, _rust_creds)
                return orjson.loads(resp_str)
            else:
                return _client.post_order(signed, order_type)

        _t_post = time.monotonic()
        resp = await asyncio.to_thread(_post)
        post_ms = (time.monotonic() - _t_post) * 1000
        if isinstance(resp, dict):
            resp.setdefault("_sign_ms", sign_ms)
            resp.setdefault("_post_ms", post_ms)
            return resp
        return {"raw": resp, "_sign_ms": sign_ms, "_post_ms": post_ms}

    async def place_capped_buy(
        self,
        token_id: str,
        max_price: float,
        usd_amount: float,
        *,
        neg_risk: bool,
        tick_size: str = "0.01",
        order_type: str = OrderType.FAK,
    ) -> dict:
        """Limit FAK buy with a max acceptable price (slippage cap)."""
        await self.ensure_ready()
        size_shares = round(usd_amount / max(max_price, 1e-3), 4)
        args = OrderArgsV2(
            token_id=token_id, price=round(max_price, 3), size=size_shares, side=BUY
        )
        options = PartialCreateOrderOptions(tick_size=tick_size, neg_risk=neg_risk)
        def _sign_and_post() -> Any:
            signed = self.client.create_order(args, options=options)
            return self.client.post_order(signed, order_type)
        resp = await asyncio.to_thread(_sign_and_post)
        return resp if isinstance(resp, dict) else {"raw": resp}

    async def place_limit(
        self,
        token_id: str,
        price: float,
        size_shares: float,
        side: str,
        *,
        neg_risk: bool,
        tick_size: str = "0.01",
        order_type: str = OrderType.GTC,
    ) -> dict:
        await self.ensure_ready()
        args = OrderArgsV2(token_id=token_id, price=price, size=size_shares, side=side)
        options = PartialCreateOrderOptions(tick_size=tick_size, neg_risk=neg_risk)
        def _sign_and_post() -> Any:
            signed = self.client.create_order(args, options=options)
            return self.client.post_order(signed, order_type)
        resp = await asyncio.to_thread(_sign_and_post)
        return resp if isinstance(resp, dict) else {"raw": resp}

    async def place_market_sell(
        self,
        token_id: str,
        size_shares: float,
        *,
        neg_risk: bool,
        tick_size: str = "0.01",
    ) -> dict:
        """True market sell using MarketOrderArgsV2.

        `neg_risk` MUST match the buy-side neg_risk for the same token —
        otherwise the sell order signs against a different exchange contract
        than the position is held in, and is rejected.
        """
        await self.ensure_ready()
        user_balance = max(self._bankroll_cache, round(size_shares, 4) * 0.02)
        args = MarketOrderArgsV2(
            token_id=token_id,
            amount=round(size_shares, 4),
            side=SELL,
            user_usdc_balance=user_balance,
        )
        options = PartialCreateOrderOptions(tick_size=tick_size, neg_risk=neg_risk)

        # Pre-sign before caller's wait_for() timeout (see place_market_buy).
        _t_sign = time.monotonic()
        signed = await asyncio.to_thread(
            self.client.create_market_order, order_args=args, options=options
        )
        sign_ms = (time.monotonic() - _t_sign) * 1000

        # Cache locals — same rationale as place_market_buy.
        _client = self.client
        _poly_addr = self._poly_addr
        _api_key = self._clob_api_key
        _debug = _DEBUG_ENABLED
        _rust_pool = self._rust_pool
        _rust_creds = self._rust_creds

        def _post() -> Any:
            if _debug:
                log.debug(
                    f"[clob.diag] SELL post sig=3 funder={_poly_addr[:10]} "
                    f"signer={getattr(signed, 'signer', '?')[:10]} "
                    f"signed.builder={str(getattr(signed, 'builder', BYTES32_ZERO))[:10]} "
                    f"api_key={_api_key[:8]} "
                    f"neg_risk={neg_risk} tick={tick_size} sign_ms={sign_ms:.1f}"
                )
            if _rust_pool and _rust_creds:
                import orjson
                payload = order_to_json_v2(signed, _api_key, OrderType.FAK, False, False)
                body_str = orjson.dumps(payload).decode("utf-8")
                resp_str = _rust_pool.post_order(body_str, _rust_creds)
                return orjson.loads(resp_str)
            else:
                return _client.post_order(signed, OrderType.FAK)

        _t_post = time.monotonic()
        resp = await asyncio.to_thread(_post)
        post_ms = (time.monotonic() - _t_post) * 1000
        if isinstance(resp, dict):
            resp.setdefault("_sign_ms", sign_ms)
            resp.setdefault("_post_ms", post_ms)
            return resp
        return {"raw": resp, "_sign_ms": sign_ms, "_post_ms": post_ms}