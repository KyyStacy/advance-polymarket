from __future__ import annotations

import asyncio
from typing import Any

from py_clob_client_v2.client import ClobClient
from py_clob_client_v2.clob_types import (
    ApiCreds,
    AssetType,
    BalanceAllowanceParams,
    MarketOrderArgsV2,
    OpenOrderParams,
    OrderArgsV2,
    OrderType,
)
from py_clob_client_v2.order_builder.constants import BUY, SELL

from ..config.settings import Settings
from ..utils.logger import get_logger

log = get_logger()


class PolymarketClobClient:
    """Async wrapper around py-clob-client-v2 (Polymarket CLOB V2, April 28 2026+).

    All blocking calls dispatched via asyncio.to_thread.
    """

    def __init__(self, settings: Settings):
        self.s = settings
        self._client: ClobClient | None = None
        self._bankroll_cache: float = 0.0  # last known pUSD balance for market order fees

    def _build_client(self) -> ClobClient:
        # funder must be None (not "") so OrderBuilder.funder falls back to
        # signer.address() for pure EOA accounts.
        funder = self.s.polymarket_address or self.s.relayer_api_key_address or None

        # Derive EOA address from private key to detect account type.
        from eth_account import Account as _Acc
        eoa_addr = _Acc.from_key(self.s.private_key).address

        # Sanity check: if POLYMARKET_ADDRESS is a proxy wallet (≠ EOA), warn
        # if SIGNATURE_TYPE=0 (EOA mode is wrong for proxy accounts).
        # Type 1 = browser/MetaMask proxy, Type 2 = Gnosis Safe (Phantom/email/Magic).
        sig_type = self.s.signature_type
        if funder and funder.lower() != eoa_addr.lower() and sig_type == 0:
            log.warning(
                f"[clob] POLYMARKET_ADDRESS ({funder[:10]}…) differs from EOA "
                f"({eoa_addr[:10]}…) but SIGNATURE_TYPE=0 — set 1 (browser) "
                "or 2 (Phantom/Gnosis Safe) in .env"
            )

        c = ClobClient(
            host=self.s.clob_host,
            chain_id=self.s.chain_id,
            key=self.s.private_key,
            signature_type=sig_type,
            funder=funder,
        )
        sig_name = {0: "EOA", 1: "POLY_PROXY", 2: "POLY_GNOSIS_SAFE", 3: "POLY_1271"}.get(
            sig_type, str(sig_type)
        )
        log.info(
            f"[clob] maker={funder or eoa_addr} signer={eoa_addr} "
            f"sig_type={sig_name}({sig_type})"
        )
        # Always derive L2 credentials from private key. Builder API credentials
        # are for builder operations, not L2 auth. Deriving from the key is reliable
        # and avoids the initial 401 when using stale/incompatible credentials.
        creds = c.create_or_derive_api_key()
        c.set_api_creds(creds)
        return c

    async def ensure_ready(self) -> None:
        if self._client is None:
            if not self.s.private_key:
                raise RuntimeError("PRIVATE_KEY is not set; cannot initialize CLOB client")
            self._client = await asyncio.to_thread(self._build_client)
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
        """Returns pUSD balance from Polymarket CLOB V2.

        V2 client automatically includes correct signature_type in the request.
        On 401: re-derives L2 credentials from private key and retries once.
        pUSD is Polymarket's internal collateral — separate from on-chain USDC.
        """
        await self.ensure_ready()
        return await self._fetch_balance()

    async def _fetch_balance(self, retry: bool = True) -> float | None:
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
            err_str = str(e)
            if retry and "401" in err_str:
                log.warning("[clob] balance 401 — re-deriving L2 credentials from private key")
                try:
                    new_creds = await asyncio.to_thread(self.client.create_or_derive_api_key)
                    self.client.set_api_creds(new_creds)
                    log.info(f"[clob] re-derived API key: {new_creds.api_key[:8]}…")
                    return await self._fetch_balance(retry=False)
                except Exception as re_err:
                    log.warning(f"[clob] re-derive failed: {re_err}")
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
    async def place_market_buy(
        self, token_id: str, usd_amount: float, order_type: str = OrderType.FAK
    ) -> dict:
        """Unbounded market buy — fills against any ask. Avoid in production:
        with thin liquidity this can walk the book up arbitrarily. Prefer
        `place_capped_buy` which caps the worst fill price."""
        await self.ensure_ready()
        args = MarketOrderArgsV2(
            token_id=token_id,
            amount=usd_amount,
            side=BUY,
            order_type=order_type,
            user_usdc_balance=self._bankroll_cache,
        )
        signed = await asyncio.to_thread(self.client.create_market_order, args)
        resp = await asyncio.to_thread(self.client.post_order, signed, order_type)
        return resp if isinstance(resp, dict) else {"raw": resp}

    async def place_capped_buy(
        self,
        token_id: str,
        max_price: float,
        usd_amount: float,
        order_type: str = OrderType.FAK,
    ) -> dict:
        """Limit FAK buy with a max acceptable price (slippage cap).

        size_shares = usd_amount / max_price (worst case). FAK fills any
        liquidity at <= max_price and kills the rest. If actual top-of-book
        is below max_price, the average fill will be lower — protecting
        against book sweeps in thin liquidity.
        """
        await self.ensure_ready()
        size_shares = round(usd_amount / max(max_price, 1e-3), 4)
        args = OrderArgsV2(
            token_id=token_id, price=round(max_price, 3), size=size_shares, side=BUY
        )
        signed = await asyncio.to_thread(self.client.create_order, args)
        resp = await asyncio.to_thread(self.client.post_order, signed, order_type)
        return resp if isinstance(resp, dict) else {"raw": resp}

    async def place_limit(
        self,
        token_id: str,
        price: float,
        size_shares: float,
        side: str,
        order_type: str = OrderType.GTC,
    ) -> dict:
        await self.ensure_ready()
        args = OrderArgsV2(token_id=token_id, price=price, size=size_shares, side=side)
        signed = await asyncio.to_thread(self.client.create_order, args)
        resp = await asyncio.to_thread(self.client.post_order, signed, order_type)
        return resp if isinstance(resp, dict) else {"raw": resp}

    async def place_market_sell(
        self,
        token_id: str,
        size_shares: float,
    ) -> dict:
        """True market sell using MarketOrderArgsV2.

        No minimum share requirement unlike limit FAK orders.
        """
        await self.ensure_ready()
        args = MarketOrderArgsV2(
            token_id=token_id,
            amount=round(size_shares, 4),
            side=SELL,
            user_usdc_balance=self._bankroll_cache,
        )
        signed = await asyncio.to_thread(self.client.create_market_order, args)
        resp = await asyncio.to_thread(self.client.post_order, signed, OrderType.FAK)
        return resp if isinstance(resp, dict) else {"raw": resp}