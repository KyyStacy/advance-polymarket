from __future__ import annotations

from dataclasses import dataclass

import aiohttp
import orjson

from ..utils.logger import get_logger
from ..utils.time_utils import current_window_start, window_slug

log = get_logger()


@dataclass
class MarketInfo:
    condition_id: str
    slug: str
    question: str
    end_iso: str | None
    window_ts: int
    up_token_id: str
    down_token_id: str
    tick_size: float
    minimum_order_size: float

    @property
    def tokens(self) -> list[str]:
        return [self.up_token_id, self.down_token_id]


class MarketDiscovery:
    """Discovers the active 5-minute BTC Up/Down market via Gamma API.

    Slug deterministically derived from current 5-minute window timestamp:
        btc-updown-5m-{window_ts}
    """

    def __init__(self, gamma_host: str):
        self.host = gamma_host.rstrip("/")
        self._session: aiohttp.ClientSession | None = None
        self._cache: dict[int, MarketInfo] = {}

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                json_serialize=lambda v: orjson.dumps(v).decode(),
                timeout=aiohttp.ClientTimeout(total=8),
            )
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    async def get_current_market(self, ts: float | None = None) -> MarketInfo | None:
        win = current_window_start(ts)
        if win in self._cache:
            return self._cache[win]
        slug = window_slug(win)
        info = await self._fetch_by_slug(slug, win)
        if info:
            self._cache[win] = info
        return info

    async def get_next_market(self, ts: float | None = None) -> MarketInfo | None:
        win = current_window_start(ts) + 300
        slug = window_slug(win)
        return await self._fetch_by_slug(slug, win)

    async def _fetch_by_slug(self, slug: str, window_ts: int) -> MarketInfo | None:
        s = await self._ensure_session()
        url = f"{self.host}/markets/slug/{slug}"
        try:
            async with s.get(url) as resp:
                if resp.status == 404:
                    return None
                if resp.status != 200:
                    log.warning(f"[gamma] {slug}: HTTP {resp.status}")
                    return None
                data = await resp.json(loads=orjson.loads)
        except Exception as e:
            log.warning(f"[gamma] fetch {slug} failed: {e}")
            return None

        # Gamma returns either a single market dict or list
        m = data[0] if isinstance(data, list) and data else data
        if not isinstance(m, dict):
            return None
        try:
            tokens_raw = m.get("clobTokenIds")
            if isinstance(tokens_raw, str):
                token_ids = orjson.loads(tokens_raw)
            else:
                token_ids = tokens_raw or []
            outcomes_raw = m.get("outcomes")
            if isinstance(outcomes_raw, str):
                outcomes = orjson.loads(outcomes_raw)
            else:
                outcomes = outcomes_raw or []
            up_idx = next(
                (i for i, o in enumerate(outcomes) if str(o).lower() == "up"), 0
            )
            down_idx = 1 - up_idx if len(outcomes) >= 2 else 1
            up_token = str(token_ids[up_idx])
            down_token = str(token_ids[down_idx])
            return MarketInfo(
                condition_id=str(m.get("conditionId") or m.get("condition_id") or ""),
                slug=str(m.get("slug") or slug),
                question=str(m.get("question") or ""),
                end_iso=m.get("endDate") or m.get("end_date"),
                window_ts=window_ts,
                up_token_id=up_token,
                down_token_id=down_token,
                tick_size=float(m.get("orderPriceMinTickSize") or 0.01),
                minimum_order_size=float(m.get("orderMinSize") or 5.0),
            )
        except Exception as e:
            log.warning(f"[gamma] parse {slug} failed: {e}")
            return None
