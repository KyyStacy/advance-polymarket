from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

from dotenv import load_dotenv


def _env(name: str, default: str | None = None, required: bool = False) -> str:
    val = os.getenv(name, default)
    if required and (val is None or val == ""):
        raise RuntimeError(f"Missing required env var: {name}")
    return val if val is not None else ""


def _envf(name: str, default: float) -> float:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    return float(v)


def _envi(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    return int(v)


def _envb(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    return v.strip().lower() in {"1", "true", "yes", "y", "on"}


@dataclass
class Settings:
    # Wallet
    private_key: str
    polymarket_address: str
    relayer_api_key: str
    relayer_api_key_address: str

    # CLOB API creds (L2 auth on /order endpoint)
    clob_api_key: str
    clob_secret: str
    clob_passphrase: str

    # Builder API creds (HMAC, from Polymarket UI — future use for gasless relayer)
    builder_api_key: str
    builder_secret: str
    builder_passphrase: str
    # Builder code (bytes32 hex) — applied to every order for fee attribution.
    # Empty string means no attribution (legacy behavior).
    builder_code: str

    # Network / clob
    signature_type: int
    chain_id: int
    clob_host: str
    gamma_host: str
    polymarket_ws: str
    polygon_rpc: str  # Polygon RPC for on-chain operations

    # Mode
    dry_run: bool

    # Risk / sizing
    min_bet_usd: float
    max_bet_mode: Literal["pct", "usd"]
    max_bet_pct: float
    max_bet_usd: float
    max_daily_loss_usd: float

    # Odds
    min_odds: float
    max_odds: float
    allow_out_of_bound_ev: bool
    ev_override_min_edge: float

    # Strategy tuning
    min_edge: float
    min_momentum_z: float
    min_leadlag_bps: float
    entry_window_start_sec: int
    entry_window_end_sec: int
    exit_hard_floor_sec: int

    # Logging
    log_level: str
    log_file: str

    # Derived
    project_root: Path = field(default_factory=Path.cwd)


def load_settings(env_path: str | None = None) -> Settings:
    root = Path(__file__).resolve().parents[2]
    load_dotenv(env_path or (root / ".env"), override=False)

    mode = (_env("MAX_BET_MODE", "pct") or "pct").strip().lower()
    if mode not in {"pct", "usd"}:
        raise ValueError("MAX_BET_MODE must be 'pct' or 'usd'")

    s = Settings(
        private_key=_env("PRIVATE_KEY", ""),
        polymarket_address=_env("POLYMARKET_ADDRESS", ""),
        relayer_api_key=_env("RELAYER_API_KEY", ""),
        relayer_api_key_address=_env("RELAYER_API_KEY_ADDRESS", ""),
        clob_api_key=_env("CLOB_API_KEY", ""),
        clob_secret=_env("CLOB_SECRET", ""),
        clob_passphrase=_env("CLOB_PASSPHRASE", ""),
        builder_api_key=_env("BUILDER_API_KEY", ""),
        builder_secret=_env("BUILDER_SECRET", ""),
        builder_passphrase=_env("BUILDER_PASSPHRASE", ""),
        builder_code=_env("BUILDER_CODE", ""),
        signature_type=_envi("SIGNATURE_TYPE", 3),
        chain_id=_envi("CHAIN_ID", 137),
        clob_host=_env("CLOB_HOST", "https://clob.polymarket.com"),
        gamma_host=_env("GAMMA_HOST", "https://gamma-api.polymarket.com"),
        polymarket_ws=_env(
            "POLYMARKET_WS",
            "wss://ws-subscriptions-clob.polymarket.com/ws/market",
        ),
        polygon_rpc=_env(
            "POLYGON_RPC",
            "https://polygon-bor-rpc.publicnode.com,https://polygon.llamarpc.com",
        ),
        dry_run=_envb("DRY_RUN", True),
        min_bet_usd=_envf("MIN_BET_USD", 2.0),
        max_bet_mode=mode,  # type: ignore[arg-type]
        max_bet_pct=_envf("MAX_BET_PCT", 5.0),
        max_bet_usd=_envf("MAX_BET_USD", 25.0),
        max_daily_loss_usd=_envf("MAX_DAILY_LOSS_USD", 20.0),
        min_odds=_envf("MIN_ODDS", 0.40),
        max_odds=_envf("MAX_ODDS", 0.65),
        allow_out_of_bound_ev=_envb("ALLOW_OUT_OF_BOUND_EV", True),
        ev_override_min_edge=_envf("EV_OVERRIDE_MIN_EDGE", 0.08),
        min_edge=_envf("MIN_EDGE", 0.04),
        min_momentum_z=_envf("MIN_MOMENTUM_Z", 1.5),
        min_leadlag_bps=_envf("MIN_LEADLAG_BPS", 4.0),
        entry_window_start_sec=_envi("ENTRY_WINDOW_START_SEC", 60),
        entry_window_end_sec=_envi("ENTRY_WINDOW_END_SEC", 270),
        exit_hard_floor_sec=_envi("EXIT_HARD_FLOOR_SEC", 15),
        log_level=_env("LOG_LEVEL", "INFO"),
        log_file=_env("LOG_FILE", "logs/bot.log"),
        project_root=root,
    )
    # POLY_1271-only enforcement. The bot signs orders with sig_type=3
    # (deposit wallet) and uses CLOB_* API creds bound to that wallet.
    # Any other sig_type would mismatch the order.signer field and trigger
    # "the order signer address has to be the address of the API KEY".
    if s.signature_type != 3:
        raise RuntimeError(
            f"SIGNATURE_TYPE must be 3 (POLY_1271). Got {s.signature_type}. "
            "This bot only supports Polymarket deposit wallets."
        )
    if not s.polymarket_address:
        raise RuntimeError(
            "POLYMARKET_ADDRESS (deposit wallet address) is required for POLY_1271."
        )
    if not (s.clob_api_key and s.clob_secret and s.clob_passphrase):
        raise RuntimeError(
            "CLOB_API_KEY / CLOB_SECRET / CLOB_PASSPHRASE are required. "
            "Create them via Polymarket portal while logged in as the deposit wallet."
        )
    # UUID format guard: a single missing/extra char silently breaks server auth
    # and surfaces as misleading "the order signer address has to be the address
    # of the API KEY" errors. Validate format up-front.
    import re as _re
    _UUID_RE = _re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")
    if not _UUID_RE.match(s.clob_api_key):
        raise RuntimeError(
            f"CLOB_API_KEY is not a valid UUID: {s.clob_api_key!r}. "
            "Expected format: 8-4-4-4-12 hex chars (e.g. '3a9cbad0-7cc6-8950-b8d9-09902171e128'). "
            "Check .env for typos."
        )
    # BUILDER_CODE is optional but if set must be a valid bytes32 hex.
    # Empty string disables attribution (legacy behavior). Non-empty but malformed
    # would silently produce orders with garbage builder field — reject up-front.
    if s.builder_code:
        _BYTES32_RE = _re.compile(r"^0x[0-9a-fA-F]{64}$")
        if not _BYTES32_RE.match(s.builder_code):
            raise RuntimeError(
                f"BUILDER_CODE is not a valid bytes32 hex: {s.builder_code!r}. "
                "Expected format: '0x' + 64 hex chars (e.g. '0x0000...000abc'). "
                "Get it from Polymarket portal under Builder section."
            )
    return s
