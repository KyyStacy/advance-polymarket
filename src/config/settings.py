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
    entry_window_start_sec: int
    entry_window_end_sec: int
    exit_hard_floor_sec: int
    exit_rapid_flip_hold_sec: float
    exit_rapid_flip_cooldown_sec: float

    # Signal Logic
    sig_momentum_drift_weight: float
    sig_leadlag_drift_weight_per_bp: float
    sig_log_now_decay: float

    # Risk Management
    risk_consecutive_loss_cooldown_sec: float
    entry_turbo_low_ask_limit: float

    # Entry Engine
    entry_min_exit_bid: float
    entry_max_spread: float
    entry_max_momentum_z: float
    entry_persistence_ticks: int

    # Exit Engine
    exit_stale_seconds: float
    exit_min_hold_seconds: float
    exit_own_fill_grace_seconds: float
    exit_take_profit_pct: float
    exit_trailing_stop_min_peak: float
    exit_trailing_stop_dist: float
    exit_profit_hold_pct: float
    exit_loss_exit_z: float
    exit_z_in_profit: float
    exit_edge_collapse_loss_frac: float
    exit_edge_collapse_profit_frac: float
    exit_ll_converge_loss_frac: float
    exit_ll_converge_profit_frac: float
    exit_ll_min_adverse_z: float
    exit_ll_min_loss_pct: float
    exit_hard_stop_loss_pct: float

    # Order Execution
    exec_order_fail_cooldown_sec: float
    exec_fill_poll_deadline_sec: float
    exec_fill_poll_interval_sec: float
    exec_fill_poll_fast_sec: float
    exec_fill_poll_fast_window_sec: float
    exec_fill_poll_backoff_mult: float
    exec_fill_poll_max_sec: float
    exec_balance_delta_epsilon: float
    exec_max_drift: float
    exec_signal_anchor_ttl_sec: float
    exec_buy_total_deadline_sec: float
    exec_buy_api_timeout_sec: float
    exec_sell_max_attempts: int
    exec_sell_retry_delay_sec: float
    exec_sell_api_timeout_sec: float
    exec_sell_fill_poll_deadline_sec: float
    exec_min_sell_usd: float
    exec_post_sell_guard_sec: float

    # Adaptive Config
    vol_choppy_threshold: float
    vol_turbo_threshold: float
    
    steady_min_edge: float
    steady_min_momentum_z: float
    steady_min_leadlag_bps: float
    steady_sigma: float

    choppy_min_edge: float
    choppy_min_momentum_z: float
    choppy_min_leadlag_bps: float
    choppy_sigma: float

    turbo_min_edge: float
    turbo_min_momentum_z: float
    turbo_min_leadlag_bps: float
    turbo_sigma: float

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
        ev_override_min_edge=_envf("EV_OVERRIDE_MIN_EDGE", 0.20),
        entry_window_start_sec=_envi("ENTRY_WINDOW_START_SEC", 60),
        entry_window_end_sec=_envi("ENTRY_WINDOW_END_SEC", 270),
        exit_hard_floor_sec=_envi("EXIT_HARD_FLOOR_SEC", 15),
        exit_rapid_flip_hold_sec=_envf("EXIT_RAPID_FLIP_HOLD_SEC", 15.0),
        exit_rapid_flip_cooldown_sec=_envf("EXIT_RAPID_FLIP_COOLDOWN_SEC", 30.0),

        # Signal Logic
        sig_momentum_drift_weight=_envf("SIG_MOMENTUM_DRIFT_WEIGHT", 0.0012),
        sig_leadlag_drift_weight_per_bp=_envf("SIG_LEADLAG_DRIFT_WEIGHT_PER_BP", 0.0002),
        sig_log_now_decay=_envf("SIG_LOG_NOW_DECAY", 0.15),

        # Risk Management
        risk_consecutive_loss_cooldown_sec=_envf("RISK_CONSECUTIVE_LOSS_COOLDOWN_SEC", 300.0),
        entry_turbo_low_ask_limit=_envf("ENTRY_TURBO_LOW_ASK_LIMIT", 0.20),
        
        # Entry Engine
        entry_min_exit_bid=_envf("ENTRY_MIN_EXIT_BID", 0.10),
        entry_max_spread=_envf("ENTRY_MAX_SPREAD", 0.15),
        entry_max_momentum_z=_envf("ENTRY_MAX_MOMENTUM_Z", 6.0),
        entry_persistence_ticks=_envi("ENTRY_PERSISTENCE_TICKS", 2),

        # Exit Engine
        exit_stale_seconds=_envf("EXIT_STALE_SECONDS", 10.0),
        exit_min_hold_seconds=_envf("EXIT_MIN_HOLD_SECONDS", 5.0),
        exit_own_fill_grace_seconds=_envf("EXIT_OWN_FILL_GRACE_SECONDS", 1.0),
        exit_take_profit_pct=_envf("EXIT_TAKE_PROFIT_PCT", 3.00),
        exit_trailing_stop_min_peak=_envf("EXIT_TRAILING_STOP_MIN_PEAK", 0.20),
        exit_trailing_stop_dist=_envf("EXIT_TRAILING_STOP_DIST", 0.12),
        exit_profit_hold_pct=_envf("EXIT_PROFIT_HOLD_PCT", 0.05),
        exit_loss_exit_z=_envf("EXIT_LOSS_EXIT_Z", 1.5),
        exit_z_in_profit=_envf("EXIT_Z_IN_PROFIT", 1.8),
        exit_edge_collapse_loss_frac=_envf("EXIT_EDGE_COLLAPSE_LOSS_FRAC", 0.3333),
        exit_edge_collapse_profit_frac=_envf("EXIT_EDGE_COLLAPSE_PROFIT_FRAC", 0.0),
        exit_ll_converge_loss_frac=_envf("EXIT_LL_CONVERGE_LOSS_FRAC", -0.25),
        exit_ll_converge_profit_frac=_envf("EXIT_LL_CONVERGE_PROFIT_FRAC", -0.50),
        exit_ll_min_adverse_z=_envf("EXIT_LL_MIN_ADVERSE_Z", 1.0),
        exit_ll_min_loss_pct=_envf("EXIT_LL_MIN_LOSS_PCT", -0.15),
        exit_hard_stop_loss_pct=_envf("EXIT_HARD_STOP_LOSS_PCT", -0.30),

        # Order Execution
        exec_order_fail_cooldown_sec=_envf("EXEC_ORDER_FAIL_COOLDOWN_SEC", 5.0),
        exec_fill_poll_deadline_sec=_envf("EXEC_FILL_POLL_DEADLINE_SEC", 10.0),
        exec_fill_poll_interval_sec=_envf("EXEC_FILL_POLL_INTERVAL_SEC", 0.25),
        exec_fill_poll_fast_sec=_envf("EXEC_FILL_POLL_FAST_SEC", 0.025),
        exec_fill_poll_fast_window_sec=_envf("EXEC_FILL_POLL_FAST_WINDOW_SEC", 2.0),
        exec_fill_poll_backoff_mult=_envf("EXEC_FILL_POLL_BACKOFF_MULT", 1.5),
        exec_fill_poll_max_sec=_envf("EXEC_FILL_POLL_MAX_SEC", 0.25),
        exec_balance_delta_epsilon=_envf("EXEC_BALANCE_DELTA_EPSILON", 0.005),
        exec_max_drift=_envf("EXEC_MAX_DRIFT", 0.03),
        exec_signal_anchor_ttl_sec=_envf("EXEC_SIGNAL_ANCHOR_TTL_SEC", 30.0),
        exec_buy_total_deadline_sec=_envf("EXEC_BUY_TOTAL_DEADLINE_SEC", 3.0),
        exec_buy_api_timeout_sec=_envf("EXEC_BUY_API_TIMEOUT_SEC", 2.5),
        exec_sell_max_attempts=_envi("EXEC_SELL_MAX_ATTEMPTS", 3),
        exec_sell_retry_delay_sec=_envf("EXEC_SELL_RETRY_DELAY_SEC", 0.02),
        exec_sell_api_timeout_sec=_envf("EXEC_SELL_API_TIMEOUT_SEC", 2.5),
        exec_sell_fill_poll_deadline_sec=_envf("EXEC_SELL_FILL_POLL_DEADLINE_SEC", 20.0),
        exec_min_sell_usd=_envf("EXEC_MIN_SELL_USD", 0.05),
        exec_post_sell_guard_sec=_envf("EXEC_POST_SELL_GUARD_SEC", 10.0),

        # Adaptive Config
        vol_choppy_threshold=_envf("VOL_CHOPPY_THRESHOLD", 4.0),
        vol_turbo_threshold=_envf("VOL_TURBO_THRESHOLD", 7.5),
        
        steady_min_edge=_envf("STEADY_MIN_EDGE", 0.10),
        steady_min_momentum_z=_envf("STEADY_MIN_MOM_Z", 2.6),
        steady_min_leadlag_bps=_envf("STEADY_MIN_LL_BPS", 2.8),
        steady_sigma=_envf("STEADY_SIGMA", 0.00008),

        choppy_min_edge=_envf("CHOPPY_MIN_EDGE", 0.14),
        choppy_min_momentum_z=_envf("CHOPPY_MIN_MOM_Z", 3.1),
        choppy_min_leadlag_bps=_envf("CHOPPY_MIN_LL_BPS", 3.3),
        choppy_sigma=_envf("CHOPPY_SIGMA", 0.00010),

        turbo_min_edge=_envf("TURBO_MIN_EDGE", 0.18),
        turbo_min_momentum_z=_envf("TURBO_MIN_MOM_Z", 3.6),
        turbo_min_leadlag_bps=_envf("TURBO_MIN_LL_BPS", 3.8),
        turbo_sigma=_envf("TURBO_SIGMA", 0.00012),

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
