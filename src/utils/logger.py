from __future__ import annotations

import sys
from pathlib import Path

from loguru import logger


_configured = False


def setup_logger(level: str = "INFO", log_file: str | None = None, console: bool = True) -> None:
    global _configured
    if _configured:
        return
    logger.remove()
    if console:
        logger.add(
            sys.stderr,
            level=level.upper(),
            format=(
                "<green>{time:HH:mm:ss.SSS}</green> "
                "<level>{level: <7}</level> "
                "<cyan>{name}</cyan>:<cyan>{line}</cyan> | "
                "<level>{message}</level>"
            ),
            enqueue=False,
            backtrace=False,
            diagnose=False,
        )
    if log_file:
        Path(log_file).parent.mkdir(parents=True, exist_ok=True)
        # Exclude [signal] logs from main log file — they go to signals.log only
        def exclude_signal_filter(record):
            return "[signal]" not in record["message"]

        logger.add(
            log_file,
            level=level.upper(),
            rotation="20 MB",
            retention="14 days",
            format=(
                "<green>{time:HH:mm:ss.SSS}</green> "
                "<level>{level: <7}</level> "
                "<cyan>{name}</cyan>:<cyan>{line}</cyan> | "
                "<level>{message}</level>"
            ),
            filter=exclude_signal_filter,
            enqueue=True,
            backtrace=False,
            diagnose=False,
        )
    _setup_signal_logger(log_file)
    _configured = True


def _setup_signal_logger(log_file: str | None) -> None:
    """Add signal handler that filters [signal] logs to separate file."""
    if log_file:
        log_dir = Path(log_file).parent
    else:
        log_dir = Path("logs")

    log_dir.mkdir(parents=True, exist_ok=True)
    signal_log_file = log_dir / "signals.log"

    def signal_filter(record):
        return "[signal]" in record["message"]

    logger.add(
        str(signal_log_file),
        level="INFO",
        rotation="20 MB",
        retention="14 days",
        format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {message}",
        filter=signal_filter,
        enqueue=True,
        backtrace=False,
        diagnose=False,
    )


def get_logger():
    return logger
