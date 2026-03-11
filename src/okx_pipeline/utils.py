"""
Utility functions: logging, retry logic, rate limiting, file helpers.
"""

import logging
import time
import functools
from pathlib import Path
from datetime import datetime

from . import config


def setup_logger(name: str, level=logging.INFO) -> logging.Logger:
    """Create a logger that writes to both console and a log file."""
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger  # already configured

    logger.setLevel(level)
    formatter = logging.Formatter(
        "%(asctime)s | %(name)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console handler
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    # File handler
    config.LOG_DIR.mkdir(parents=True, exist_ok=True)
    date_str = datetime.now().strftime("%Y%m%d")
    fh = logging.FileHandler(config.LOG_DIR / f"{name}_{date_str}.log")
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    return logger


class RateLimiter:
    """Simple per-key rate limiter using time.sleep."""

    def __init__(self):
        self._last_call: dict[str, float] = {}

    def wait(self, key: str, min_interval: float):
        now = time.monotonic()
        last = self._last_call.get(key, 0.0)
        elapsed = now - last
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)
        self._last_call[key] = time.monotonic()


# Global rate limiter instance
rate_limiter = RateLimiter()


def retry_with_backoff(
    max_retries: int = 5,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    retryable_exceptions: tuple = (Exception,),
):
    """Decorator for exponential backoff retry."""

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            logger = logging.getLogger("retry")
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except retryable_exceptions as e:
                    if attempt == max_retries:
                        logger.error(
                            f"{func.__name__} failed after {max_retries} retries: {e}"
                        )
                        raise
                    delay = min(base_delay * (2 ** attempt), max_delay)
                    logger.warning(
                        f"{func.__name__} attempt {attempt + 1} failed: {e}. "
                        f"Retrying in {delay:.1f}s..."
                    )
                    time.sleep(delay)

        return wrapper

    return decorator


def ensure_dirs():
    """Create all required data directories."""
    for d in [
        config.FUNDING_DIR,
        config.SPOT_OB_DIR,
        config.PERP_OB_DIR,
        config.PROCESSED_DIR,
        config.LOG_DIR,
        config.MANIFEST_DIR,
    ]:
        d.mkdir(parents=True, exist_ok=True)


def ts_ms_to_datetime(ts_ms) -> datetime:
    """Convert millisecond timestamp to datetime."""
    return datetime.utcfromtimestamp(int(ts_ms) / 1000)


def datetime_to_ts_ms(dt: datetime) -> str:
    """Convert datetime to millisecond timestamp string."""
    return str(int(dt.timestamp() * 1000))


def coin_from_inst_id(inst_id: str) -> str:
    """Extract coin name from instrument ID, e.g. 'BTC-USDT-SWAP' -> 'BTC'."""
    return inst_id.split("-")[0]


def safe_filename(inst_id: str) -> str:
    """Convert instrument ID to a filename-safe string: BTC-USDT-SWAP -> BTC_USDT_SWAP."""
    return inst_id.replace("-", "_")
