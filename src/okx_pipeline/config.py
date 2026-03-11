"""
Configuration for the OKX data pipeline.
All constants, paths, API endpoints, and rate limits.
"""

from pathlib import Path

# ── Coins & Instruments ──────────────────────────────────────────────────────

COINS = ["BTC", "ETH", "BNB", "SOL", "XRP", "AVAX", "DOGE"]

# Expected OKX instrument IDs (verified via instruments endpoint)
SPOT_TEMPLATE = "{coin}-USDT"
PERP_TEMPLATE = "{coin}-USDT-SWAP"

# ── Paths ────────────────────────────────────────────────────────────────────

PIPELINE_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PIPELINE_ROOT / "data" / "okx"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"

FUNDING_DIR = RAW_DIR / "funding_rates"
SPOT_OB_DIR = RAW_DIR / "orderbooks_spot"
PERP_OB_DIR = RAW_DIR / "orderbooks_perp"

LOG_DIR = PIPELINE_ROOT / "logs"
MANIFEST_DIR = PIPELINE_ROOT / "manifest"
MANIFEST_FILE = MANIFEST_DIR / "download_state.json"

# ── OKX REST API (public, no auth) ──────────────────────────────────────────

OKX_BASE_URL = "https://www.okx.com"

# Public endpoints
EP_INSTRUMENTS = "/api/v5/public/instruments"
EP_FUNDING_RATE_HISTORY = "/api/v5/public/funding-rate-history"
EP_ORDERBOOK = "/api/v5/market/books"

# Private API endpoints (for historical data export system)
EP_TRADE_DATA_INSTRUMENTS = "/priapi/v5/broker/public/trade-data/instruments"
EP_TRADE_DATA_DOWNLOAD = "/priapi/v5/broker/public/trade-data/download-link"
EP_TRADE_DATA_COINS = "/priapi/v5/broker/public/trade-data/coins"

# CDN bulk download endpoints
EP_CDN_LIST = "/priapi/v5/broker/public/v2/orderRecord"
CDN_STATIC_BASE = "https://static.okx.com"

# ── Export API module codes ──────────────────────────────────────────────────
# Reverse-engineered from the OKX historical data page JS bundle.

MODULE_TRADE_HISTORY = "1"
MODULE_CANDLESTICK = "2"
MODULE_FUNDING_RATES = "3"
MODULE_ORDER_BOOK_400 = "4"
MODULE_ORDER_BOOK_5000 = "5"
MODULE_ORDER_BOOK = "10"
MODULE_INTEREST_RATE = "11"

# ── Rate Limits ──────────────────────────────────────────────────────────────
# OKX rate limits are per IP for public endpoints.
# Values are in seconds between requests.

RATE_LIMIT_FUNDING_HISTORY = 0.22  # 10 req / 2 sec
RATE_LIMIT_INSTRUMENTS = 0.12      # 20 req / 2 sec
RATE_LIMIT_ORDERBOOK = 0.12        # 20 req / 2 sec
RATE_LIMIT_CDN = 0.5               # conservative for CDN
RATE_LIMIT_EXPORT = 2.0            # conservative for export API

# ── Pagination ───────────────────────────────────────────────────────────────

FUNDING_PAGE_SIZE = 100  # max per request

# ── Chunking ─────────────────────────────────────────────────────────────────

DEFAULT_CHUNK_DAYS = 14
MIN_CHUNK_DAYS = 1

# Order book depth
ORDERBOOK_DEPTH = 400

# ── Data availability dates ──────────────────────────────────────────────────
# From the OKX historical data page description.

FUNDING_RATE_START = "2022-03-01"
ORDERBOOK_START = "2023-03-01"

# ── CDN known paths ─────────────────────────────────────────────────────────

CDN_SWAPRATE_PATH = "cdn/okex/traderecords/swaprate/monthly"
