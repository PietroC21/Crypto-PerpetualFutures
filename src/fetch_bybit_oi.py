"""
fetch_bybit_oi.py
-----------------
Pulls historical open interest (1h) for all 15 universe symbols from Bybit.

Why Bybit and not Binance:
  Binance's OI history endpoint caps at ~30 days regardless of timeframe.
  Bybit's public API has OI going back to ~Dec 2020 for BTC/ETH and to each
  symbol's listing date for altcoins — no API key required.

Output: data/raw/binance/open_interest/<SYMBOL>.parquet
  Columns: open_interest (contracts, base currency)
  Index:   datetime (UTC, 1h frequency)

The OI filter in the strategy uses a 90-day rolling ratio, so storing in
contracts is correct — the ratio is price-independent.

Usage:
    python src/fetch_bybit_oi.py              # full pull, all symbols
    python src/fetch_bybit_oi.py --symbols BTC ETH
"""

import argparse
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
from tqdm import tqdm

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "data" / "raw" / "binance" / "open_interest"

# Bybit linear perpetual symbols (all USDT-margined)
# Matches our universe — POL is the current name after MATIC rebrand
UNIVERSE = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
    "DOGEUSDT", "AVAXUSDT", "POLUSDT", "LINKUSDT", "ADAUSDT",
    "DOTUSDT", "ATOMUSDT", "LTCUSDT", "UNIUSDT", "ARBUSDT",
]

DEFAULT_START = "2020-12-01"   # Bybit USDT perps launched ~late 2020; no data before this
BYBIT_OI_URL  = "https://api.bybit.com/v5/market/open-interest"
LIMIT         = 200            # Bybit max per request
SLEEP_S       = 0.15           # ~6 req/s — well under Bybit public rate limits


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def dt_to_ms(dt_str: str) -> int:
    dt = datetime.strptime(dt_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def save(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=True)


# ---------------------------------------------------------------------------
# Fetch
# ---------------------------------------------------------------------------

def fetch_oi(symbol: str, start_ms: int, end_ms: int) -> pd.DataFrame:
    """
    Pull OI history from Bybit for one symbol between start_ms and end_ms.
    Bybit returns newest-first so we paginate backwards using endTime.
    """
    rows = []
    cursor_end = end_ms

    while cursor_end > start_ms:
        params = {
            "category":    "linear",
            "symbol":      symbol,
            "intervalTime": "1h",
            "limit":       LIMIT,
            "endTime":     cursor_end,
        }
        try:
            resp = requests.get(BYBIT_OI_URL, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as e:
            print(f"  [{symbol}] request error: {e}")
            break

        if data.get("retCode") != 0:
            print(f"  [{symbol}] API error: {data.get('retMsg')}")
            break

        batch = data["result"]["list"]  # newest first
        if not batch:
            break

        for item in batch:
            ts = int(item["timestamp"])
            if ts < start_ms:
                continue
            rows.append({
                "datetime":     pd.to_datetime(ts, unit="ms", utc=True),
                "open_interest": float(item["openInterest"]),
            })

        oldest_ts = int(batch[-1]["timestamp"])
        if oldest_ts <= start_ms:
            break

        cursor_end = oldest_ts - 1
        time.sleep(SLEEP_S)

    if not rows:
        return pd.DataFrame()

    df = (
        pd.DataFrame(rows)
        .set_index("datetime")
        .sort_index()
    )
    df = df[~df.index.duplicated(keep="first")]
    return df


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def run(symbols: list[str], start: str, end: str | None) -> None:
    start_ms = dt_to_ms(start)
    end_ms   = dt_to_ms(end) if end else now_ms()

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Fetching OI from Bybit | {start} → {end or 'now'} | {len(symbols)} symbols\n")

    for symbol in tqdm(symbols, desc="OI"):
        out = OUT_DIR / f"{symbol}.parquet"

        if out.exists():
            existing = pd.read_parquet(out)
            last_ms  = int(existing.index.max().timestamp() * 1000) + 1
            print(f"  [{symbol}] resuming from {existing.index.max().date()}")
            df_new = fetch_oi(symbol, last_ms, end_ms)
            if not df_new.empty:
                df = pd.concat([existing, df_new])
                df = df[~df.index.duplicated(keep="first")].sort_index()
            else:
                df = existing
        else:
            df = fetch_oi(symbol, start_ms, end_ms)

        if not df.empty:
            save(df, out)
            span = f"{df.index.min().date()} → {df.index.max().date()}"
            print(f"  [{symbol}] {len(df):>6} rows  |  {span}")
        else:
            print(f"  [{symbol}] no data returned — symbol may not have existed yet")

        print()

    print("Done. OI files saved to data/raw/binance/open_interest/")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch Bybit open interest history.")
    parser.add_argument("--symbols", nargs="+", default=None,
                        help="Subset of symbols, e.g. BTC ETH. Defaults to full universe.")
    parser.add_argument("--start", default=DEFAULT_START,
                        help="Start date YYYY-MM-DD (default: 2020-12-01)")
    parser.add_argument("--end", default=None,
                        help="End date YYYY-MM-DD (default: today)")
    args = parser.parse_args()

    if args.symbols:
        lookup = {s.replace("USDT", ""): s for s in UNIVERSE}
        selected = [lookup[s.upper()] for s in args.symbols if s.upper() in lookup]
        if not selected:
            raise SystemExit(f"No valid symbols found. Available: {list(lookup.keys())}")
        symbols = selected
    else:
        symbols = UNIVERSE

    run(symbols, args.start, args.end)
