"""
fetch_binance.py
----------------
Pulls all Binance data needed for the funding-rate carry strategy:
  1. Funding rates        (8h, per perp symbol)
  2. Perp OHLCV           (8h candles, mark price)
  3. Spot OHLCV           (8h candles, spot index)
  4. Open interest        (8h, per perp symbol)

Output: data/raw/binance/{funding_rates,ohlcv_perp,ohlcv_spot,open_interest}/<SYMBOL>.parquet

Usage:
    python src/fetch_binance.py                  # full pull Jan 2020 → today
    python src/fetch_binance.py --start 2023-01-01 --end 2024-01-01
    python src/fetch_binance.py --symbols BTC ETH  # subset
"""

import argparse
import time
from datetime import datetime, timezone
from pathlib import Path

import ccxt
import pandas as pd
from tqdm import tqdm

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw" / "binance"

# Top-15 universe from the proposal (perp symbols on Binance)
UNIVERSE = [
    "BTC/USDT:USDT",
    "ETH/USDT:USDT",
    "SOL/USDT:USDT",
    "BNB/USDT:USDT",
    "XRP/USDT:USDT",
    "DOGE/USDT:USDT",
    "AVAX/USDT:USDT",
    "POL/USDT:USDT",   # MATIC rebranded → POL on Binance in Sept 2023
    "LINK/USDT:USDT",
    "ADA/USDT:USDT",
    "DOT/USDT:USDT",
    "ATOM/USDT:USDT",
    "LTC/USDT:USDT",
    "UNI/USDT:USDT",
    "ARB/USDT:USDT",
]

# Spot symbols map: perp → spot (for spot OHLCV pulls)
SPOT_SYMBOLS = {s: s.split(":")[0] for s in UNIVERSE}  # e.g. BTC/USDT:USDT → BTC/USDT

DEFAULT_START = "2020-01-01"
TIMEFRAME = "8h"
LIMIT = 500          # max candles per request (Binance allows up to 1500 for OHLCV)
RATE_LIMIT_MS = 200  # ms between requests — conservative to stay under Binance limits


# ---------------------------------------------------------------------------
# Exchange setup
# ---------------------------------------------------------------------------

def build_exchange() -> ccxt.binanceusdm:
    """Return an authenticated-free Binance USD-M futures exchange instance."""
    ex = ccxt.binanceusdm({"enableRateLimit": True})
    try:
        ex.load_markets()
    except ccxt.ExchangeNotAvailable as e:
        if "451" in str(e) or "restricted location" in str(e):
            raise SystemExit(
                "\n[ERROR] Binance returned 451 — geo-restriction detected.\n"
                "Connect your VPN to a non-US server and retry.\n"
            ) from None
        raise
    return ex


def build_spot_exchange() -> ccxt.binance:
    ex = ccxt.binance({"enableRateLimit": True})
    try:
        ex.load_markets()
    except ccxt.ExchangeNotAvailable as e:
        if "451" in str(e) or "restricted location" in str(e):
            raise SystemExit(
                "\n[ERROR] Binance returned 451 — geo-restriction detected.\n"
                "Connect your VPN to a non-US server and retry.\n"
            ) from None
        raise
    return ex


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def ts_to_ms(dt_str: str) -> int:
    """'2020-01-01' → Unix ms UTC."""
    dt = datetime.strptime(dt_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def save(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=True)


def short_name(symbol: str) -> str:
    """'BTC/USDT:USDT' → 'BTCUSDT'"""
    return symbol.split("/")[0] + "USDT"


# ---------------------------------------------------------------------------
# 1. Funding rates
# ---------------------------------------------------------------------------

def fetch_funding_rates(ex: ccxt.binanceusdm, symbol: str, since_ms: int, until_ms: int) -> pd.DataFrame:
    """
    Paginate fetch_funding_rate_history() for a single symbol.
    Returns DataFrame indexed by datetime UTC.
    Retries up to 3 times on transient 403/network errors (e.g. VPN hiccup).
    """
    rows = []
    cursor = since_ms

    while cursor < until_ms:
        for attempt in range(3):
            try:
                batch = ex.fetch_funding_rate_history(symbol, since=cursor, limit=LIMIT)
                break
            except ccxt.BaseError as e:
                wait = 2 ** (attempt + 1)  # 2s, 4s, 8s
                print(f"  [funding] {symbol} error (attempt {attempt+1}/3): {e} — retrying in {wait}s")
                time.sleep(wait)
        else:
            print(f"  [funding] {symbol} gave up after 3 attempts at cursor {cursor}")
            break

        if not batch:
            break

        for r in batch:
            rows.append({
                "datetime": pd.to_datetime(r["timestamp"], unit="ms", utc=True),
                "funding_rate": r["fundingRate"],
            })

        last_ts = batch[-1]["timestamp"]
        if last_ts <= cursor:
            break  # no progress — stop
        cursor = last_ts + 1
        time.sleep(RATE_LIMIT_MS / 1000)

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows).set_index("datetime").sort_index()
    df = df[~df.index.duplicated(keep="first")]
    # Trim to requested window
    df = df[df.index <= pd.Timestamp(until_ms, unit="ms", tz="UTC")]
    return df


# ---------------------------------------------------------------------------
# 2. Perp OHLCV (mark price candles)
# ---------------------------------------------------------------------------

def fetch_ohlcv_perp(ex: ccxt.binanceusdm, symbol: str, since_ms: int, until_ms: int) -> pd.DataFrame:
    """
    Fetch 8h mark-price OHLCV for a perp symbol.
    Binance USD-M: fetch_ohlcv with price_type='mark' where supported,
    else standard ohlcv (which uses mark price on futures by default).
    """
    rows = []
    cursor = since_ms

    while cursor < until_ms:
        try:
            batch = ex.fetch_ohlcv(symbol, timeframe=TIMEFRAME, since=cursor, limit=LIMIT)
        except ccxt.BaseError as e:
            print(f"  [ohlcv_perp] {symbol} error: {e}")
            time.sleep(2)
            break

        if not batch:
            break

        for c in batch:
            rows.append({
                "datetime": pd.to_datetime(c[0], unit="ms", utc=True),
                "open": c[1], "high": c[2], "low": c[3],
                "close": c[4], "volume": c[5],
            })

        last_ts = batch[-1][0]
        if last_ts <= cursor:
            break
        cursor = last_ts + 1
        time.sleep(RATE_LIMIT_MS / 1000)

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows).set_index("datetime").sort_index()
    df = df[~df.index.duplicated(keep="first")]
    df = df[df.index <= pd.Timestamp(until_ms, unit="ms", tz="UTC")]
    return df


# ---------------------------------------------------------------------------
# 3. Spot OHLCV
# ---------------------------------------------------------------------------

def fetch_ohlcv_spot(ex: ccxt.binance, symbol: str, since_ms: int, until_ms: int) -> pd.DataFrame:
    """Fetch 8h spot OHLCV (spot exchange)."""
    rows = []
    cursor = since_ms

    while cursor < until_ms:
        try:
            batch = ex.fetch_ohlcv(symbol, timeframe=TIMEFRAME, since=cursor, limit=LIMIT)
        except ccxt.BaseError as e:
            print(f"  [ohlcv_spot] {symbol} error: {e}")
            time.sleep(2)
            break

        if not batch:
            break

        for c in batch:
            rows.append({
                "datetime": pd.to_datetime(c[0], unit="ms", utc=True),
                "open": c[1], "high": c[2], "low": c[3],
                "close": c[4], "volume": c[5],
            })

        last_ts = batch[-1][0]
        if last_ts <= cursor:
            break
        cursor = last_ts + 1
        time.sleep(RATE_LIMIT_MS / 1000)

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows).set_index("datetime").sort_index()
    df = df[~df.index.duplicated(keep="first")]
    df = df[df.index <= pd.Timestamp(until_ms, unit="ms", tz="UTC")]
    return df


# ---------------------------------------------------------------------------
# 4. Open interest
# ---------------------------------------------------------------------------

OI_TIMEFRAME = "1h"  # Binance OI endpoint valid periods: 5m,15m,30m,1h,2h,4h,6h,12h,1d — no 8h


def fetch_open_interest(ex: ccxt.binanceusdm, symbol: str, since_ms: int, until_ms: int) -> pd.DataFrame:
    """
    Fetch historical open interest at 1h intervals (finest Binance supports near 8h).
    Stored as-is; build_panel.py resamples to align with 8h funding timestamps.
    """
    rows = []
    cursor = since_ms

    while cursor < until_ms:
        try:
            batch = ex.fetch_open_interest_history(symbol, timeframe=OI_TIMEFRAME, since=cursor, limit=LIMIT)
        except ccxt.BaseError as e:
            print(f"  [OI] {symbol} error: {e}")
            time.sleep(2)
            break

        if not batch:
            break

        for r in batch:
            rows.append({
                "datetime": pd.to_datetime(r["timestamp"], unit="ms", utc=True),
                "open_interest_value": r.get("openInterestValue"),  # in USD
                "open_interest": r.get("openInterest"),             # in contracts/coins
            })

        last_ts = batch[-1]["timestamp"]
        if last_ts <= cursor:
            break
        cursor = last_ts + 1
        time.sleep(RATE_LIMIT_MS / 1000)

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows).set_index("datetime").sort_index()
    df = df[~df.index.duplicated(keep="first")]
    df = df[df.index <= pd.Timestamp(until_ms, unit="ms", tz="UTC")]
    return df


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------

def run(symbols: list[str], start: str, end: str) -> None:
    since_ms = ts_to_ms(start)
    until_ms = ts_to_ms(end) if end else now_ms()

    print(f"Initialising exchanges...")
    ex_perp = build_exchange()
    ex_spot = build_spot_exchange()

    print(f"Fetching {len(symbols)} symbols | {start} → {end or 'now'}")
    print(f"Timeframe: {TIMEFRAME}\n")

    for symbol in tqdm(symbols, desc="Symbols"):
        name = short_name(symbol)
        spot_sym = SPOT_SYMBOLS[symbol]

        # --- Funding rates ---
        out = RAW / "funding_rates" / f"{name}.parquet"
        if out.exists():
            existing = pd.read_parquet(out)
            last_ts = int(existing.index.max().timestamp() * 1000) + 1
            print(f"  [{name}] funding: resuming from {existing.index.max().date()}")
            df = fetch_funding_rates(ex_perp, symbol, last_ts, until_ms)
            if not df.empty:
                df = pd.concat([existing, df])
                df = df[~df.index.duplicated(keep="first")].sort_index()
        else:
            df = fetch_funding_rates(ex_perp, symbol, since_ms, until_ms)

        if not df.empty:
            save(df, out)
            print(f"  [{name}] funding: {len(df)} rows → {out.name}")
        else:
            print(f"  [{name}] funding: no data returned")

        # --- Perp OHLCV ---
        out = RAW / "ohlcv_perp" / f"{name}.parquet"
        if out.exists():
            existing = pd.read_parquet(out)
            last_ts = int(existing.index.max().timestamp() * 1000) + 1
            df = fetch_ohlcv_perp(ex_perp, symbol, last_ts, until_ms)
            if not df.empty:
                df = pd.concat([existing, df])
                df = df[~df.index.duplicated(keep="first")].sort_index()
        else:
            df = fetch_ohlcv_perp(ex_perp, symbol, since_ms, until_ms)

        if not df.empty:
            save(df, out)
            print(f"  [{name}] ohlcv_perp: {len(df)} rows")

        # --- Spot OHLCV ---
        out = RAW / "ohlcv_spot" / f"{name}.parquet"
        if out.exists():
            existing = pd.read_parquet(out)
            last_ts = int(existing.index.max().timestamp() * 1000) + 1
            df = fetch_ohlcv_spot(ex_spot, spot_sym, last_ts, until_ms)
            if not df.empty:
                df = pd.concat([existing, df])
                df = df[~df.index.duplicated(keep="first")].sort_index()
        else:
            df = fetch_ohlcv_spot(ex_spot, spot_sym, since_ms, until_ms)

        if not df.empty:
            save(df, out)
            print(f"  [{name}] ohlcv_spot: {len(df)} rows")

        # --- Open interest ---
        out = RAW / "open_interest" / f"{name}.parquet"
        if out.exists():
            existing = pd.read_parquet(out)
            last_ts = int(existing.index.max().timestamp() * 1000) + 1
            df = fetch_open_interest(ex_perp, symbol, last_ts, until_ms)
            if not df.empty:
                df = pd.concat([existing, df])
                df = df[~df.index.duplicated(keep="first")].sort_index()
        else:
            df = fetch_open_interest(ex_perp, symbol, since_ms, until_ms)

        if not df.empty:
            save(df, out)
            print(f"  [{name}] open_interest: {len(df)} rows")

        print()

    print("Done. All files saved to data/raw/binance/")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch Binance perpetual futures data.")
    parser.add_argument("--symbols", nargs="+", default=None,
                        help="Subset of symbols to fetch, e.g. BTC ETH SOL. Defaults to full universe.")
    parser.add_argument("--start", default=DEFAULT_START,
                        help="Start date YYYY-MM-DD (default: 2020-01-01)")
    parser.add_argument("--end", default=None,
                        help="End date YYYY-MM-DD (default: today)")
    args = parser.parse_args()

    if args.symbols:
        # Accept short names like BTC or full like BTC/USDT:USDT
        lookup = {short_name(s): s for s in UNIVERSE}
        selected = []
        for sym in args.symbols:
            key = sym.upper().replace("USDT", "") + "USDT"
            if key in lookup:
                selected.append(lookup[key])
            elif sym in UNIVERSE:
                selected.append(sym)
            else:
                print(f"Warning: '{sym}' not in universe, skipping.")
        symbols = selected or UNIVERSE
    else:
        symbols = UNIVERSE

    run(symbols, args.start, args.end or datetime.now(timezone.utc).strftime("%Y-%m-%d"))
