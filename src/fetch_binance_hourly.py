import ccxt
import pandas as pd
import time

# ── EXCHANGE CONSTANTS ───────────────────────────────────────────────────────
EXCHANGE_CONSTANTS = {
    "binance": {
        "funding_interval_hours": 8,
        "perp_taker_fee_bps": 5.0,
        "spot_taker_fee_bps": 10.0,
    }
}

# ── INIT ─────────────────────────────────────────────────────────────────────
def _get_exchanges():
    perp = ccxt.binance({
        "enableRateLimit": True,
        "options": {"defaultType": "swap"},
    })
    spot = ccxt.binance({
        "enableRateLimit": True,
        "options": {"defaultType": "spot"},
    })
    return perp, spot


# ── INTERNAL HELPERS ──────────────────────────────────────────────────────────
def _fetch_ohlcv(exchange, symbol, timeframe, start_date, end_date, limit=1000):
    since = exchange.parse8601(start_date)
    end_ts = exchange.parse8601(end_date)
    all_rows = []

    while True:
        batch = exchange.fetch_ohlcv(symbol, timeframe, since=since, limit=limit)
        if not batch:
            break
        batch = [r for r in batch if r[0] <= end_ts]
        all_rows.extend(batch)
        last_ts = all_rows[-1][0]
        print(f"  [{symbol}] up to {pd.to_datetime(last_ts, unit='ms', utc=True)}  |  rows: {len(all_rows)}")

        if last_ts >= end_ts or len(batch) < limit:
            break

        since = last_ts + 1
        time.sleep(exchange.rateLimit / 1000)

    df = pd.DataFrame(all_rows, columns=["timestamp", "open", "high", "low", "close", "volume"])
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    return df.set_index("timestamp")


def _fetch_funding_rates(exchange, symbol, start_date, end_date, limit=1000):
    since = exchange.parse8601(start_date)
    end_ts = exchange.parse8601(end_date)
    all_rows = []

    while True:
        batch = exchange.fetch_funding_rate_history(symbol, since=since, limit=limit)
        if not batch:
            break
        batch = [r for r in batch if r["timestamp"] <= end_ts]
        all_rows.extend(batch)
        last_ts = all_rows[-1]["timestamp"]
        print(f"  [funding] up to {pd.to_datetime(last_ts, unit='ms', utc=True)}  |  rows: {len(all_rows)}")

        if last_ts >= end_ts or len(batch) < limit:
            break

        since = last_ts + 1
        time.sleep(exchange.rateLimit / 1000)

    df = pd.DataFrame([{
        "timestamp": pd.to_datetime(r["timestamp"], unit="ms", utc=True),
        "funding_rate_raw": float(r["fundingRate"]),
    } for r in all_rows]).set_index("timestamp")
    return df


def _fetch_open_interest(exchange, symbol, start_date, end_date, limit=1000):
    # Ensure symbol is in Binance's required format (e.g., 'BTCUSDT')
    # Ensure start_date is not before OI availability
    OI_MIN_START_DATE = "2023-12-01T00:00:00Z"
    start_date = max(start_date, OI_MIN_START_DATE)
    since = exchange.parse8601(start_date)
    end_ts = exchange.parse8601(end_date)
    print(f"[DEBUG] _fetch_open_interest: symbol={symbol}, since={since} ({pd.to_datetime(since, unit='ms', utc=True)}), end_ts={end_ts} ({pd.to_datetime(end_ts, unit='ms', utc=True)})")
    all_rows = []

    while True:
        print(f"[DEBUG] fetch_open_interest_history call: symbol={symbol}, since={since}, limit={limit}")
        try:
            batch = exchange.fetch_open_interest_history(symbol, "1h", since=since, limit=limit)
        except Exception as e:
            print(f"[ERROR] fetch_open_interest_history failed: {e}")
            break
        if not batch:
            break
        batch = [r for r in batch if r["timestamp"] <= end_ts]
        all_rows.extend(batch)
        last_ts = all_rows[-1]["timestamp"]
        print(f"  [OI] up to {pd.to_datetime(last_ts, unit='ms', utc=True)}  |  rows: {len(all_rows)}")

        if last_ts >= end_ts or len(batch) < limit:
            break

        since = last_ts + 1
        time.sleep(exchange.rateLimit / 1000)

    if not all_rows:
        print("[WARN] No open interest data fetched.")
        return pd.DataFrame(columns=["open_interest_usd"])

    df = pd.DataFrame([{
        "timestamp": pd.to_datetime(r["timestamp"], unit="ms", utc=True),
        "open_interest_usd": float(r["openInterestValue"]),
    } for r in all_rows]).set_index("timestamp")
    return df
# ── STANDALONE OI FETCHER ───────────────────────────────────────────────────
def fetch_binance_open_interest(
    asset: str = "BTC",
    start_date: str = "2023-12-01T00:00:00Z",
    end_date: str = "2024-01-01T00:00:00Z",
    limit: int = 1000
) -> pd.DataFrame:
    """
    Fetches open interest for a single asset from Binance, returns a DataFrame.
    """
    perp_ex, _ = _get_exchanges()
    oi_symbol = f"{asset}USDT"
    return _fetch_open_interest(perp_ex, oi_symbol, start_date, end_date, limit=limit)


# ── PUBLIC FUNCTION ───────────────────────────────────────────────────────────
def fetch_binance(
    asset: str          = "BTC",
    start_date: str     = "2022-01-01T00:00:00Z",
    end_date: str       = "2024-01-01T00:00:00Z",
    timeframe: str      = "1h",
    spread_bps: float   = 2.0,   # symmetric half-spread applied to close price
) -> pd.DataFrame:
    """
    Fetches and assembles a clean hourly dataframe for one asset on Binance.

    Columns returned:
        mark_price_binance, best_bid_binance, best_ask_binance,
        spot_best_bid_binance, spot_best_ask_binance,
        funding_rate_raw_binance, open_interest_usd_binance

    Parameters
    ----------
    asset       : ticker string, e.g. 'BTC', 'ETH', 'SOL'
    start_date  : ISO8601 string
    end_date    : ISO8601 string
    timeframe   : OHLCV candle size, default '1h'
    spread_bps  : half-spread in bps applied symmetrically around close price.
                  bid = close * (1 - spread_bps/10000)
                  ask = close * (1 + spread_bps/10000)
                  Recommended: 2 bps for BTC/ETH, 5–10 bps for altcoins.
    """
    perp_symbol = f"{asset}/USDT:USDT"
    spot_symbol = f"{asset}/USDT"
    half_spread = spread_bps / 10_000

    perp_ex, spot_ex = _get_exchanges()

    print(f"\n{'='*55}")
    print(f"Fetching Binance data — {asset} | {start_date} → {end_date}")
    print(f"Spread assumption: ±{spread_bps} bps around close")
    print(f"{'='*55}")

    print("\n[1/4] Perp OHLCV...")
    perp_ohlcv = _fetch_ohlcv(perp_ex, perp_symbol, timeframe, start_date, end_date)

    print("\n[2/4] Spot OHLCV...")
    spot_ohlcv = _fetch_ohlcv(spot_ex, spot_symbol, timeframe, start_date, end_date)

    print("\n[3/4] Funding rates...")
    funding    = _fetch_funding_rates(perp_ex, perp_symbol, start_date, end_date)

    # ── ASSEMBLE ──────────────────────────────────────────────────────────────
    df = perp_ohlcv[["close"]].rename(columns={"close": "mark_price_binance"})

    # Derive bid/ask from close using configurable spread
    df["best_bid_binance"] = df["mark_price_binance"] * (1 - half_spread)
    df["best_ask_binance"] = df["mark_price_binance"] * (1 + half_spread)

    df["spot_best_bid_binance"] = spot_ohlcv["close"] * (1 - half_spread)
    df["spot_best_ask_binance"] = spot_ohlcv["close"] * (1 + half_spread)

    # Forward-fill funding rate across the 8 hourly rows between settlements
    df = df.join(funding, how="left")
    df["funding_rate_raw_binance"] = df["funding_rate_raw"].ffill()
    df = df.drop(columns=["funding_rate_raw"])

    df.index.name = "timestamp"
    df = df.sort_index()

    print(f"\n✅ Done — {len(df)} rows assembled.")
    return df
