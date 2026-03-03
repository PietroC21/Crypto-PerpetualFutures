import requests
import pandas as pd
import time
import numpy as np

# ── EXCHANGE CONSTANTS ───────────────────────────────────────────────────────
EXCHANGE_CONSTANTS = {
    "deribit": {
        "funding_interval_hours": 8,  # Deribit funding is every 8h for perps
        "perp_taker_fee_bps": 5.0,   # Example, check actual fee schedule
        "spot_taker_fee_bps": None,  # Deribit does not have spot
    }
}

# ── API ENDPOINTS ───────────────────────────────────────────────────────────
DERIBIT_API_URL = "https://www.deribit.com/api/v2/public/"

# ── HELPERS ─────────────────────────────────────────────────────────────────
def _fetch_ohlcv_deribit(instrument_name, start_date, end_date, timeframe="1h", limit=1000, spread_bps=2.0):
    """
    Fetches OHLCV data from Deribit for a given instrument and time range.
    If best bid/ask is not available, derives from close price using spread_bps.
    """
    # Deribit uses unix timestamps in ms
    since = int(pd.to_datetime(start_date).timestamp() * 1000)
    end_ts = int(pd.to_datetime(end_date).timestamp() * 1000)
    resolution = 60 if timeframe == "1h" else timeframe
    all_dfs = []
    fetch_end = end_ts
    max_loops = 10000  # safety to avoid infinite loop
    loops = 0
    print(f"[DEBUG] Requested range: {pd.to_datetime(since, unit='ms', utc=True)} to {pd.to_datetime(fetch_end, unit='ms', utc=True)}")
    while fetch_end > since and loops < max_loops:
        # Always fetch up to limit candles ending at fetch_end
        params = {
            "instrument_name": instrument_name,
            "start_timestamp": since,  # Deribit ignores this if range is too wide, but required
            "end_timestamp": fetch_end,
            "resolution": resolution,
            "limit": limit
        }
        print(f"[DEBUG] Loop {loops+1}: fetch_end={pd.to_datetime(fetch_end, unit='ms', utc=True)}")
        print(f"[DEBUG] Request params: {params}")
        resp = requests.get(DERIBIT_API_URL + "get_tradingview_chart_data", params=params)
        if resp.status_code != 200:
            print(f"Deribit API error: {resp.status_code}")
            break
        data = resp.json()
        result = data.get("result", {})
        required_fields = ["ticks", "open", "high", "low", "close", "volume"]
        if not all(field in result for field in required_fields):
            print("Missing fields in Deribit response, stopping batch fetch.")
            break
        if not result["ticks"]:
            print("No more data returned from Deribit, stopping batch fetch.")
            break
        print(f"[DEBUG] Received {len(result['ticks'])} rows, first: {pd.to_datetime(result['ticks'][0], unit='ms', utc=True)}, last: {pd.to_datetime(result['ticks'][-1], unit='ms', utc=True)}")
        df = pd.DataFrame({
            "timestamp": result["ticks"],
            "open": result["open"],
            "high": result["high"],
            "low": result["low"],
            "close": result["close"],
            "volume": result["volume"]
        })
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
        df = df.set_index("timestamp")
        all_dfs.append(df)
        # If we received less than limit, we've reached the oldest available data
        if len(df) < limit:
            print(f"[DEBUG] Received less than limit ({limit}), breaking loop.")
            break
        # Otherwise, move fetch_end to the oldest timestamp in this batch minus 1ms
        oldest_ts = int(df.index[0].timestamp() * 1000)
        if oldest_ts >= fetch_end:
            print(f"[DEBUG] oldest_ts >= fetch_end ({oldest_ts} >= {fetch_end}), breaking loop to avoid infinite loop.")
            break
        fetch_end = oldest_ts - 1
        loops += 1
        time.sleep(0.1)  # be gentle to API
    if not all_dfs:
        print("[DEBUG] No dataframes collected, returning empty DataFrame.")
        return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])
    # Concatenate in reverse (oldest first)
    df = pd.concat(all_dfs[::-1])
    df = df[~df.index.duplicated(keep="first")]
    # Filter to requested range (API may overshoot)
    df = df[(df.index >= pd.to_datetime(start_date)) & (df.index <= pd.to_datetime(end_date))]
    print(f"[DEBUG] Final dataframe shape: {df.shape}, first index: {df.index[0] if not df.empty else None}, last index: {df.index[-1] if not df.empty else None}")
    # Derive bid/ask from close if no historical L2 available
    half_spread = spread_bps / 10_000
    df["best_bid_deribit"] = df["close"] * (1 - half_spread)
    df["best_ask_deribit"] = df["close"] * (1 + half_spread)
    df["mark_price_deribit"] = df["close"]
    return df

def _fetch_funding_rates_deribit(instrument_name, start_date, end_date):
    # Deribit does not provide historical funding via public API, so we use mark price and compute from index/funding rate endpoint if needed
    # For now, return NaN
    idx = pd.date_range(start=start_date, end=end_date, freq="1h", tz="UTC")
    return pd.DataFrame(index=idx, data={"funding_rate_raw_deribit": np.nan})

def _fetch_open_interest_deribit(instrument_name, start_date, end_date, limit=1000):
    # Deribit does not provide historical OI via public API, only current OI. We'll fill with NaN for now.
    idx = pd.date_range(start=start_date, end=end_date, freq="1h", tz="UTC")
    return pd.DataFrame(index=idx, data={"open_interest_usd_deribit": np.nan})

# ── PUBLIC FUNCTION ─────────────────────────────────────────────────────────
def fetch_deribit(
    asset: str = "BTC",
    start_date: str = "2020-01-01T00:00:00Z",
    end_date: str = "2024-01-01T00:00:00Z",
    timeframe: str = "1h",
    spread_bps: float = 2.0,
) -> pd.DataFrame:
    """
    Fetches and assembles a clean hourly dataframe for one asset on Deribit.
    Columns returned:
        mark_price_deribit, best_bid_deribit, best_ask_deribit,
        funding_rate_raw_deribit, open_interest_usd_deribit
    Parameters
    ----------
    asset       : ticker string, e.g. 'BTC', 'ETH'
    start_date  : ISO8601 string
    end_date    : ISO8601 string
    timeframe   : OHLCV candle size, default '1h'
    spread_bps  : half-spread in bps applied symmetrically around close price.
    """
    perp_symbol = f"{asset}-PERPETUAL"
    print(f"\n{'='*55}")
    print(f"Fetching Deribit data — {asset} | {start_date} → {end_date}")
    print(f"Spread assumption: ±{spread_bps} bps around close")
    print(f"{'='*55}")
    print("\n[1/3] Perp OHLCV...")
    perp_ohlcv = _fetch_ohlcv_deribit(perp_symbol, start_date, end_date, timeframe, spread_bps=spread_bps)
    if perp_ohlcv.empty:
        print("No OHLCV data returned from Deribit for the given parameters.")
        # Return an empty DataFrame with the expected columns for downstream compatibility
        idx = pd.date_range(start=start_date, end=end_date, freq="1h", tz="UTC")
        df = pd.DataFrame(index=idx, columns=[
            "mark_price_deribit", "best_bid_deribit", "best_ask_deribit",
            "funding_rate_raw_deribit", "open_interest_usd_deribit"
        ])
        df.index.name = "timestamp"
        return df
    print("\n[2/3] Funding rates...")
    funding = _fetch_funding_rates_deribit(perp_symbol, start_date, end_date)
    print("\n[3/3] Open interest...")
    oi = _fetch_open_interest_deribit(perp_symbol, start_date, end_date)
    # ── ASSEMBLE ──────────────────────────────────────────────────────────────
    df = perp_ohlcv[["mark_price_deribit", "best_bid_deribit", "best_ask_deribit"]].copy()
    df = df.join(funding, how="left")
    df = df.join(oi, how="left")
    df.index.name = "timestamp"
    df = df.sort_index()
    print(f"\n✅ Done — {len(df)} rows assembled.")
    return df

import requests
import pandas as pd
import time
from datetime import datetime, timezone, timedelta

DERIBIT_BASE = "https://www.deribit.com/api/v2/public"

DERIBIT_SYMBOLS = {
    "BTC": "BTC-PERPETUAL",
    "ETH": "ETH-PERPETUAL",
    "SOL": "SOL-PERPETUAL",
}

def fetch_deribit_funding_df(
    asset: str      = "BTC",
    start_date: str = "2020-01-01",
    end_date: str   = "2024-01-01",
    chunk_days: int = 30,
    sleep_s: float  = 0.3,
) -> pd.DataFrame:
    """
    Fetches Deribit hourly funding rate history and returns a DataFrame
    ready to merge with your main df on the timestamp index.

    Returns
    -------
    pd.DataFrame with:
        index   : timestamp (UTC, hourly)
        columns : funding_rate_raw_deribit (float)

    Merge with your main df
    -----------------------
        df = df.join(fetch_deribit_funding_df("BTC", ...), how="left")
    """
    instrument = DERIBIT_SYMBOLS.get(asset.upper())
    if not instrument:
        raise ValueError(f"Unknown asset '{asset}'. Available: {list(DERIBIT_SYMBOLS.keys())}")

    start_dt = datetime.fromisoformat(start_date).replace(tzinfo=timezone.utc)
    end_dt   = datetime.fromisoformat(end_date).replace(tzinfo=timezone.utc)
    chunk    = timedelta(days=chunk_days)

    # ── Paginate through chunks ───────────────────────────────────────────────
    all_rows = []
    cursor   = start_dt

    print(f"\nFetching Deribit funding — {instrument} | {start_date} → {end_date}")

    while cursor < end_dt:
        window_end = min(cursor + chunk, end_dt)

        params = {
            "instrument_name": instrument,
            "start_timestamp": int(cursor.timestamp() * 1000),
            "end_timestamp":   int(window_end.timestamp() * 1000),
        }
        resp = requests.get(
            f"{DERIBIT_BASE}/get_funding_rate_history",
            params=params,
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()

        if "error" in data:
            raise RuntimeError(f"Deribit API error: {data['error']}")

        batch = data.get("result", [])
        all_rows.extend(batch)
        print(f"  {cursor.date()} → {window_end.date()} | rows: {len(batch)} | total: {len(all_rows)}")

        cursor = window_end
        time.sleep(sleep_s)

    if not all_rows:
        raise RuntimeError("No data returned — check instrument name and date range.")

    # ── Build clean hourly DataFrame ──────────────────────────────────────────
    raw = pd.DataFrame(all_rows)
    raw["timestamp"] = pd.to_datetime(raw["timestamp"], unit="ms", utc=True)
    raw = raw.set_index("timestamp").sort_index()

    # Enforce a clean hourly grid — gaps become explicit NaN
    full_index = pd.date_range(
        start = raw.index.min(),
        end   = raw.index.max(),
        freq  = "1h",
        tz    = "UTC",
    )

    df_out = (
        raw["interest_1h"]
        .reindex(full_index)
        .rename("funding_rate_raw_deribit")
        .rename_axis("timestamp")
        .to_frame()
    )

    n_gaps = df_out["funding_rate_raw_deribit"].isna().sum()
    if n_gaps > 0:
        print(f"⚠️  {n_gaps} NaN rows — genuine API gaps.")

    print(f"\n✅ Done — {len(df_out)} hourly rows.")
    return df_out
