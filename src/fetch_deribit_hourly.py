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
    all_rows = []
    while since < end_ts:
        params = {
            "instrument_name": instrument_name,
            "start_timestamp": since,
            "end_timestamp": end_ts,
            "resolution": 3600,  # 1h in seconds
            "limit": limit
        }
        resp = requests.get(DERIBIT_API_URL + "get_tradingview_chart_data", params=params)
        data = resp.json()
        if not data.get("result") or not data["result"].get("ticks"):
            break
        ticks = data["result"]["ticks"]
        for t in ticks:
            if t[0] > end_ts:
                continue
            all_rows.append(t)
        if len(ticks) < limit:
            break
        since = ticks[-1][0] + 1
        time.sleep(0.2)
    if not all_rows:
        return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])
    df = pd.DataFrame(all_rows, columns=["timestamp", "open", "high", "low", "close", "volume"])
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    df = df.set_index("timestamp")
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
