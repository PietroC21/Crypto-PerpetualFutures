import requests
import pandas as pd
import time
import numpy as np
from datetime import datetime, timezone, timedelta

# ── EXCHANGE CONSTANTS ───────────────────────────────────────────────────────
EXCHANGE_CONSTANTS = {
    "deribit": {
        "funding_interval_hours": 8,
        "perp_taker_fee_bps": 3.0,   # Deribit taker: 0.03% for BTC/ETH inverse, 0.05% for linear
        "spot_taker_fee_bps": None,   # no spot market on Deribit
    }
}

# Correct instrument names — BTC/ETH are inverse, rest are USDC-settled linear
DERIBIT_SYMBOLS = {
    "BTC":  "BTC-PERPETUAL",           # inverse — margined in BTC
    "ETH":  "ETH-PERPETUAL",           # inverse — margined in ETH
    "SOL":  "SOL_USDC-PERPETUAL",      # linear  — margined in USDC
    "XRP":  "XRP_USDC-PERPETUAL",      # linear  — margined in USDC
    "BNB":  "BNB_USDC-PERPETUAL",      # linear  — only available since June 2025
    "AVAX": "AVAX_USDC-PERPETUAL",     # linear  — margined in USDC
    "DOGE": "DOGE_USDC-PERPETUAL",     # linear  — margined in USDC
}

# Inverse contracts: price quoted in USD but margined/settled in the base coin
# Funding rate must be multiplied by mark_price to get USD-equivalent rate
INVERSE_ASSETS = {"BTC", "ETH"}

# Per-asset spread assumptions in bps (half-spread applied symmetrically)
SPREAD_BPS: dict[str, float] = {
    "BTC":  1.5,
    "ETH":  2.0,
    "SOL":  4.0,
    "XRP":  4.0,
    "BNB":  4.0,
    "AVAX": 6.0,
    "DOGE": 7.0,
}

DERIBIT_API_URL = "https://www.deribit.com/api/v2/public"


# ── HELPERS ──────────────────────────────────────────────────────────────────
def _to_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def _fetch_ohlcv_deribit(
    instrument_name: str,
    asset: str,
    start_date: str,
    end_date: str,
    timeframe: str = "1h",
    limit: int = 1000,
) -> pd.DataFrame:
    """
    Fetches OHLCV from Deribit for a given instrument.

    For inverse contracts (BTC, ETH):
        - Raw close is in USD (price of 1 BTC/ETH in USD) — no conversion needed
          for mark_price, best_bid, best_ask (already USD-denominated)

    Returns columns: open, high, low, close, volume
    All prices are in USD regardless of margin currency.
    """
    since    = _to_ms(pd.to_datetime(start_date, utc=True))
    end_ts   = _to_ms(pd.to_datetime(end_date,   utc=True))
    resolution = 60 if timeframe == "1h" else int(timeframe)

    all_dfs  = []
    fetch_end = end_ts
    loops    = 0

    print(f"  Fetching OHLCV for {instrument_name}...")

    while fetch_end > since and loops < 10_000:
        params = {
            "instrument_name": instrument_name,
            "start_timestamp": since,
            "end_timestamp":   fetch_end,
            "resolution":      resolution,
            "limit":           limit,
        }
        resp = requests.get(f"{DERIBIT_API_URL}/get_tradingview_chart_data", params=params, timeout=10)
        resp.raise_for_status()
        result = resp.json().get("result", {})

        required = ["ticks", "open", "high", "low", "close", "volume"]
        if not all(f in result for f in required) or not result["ticks"]:
            break

        df = pd.DataFrame({
            "timestamp": pd.to_datetime(result["ticks"], unit="ms", utc=True),
            "open":   result["open"],
            "high":   result["high"],
            "low":    result["low"],
            "close":  result["close"],
            "volume": result["volume"],
        }).set_index("timestamp")

        all_dfs.append(df)
        print(f"    {df.index[0]} → {df.index[-1]} | rows: {len(df)} | total: {sum(len(d) for d in all_dfs)}")

        if len(df) < limit:
            break

        oldest_ts = _to_ms(df.index[0])
        if oldest_ts >= fetch_end:
            break
        fetch_end = oldest_ts - 1
        loops += 1
        time.sleep(0.15)

    if not all_dfs:
        return pd.DataFrame()

    df = pd.concat(all_dfs[::-1])
    df = df[~df.index.duplicated(keep="first")].sort_index()
    df = df[
        (df.index >= pd.to_datetime(start_date, utc=True)) &
        (df.index <= pd.to_datetime(end_date,   utc=True))
    ]
    return df


def _fetch_funding_chunk_deribit(instrument: str, start_dt: datetime, end_dt: datetime) -> list:
    params = {
        "instrument_name": instrument,
        "start_timestamp": _to_ms(start_dt),
        "end_timestamp":   _to_ms(end_dt),
    }
    resp = requests.get(f"{DERIBIT_API_URL}/get_funding_rate_history", params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    if "error" in data:
        raise RuntimeError(f"Deribit API error: {data['error']}")
    return data.get("result", [])


# ── PUBLIC FUNCTION ───────────────────────────────────────────────────────────
def fetch_deribit(
    asset: str      = "BTC",
    start_date: str = "2020-01-01T00:00:00Z",
    end_date: str   = "2024-01-01T00:00:00Z",
    timeframe: str  = "1h",
    chunk_days: int = 30,
    sleep_s: float  = 0.3,
) -> pd.DataFrame:
    """
    Fetches and assembles a clean hourly Deribit dataframe for one asset.

    Columns returned
    ----------------
    mark_price_deribit        — USD price of the perp (USD for all assets)
    best_bid_deribit          — mark_price × (1 − spread/10000)
    best_ask_deribit          — mark_price × (1 + spread/10000)
    funding_rate_raw_deribit  — interest_1h as published (dimensionless rate)
    funding_rate_usd_deribit  — funding_rate_raw × mark_price
                                For linear (USDC) contracts this equals funding_rate_raw
                                (already USD-per-USD-notional).
                                For inverse (BTC/ETH) contracts the raw rate is per-coin-notional;
                                multiplying by mark_price converts to USD-per-USD-notional,
                                making it directly comparable across all assets.

    Notes
    -----
    - BNB history only exists from June 2025. Earlier rows are NaN — no crash.
    - BTC/ETH are inverse contracts but prices are quoted in USD throughout,
      so best_bid/best_ask are already in USD. Only the funding rate needs conversion.
    - No open_interest column: Deribit does not expose historical OI via public API.
    - spread_bps is asset-specific (see SPREAD_BPS dict).

    Parameters
    ----------
    asset       : ticker, e.g. 'BTC', 'ETH', 'SOL'
    start_date  : ISO8601 string
    end_date    : ISO8601 string
    timeframe   : candle resolution, default '1h'
    chunk_days  : days per funding rate API chunk
    sleep_s     : pause between API requests
    """
    asset_upper  = asset.upper()
    instrument   = DERIBIT_SYMBOLS.get(asset_upper)
    spread_bps   = SPREAD_BPS.get(asset_upper, 5.0)
    is_inverse   = asset_upper in INVERSE_ASSETS
    half_spread  = spread_bps / 10_000

    if not instrument:
        raise ValueError(f"Unknown asset '{asset}'. Available: {list(DERIBIT_SYMBOLS.keys())}")

    print(f"\n{'='*55}")
    print(f"Fetching Deribit — {asset} ({instrument})")
    print(f"Range     : {start_date} → {end_date}")
    print(f"Spread    : ±{spread_bps} bps  |  Inverse: {is_inverse}")
    print(f"{'='*55}")

    # ── Full hourly index — all gaps will be explicit NaN ─────────────────────
    full_index = pd.date_range(
        start = start_date,
        end   = end_date,
        freq  = "1h",
        tz    = "UTC",
        inclusive = "left",
    )
    df = pd.DataFrame(index=full_index)
    df.index.name = "timestamp"

    # ── [1/2] OHLCV ──────────────────────────────────────────────────────────
    print("\n[1/2] Perp OHLCV...")
    ohlcv = _fetch_ohlcv_deribit(instrument, asset_upper, start_date, end_date, timeframe)

    if ohlcv.empty:
        print(f"  ⚠️  No OHLCV data returned — filling all price columns with NaN.")
        df["mark_price_deribit"] = np.nan
        df["best_bid_deribit"]   = np.nan
        df["best_ask_deribit"]   = np.nan
    else:
        # Reindex onto clean hourly grid — missing candles become NaN
        ohlcv = ohlcv.reindex(full_index)
        df["mark_price_deribit"] = ohlcv["close"]
        df["best_bid_deribit"]   = ohlcv["close"] * (1 - half_spread)
        df["best_ask_deribit"]   = ohlcv["close"] * (1 + half_spread)

    # ── [2/2] Funding rates ───────────────────────────────────────────────────
    print("\n[2/2] Funding rate history...")

    start_dt = datetime.fromisoformat(start_date.replace("Z", "+00:00"))
    end_dt   = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
    chunk    = timedelta(days=chunk_days)
    cursor   = start_dt
    all_rows = []

    while cursor < end_dt:
        window_end = min(cursor + chunk, end_dt)
        try:
            batch = _fetch_funding_chunk_deribit(instrument, cursor, window_end)
            all_rows.extend(batch)
            print(f"  {cursor.date()} → {window_end.date()} | rows: {len(batch)} | total: {len(all_rows)}")
        except Exception as e:
            print(f"  ⚠️  Chunk {cursor.date()} failed: {e} — filling with NaN")
        cursor = window_end
        time.sleep(sleep_s)

    if all_rows:
        raw = pd.DataFrame(all_rows)
        raw["timestamp"] = pd.to_datetime(raw["timestamp"], unit="ms", utc=True)
        raw = raw.set_index("timestamp").sort_index()

        funding_raw = raw["interest_1h"].reindex(full_index)
    else:
        print(f"  ⚠️  No funding data returned — filling with NaN (expected for BNB before June 2025).")
        funding_raw = pd.Series(np.nan, index=full_index)

    df["funding_rate_raw_deribit"] = funding_raw

    # ── USD-equivalent funding rate ───────────────────────────────────────────
    # Linear (USDC) contracts: raw rate is already per-USD-notional → no change
    # Inverse (BTC/ETH) contracts: raw rate is per-coin-notional
    #   → multiply by mark_price to get per-USD-notional (comparable across assets)
    if is_inverse:
        df["funding_rate_usd_deribit"] = df["funding_rate_raw_deribit"] * df["mark_price_deribit"]
    else:
        df["funding_rate_usd_deribit"] = df["funding_rate_raw_deribit"]

    n_nan_price   = df["mark_price_deribit"].isna().sum()
    n_nan_funding = df["funding_rate_raw_deribit"].isna().sum()
    print(f"\n✅ Done — {len(df)} rows | NaN prices: {n_nan_price} | NaN funding: {n_nan_funding}")

    return df
