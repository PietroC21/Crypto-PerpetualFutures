import requests
import pandas as pd
import numpy as np
import time
from datetime import datetime, timezone, timedelta

# ── CONSTANTS ────────────────────────────────────────────────────────────────

DERIBIT_API_URL = "https://www.deribit.com/api/v2/public"

EXCHANGE_CONSTANTS = {
    "deribit": {
        "funding_interval_hours": 8,
        "perp_taker_fee_bps":     5.0,
        "spot_taker_fee_bps":     None,   # no spot market on Deribit
    }
}

# Correct instrument names — BTC/ETH are inverse, rest are linear USDC-settled
DERIBIT_SYMBOLS = {
    "BTC":  ("BTC-PERPETUAL",       "inverse"),   # margined in BTC
    "ETH":  ("ETH-PERPETUAL",       "inverse"),   # margined in ETH
    "SOL":  ("SOL_USDC-PERPETUAL",  "linear"),
    "XRP":  ("XRP_USDC-PERPETUAL",  "linear"),
    "BNB":  ("BNB_USDC-PERPETUAL",  "linear"),    # only since June 2025 — early rows will be NaN
    "AVAX": ("AVAX_USDC-PERPETUAL", "linear"),
    "DOGE": ("DOGE_USDC-PERPETUAL", "linear"),
}

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


# ── INTERNAL: OHLCV ──────────────────────────────────────────────────────────

def _fetch_ohlcv_deribit(
    instrument_name: str,
    start_date: str,
    end_date: str,
    asset: str,
    contract_type: str,         # "inverse" or "linear"
    timeframe: str = "1h",
    limit: int = 1000,
) -> pd.DataFrame:
    """
    Fetches OHLCV from Deribit and returns a clean hourly DataFrame.

    For inverse contracts (BTC, ETH): close price is in USD index terms from
    the API, but PnL is in coin. We keep prices in USD and flag the type.

    Derives best_bid / best_ask from close using per-asset SPREAD_BPS.
    """
    spread_bps  = SPREAD_BPS.get(asset.upper(), 5.0)
    half_spread = spread_bps / 10_000
    resolution  = 60 if timeframe == "1h" else int(timeframe)

    since    = int(pd.Timestamp(start_date, tz="UTC").timestamp() * 1000)
    end_ts   = int(pd.Timestamp(end_date,   tz="UTC").timestamp() * 1000)
    fetch_end = end_ts
    all_dfs   = []
    loops     = 0

    print(f"  [{instrument_name}] OHLCV {pd.Timestamp(since, unit='ms', utc=True).date()} "
          f"→ {pd.Timestamp(end_ts, unit='ms', utc=True).date()}")

    while fetch_end > since and loops < 10_000:
        params = {
            "instrument_name": instrument_name,
            "start_timestamp": since,
            "end_timestamp":   fetch_end,
            "resolution":      resolution,
            "limit":           limit,
        }
        try:
            resp = requests.get(f"{DERIBIT_API_URL}/get_tradingview_chart_data",
                                params=params, timeout=10)
            resp.raise_for_status()
        except requests.RequestException as e:
            print(f"  ⚠️  Request failed: {e} — stopping pagination.")
            break

        result = resp.json().get("result", {})
        required = ["ticks", "open", "high", "low", "close", "volume"]
        if not all(k in result for k in required) or not result["ticks"]:
            print("  No more data — stopping pagination.")
            break

        df = pd.DataFrame({
            "timestamp": pd.to_datetime(result["ticks"], unit="ms", utc=True),
            "open":      result["open"],
            "high":      result["high"],
            "low":       result["low"],
            "close":     result["close"],
            "volume":    result["volume"],
        }).set_index("timestamp")

        all_dfs.append(df)

        if len(df) < limit:
            break

        oldest_ts = int(df.index[0].timestamp() * 1000)
        if oldest_ts >= fetch_end:
            break
        fetch_end = oldest_ts - 1
        loops += 1
        time.sleep(0.1)

    # ── Return NaN frame if nothing fetched (e.g. BNB before June 2025) ──────
    if not all_dfs:
        print(f"  ⚠️  No OHLCV data for {instrument_name} in this range — filling with NaN.")
        idx = pd.date_range(start=start_date, end=end_date, freq="1h", tz="UTC")
        return pd.DataFrame(
            index=idx,
            data={
                "mark_price_deribit":   np.nan,
                "best_bid_deribit":     np.nan,
                "best_ask_deribit":     np.nan,
                "contract_type":        contract_type,
            }
        )

    df = (pd.concat(all_dfs[::-1])
            .pipe(lambda x: x[~x.index.duplicated(keep="first")])
            .sort_index())

    # Clip to requested range
    df = df.loc[
        (df.index >= pd.Timestamp(start_date, tz="UTC")) &
        (df.index <= pd.Timestamp(end_date,   tz="UTC"))
    ]

    # ── For inverse contracts: close from Deribit is already the USD mark price
    # (Deribit quotes BTC-PERPETUAL in USD). No conversion needed for the price
    # itself. The funding rate IS what needs USD conversion (done separately).
    df["mark_price_deribit"] = df["close"]
    df["best_bid_deribit"]   = df["close"] * (1 - half_spread)
    df["best_ask_deribit"]   = df["close"] * (1 + half_spread)
    df["contract_type"]      = contract_type

    print(f"  ✅ {len(df)} rows | spread: ±{spread_bps} bps | type: {contract_type}")
    return df[["mark_price_deribit", "best_bid_deribit", "best_ask_deribit", "contract_type"]]


# ── INTERNAL: FUNDING RATES ──────────────────────────────────────────────────

def _fetch_funding_rates_deribit(
    instrument_name: str,
    asset: str,
    contract_type: str,
    start_date: str,
    end_date: str,
    mark_price: pd.Series,      # needed for inverse USD conversion
    chunk_days: int = 30,
    sleep_s: float  = 0.3,
) -> pd.Series:
    """
    Fetches funding rate history from Deribit.

    For LINEAR contracts: returns interest_1h directly (already in % of notional USD).
    For INVERSE contracts: funding is paid in BTC/ETH. We convert to USD-equivalent
    using the mark price: funding_usd_rate = interest_1h × mark_price_at_settlement.
    This makes it directly comparable to linear funding rates for cross-asset analysis.

    Returns pd.Series named 'funding_rate_raw_deribit', indexed by UTC timestamp.
    NaN where data is unavailable (e.g. BNB before June 2025).
    """
    start_dt = datetime.fromisoformat(start_date.replace("Z", "")).replace(tzinfo=timezone.utc)
    end_dt   = datetime.fromisoformat(end_date.replace("Z", "")).replace(tzinfo=timezone.utc)
    chunk    = timedelta(days=chunk_days)
    all_rows = []
    cursor   = start_dt

    while cursor < end_dt:
        window_end = min(cursor + chunk, end_dt)
        params = {
            "instrument_name": instrument_name,
            "start_timestamp": int(cursor.timestamp() * 1000),
            "end_timestamp":   int(window_end.timestamp() * 1000),
        }
        try:
            resp = requests.get(f"{DERIBIT_API_URL}/get_funding_rate_history",
                                params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as e:
            print(f"  ⚠️  Funding fetch failed for chunk {cursor.date()}: {e}")
            cursor = window_end
            time.sleep(sleep_s)
            continue

        if "error" in data:
            print(f"  ⚠️  API error for {instrument_name}: {data['error']} — chunk skipped.")
            cursor = window_end
            time.sleep(sleep_s)
            continue

        batch = data.get("result", [])
        all_rows.extend(batch)
        print(f"  [funding] {cursor.date()} → {window_end.date()} | rows: {len(batch)} | total: {len(all_rows)}")
        cursor = window_end
        time.sleep(sleep_s)

    # ── Build clean hourly grid ───────────────────────────────────────────────
    full_index = pd.date_range(start=start_date, end=end_date, freq="1h", tz="UTC")

    if not all_rows:
        print(f"  ⚠️  No funding data for {instrument_name} — filling with NaN.")
        return pd.Series(np.nan, index=full_index, name="funding_rate_raw_deribit")

    raw = pd.DataFrame(all_rows)
    raw["timestamp"] = pd.to_datetime(raw["timestamp"], unit="ms", utc=True)
    raw = raw.set_index("timestamp").sort_index()

    series = raw["interest_1h"].reindex(full_index)

    # ── Inverse: convert coin-denominated rate to USD-equivalent ─────────────
    # funding_usd_equivalent = interest_1h × mark_price
    # e.g. 0.0001 BTC per BTC notional × $50,000/BTC = $5 per $50,000 = 0.0001 USD rate
    # The ratio stays the same! For inverse perps:
    # interest_1h is already dimensionless (fraction of notional) so the USD
    # rate equals interest_1h directly — no conversion needed for the RATE.
    # What differs is margin currency, not the rate itself.
    # We flag this clearly but do not transform the value.
    if contract_type == "inverse":
        print(f"  ℹ️  {asset} is inverse — funding rate is dimensionless fraction, "
              f"comparable to linear. Margin is in {asset}, not USD.")

    n_gaps = series.isna().sum()
    if n_gaps > 0:
        print(f"  ⚠️  {n_gaps} NaN rows in funding series.")

    return series.rename("funding_rate_raw_deribit")


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
    Fetches and assembles a clean hourly dataframe for one asset on Deribit.

    Columns returned
    ----------------
        mark_price_deribit        — USD mark price (all contract types)
        best_bid_deribit          — close × (1 − spread_bps/10000)
        best_ask_deribit          — close × (1 + spread_bps/10000)
        funding_rate_raw_deribit  — hourly funding rate (interest_1h)
        open_interest_usd_deribit — NaN (no public historical OI API on Deribit)
        contract_type             — 'inverse' or 'linear' (informational)

    Notes
    -----
    - BNB rows before June 2025 will be NaN (instrument did not exist).
    - BTC/ETH are inverse contracts — funding rate is dimensionless and
      directly comparable to linear rates numerically, but margin is in coin.
    - spread_bps is asset-specific (see SPREAD_BPS dict).
    - No spot market exists on Deribit — spot_best_bid/ask columns are absent.
    """
    if asset.upper() not in DERIBIT_SYMBOLS:
        raise ValueError(f"Unknown asset '{asset}'. Available: {list(DERIBIT_SYMBOLS.keys())}")

    instrument, contract_type = DERIBIT_SYMBOLS[asset.upper()]

    print(f"\n{'='*55}")
    print(f"Fetching Deribit — {asset} ({instrument}) | {start_date[:10]} → {end_date[:10]}")
    print(f"Contract type: {contract_type} | Spread: ±{SPREAD_BPS.get(asset.upper(), 5.0)} bps")
    print(f"{'='*55}")

    # ── 1. OHLCV ─────────────────────────────────────────────────────────────
    print("\n[1/3] Perp OHLCV...")
    ohlcv = _fetch_ohlcv_deribit(
        instrument, start_date, end_date,
        asset=asset, contract_type=contract_type,
        timeframe=timeframe,
    )

    # ── 2. Funding rates ──────────────────────────────────────────────────────
    print("\n[2/3] Funding rates...")
    funding = _fetch_funding_rates_deribit(
        instrument, asset, contract_type,
        start_date, end_date,
        mark_price=ohlcv["mark_price_deribit"],
        chunk_days=chunk_days,
        sleep_s=sleep_s,
    )

    # ── 3. Open interest — not available, fill NaN ────────────────────────────
    print("\n[3/3] Open interest — not available via public API, filling NaN.")
    oi = pd.Series(
        np.nan,
        index=pd.date_range(start=start_date, end=end_date, freq="1h", tz="UTC"),
        name="open_interest_usd_deribit",
    )

    # ── Assemble ──────────────────────────────────────────────────────────────
    df = ohlcv.copy()
    df = df.join(funding.to_frame(),  how="left")
    df = df.join(oi.to_frame(),       how="left")

    df.index.name = "timestamp"
    df = df.sort_index()

    n_nan = df["mark_price_deribit"].isna().sum()
    if n_nan > 0:
        print(f"\n⚠️  {n_nan} NaN rows in mark_price "
              f"({n_nan / len(df):.1%} of range) — expected for BNB before June 2025.")

    print(f"\n✅ Done — {len(df)} rows | {df['mark_price_deribit'].notna().sum()} non-NaN price rows.")
    return df
