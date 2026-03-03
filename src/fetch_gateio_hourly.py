import ccxt
import pandas as pd
import time
import requests

# ── EXCHANGE CONSTANTS ───────────────────────────────────────────────────────
EXCHANGE_CONSTANTS = {
    "gateio": {
        "funding_interval_hours": 8,
        "perp_taker_fee_bps": 5.0,
        "spot_taker_fee_bps": 10.0,
    }
}

# Gate.io limits OHLCV (perp & spot) to the most recent 10,000 candles.
# For 1h candles that is ~13 months of history.
# Funding rates have no such limit and go back to at least 2022.
# Open interest history is fetched via the native contract_stats endpoint
# because ccxt's fetch_open_interest_history returns empty for Gate.io.
OHLCV_MAX_CANDLES = 10_000


# ── INIT ─────────────────────────────────────────────────────────────────────
def _get_exchanges():
    perp = ccxt.gateio({
        "enableRateLimit": True,
        "options": {"defaultType": "swap"},
    })
    spot = ccxt.gateio({
        "enableRateLimit": True,
        "options": {"defaultType": "spot"},
    })
    return perp, spot


def _clamp_ohlcv_start(start_date: str, timeframe: str = "1h") -> str:
    """
    Gate.io only serves the most recent 10,000 candles.
    If start_date is older than that, clamp it forward and warn.
    """
    tf_ms = {"1h": 3_600_000}
    ms_per_candle = tf_ms.get(timeframe, 3_600_000)
    # Use 9,500 instead of 10,000 as a safety margin — the exact cutoff
    # can shift slightly between perp/spot endpoints and by the time the
    # request reaches the server a few candles may have elapsed.
    safe_max = OHLCV_MAX_CANDLES - 500
    earliest_ms = int(time.time() * 1000) - safe_max * ms_per_candle
    requested_ms = int(pd.Timestamp(start_date).timestamp() * 1000)

    if requested_ms < earliest_ms:
        clamped = (
            pd.to_datetime(earliest_ms, unit="ms", utc=True)
            .floor("h")
            .strftime("%Y-%m-%dT%H:%M:%SZ")
        )
        print(f"[WARN] Gate.io OHLCV limited to last {OHLCV_MAX_CANDLES} candles. "
              f"Clamping start from {start_date} → {clamped}")
        return clamped
    return start_date


# ── INTERNAL HELPERS ──────────────────────────────────────────────────────────
def _fetch_ohlcv(exchange, symbol, timeframe, start_date, end_date, limit=1000):
    start_date = _clamp_ohlcv_start(start_date, timeframe)
    since = exchange.parse8601(start_date)
    end_ts = exchange.parse8601(end_date)

    if since > end_ts:
        print(f"  [{symbol}] start after end after clamping — no OHLCV available for this range")
        return pd.DataFrame(columns=["open", "high", "low", "close", "volume"])

    all_rows = []

    while True:
        batch = exchange.fetch_ohlcv(symbol, timeframe, since=since, limit=limit)
        if not batch:
            break
        batch = [r for r in batch if r[0] <= end_ts]
        if not batch:
            break
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


def _fetch_funding_rates(exchange, symbol, start_date, end_date):
    # Gate.io caps funding rate responses at ~90 rows per request
    # (30 days * 3 settlements/day) regardless of the limit param.
    # We use limit=90 so the "partial batch" pagination logic works.
    limit = 90
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


def _ensure_utc(ts_like):
    ts = pd.Timestamp(ts_like)
    if ts.tzinfo is None:
        return ts.tz_localize("UTC")
    return ts.tz_convert("UTC")


def _finalize_oi_rows(rows):
    if not rows:
        return pd.DataFrame(columns=["open_interest_usd"])
    df = pd.DataFrame(rows)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df = df.sort_values("timestamp").drop_duplicates("timestamp", keep="last")
    return df.set_index("timestamp")[["open_interest_usd"]]


def _fetch_open_interest_ccxt(exchange, perp_symbol, start_ts, end_ts, limit=100):
    """Try unified ccxt OI endpoint first."""
    since_ms = int(start_ts.timestamp() * 1000)
    end_ms = int(end_ts.timestamp() * 1000)
    limit = max(1, min(int(limit), 200))
    all_rows = []

    while since_ms < end_ms:
        try:
            batch = exchange.fetch_open_interest_history(perp_symbol, "1h", since=since_ms, limit=limit)
        except Exception as e:
            print(f"[WARN] ccxt OI history failed: {e}")
            break

        if not batch:
            break

        for row in batch:
            ts_ms = row.get("timestamp")
            if ts_ms is None:
                continue
            if ts_ms > end_ms:
                continue

            info = row.get("info") or {}
            oi_val = row.get("openInterestValue")
            if oi_val is None:
                oi_val = info.get("open_interest_usd")
            if oi_val is None:
                oi_val = info.get("sum_open_interest")
            if oi_val is None:
                oi_val = info.get("open_interest")
            if oi_val is None:
                continue

            all_rows.append({
                "timestamp": pd.to_datetime(ts_ms, unit="ms", utc=True),
                "open_interest_usd": float(oi_val),
            })

        last_ts_ms = batch[-1].get("timestamp")
        if last_ts_ms is None:
            break

        print(f"  [OI-ccxt] up to {pd.to_datetime(last_ts_ms, unit='ms', utc=True)}  |  rows: {len(all_rows)}")

        if last_ts_ms >= end_ms or len(batch) < limit:
            break
        if last_ts_ms < since_ms:
            break

        since_ms = last_ts_ms + 1
        time.sleep(exchange.rateLimit / 1000)

    return _finalize_oi_rows(all_rows)


def _fetch_open_interest_raw(exchange, contract, start_ts, end_ts, limit=100, from_unit="s"):
    """
    Fallback raw endpoint using either second- or millisecond-based 'from'.

    Important: Gate.io contract_stats can return the latest `limit` rows in the
    [from, to] range (not necessarily paginating from `from`). To avoid missing
    history, query fixed-size windows whose width fits in `limit` rows.
    """
    since_s = int(start_ts.timestamp())
    end_s = int(end_ts.timestamp())
    limit = max(1, min(int(limit), 200))
    all_rows = []
    step_hours = max(1, limit - 1)
    step_s = step_hours * 3600
    win_start = since_s

    while win_start < end_s:
        win_end = min(win_start + step_s, end_s)
        req_from = win_start if from_unit == "s" else win_start * 1000
        req_to = win_end if from_unit == "s" else win_end * 1000
        try:
            batch = exchange.publicFuturesGetSettleContractStats({
                "settle": "usdt",
                "contract": contract,
                "from": req_from,
                "to": req_to,
                "interval": "1h",
                "limit": limit,
            })
        except Exception as e:
            print(f"[WARN] raw OI ({from_unit}) failed: {e}")
            break

        if not batch:
            # advance to next window even if one window is empty
            win_start = win_end + 3600
            continue

        for entry in batch:
            raw_ts = entry.get("time")
            if raw_ts is None:
                continue
            ts_s = int(float(raw_ts))
            if ts_s > 10_000_000_000:
                ts_s //= 1000
            if ts_s > end_s:
                break

            oi_val = entry.get("open_interest_usd")
            if oi_val is None:
                oi_val = entry.get("sum_open_interest")
            if oi_val is None:
                oi_val = entry.get("open_interest")
            if oi_val is None:
                continue

            all_rows.append({
                "timestamp": pd.to_datetime(ts_s, unit="s", utc=True),
                "open_interest_usd": float(oi_val),
            })

        # progress based on window end (deterministic), not batch[-1] semantics
        print(f"  [OI-raw-{from_unit}] window {pd.to_datetime(win_start, unit='s', utc=True)}"
              f" → {pd.to_datetime(win_end, unit='s', utc=True)}  |  rows: {len(all_rows)}")
        win_start = win_end + 3600
        time.sleep(exchange.rateLimit / 1000)

    return _finalize_oi_rows(all_rows)


def _fetch_open_interest_http(contract, start_ts, end_ts, limit=100):
    """Direct HTTP fallback independent of ccxt raw wrappers."""
    since_s = int(start_ts.timestamp())
    end_s = int(end_ts.timestamp())
    limit = max(1, min(int(limit), 200))
    step_hours = max(1, limit - 1)
    step_s = step_hours * 3600
    win_start = since_s
    all_rows = []

    while win_start < end_s:
        win_end = min(win_start + step_s, end_s)
        try:
            resp = requests.get(
                "https://api.gateio.ws/api/v4/futures/usdt/contract_stats",
                params={
                    "contract": contract,
                    "from": win_start,
                    "to": win_end,
                    "interval": "1h",
                    "limit": limit,
                },
                timeout=20,
            )
            resp.raise_for_status()
            batch = resp.json()
        except Exception as e:
            print(f"[WARN] http OI failed: {e}")
            break

        if not isinstance(batch, list):
            print(f"[WARN] http OI unexpected payload type: {type(batch)}")
            break

        for entry in batch:
            raw_ts = entry.get("time")
            if raw_ts is None:
                continue
            ts_s = int(float(raw_ts))
            if ts_s > 10_000_000_000:
                ts_s //= 1000
            if ts_s < since_s or ts_s > end_s:
                continue

            oi_val = entry.get("open_interest_usd")
            if oi_val is None:
                oi_val = entry.get("sum_open_interest")
            if oi_val is None:
                oi = entry.get("open_interest")
                px = entry.get("mark_price")
                if oi is not None and px is not None:
                    oi_val = float(oi) * float(px)
                else:
                    oi_val = oi
            if oi_val is None:
                continue

            all_rows.append({
                "timestamp": pd.to_datetime(ts_s, unit="s", utc=True),
                "open_interest_usd": float(oi_val),
            })

        print(f"  [OI-http] window {pd.to_datetime(win_start, unit='s', utc=True)}"
              f" → {pd.to_datetime(win_end, unit='s', utc=True)}  |  rows: {len(all_rows)}")
        win_start = win_end + 3600
        time.sleep(0.05)

    return _finalize_oi_rows(all_rows)


def _fetch_open_interest(exchange, asset, start_date, end_date, limit=1000):
    """
    Fetch hourly OI for Gate.io.
    Try unified ccxt first, then raw endpoint fallbacks.
    """
    contract = f"{asset}_USDT"
    perp_symbol = f"{asset}/USDT:USDT"
    start_ts = _ensure_utc(start_date).floor("h")
    end_ts = _ensure_utc(end_date).ceil("h")
    safe_limit = min(int(limit), 100)

    if start_ts >= end_ts:
        print("[WARN] OI start >= end after normalization.")
        return pd.DataFrame(columns=["open_interest_usd"])

    print("  [OI] strategy 1/3: ccxt unified fetch_open_interest_history")
    oi = _fetch_open_interest_ccxt(exchange, perp_symbol, start_ts, end_ts, limit=safe_limit)
    if not oi.empty:
        return oi

    print("  [OI] strategy 2/3: raw contract_stats with second-based 'from'")
    oi = _fetch_open_interest_raw(exchange, contract, start_ts, end_ts, limit=safe_limit, from_unit="s")
    if not oi.empty:
        return oi

    print("  [OI] strategy 3/3: raw contract_stats with millisecond-based 'from'")
    oi = _fetch_open_interest_raw(exchange, contract, start_ts, end_ts, limit=safe_limit, from_unit="ms")
    if not oi.empty:
        return oi

    print("  [OI] strategy 4/4: direct HTTP contract_stats fallback")
    oi = _fetch_open_interest_http(contract, start_ts, end_ts, limit=safe_limit)
    if not oi.empty:
        return oi

    print("[WARN] No open interest data fetched from any strategy.")
    return pd.DataFrame(columns=["open_interest_usd"])


# ── STANDALONE OI FETCHER ───────────────────────────────────────────────────
def fetch_gateio_open_interest(
    asset: str = "BTC",
    start_date: str = "2023-12-01T00:00:00Z",
    end_date: str = "2024-01-01T00:00:00Z",
    limit: int = 1000
) -> pd.DataFrame:
    """
    Fetches open interest for a single asset from Gate.io, returns a DataFrame.
    """
    perp_ex, _ = _get_exchanges()
    perp_ex.load_markets()
    return _fetch_open_interest(perp_ex, asset, start_date, end_date, limit=limit)


# ── PUBLIC FUNCTION ───────────────────────────────────────────────────────────
def fetch_gateio(
    asset: str          = "BTC",
    start_date: str     = "2022-01-01T00:00:00Z",
    end_date: str       = "2024-01-01T00:00:00Z",
    timeframe: str      = "1h",
    spread_bps: float   = 2.0,
) -> pd.DataFrame:
    """
    Fetches and assembles a clean hourly dataframe for one asset on Gate.io.

    Columns returned:
        mark_price_gateio, best_bid_gateio, best_ask_gateio,
        spot_best_bid_gateio, spot_best_ask_gateio,
        funding_rate_raw_gateio, open_interest_usd_gateio

    Parameters
    ----------
    asset       : ticker string, e.g. 'BTC', 'ETH', 'SOL'
    start_date  : ISO8601 string
    end_date    : ISO8601 string
    timeframe   : OHLCV candle size, default '1h'
    spread_bps  : half-spread in bps applied symmetrically around close price.
                  bid = close * (1 - spread_bps/10000)
                  ask = close * (1 + spread_bps/10000)

    Notes
    -----
    Gate.io OHLCV (perp & spot) is limited to the most recent ~10,000 candles
    (~13 months for 1h). If start_date is older, it is clamped forward
    automatically. Funding rates have no such limit.
    """
    perp_symbol = f"{asset}/USDT:USDT"
    spot_symbol = f"{asset}/USDT"
    half_spread = spread_bps / 10_000

    perp_ex, spot_ex = _get_exchanges()
    perp_ex.load_markets()
    spot_ex.load_markets()

    print(f"\n{'='*55}")
    print(f"Fetching Gate.io data — {asset} | {start_date} → {end_date}")
    print(f"Spread assumption: ±{spread_bps} bps around close")
    print(f"{'='*55}")

    print("\n[1/4] Perp OHLCV...")
    perp_ohlcv = _fetch_ohlcv(perp_ex, perp_symbol, timeframe, start_date, end_date)

    print("\n[2/4] Spot OHLCV...")
    spot_ohlcv = _fetch_ohlcv(spot_ex, spot_symbol, timeframe, start_date, end_date)

    # Use the effective OHLCV start (after clamping) for dependent series.
    # Requesting Gate.io OI from very old dates can return empty.
    ohlcv_start = perp_ohlcv.index.min() if not perp_ohlcv.empty else pd.Timestamp(start_date, tz="UTC")
    if ohlcv_start.tzinfo is None:
        ohlcv_start = ohlcv_start.tz_localize("UTC")
    else:
        ohlcv_start = ohlcv_start.tz_convert("UTC")

    # Fetch funding starting 2 days before OHLCV so forward-fill has a
    # seed value even when OHLCV start doesn't align with a settlement hour.
    funding_start = (ohlcv_start - pd.Timedelta(days=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
    oi_start = ohlcv_start.floor("h").strftime("%Y-%m-%dT%H:%M:%SZ")

    print("\n[3/4] Funding rates...")
    funding    = _fetch_funding_rates(perp_ex, perp_symbol, funding_start, end_date)

    print("\n[4/4] Open interest...")
    oi         = _fetch_open_interest(perp_ex, asset, oi_start, end_date)

    # ── ASSEMBLE ──────────────────────────────────────────────────────────────
    df = perp_ohlcv[["close"]].rename(columns={"close": "mark_price_gateio"})

    df["best_bid_gateio"] = df["mark_price_gateio"] * (1 - half_spread)
    df["best_ask_gateio"] = df["mark_price_gateio"] * (1 + half_spread)

    df["spot_best_bid_gateio"] = spot_ohlcv["close"] * (1 - half_spread)
    df["spot_best_ask_gateio"] = spot_ohlcv["close"] * (1 + half_spread)

    # Forward-fill funding rate across the 8 hourly rows between settlements.
    # Gate.io funding settles at 00:00/08:00/16:00 UTC — these timestamps
    # may not appear in the OHLCV index (e.g. OHLCV starts at 01:00).
    # Use merge_asof to pick up the most recent funding rate at or before
    # each OHLCV timestamp, which handles the alignment automatically.
    if not funding.empty:
        df = pd.merge_asof(
            df.sort_index(),
            funding.rename(columns={"funding_rate_raw": "funding_rate_raw_gateio"}).sort_index(),
            left_index=True,
            right_index=True,
            direction="backward",
        )
    else:
        df["funding_rate_raw_gateio"] = float("nan")

    # Align OI to OHLCV timestamps (exact join can miss if API timestamps
    # are offset from the candle grid by a few seconds/minutes).
    if not oi.empty:
        df = pd.merge_asof(
            df.sort_index(),
            oi.rename(columns={"open_interest_usd": "open_interest_usd_gateio"}).sort_index(),
            left_index=True,
            right_index=True,
            direction="backward",
            tolerance=pd.Timedelta(hours=2),
        )
    else:
        df["open_interest_usd_gateio"] = float("nan")

    df.index.name = "timestamp"
    df = df.sort_index()

    print(f"\nDone — {len(df)} rows assembled.")
    return df
