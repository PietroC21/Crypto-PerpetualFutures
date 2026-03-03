"""src/fetch_hyperliquid.py
Fetch hourly perp data from Hyperliquid.

Output columns (_hyperliquid suffix):
    mark_price_hyperliquid       — perp mark price (close of 1h candle)
    best_bid_hyperliquid         — derived: mark × (1 − spread_bps/10000)
    best_ask_hyperliquid         — derived: mark × (1 + spread_bps/10000)
    funding_rate_raw_hyperliquid — raw 1h funding rate (HL settles every hour)

Note: Hyperliquid has no native spot market — spot columns are omitted entirely.

API constraints discovered empirically
---------------------------------------
  candleSnapshot : returns the most recent 5 000 rows per call.
                   For 1h candles → ≈ 208 days only.
                   For 8h candles → 5 000 × 8h ≈ 4.6 years (covers all HL history).
                   endTime parameter is not supported — returns 0 rows.
                   Strategy: fetch 8h candles first (full history), then 1h candles
                   (recent precision); merge with 1h taking priority.
  fundingHistory : full history available (BTC from 2023-05-12).
                   Returns 500 rows per call; paginate via startTime.
                   endTime supported and working.

Exchange constants
------------------
    funding_interval_hours : 1      (vs Binance's 8)
    perp_taker_fee_bps     : 2.5    (rebate tiers exist for market-makers)
    spot_taker_fee_bps     : None   (no spot market)

Usage
-----
    from src.fetch_hyperliquid import fetch_hyperliquid

    df = fetch_hyperliquid("BTC", "2023-05-12T00:00:00Z", "2026-01-01T00:00:00Z")
    df.to_parquet("data/hyperliquid_BTC_2023-05-12_2026-01-01.parquet")
"""

from __future__ import annotations

import time
from typing import Optional

import pandas as pd
import requests

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

HL_URL = "https://api.hyperliquid.xyz/info"

EXCHANGE_CONSTANTS = {
    "hyperliquid": {
        "funding_interval_hours": 1,
        "perp_taker_fee_bps":     2.5,
        "spot_taker_fee_bps":     None,   # no spot market
    }
}

# Earliest available data per coin (empirically verified)
HL_HISTORY_START: dict[str, str] = {
    "BTC": "2023-05-12T00:00:00Z",
    "ETH": "2023-05-12T00:00:00Z",
    # Other coins may start later; the API will return empty for earlier dates.
}

_MAX_OHLCV_ROWS    = 5_000   # hard API limit per candleSnapshot call
_MAX_FUNDING_ROWS  = 500     # hard API limit per fundingHistory call
_RETRY_ATTEMPTS    = 6
_RETRY_BACKOFF     = 5       # seconds base; waits 5, 10, 20, 40, 80s on successive 429s
_PAGE_SLEEP        = 0.5     # seconds between pagination requests


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _post(payload: dict) -> list | dict:
    """POST to Hyperliquid info endpoint with retry logic."""
    for attempt in range(_RETRY_ATTEMPTS):
        try:
            r = requests.post(HL_URL, json=payload, timeout=30)
            r.raise_for_status()
            return r.json()
        except Exception as exc:
            if attempt == _RETRY_ATTEMPTS - 1:
                raise
            wait = _RETRY_BACKOFF ** attempt
            print(f"  [WARN] request failed ({exc}), retrying in {wait}s…")
            time.sleep(wait)


def _fetch_single_candles(coin: str, interval: str, start_ms: int) -> pd.DataFrame:
    """
    One raw candleSnapshot call → DataFrame with DatetimeIndex (UTC).
    Returns at most 5 000 rows (the most recent ones).
    """
    print(f"  Requesting candleSnapshot — {coin} {interval}…", end=" ", flush=True)
    data = _post({
        "type": "candleSnapshot",
        "req": {
            "coin":      coin,
            "interval":  interval,
            "startTime": start_ms,
        },
    })

    if not data:
        print("no data returned.")
        return pd.DataFrame(columns=["open", "high", "low", "close", "volume"])

    rows = []
    for c in data:
        ts = c["t"]          # open-time of candle (milliseconds)
        if ts < start_ms:
            continue
        rows.append({
            "timestamp": pd.Timestamp(ts, unit="ms", tz="UTC"),
            "open":      float(c["o"]),
            "high":      float(c["h"]),
            "low":       float(c["l"]),
            "close":     float(c["c"]),
            "volume":    float(c["v"]),
        })

    df = pd.DataFrame(rows)
    if df.empty:
        print("no rows in range.")
        return df

    df = df.set_index("timestamp").sort_index()
    df = df[~df.index.duplicated(keep="last")]
    actual_start = df.index[0].date()
    actual_end   = df.index[-1].date()
    print(f"{len(df)} rows  ({actual_start} → {actual_end})")
    return df


def _fetch_ohlcv_hl(coin: str, interval: str, start_ms: int) -> pd.DataFrame:
    """
    Fetch perp OHLCV candles from Hyperliquid with full history coverage.

    For the default 1h interval, makes TWO candleSnapshot calls:
      1. 8h candles  — 5,000 × 8h ≈ 4.6 years, covering all HL history
                       from 2023-05-12.  Resampled to 1h by forward-filling
                       each 8h close across its eight hourly slots.
      2. 1h candles  — most recent 5,000 hours (≈ 208 days), exact prices.

    The two are merged: 1h data overwrites the 8h-resampled values wherever
    both exist, so recent rows are exact and older rows are approximations.

    For any other interval, a single candleSnapshot call is made.
    """
    if interval != "1h":
        return _fetch_single_candles(coin, interval, start_ms)

    # ── Step 1: 8h candles → resample to 1h (full history) ───────────────
    df_8h = _fetch_single_candles(coin, "8h", start_ms)

    if not df_8h.empty:
        # Extend index to cover the last 8h period fully (open-time + 7 h)
        full_1h_idx = pd.date_range(
            start=df_8h.index[0],
            end=df_8h.index[-1] + pd.Timedelta(hours=7),
            freq="1h",
            tz="UTC",
        )
        # Each 8h close is held constant across its 8 hourly slots
        df_hist = df_8h[["close"]].reindex(full_1h_idx).ffill()
        df_hist.index.name = "timestamp"
    else:
        df_hist = pd.DataFrame(columns=["close"])

    # ── Step 2: 1h candles → recent exact prices ──────────────────────────
    df_1h = _fetch_single_candles(coin, "1h", start_ms)

    # ── Merge: 1h overwrites 8h where both exist ──────────────────────────
    if df_hist.empty and df_1h.empty:
        return pd.DataFrame(columns=["close"])

    if df_hist.empty:
        df = df_1h[["close"]].copy()
    elif df_1h.empty:
        df = df_hist.copy()
    else:
        # Union of both indices; 8h-resampled as base, 1h overrides
        df = df_hist.reindex(df_hist.index.union(df_1h.index)).ffill()
        df.update(df_1h[["close"]])

    df = df.sort_index()
    df = df[~df.index.duplicated(keep="last")]
    # Apply start_ms filter (8h resampled may start exactly at start_ms)
    df = df[df.index >= pd.Timestamp(start_ms, unit="ms", tz="UTC")]

    if df.empty:
        return df

    actual_start = df.index[0].date()
    actual_end   = df.index[-1].date()
    print(f"  Combined OHLCV : {len(df):,} rows  ({actual_start} → {actual_end})")
    return df


def _fetch_funding_hl(
    coin:     str,
    start_ms: int,
    end_ms:   int,
) -> pd.DataFrame:
    """
    Fetch hourly funding rate history from Hyperliquid with forward pagination.

    Paginates 500 rows at a time until end_ms is reached or no more data.
    """
    all_rows: list[dict] = []
    since    = start_ms

    while True:
        data = _post({
            "type":      "fundingHistory",
            "coin":      coin,
            "startTime": since,
            "endTime":   end_ms,
        })

        if not data:
            break

        for r in data:
            ts = int(r["time"])
            if ts > end_ms:
                break
            all_rows.append({
                "timestamp":         pd.Timestamp(ts, unit="ms", tz="UTC"),
                "funding_rate_raw":  float(r["fundingRate"]),
            })

        last_ts = int(data[-1]["time"])
        print(
            f"  [funding {coin}] up to "
            f"{pd.Timestamp(last_ts, unit='ms', tz='UTC').strftime('%Y-%m-%d %H:%M')} "
            f"| rows so far: {len(all_rows)}"
        )

        if last_ts >= end_ms or len(data) < _MAX_FUNDING_ROWS:
            break

        since = last_ts + 1  # next millisecond to avoid overlap
        time.sleep(_PAGE_SLEEP)

    if not all_rows:
        return pd.DataFrame(columns=["funding_rate_raw"])

    df = pd.DataFrame(all_rows).set_index("timestamp").sort_index()
    # Floor to the hour to remove sub-millisecond jitter from HL timestamps
    df.index = df.index.floor("h")
    df = df[~df.index.duplicated(keep="last")]
    return df


# ---------------------------------------------------------------------------
# Public function
# ---------------------------------------------------------------------------

def fetch_hyperliquid(
    asset:      str   = "BTC",
    start_date: str   = "2023-05-12T00:00:00Z",
    end_date:   str   = "2026-01-01T00:00:00Z",
    timeframe:  str   = "1h",
    spread_bps: float = 2.0,
) -> pd.DataFrame:
    """
    Fetch hourly Hyperliquid perp data for one asset.

    Parameters
    ----------
    asset      : coin ticker, e.g. 'BTC', 'ETH', 'SOL'
    start_date : ISO-8601 UTC string.  Effective minimum is 2023-05-12 for BTC/ETH.
    end_date   : ISO-8601 UTC string.
    timeframe  : candle interval, default '1h'.
    spread_bps : half-spread in bps applied symmetrically to mark price.
                 bid = mark × (1 − spread_bps/10000)
                 ask = mark × (1 + spread_bps/10000)
                 Hyperliquid BTC/ETH typically trades at 1–2 bps half-spread.

    Returns
    -------
    pd.DataFrame with DatetimeIndex (UTC, hourly) and columns:
        mark_price_hyperliquid       — full history from 2023-05-12 (8h-resampled for
                                   older data, exact 1h for the most recent ~208 days)
        best_bid_hyperliquid
        best_ask_hyperliquid
        funding_rate_raw_hyperliquid — 1h funding rate, full history from 2023-05-12

    Notes
    -----
    - OHLCV data: fetched via two candleSnapshot calls — 8h candles (covers
      all HL history since 2023-05-12, resampled to 1h via forward-fill) plus
      1h candles (most recent ≈ 208 days of exact prices, overrides the 8h).
      Full mark price coverage from 2023-05-12 onwards.
      Funding rate history is available in full from 2023-05-12 onwards.
    - Hyperliquid settles funding every hour (not every 8h like Binance).
      Aggregate to 8h by compounding: (1+r1)×(1+r2)×…×(1+r8) − 1.
    """
    coin        = asset.upper()
    half_spread = spread_bps / 10_000
    start_ms    = int(pd.Timestamp(start_date).timestamp() * 1_000)
    end_ms      = int(pd.Timestamp(end_date).timestamp()   * 1_000)

    print(f"\n{'='*60}")
    print(f"Fetching Hyperliquid data — {coin} | {start_date} → {end_date}")
    print(f"Spread assumption: ±{spread_bps} bps around mark price")
    print(f"{'='*60}")

    # ── 1. OHLCV (perp mark price) ────────────────────────────────────────
    print("\n[1/2] Perp OHLCV (8h full history + 1h recent, merged)…")
    ohlcv = _fetch_ohlcv_hl(coin, timeframe, start_ms)

    # ── 2. Funding rates ──────────────────────────────────────────────────
    print("\n[2/2] Funding rates (full history, paginated)…")
    funding = _fetch_funding_hl(coin, start_ms, end_ms)

    # ── Assemble: build a complete hourly grid ─────────────────────────────
    # Always use a strict 1h date_range — do NOT use funding.index directly.
    # HL settled funding every 8h early on (before switching to 1h), so the
    # funding index has 8h gaps.  We forward-fill the funding rate across
    # those gaps (last known settlement, no lookahead bias).
    if not funding.empty:
        data_start = funding.index[0]
    elif not ohlcv.empty:
        data_start = ohlcv.index[0]
    else:
        data_start = pd.Timestamp(start_date, tz="UTC")

    idx = pd.date_range(
        start=data_start,
        end=pd.Timestamp(end_date, tz="UTC") - pd.Timedelta(hours=1),
        freq="1h",
        tz="UTC",
    )

    df = pd.DataFrame(index=idx)
    df.index.name = "timestamp"

    # Mark price from OHLCV close (8h-resampled for history, 1h exact for recent)
    if not ohlcv.empty:
        df["mark_price_hyperliquid"] = ohlcv["close"].reindex(idx)
    else:
        df["mark_price_hyperliquid"] = float("nan")

    # Bid/ask derived from mark price
    df["best_bid_hyperliquid"] = df["mark_price_hyperliquid"] * (1 - half_spread)
    df["best_ask_hyperliquid"] = df["mark_price_hyperliquid"] * (1 + half_spread)

    # Funding rate — ffill across hours with no settlement (8h-era gaps)
    if not funding.empty:
        df["funding_rate_raw_hyperliquid"] = (
            funding["funding_rate_raw"].reindex(idx).ffill()
        )
    else:
        df["funding_rate_raw_hyperliquid"] = float("nan")

    df = df.sort_index()

    # ── Summary ──────────────────────────────────────────────────────────
    n_funding = df["funding_rate_raw_hyperliquid"].notna().sum()
    n_ohlcv   = df["mark_price_hyperliquid"].notna().sum()
    print(f"\nDone — {len(df):,} rows total")
    print(f"   Funding rows   : {n_funding:,}  "
          f"({df[df['funding_rate_raw_hyperliquid'].notna()].index[0].date() if n_funding else 'n/a'} → "
          f"{df[df['funding_rate_raw_hyperliquid'].notna()].index[-1].date() if n_funding else 'n/a'})")
    print(f"   OHLCV rows     : {n_ohlcv:,}  "
          f"({df[df['mark_price_hyperliquid'].notna()].index[0].date() if n_ohlcv else 'n/a'} → "
          f"{df[df['mark_price_hyperliquid'].notna()].index[-1].date() if n_ohlcv else 'n/a'})")
    print(f"   Null funding   : {df['funding_rate_raw_hyperliquid'].isna().sum():,}")

    return df


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    from pathlib import Path

    # UNIVERSE from strategy.py: strip USDT suffix for HL coin names
    UNIVERSE_HL = ["BTC", "ETH", "SOL", "BNB", "XRP", "DOGE", "AVAX"]

    parser = argparse.ArgumentParser(
        description="Fetch Hyperliquid hourly perp data for one or all universe assets."
    )
    parser.add_argument("--asset",   default=None,
                        help="Single coin ticker, e.g. BTC. Omit to fetch all 7.")
    parser.add_argument("--start",   default="2023-05-12T00:00:00Z",
                        help="Start date ISO-8601 UTC (default: 2023-05-12)")
    parser.add_argument("--end",     default="2026-01-01T00:00:00Z",
                        help="End date ISO-8601 UTC (default: 2026-01-01)")
    parser.add_argument("--spread",  type=float, default=2.0,
                        help="Half-spread in bps (default: 2.0)")
    args = parser.parse_args()

    assets = [args.asset] if args.asset else UNIVERSE_HL
    start_slug = args.start[:10]
    end_slug   = args.end[:10]

    failed = []
    for asset in assets:
        try:
            df = fetch_hyperliquid(
                asset=asset,
                start_date=args.start,
                end_date=args.end,
                spread_bps=args.spread,
            )
            # Save into per-currency folder: data/{ASSET}/hyperliquid_{ASSET}_...parquet
            out_dir = Path("data") / asset
            out_dir.mkdir(parents=True, exist_ok=True)
            out_path = out_dir / f"hyperliquid_{asset}_{start_slug}_{end_slug}.parquet"
            df.to_parquet(out_path)
            print(f"Saved → {out_path}\n")
        except Exception as exc:
            print(f"[ERROR] {asset}: {exc}")
            failed.append(asset)

    if failed:
        print(f"\nFailed assets: {failed}")
    else:
        print(f"\nAll {len(assets)} assets saved to data/{{ASSET}}/")
