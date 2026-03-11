#!/usr/bin/env python3
"""
Download hourly candlestick data from OKX REST API.

Uses two endpoints:
  /api/v5/market/candles        — recent data (last ~1440 bars)
  /api/v5/market/history-candles — older data (full history)

Both endpoints return arrays of:
  [ts_ms, open, high, low, close, vol, volCcy, volCcyQuote, confirm]

Pagination: use `after` param (return data BEFORE this timestamp).
Max 300 bars per request.

Output CSVs:
  data/okx/raw/candles_perp/{COIN}_USDT_SWAP_perp_candles_1h.csv
  data/okx/raw/candles_spot/{COIN}_USDT_spot_candles_1h.csv

Columns: timestamp, open, high, low, close, volume
(timestamp in milliseconds for consistency with OKX conventions)
"""
from __future__ import annotations
import time, sys, requests
from pathlib import Path
from datetime import datetime
import pandas as pd

COINS = ["BTC", "ETH", "BNB", "SOL", "XRP", "AVAX", "DOGE"]
PIPELINE_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PIPELINE_ROOT / "data" / "okx" / "raw"

OKX_BASE = "https://www.okx.com"
RATE_SLEEP = 0.12  # 20 req / 2 sec

sess = requests.Session()
sess.headers.update({"Accept": "application/json"})


def fetch_candles_page(inst_id: str, bar: str, after: int | None, endpoint: str) -> list:
    """Fetch one page of candles. Returns list of candle arrays."""
    time.sleep(RATE_SLEEP)
    url = f"{OKX_BASE}{endpoint}"
    params = {"instId": inst_id, "bar": bar, "limit": "300"}
    if after:
        params["after"] = str(after)
    resp = sess.get(url, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    if data.get("code") != "0":
        return []
    return data.get("data", [])


def download_all_candles(inst_id: str, bar: str = "1H",
                         stop_ts_ms: int | None = None) -> list[dict]:
    """
    Download full candle history for an instrument by paginating backward.

    First uses /market/candles for recent data, then /market/history-candles
    for older data.
    """
    records = []
    after = None  # start from most recent

    # Phase 1: /market/candles (recent)
    for _ in range(10):  # ~3000 bars max from this endpoint
        batch = fetch_candles_page(inst_id, bar, after, "/api/v5/market/candles")
        if not batch:
            break
        for c in batch:
            ts_ms = int(c[0])
            if stop_ts_ms and ts_ms < stop_ts_ms:
                continue
            records.append({
                "timestamp": ts_ms,
                "open": float(c[1]),
                "high": float(c[2]),
                "low": float(c[3]),
                "close": float(c[4]),
                "volume": float(c[5]),
            })
        after = int(batch[-1][0])  # oldest ts in this batch
        if len(batch) < 300:
            break

    sys.stdout.write(f"  recent: {len(records)} bars...")
    sys.stdout.flush()

    # Phase 2: /market/history-candles (older)
    page = 0
    while True:
        batch = fetch_candles_page(inst_id, bar, after, "/api/v5/market/history-candles")
        if not batch:
            break
        for c in batch:
            ts_ms = int(c[0])
            if stop_ts_ms and ts_ms < stop_ts_ms:
                continue
            records.append({
                "timestamp": ts_ms,
                "open": float(c[1]),
                "high": float(c[2]),
                "low": float(c[3]),
                "close": float(c[4]),
                "volume": float(c[5]),
            })
        after = int(batch[-1][0])
        page += 1
        if page % 20 == 0:
            sys.stdout.write(f"\r  {inst_id}: {len(records):,} bars (page {page})...")
            sys.stdout.flush()
        if len(batch) < 300:
            break
        # Safety: if we've hit the stop, break
        if stop_ts_ms and int(batch[-1][0]) <= stop_ts_ms:
            break

    return records


def download_candles_for_coin(coin: str):
    perp_inst = f"{coin}-USDT-SWAP"
    spot_inst = f"{coin}-USDT"

    for label, inst_id, out_dir in [
        ("perp", perp_inst, RAW_DIR / "candles_perp"),
        ("spot", spot_inst, RAW_DIR / "candles_spot"),
    ]:
        out_dir.mkdir(parents=True, exist_ok=True)
        safe_name = inst_id.replace("-", "_")
        out_path = out_dir / f"{safe_name}_{label}_candles_1h.csv"

        print(f"\n  {inst_id} ({label}):", end="")
        records = download_all_candles(inst_id)

        if not records:
            print(f" NO DATA")
            continue

        df = pd.DataFrame(records)
        df = df.drop_duplicates(subset=["timestamp"]).sort_values("timestamp")
        df.to_csv(out_path, index=False)

        ts_min = datetime.utcfromtimestamp(df["timestamp"].min() / 1000)
        ts_max = datetime.utcfromtimestamp(df["timestamp"].max() / 1000)
        print(
            f"\r  {inst_id} ({label}): {len(df):,} candles  "
            f"{ts_min.strftime('%Y-%m-%d')} -> {ts_max.strftime('%Y-%m-%d')}"
            + " " * 20
        )


def main():
    print("OKX Hourly Candle Download")
    print("=" * 60)
    for coin in COINS:
        print(f"\n{coin}:")
        download_candles_for_coin(coin)
    print("\nDone.")


if __name__ == "__main__":
    main()
