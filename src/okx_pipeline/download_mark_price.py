#!/usr/bin/env python3
"""
Download hourly mark-price candle data from OKX REST API.

Endpoints:
  /api/v5/market/mark-price-candles         — recent (~1440 bars)
  /api/v5/market/history-mark-price-candles  — full history (back to ~Jan 2020)

Response arrays have 6 fields (no volume):
  [ts_ms, open, high, low, close, confirm]

Pagination: use `after` param (return data BEFORE this timestamp).
Max 100 bars per request for history endpoint.

Output CSVs:
  data/okx/raw/mark_price_candles/{COIN}_USDT_SWAP_mark_price_1h.csv

Columns: timestamp, open, high, low, close
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
OUT_DIR = RAW_DIR / "mark_price_candles"

OKX_BASE = "https://www.okx.com"
RATE_SLEEP = 0.12

sess = requests.Session()
sess.headers.update({"Accept": "application/json"})


def fetch_page(inst_id: str, bar: str, after: int | None, endpoint: str,
               limit: int = 100) -> list:
    """Fetch one page of mark-price candles."""
    time.sleep(RATE_SLEEP)
    url = f"{OKX_BASE}{endpoint}"
    params = {"instId": inst_id, "bar": bar, "limit": str(limit)}
    if after:
        params["after"] = str(after)
    resp = sess.get(url, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    if data.get("code") != "0":
        return []
    return data.get("data", [])


def download_all_mark_price(inst_id: str, bar: str = "1H") -> list[dict]:
    """Download full mark-price candle history by paginating backward."""
    records = []
    after = None

    # Phase 1: /market/mark-price-candles (recent, up to 300 per page)
    for _ in range(10):
        batch = fetch_page(inst_id, bar, after,
                           "/api/v5/market/mark-price-candles", limit=300)
        if not batch:
            break
        for c in batch:
            records.append({
                "timestamp": int(c[0]),
                "open": float(c[1]),
                "high": float(c[2]),
                "low": float(c[3]),
                "close": float(c[4]),
            })
        after = int(batch[-1][0])
        if len(batch) < 300:
            break

    sys.stdout.write(f"  recent: {len(records)} bars...")
    sys.stdout.flush()

    # Phase 2: /market/history-mark-price-candles (older, max 100 per page)
    page = 0
    while True:
        batch = fetch_page(inst_id, bar, after,
                           "/api/v5/market/history-mark-price-candles",
                           limit=100)
        if not batch:
            break
        for c in batch:
            records.append({
                "timestamp": int(c[0]),
                "open": float(c[1]),
                "high": float(c[2]),
                "low": float(c[3]),
                "close": float(c[4]),
            })
        after = int(batch[-1][0])
        page += 1
        if page % 20 == 0:
            sys.stdout.write(
                f"\r  {inst_id}: {len(records):,} bars (page {page})..."
            )
            sys.stdout.flush()
        if len(batch) < 100:
            break
    return records


def download_for_coin(coin: str):
    inst_id = f"{coin}-USDT-SWAP"
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    safe_name = inst_id.replace("-", "_")
    out_path = OUT_DIR / f"{safe_name}_mark_price_1h.csv"

    print(f"\n  {inst_id}:", end="")
    records = download_all_mark_price(inst_id)

    if not records:
        print(" NO DATA")
        return

    df = pd.DataFrame(records)
    df = df.drop_duplicates(subset=["timestamp"]).sort_values("timestamp")
    df.to_csv(out_path, index=False)

    ts_min = datetime.utcfromtimestamp(df["timestamp"].min() / 1000)
    ts_max = datetime.utcfromtimestamp(df["timestamp"].max() / 1000)
    print(
        f"\r  {inst_id}: {len(df):,} candles  "
        f"{ts_min.strftime('%Y-%m-%d')} -> {ts_max.strftime('%Y-%m-%d')}"
        + " " * 20
    )


def main():
    print("OKX Mark Price Candle Download")
    print("=" * 60)
    for coin in COINS:
        print(f"\n{coin}:")
        download_for_coin(coin)
    print("\nDone.")


if __name__ == "__main__":
    main()
