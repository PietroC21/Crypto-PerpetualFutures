#!/usr/bin/env python3
"""
Download hourly open-interest history from OKX Rubik Stats API.

Endpoint:
  /api/v5/rubik/stat/contracts/open-interest-history

Returns arrays of:
  [ts_ms, oi_contracts, oi_coin, oi_usd]

Pagination: use `end` param (returns records OLDER than this timestamp).
Max 100 records per request. Only ~2 months of rolling data available.

Output CSVs:
  data/okx/raw/open_interest/{COIN}_USDT_SWAP_open_interest_1h.csv

Columns: timestamp, oi_contracts, oi_coin, oi_usd
"""
from __future__ import annotations
import time, sys, requests
from pathlib import Path
from datetime import datetime
import pandas as pd

COINS = ["BTC", "ETH", "BNB", "SOL", "XRP", "AVAX", "DOGE"]
PIPELINE_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PIPELINE_ROOT / "data" / "okx" / "raw"
OUT_DIR = RAW_DIR / "open_interest"

OKX_BASE = "https://www.okx.com"
RATE_SLEEP = 0.12

sess = requests.Session()
sess.headers.update({"Accept": "application/json"})


def download_oi_history(inst_id: str, period: str = "1H") -> list[dict]:
    """Download all available OI history by paginating backward."""
    records = []
    end_ts = None

    while True:
        time.sleep(RATE_SLEEP)
        params = {"instId": inst_id, "period": period}
        if end_ts:
            params["end"] = str(end_ts)
        resp = sess.get(
            f"{OKX_BASE}/api/v5/rubik/stat/contracts/open-interest-history",
            params=params, timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("code") != "0":
            break
        batch = data.get("data", [])
        if not batch:
            break
        for row in batch:
            records.append({
                "timestamp": int(row[0]),
                "oi_contracts": float(row[1]),
                "oi_coin": float(row[2]),
                "oi_usd": float(row[3]),
            })
        end_ts = int(batch[-1][0])
        if len(batch) < 100:
            break
    return records


def download_for_coin(coin: str):
    inst_id = f"{coin}-USDT-SWAP"
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    safe_name = inst_id.replace("-", "_")
    out_path = OUT_DIR / f"{safe_name}_open_interest_1h.csv"

    print(f"  {inst_id}:", end=" ")
    records = download_oi_history(inst_id)

    if not records:
        print("NO DATA")
        return

    df = pd.DataFrame(records)
    df = df.drop_duplicates(subset=["timestamp"]).sort_values("timestamp")
    df.to_csv(out_path, index=False)

    ts_min = datetime.utcfromtimestamp(df["timestamp"].min() / 1000)
    ts_max = datetime.utcfromtimestamp(df["timestamp"].max() / 1000)
    print(
        f"{len(df):,} records  "
        f"{ts_min.strftime('%Y-%m-%d')} -> {ts_max.strftime('%Y-%m-%d')}"
    )


def main():
    print("OKX Open Interest Download")
    print("=" * 60)
    for coin in COINS:
        download_for_coin(coin)
    print("\nDone.")


if __name__ == "__main__":
    main()
