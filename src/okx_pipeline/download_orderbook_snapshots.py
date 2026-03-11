#!/usr/bin/env python3
"""
Download historical order book data from OKX and extract best bid/ask.

Uses the OKX API:
  GET /api/v5/public/market-data-history
    module=4 (50-level orderbook), instType=SWAP/SPOT, dateAggrType=daily

IMPORTANT: This API is GEO-RESTRICTED and returns empty results from
US IP addresses. You may need a VPN to use this script.

The API returns download URLs for ZIP files containing CSV orderbook data.
We download each ZIP, extract the CSV, and compute best bid/ask per hour.

Output CSVs:
  data/okx/raw/orderbook_snapshots/{COIN}_USDT_SWAP_perp_bid_ask_1h.csv
  data/okx/raw/orderbook_snapshots/{COIN}_USDT_spot_bid_ask_1h.csv

Columns: timestamp, best_bid, best_ask
  timestamp = Unix milliseconds (OKX convention)
"""
from __future__ import annotations

import io
import sys
import time
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests

# ── Configuration ─────────────────────────────────────────────────────────────

COINS = ["BTC", "ETH", "BNB", "SOL", "XRP", "AVAX", "DOGE"]
PIPELINE_ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = PIPELINE_ROOT / "data" / "okx" / "raw" / "orderbook_snapshots"

OKX_BASE = "https://www.okx.com"
RATE_SLEEP = 0.15

# OKX data availability: Order Book L2 from March 2023 onwards
START_DATE = datetime(2023, 3, 1, tzinfo=timezone.utc)

# Module 4 = 50-level orderbook (smallest files)
MODULE = "4"

MARKETS = [
    {"instType": "SWAP", "suffix": "SWAP", "label": "perp", "instFmt": "{coin}-USDT-SWAP"},
    {"instType": "SPOT", "suffix": "",      "label": "spot", "instFmt": "{coin}-USDT"},
]

sess = requests.Session()
sess.headers.update({"Accept": "application/json"})


# ── Helpers ───────────────────────────────────────────────────────────────────

def get_download_urls(
    inst_type: str,
    inst_id: str,
    begin: str,
    end: str,
) -> list[dict]:
    """Call OKX market-data-history API to get download URLs."""
    resp = sess.get(
        f"{OKX_BASE}/api/v5/public/market-data-history",
        params={
            "module": MODULE,
            "instType": inst_type,
            "dateAggrType": "daily",
            "begin": begin,
            "end": end,
            "instIdList": inst_id,
        },
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    if data.get("code") != "0":
        return []
    if not data.get("data"):
        return []
    return data["data"][0].get("details", [])


def download_and_extract_bid_ask(file_url: str) -> list[dict]:
    """Download a ZIP file and extract best bid/ask per hour from orderbook CSV."""
    try:
        resp = sess.get(file_url, timeout=120)
        if resp.status_code != 200:
            return []
    except Exception:
        return []

    records = []
    try:
        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            for name in zf.namelist():
                if not name.endswith(".csv"):
                    continue
                with zf.open(name) as f:
                    df = pd.read_csv(f)

                # OKX orderbook CSV format varies, but typically:
                # timestamp, side, price, size, ...
                # We need to find the best bid and ask per hour

                if "ts" in df.columns:
                    ts_col = "ts"
                elif "timestamp" in df.columns:
                    ts_col = "timestamp"
                else:
                    continue

                if "side" in df.columns and "px" in df.columns:
                    # Standard format: ts, side (bid/ask), px, sz
                    bids = df[df["side"] == "bid"].copy()
                    asks = df[df["side"] == "ask"].copy()
                elif "bids_0_px" in df.columns:
                    # Pre-processed format with top-of-book
                    df["hour_ts"] = (df[ts_col].astype(int) // 3600000) * 3600000
                    for hour_ts, group in df.groupby("hour_ts"):
                        last = group.iloc[-1]
                        records.append({
                            "timestamp": int(hour_ts),
                            "best_bid": float(last["bids_0_px"]),
                            "best_ask": float(last["asks_0_px"]),
                        })
                    continue
                else:
                    # Try to parse whatever format we find
                    continue

                # Floor to hour and get last snapshot per hour
                bids[ts_col] = bids[ts_col].astype(int)
                asks[ts_col] = asks[ts_col].astype(int)
                bids["hour_ts"] = (bids[ts_col] // 3600000) * 3600000
                asks["hour_ts"] = (asks[ts_col] // 3600000) * 3600000

                best_bids = bids.sort_values(ts_col).groupby("hour_ts").agg(
                    best_bid=("px", "last")
                ).reset_index()
                best_asks = asks.sort_values(ts_col).groupby("hour_ts").agg(
                    best_ask=("px", "last")
                ).reset_index()

                merged = best_bids.merge(best_asks, on="hour_ts", how="outer")
                for _, row in merged.iterrows():
                    records.append({
                        "timestamp": int(row["hour_ts"]),
                        "best_bid": float(row["best_bid"]) if pd.notna(row["best_bid"]) else None,
                        "best_ask": float(row["best_ask"]) if pd.notna(row["best_ask"]) else None,
                    })

    except Exception as exc:
        sys.stdout.write(f"\n  WARN: Error processing ZIP: {exc}\n")

    return records


def download_coin_market(coin: str, market: dict) -> list[dict]:
    """Download all available orderbook data for one coin+market."""
    inst_id = market["instFmt"].format(coin=coin)
    inst_type = market["instType"]
    label = market["label"]

    now = datetime.now(timezone.utc)
    all_records = []

    # Process month by month
    current = START_DATE
    while current < now:
        month_end = min(
            (current.replace(day=1) + timedelta(days=32)).replace(day=1) - timedelta(days=1),
            now
        )
        begin = current.strftime("%Y%m%d")
        end = month_end.strftime("%Y%m%d")

        time.sleep(RATE_SLEEP)
        details = get_download_urls(inst_type, inst_id, begin, end)

        if not details:
            sys.stdout.write(
                f"\r    {inst_id} {label}: {current.strftime('%Y-%m')} — no data (geo-restricted?)"
            )
            sys.stdout.flush()
            # Move to next month
            current = (current.replace(day=1) + timedelta(days=32)).replace(day=1)
            continue

        sys.stdout.write(
            f"\r    {inst_id} {label}: {current.strftime('%Y-%m')} — {len(details)} files"
        )
        sys.stdout.flush()

        for detail in details:
            href = detail.get("fileHref")
            if not href or detail.get("state") != "finished":
                continue
            records = download_and_extract_bid_ask(href)
            all_records.extend(records)

        current = (current.replace(day=1) + timedelta(days=32)).replace(day=1)

    return all_records


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    print("OKX Orderbook Snapshot Download (Best Bid/Ask)")
    print("=" * 60)
    print("WARNING: This API is geo-restricted from US IPs.")
    print("         If you see empty results, try using a VPN.")
    print("=" * 60)

    # Quick connectivity test
    details = get_download_urls("SWAP", "BTC-USDT-SWAP", "20250301", "20250305")
    if not details:
        print("\nERROR: API returned no data. Likely geo-restricted from your IP.")
        print("       Please use a VPN and try again.")
        sys.exit(1)
    else:
        print(f"  API test OK: {len(details)} files available\n")

    for coin in COINS:
        print(f"\n  {coin}:")
        for market in MARKETS:
            label = market["label"]
            suffix = market["suffix"]
            if suffix:
                out_name = f"{coin}_USDT_{suffix}_{label}_bid_ask_1h.csv"
            else:
                out_name = f"{coin}_USDT_{label}_bid_ask_1h.csv"
            out_path = OUT_DIR / out_name

            records = download_coin_market(coin, market)

            if not records:
                print(f"\n    {label}: NO DATA")
                continue

            df = pd.DataFrame(records).dropna()
            df = df.drop_duplicates(subset=["timestamp"]).sort_values("timestamp")
            df.to_csv(out_path, index=False)

            ts_min = datetime.utcfromtimestamp(df["timestamp"].min() / 1000)
            ts_max = datetime.utcfromtimestamp(df["timestamp"].max() / 1000)
            print(
                f"\n    {label}: {len(df):,} records  "
                f"{ts_min.strftime('%Y-%m-%d')} -> {ts_max.strftime('%Y-%m-%d')}"
            )

    print("\nDone.")


if __name__ == "__main__":
    main()
