#!/usr/bin/env python3
"""
Download raw trade data from OKX and aggregate into hourly OHLCV + VWAP.

Uses two strategies:
  1. OKX Export API (module=1) — bulk trade ZIPs requiring browser cookies.
     Provides full history back to ~Sept 2021.
  2. REST API fallback — /api/v5/market/history-trades (last ~3 months only).

OKX timestamps are in MILLISECONDS throughout.

Output CSVs (aggregated hourly bars):
  data/okx/raw/trades_perp/{COIN}_USDT_SWAP_perp_trades_1h.csv
  data/okx/raw/trades_spot/{COIN}_USDT_spot_trades_1h.csv

Columns: timestamp, open, high, low, close, volume, vwap
(timestamp in milliseconds, consistent with OKX conventions)
"""
from __future__ import annotations

import io
import json
import sys
import time
import zipfile
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import requests

# Add parent to path for src imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.okx_client import OKXClient
from src.config import (
    COINS,
    MODULE_TRADE_HISTORY,
    OKX_BASE_URL,
    RATE_LIMIT_EXPORT,
)

PIPELINE_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PIPELINE_ROOT / "data" / "okx" / "raw"
RATE_SLEEP = 0.12  # 20 req / 2 sec

HOUR_MS = 3600 * 1000


# ═══════════════════════════════════════════════════════════════════════════
#  Aggregation helpers (millisecond timestamps)
# ═══════════════════════════════════════════════════════════════════════════

def aggregate_trades_to_hourly(trades: list[dict]) -> list[dict]:
    """
    Aggregate raw trades into hourly OHLCV + VWAP bars.

    Each trade: {timestamp_ms, price, amount}
    Output: {timestamp, open, high, low, close, volume, vwap, pv_sum}

    Timestamps in milliseconds (OKX convention).
    VWAP = sum(price * amount) / sum(amount)
    """
    if not trades:
        return []

    buckets: dict[int, list[dict]] = defaultdict(list)
    for t in trades:
        hour_ts = t["timestamp_ms"] // HOUR_MS * HOUR_MS
        buckets[hour_ts].append(t)

    bars = []
    for hour_ts in sorted(buckets.keys()):
        bucket = sorted(buckets[hour_ts], key=lambda x: x["timestamp_ms"])

        prices = [t["price"] for t in bucket]
        amounts = [t["amount"] for t in bucket]

        total_volume = sum(amounts)
        pv_sum = sum(p * a for p, a in zip(prices, amounts))

        bars.append({
            "timestamp": hour_ts,
            "open": prices[0],
            "high": max(prices),
            "low": min(prices),
            "close": prices[-1],
            "volume": total_volume,
            "vwap": pv_sum / total_volume if total_volume > 0 else prices[-1],
            "pv_sum": pv_sum,
        })

    return bars


def merge_partial_bars(bars: list[dict]) -> pd.DataFrame:
    """
    Merge hourly bars that may have been split across download chunks.
    Re-aggregates OHLCV + VWAP properly using pv_sum.
    """
    df = pd.DataFrame(bars)
    if df.empty:
        return df

    merged = (
        df.sort_values(["timestamp", "open"])
        .groupby("timestamp")
        .agg(
            open=("open", "first"),
            high=("high", "max"),
            low=("low", "min"),
            close=("close", "last"),
            volume=("volume", "sum"),
            pv_sum=("pv_sum", "sum"),
        )
        .reset_index()
    )

    merged["vwap"] = merged["pv_sum"] / merged["volume"]
    merged.loc[merged["volume"] == 0, "vwap"] = merged.loc[merged["volume"] == 0, "close"]
    merged = merged.drop(columns=["pv_sum"])

    return merged.sort_values("timestamp").reset_index(drop=True)


# ═══════════════════════════════════════════════════════════════════════════
#  OKX Export API download
# ═══════════════════════════════════════════════════════════════════════════

def parse_okx_trade_csv(csv_text: str) -> list[dict]:
    """
    Parse an OKX trade CSV (from export ZIP).

    OKX trade CSV header: tradeId,side,sz,px,ts
    Returns list of {timestamp_ms, price, amount}.
    """
    records = []
    lines = csv_text.strip().split("\n")
    if not lines:
        return records

    # Skip header if present
    start = 1 if lines[0].startswith("tradeId") or lines[0].startswith("trade_id") else 0

    for line in lines[start:]:
        parts = line.strip().split(",")
        if len(parts) < 5:
            continue
        try:
            records.append({
                "timestamp_ms": int(parts[4]),
                "price": float(parts[3]),
                "amount": abs(float(parts[2])),
            })
        except (ValueError, IndexError):
            continue

    return records


def download_via_export_api(
    client: OKXClient,
    inst_type: str,
    inst_family: str,
    begin_date: datetime,
    end_date: datetime,
) -> list[dict]:
    """
    Use OKX export API (module=1 for trades) to download bulk trade data.

    Downloads in monthly chunks to avoid timeouts.
    Returns list of hourly bars (not raw trades — aggregated per chunk).
    """
    all_bars: list[dict] = []
    cursor = begin_date
    chunk_count = 0

    while cursor < end_date:
        chunk_end = min(cursor + timedelta(days=30), end_date)
        begin_ms = str(int(cursor.timestamp() * 1000))
        end_ms = str(int(chunk_end.timestamp() * 1000))

        try:
            result = client.export_request_download(
                module=MODULE_TRADE_HISTORY,
                inst_type=inst_type,
                inst_families=[inst_family],
                begin_ms=begin_ms,
                end_ms=end_ms,
                date_aggr="daily",
            )
        except Exception as exc:
            print(f"\n    Export API error for {inst_family} {cursor.strftime('%Y-%m')}: {exc}")
            cursor = chunk_end
            continue

        details = result.get("details", [])
        chunk_trades = 0

        for detail in details:
            url = detail.get("downloadLink", "")
            if not url:
                continue
            try:
                content = client.download_file(url)
                with zipfile.ZipFile(io.BytesIO(content)) as zf:
                    for csv_name in zf.namelist():
                        if csv_name.endswith(".csv"):
                            csv_text = zf.read(csv_name).decode("utf-8", errors="replace")
                            trades = parse_okx_trade_csv(csv_text)
                            if trades:
                                bars = aggregate_trades_to_hourly(trades)
                                all_bars.extend(bars)
                                chunk_trades += len(trades)
                                del trades  # free memory
            except Exception as exc:
                print(f"\n    Download error: {exc}")
                continue

        chunk_count += 1
        sys.stdout.write(
            f"\r    Export API: {chunk_count} chunks, "
            f"{len(all_bars):,} bars ({chunk_trades:,} trades this chunk)..."
        )
        sys.stdout.flush()

        cursor = chunk_end

    return all_bars


# ═══════════════════════════════════════════════════════════════════════════
#  REST API fallback (recent ~3 months only)
# ═══════════════════════════════════════════════════════════════════════════

def download_via_rest_api(inst_id: str) -> list[dict]:
    """
    Download recent trades via REST API (/api/v5/market/history-trades).

    Paginates backward. Returns raw trade dicts.
    Limited to approximately 3 months of history.
    """
    sess = requests.Session()
    sess.headers.update({"Accept": "application/json"})

    records: list[dict] = []
    after = None
    max_pages = 500  # safety limit (~50K trades)

    for page in range(max_pages):
        time.sleep(RATE_SLEEP)
        params: dict = {"instId": inst_id, "limit": "100"}
        if after:
            params["after"] = str(after)

        try:
            resp = sess.get(
                f"{OKX_BASE_URL}/api/v5/market/history-trades",
                params=params, timeout=30,
            )
            data = resp.json()
        except Exception:
            break

        if data.get("code") != "0":
            break

        batch = data.get("data", [])
        if not batch:
            break

        for t in batch:
            records.append({
                "timestamp_ms": int(t["ts"]),
                "price": float(t["px"]),
                "amount": abs(float(t["sz"])),
            })

        after = batch[-1]["tradeId"]
        if page % 50 == 0 and page > 0:
            sys.stdout.write(f"\r    REST API: {len(records):,} trades (page {page})...")
            sys.stdout.flush()

        if len(batch) < 100:
            break

    return records


# ═══════════════════════════════════════════════════════════════════════════
#  Main download logic per coin
# ═══════════════════════════════════════════════════════════════════════════

def download_trades_for_coin(
    coin: str,
    client: OKXClient | None = None,
    start_date: str = "2021-09-01",
    use_export: bool = True,
):
    """
    Download trade data for a single coin (both perp and spot).

    If client is provided and use_export=True, uses the export API.
    Otherwise falls back to REST API (limited history).
    """
    for label, inst_type, inst_id, inst_family, out_dir in [
        ("perp", "SWAP", f"{coin}-USDT-SWAP", f"{coin}-USDT",
         RAW_DIR / "trades_perp"),
        ("spot", "SPOT", f"{coin}-USDT", f"{coin}-USDT",
         RAW_DIR / "trades_spot"),
    ]:
        out_dir.mkdir(parents=True, exist_ok=True)
        safe_name = inst_id.replace("-", "_")
        out_path = out_dir / f"{safe_name}_{label}_trades_1h.csv"

        print(f"\n  {inst_id} ({label}):", end="")

        all_bars: list[dict] = []

        if client and use_export:
            # Export API: full history in monthly chunks
            begin = datetime.strptime(start_date, "%Y-%m-%d")
            end = datetime.utcnow()
            all_bars = download_via_export_api(
                client, inst_type, inst_family, begin, end
            )
        else:
            # REST API fallback: recent ~3 months only
            raw_trades = download_via_rest_api(inst_id)
            if raw_trades:
                all_bars = aggregate_trades_to_hourly(raw_trades)
                print(f" {len(raw_trades):,} trades from REST API", end="")

        if not all_bars:
            print(f" NO TRADE DATA")
            continue

        # Merge partial bars
        df = merge_partial_bars(all_bars)
        df.to_csv(out_path, index=False)

        ts_min = datetime.utcfromtimestamp(df["timestamp"].min() / 1000)
        ts_max = datetime.utcfromtimestamp(df["timestamp"].max() / 1000)
        print(
            f"\r  {inst_id} ({label}): {len(df):,} hourly bars  "
            f"{ts_min.strftime('%Y-%m-%d')} -> {ts_max.strftime('%Y-%m-%d')}"
            + " " * 20
        )


# ═══════════════════════════════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════════════════════════════

def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Download OKX trade data and aggregate to hourly OHLCV + VWAP."
    )
    parser.add_argument(
        "--coins", nargs="*", default=None,
        help="Download only these coins (default: all 7).",
    )
    parser.add_argument(
        "--cookies", default=None,
        help="Path to JSON file with OKX browser cookies (for export API).",
    )
    parser.add_argument(
        "--start", default="2021-09-01",
        help="Start date for download (default: 2021-09-01).",
    )
    parser.add_argument(
        "--rest-only", action="store_true",
        help="Use REST API only (no export API, ~3 months of data).",
    )
    args = parser.parse_args()

    coins = args.coins or COINS

    # Set up client with cookies if provided
    client = None
    use_export = not args.rest_only
    if args.cookies and use_export:
        cookie_path = Path(args.cookies)
        if cookie_path.exists():
            with open(cookie_path) as f:
                cookies = json.load(f)
            client = OKXClient(session_cookies=cookies)
            print(f"Loaded cookies from {cookie_path}")
        else:
            print(f"WARNING: Cookie file not found: {cookie_path}")
            print("Falling back to REST API (limited history).")
            use_export = False
    elif use_export and not args.rest_only:
        print("No --cookies provided. Using REST API (limited to ~3 months).")
        use_export = False

    print("OKX Trade Data Download → Hourly OHLCV + VWAP")
    print("=" * 60)
    for coin in coins:
        print(f"\n{coin}:")
        download_trades_for_coin(
            coin,
            client=client,
            start_date=args.start,
            use_export=use_export,
        )
    print("\nDone.")


if __name__ == "__main__":
    main()
