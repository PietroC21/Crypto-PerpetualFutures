"""
fetch_ob.py
-----------
Fetches real historical order book imbalance (OBI) from Binance Vision's
public data archive (data.binance.vision) using parallel downloads.

Data source
-----------
Binance Vision stores daily bookDepth snapshots for USDT-M perpetual futures:

    https://data.binance.vision/data/futures/um/daily/bookDepth/
        {SYMBOL}/{SYMBOL}-bookDepth-{YYYY-MM-DD}.zip

Each file contains order book snapshots taken roughly every 30 seconds.
Each snapshot has 10 rows — one per percentage-depth level:

    percentage  |  side  |  meaning
    ------------|--------|------------------------------------------
       -1       |  bid   |  cumulative bid depth within 1% of mid
       -2 … -5  |  bid   |  cumulative bid depth within N% of mid
       +1       |  ask   |  cumulative ask depth within 1% of mid
       +2 … +5  |  ask   |  cumulative ask depth within N% of mid

We use the ±1% cumulative depth (tightest, most liquid band):

    OBI = (bid_depth_1pct − ask_depth_1pct)
          ─────────────────────────────────────
          (bid_depth_1pct + ask_depth_1pct)

  +1  →  fully bid-heavy  →  long-side crowding → positive funding likely to persist
  −1  →  fully ask-heavy  →  short-side crowding → negative funding likely to persist
   0  →  balanced

Pre-settlement window
---------------------
Each 8h funding settlement occurs at 00:00, 08:00, 16:00 UTC.
We average all OBI snapshots in the 15-minute window before each settlement
(typically ~30 snapshots):

    Snapshots 23:45–00:00  →  averaged OBI assigned to settlement 00:00
    Snapshots 07:45–08:00  →  averaged OBI assigned to settlement 08:00
    Snapshots 15:45–16:00  →  averaged OBI assigned to settlement 16:00

Parallelisation
---------------
Binance Vision is a public CDN (static S3 files) — no API rate limits.
Downloads run in parallel using ThreadPoolExecutor (default: 20 workers).
Peak RAM usage: ~20 workers × ~3 MB/file ≈ 60 MB. Safe on any machine.

Expected runtime: ~6-8 min for full 5-year history across 7 symbols.

Output
------
data/raw/binance/order_book/<SYMBOL>.parquet
  Index:   datetime (UTC, 8h settlement time — 00:00/08:00/16:00)
  Columns: obi, bid_depth, ask_depth, n_snapshots

Usage
-----
    # Full pull — all 7 universe symbols (~6-8 min, VPN required)
    python src/fetch_ob.py

    # Subset
    python src/fetch_ob.py --symbols BTC ETH SOL

    # Custom date range
    python src/fetch_ob.py --start 2022-01-01 --end 2023-01-01

    # Adjust parallelism (default 20, reduce if connection is unstable)
    python src/fetch_ob.py --workers 10

Notes
-----
- VPN required: Binance blocks US IPs.
- Data availability: bookDepth archive typically starts mid-2020.
  Dates before listing or before archive start return 404 and are skipped.
- Idempotent: re-running resumes from the last saved date per symbol.
"""

import argparse
import io
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests
from tqdm import tqdm

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

ROOT    = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "data" / "raw" / "binance" / "order_book"

UNIVERSE = [
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT",
    "BNBUSDT",
    "XRPUSDT",
    "DOGEUSDT",
    "AVAXUSDT",
]

DEFAULT_START       = "2020-09-01"   # bookDepth archive rarely has data before mid-2020
DEFAULT_WORKERS     = 20             # parallel download threads (CDN, no rate limit)
VISION_BASE         = "https://data.binance.vision/data/futures/um/daily/bookDepth"
PRESETTLEMENT_HOURS = {7, 15, 23}    # pre-settlement window start hours (HH:45 UTC)


# ---------------------------------------------------------------------------
# Download & parse (called from worker threads — must be thread-safe)
# ---------------------------------------------------------------------------

def download_day(symbol: str, date_str: str) -> pd.DataFrame | None:
    """
    Download one day's bookDepth ZIP from Binance Vision and parse it.

    Returns DataFrame with columns [timestamp, percentage, depth, notional],
    or None if the file does not exist (404) or download fails.
    Thread-safe: uses only local variables and immutable constants.
    """
    url = f"{VISION_BASE}/{symbol}/{symbol}-bookDepth-{date_str}.zip"
    try:
        resp = requests.get(url, timeout=60)
    except requests.RequestException:
        return None

    if resp.status_code == 404:
        return None
    if not resp.ok:
        return None

    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        with zf.open(zf.namelist()[0]) as f:
            df = pd.read_csv(f, parse_dates=["timestamp"])

    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    return df


# ---------------------------------------------------------------------------
# Compute OBI for pre-settlement windows (thread-safe, pure function)
# ---------------------------------------------------------------------------

def compute_obi_windows(df: pd.DataFrame) -> pd.DataFrame:
    """
    From one day's bookDepth data, compute average OBI for each pre-settlement
    window (15 minutes before each 8h funding settlement).

    Uses cumulative ±1% depth:
        OBI = (bid_1pct − ask_1pct) / (bid_1pct + ask_1pct)

    Returns DataFrame indexed by settlement datetime with columns:
        obi, bid_depth, ask_depth, n_snapshots
    """
    df = df.copy()
    df["window_start"] = df["timestamp"].dt.floor("15min")

    # Keep only the three pre-settlement windows: HH:45 for HH in {7, 15, 23}
    df = df[
        (df["window_start"].dt.minute == 45) &
        (df["window_start"].dt.hour.isin(PRESETTLEMENT_HOURS))
    ].copy()

    if df.empty:
        return pd.DataFrame()

    # Settlement = window_start + 15 min → 23:45→00:00, 07:45→08:00, 15:45→16:00
    df["settlement"] = df["window_start"] + pd.Timedelta(minutes=15)

    # Extract ±1% cumulative depth per snapshot
    bids = (
        df[df["percentage"] == -1][["timestamp", "settlement", "depth"]]
        .rename(columns={"depth": "bid"})
    )
    asks = (
        df[df["percentage"] == 1][["timestamp", "settlement", "depth"]]
        .rename(columns={"depth": "ask"})
    )

    snaps = bids.merge(asks, on=["timestamp", "settlement"])
    if snaps.empty:
        return pd.DataFrame()

    total = snaps["bid"] + snaps["ask"]
    snaps["obi"] = (snaps["bid"] - snaps["ask"]) / total.replace(0, float("nan"))

    result = snaps.groupby("settlement").agg(
        obi=("obi", "mean"),
        bid_depth=("bid", "mean"),
        ask_depth=("ask", "mean"),
        n_snapshots=("obi", "count"),
    )
    result.index.name = "datetime"
    return result


def fetch_one(symbol: str, date_str: str) -> tuple[str, pd.DataFrame]:
    """Worker function: download + process one (symbol, date). Thread-safe."""
    raw = download_day(symbol, date_str)
    if raw is None:
        return symbol, pd.DataFrame()
    return symbol, compute_obi_windows(raw)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def date_range(start: str, end: str) -> list[str]:
    dates   = []
    current = datetime.strptime(start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    stop    = datetime.strptime(end,   "%Y-%m-%d").replace(tzinfo=timezone.utc)
    while current <= stop:
        dates.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)
    return dates


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def run(symbols: list[str], start: str, end: str | None, max_workers: int) -> None:
    end = end or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # --- Per-symbol resume: determine fetch_start for each symbol ---
    existing_data: dict[str, pd.DataFrame] = {}
    tasks: list[tuple[str, str]] = []

    for symbol in symbols:
        out = OUT_DIR / f"{symbol}.parquet"
        if out.exists():
            existing = pd.read_parquet(out)
            existing_data[symbol] = existing
            last_saved  = existing.index.max().date()
            fetch_start = (
                pd.Timestamp(last_saved) + pd.Timedelta(days=1)
            ).strftime("%Y-%m-%d")
            if fetch_start > end:
                print(f"  [{symbol}] already up to date (last: {last_saved})")
                continue
            print(f"  [{symbol}] resuming from {fetch_start}")
        else:
            fetch_start = start

        for date_str in date_range(fetch_start, end):
            tasks.append((symbol, date_str))

    if not tasks:
        print("\nAll symbols up to date.")
        return

    print(
        f"\nFetching OBI from Binance Vision  |  {start} → {end}"
        f"\n{len(symbols)} symbols  |  {len(tasks):,} files  |  {max_workers} workers\n"
    )

    # --- Parallel download ---
    new_frames: dict[str, list[pd.DataFrame]] = {sym: [] for sym in symbols}

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(fetch_one, sym, date): (sym, date)
            for sym, date in tasks
        }
        for future in tqdm(as_completed(futures), total=len(futures), desc="Downloading"):
            sym, obi = future.result()
            if not obi.empty:
                new_frames[sym].append(obi)

    # --- Save per symbol ---
    print("\nSaving...")
    for symbol in symbols:
        out    = OUT_DIR / f"{symbol}.parquet"
        frames = []
        if symbol in existing_data:
            frames.append(existing_data[symbol])
        frames.extend(new_frames[symbol])

        if not frames:
            print(f"  [{symbol}] no data (symbol may not have bookDepth coverage)")
            continue

        combined = pd.concat(frames)
        combined = combined[~combined.index.duplicated(keep="last")].sort_index()
        combined.to_parquet(out, index=True)

        span     = f"{combined.index.min().date()} → {combined.index.max().date()}"
        null_obi = combined["obi"].isna().sum()
        n_new    = sum(len(f) for f in new_frames[symbol])
        print(f"  [{symbol}] {len(combined):>6} rows  |  {span}  |  {null_obi} null OBI  |  +{n_new} new")

    print(f"\nDone. OBI files saved to {OUT_DIR}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Fetch pre-settlement OBI from Binance Vision (parallel)."
    )
    parser.add_argument(
        "--symbols", nargs="+", default=None,
        help="Subset of symbols, e.g. BTC ETH SOL. Defaults to full universe."
    )
    parser.add_argument(
        "--start", default=DEFAULT_START,
        help=f"Start date YYYY-MM-DD (default: {DEFAULT_START})"
    )
    parser.add_argument(
        "--end", default=None,
        help="End date YYYY-MM-DD (default: today)"
    )
    parser.add_argument(
        "--workers", type=int, default=DEFAULT_WORKERS,
        help=f"Parallel download threads (default: {DEFAULT_WORKERS}). "
             "Reduce if connection is unstable."
    )
    args = parser.parse_args()

    if args.symbols:
        lookup   = {s.replace("USDT", ""): s for s in UNIVERSE}
        selected = [lookup[s.upper()] for s in args.symbols if s.upper() in lookup]
        if not selected:
            raise SystemExit(f"No valid symbols. Available: {list(lookup.keys())}")
        symbols = selected
    else:
        symbols = UNIVERSE

    run(symbols, args.start, args.end, args.workers)
