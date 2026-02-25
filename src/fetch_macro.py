"""
fetch_macro.py
--------------
Pulls macro data needed for the funding-rate carry strategy:

  1. VIX daily close       → data/raw/macro/vix_daily.csv       [Bloomberg]
  2. SPY daily OHLCV       → data/raw/macro/spy_daily.csv       [Bloomberg]
  3. USD 3-month T-bill    → data/raw/macro/fred_rfr.csv        [FRED, free]

HOW TO RUN
----------
Parts 1 & 2 require the Bloomberg Terminal to be open and logged in.
Run this script on the university Bloomberg computer:

    # Install dependencies first (one-time, on the Bloomberg computer):
    pip install pdblp pandas requests

    # Then run:
    python fetch_macro.py

Part 3 (FRED) hits a public HTTP endpoint and can run anywhere.
Use --fred-only if you want to run that part on your own machine.

    python fetch_macro.py --fred-only

OUTPUT
------
Three CSV files are saved to ./macro_output/ (same folder as the script).
Copy them to:  data/raw/macro/  on your main machine.

SAMPLE PERIOD
-------------
Jan 2020 – Dec 2025 (matches the crypto backtest window).
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

START = "2020-01-01"
END   = "2025-12-31"

# Output lands here — copy these 3 files back to data/raw/macro/ on your machine
OUT_DIR = Path(__file__).resolve().parent / "macro_output"


# ---------------------------------------------------------------------------
# Bloomberg (pdblp)
# ---------------------------------------------------------------------------

def fetch_bloomberg(start: str, end: str) -> None:
    """Pull VIX and SPY from Bloomberg terminal using pdblp."""
    try:
        import pdblp
    except ImportError:
        raise SystemExit(
            "\n[ERROR] pdblp not installed.\n"
            "Run:  pip install pdblp\n"
            "Then retry.\n"
        )

    # Format dates for Bloomberg: YYYYMMDD
    bbg_start = start.replace("-", "")
    bbg_end   = end.replace("-", "")

    print("Connecting to Bloomberg Terminal...")
    con = pdblp.BCon(debug=False, port=8194, timeout=5000)
    try:
        con.start()
    except Exception as e:
        raise SystemExit(
            f"\n[ERROR] Could not connect to Bloomberg: {e}\n"
            "Make sure the Bloomberg Terminal is open and logged in.\n"
        ) from None

    print(f"Connected. Fetching data {start} → {end}\n")

    # --- VIX ---
    print("  Fetching VIX Index...")
    vix_raw = con.bdh("VIX Index", ["PX_LAST"], bbg_start, bbg_end)
    # pdblp returns MultiIndex columns: (ticker, field) — flatten
    vix_raw.columns = ["_".join(col).strip() for col in vix_raw.columns]
    vix = vix_raw.rename(columns={"VIX Index_PX_LAST": "vix_close"})
    vix.index.name = "date"
    vix_path = OUT_DIR / "vix_daily.csv"
    vix.to_csv(vix_path)
    print(f"  VIX: {len(vix)} rows  →  {vix_path}")

    # --- SPY ---
    print("  Fetching SPY US Equity...")
    spy_fields = ["PX_OPEN", "PX_HIGH", "PX_LOW", "PX_LAST", "PX_VOLUME"]
    spy_raw = con.bdh("SPY US Equity", spy_fields, bbg_start, bbg_end)
    spy_raw.columns = ["_".join(col).strip() for col in spy_raw.columns]
    spy = spy_raw.rename(columns={
        "SPY US Equity_PX_OPEN":   "open",
        "SPY US Equity_PX_HIGH":   "high",
        "SPY US Equity_PX_LOW":    "low",
        "SPY US Equity_PX_LAST":   "close",
        "SPY US Equity_PX_VOLUME": "volume",
    })
    spy.index.name = "date"
    spy_path = OUT_DIR / "spy_daily.csv"
    spy.to_csv(spy_path)
    print(f"  SPY: {len(spy)} rows  →  {spy_path}")

    con.stop()
    print("\nBloomberg connection closed.\n")


# ---------------------------------------------------------------------------
# FRED (free public API — no key needed)
# ---------------------------------------------------------------------------

def fetch_fred(start: str, end: str) -> None:
    """Pull USD 3-month T-bill rate from FRED public CSV endpoint."""
    print("  Fetching FRED DTB3 (3-month T-bill)...")

    url = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DTB3"
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        raise SystemExit(f"\n[ERROR] FRED request failed: {e}\n") from None

    from io import StringIO
    rfr = pd.read_csv(StringIO(resp.text), parse_dates=["observation_date"])
    rfr = rfr.rename(columns={"observation_date": "date", "DTB3": "rfr_3m_annualised_pct"})
    rfr = rfr.set_index("date").sort_index()

    # FRED reports "." for missing values — replace with NaN and forward-fill
    rfr["rfr_3m_annualised_pct"] = pd.to_numeric(
        rfr["rfr_3m_annualised_pct"], errors="coerce"
    ).ffill()

    # Trim to backtest window
    rfr = rfr.loc[start:end]

    # Convert annualised % → daily decimal (÷ 365 ÷ 100)
    # Keep both — notebook uses whichever is appropriate
    rfr["rfr_daily_decimal"] = rfr["rfr_3m_annualised_pct"] / 365 / 100

    rfr_path = OUT_DIR / "fred_rfr.csv"
    rfr.to_csv(rfr_path)
    print(f"  FRED: {len(rfr)} rows  →  {rfr_path}\n")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(bloomberg: bool, fred: bool, start: str, end: str) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Output directory: {OUT_DIR}\n")

    if bloomberg:
        fetch_bloomberg(start, end)

    if fred:
        fetch_fred(start, end)

    print("=" * 55)
    print("Done. Copy these files to  data/raw/macro/  on your main machine:")
    for f in sorted(OUT_DIR.glob("*.csv")):
        print(f"  {f.name}")
    print("=" * 55)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch macro data (Bloomberg + FRED).")
    parser.add_argument("--fred-only", action="store_true",
                        help="Skip Bloomberg — only pull FRED T-bill data. Runs anywhere.")
    parser.add_argument("--bloomberg-only", action="store_true",
                        help="Skip FRED — only pull VIX + SPY from Bloomberg.")
    parser.add_argument("--start", default=START, help=f"Start date YYYY-MM-DD (default: {START})")
    parser.add_argument("--end",   default=END,   help=f"End date YYYY-MM-DD (default: {END})")
    args = parser.parse_args()

    do_bloomberg = not args.fred_only
    do_fred      = not args.bloomberg_only

    run(do_bloomberg, do_fred, args.start, args.end)
