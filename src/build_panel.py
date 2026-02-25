"""
build_panel.py
--------------
Merges all raw data into data/processed/master_panel.parquet

Index:   pd.MultiIndex of (datetime [UTC, 8h], symbol)
Columns:
  funding_rate                     — funding rate at settlement (00:00, 08:00, 16:00 UTC)
  perp_open/high/low/close/volume  — mark-price 8h OHLCV
  spot_close                       — spot index 8h close price
  open_interest                    — OI in base currency (Bybit 1h → aligned to 8h grid)
  vix_close                        — VIX daily (forward-filled to 8h)
  spy_close                        — SPY daily close (forward-filled to 8h)
  rfr_daily_decimal                — 3m T-bill daily rate (forward-filled to 8h)

Usage:
    python src/build_panel.py
"""

from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

ROOT        = Path(__file__).resolve().parents[1]
RAW_BINANCE = ROOT / "data" / "raw" / "binance"
RAW_MACRO   = ROOT / "data" / "raw" / "macro"
OUT         = ROOT / "data" / "processed" / "master_panel.parquet"

SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
    "DOGEUSDT", "AVAXUSDT", "POLUSDT", "LINKUSDT", "ADAUSDT",
    "DOTUSDT", "ATOMUSDT", "LTCUSDT", "UNIUSDT", "ARBUSDT",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_parquet(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_parquet(path)
    # Binance occasionally stores timestamps with sub-second offsets (e.g. 00:00:00.002).
    # Round to nearest second so all series align on the same grid.
    if isinstance(df.index, pd.DatetimeIndex):
        df.index = df.index.round("s")
        df = df[~df.index.duplicated(keep="first")]
    return df


def load_macro_csv(path: Path) -> pd.DataFrame:
    """Load a daily macro CSV with a 'date' index column; returns UTC DatetimeIndex."""
    df = pd.read_csv(path, parse_dates=["date"], index_col="date")
    df.index = pd.to_datetime(df.index, utc=True)
    return df.sort_index()


# ---------------------------------------------------------------------------
# Build canonical 8h grid
# ---------------------------------------------------------------------------

def build_8h_grid() -> pd.DatetimeIndex:
    """
    Union of all funding-rate timestamps across all symbols.
    These are the canonical settlement timestamps the strategy runs on.
    """
    # Use load_parquet so timestamp rounding is applied consistently with load_symbol
    all_indices = []
    for sym in SYMBOLS:
        df = load_parquet(RAW_BINANCE / "funding_rates" / f"{sym}.parquet")
        if not df.empty:
            all_indices.append(df.index)

    if not all_indices:
        raise FileNotFoundError("No funding rate files found in data/raw/binance/funding_rates/")

    grid = all_indices[0]
    for idx in all_indices[1:]:
        grid = grid.union(idx)

    return grid.sort_values()


# ---------------------------------------------------------------------------
# Load per-symbol data
# ---------------------------------------------------------------------------

def load_symbol(sym: str, grid: pd.DatetimeIndex) -> pd.DataFrame:
    """
    Load all series for one symbol and align them to the 8h grid.
    Returns a DataFrame indexed by grid timestamps.
    """
    # Funding rate
    fr = load_parquet(RAW_BINANCE / "funding_rates" / f"{sym}.parquet")
    if not fr.empty:
        fr = fr[["funding_rate"]]

    # Perp OHLCV — prefix columns with "perp_"
    perp = load_parquet(RAW_BINANCE / "ohlcv_perp" / f"{sym}.parquet")
    if not perp.empty:
        perp = perp.rename(columns={c: f"perp_{c}" for c in perp.columns})

    # Spot close only
    spot = load_parquet(RAW_BINANCE / "ohlcv_spot" / f"{sym}.parquet")
    if not spot.empty and "close" in spot.columns:
        spot = spot[["close"]].rename(columns={"close": "spot_close"})

    # OI (1h from Bybit) — forward-fill to 8h grid
    oi = load_parquet(RAW_BINANCE / "open_interest" / f"{sym}.parquet")
    if not oi.empty and "open_interest" in oi.columns:
        oi = oi[["open_interest"]]

    # Merge funding + perp + spot on exact timestamps (all already on 8h grid)
    frames = [df for df in [fr, perp, spot] if not df.empty]
    if not frames:
        return pd.DataFrame(index=grid)

    merged = frames[0]
    for df in frames[1:]:
        merged = merged.join(df, how="outer")

    # Reindex to canonical grid — these series should match it exactly;
    # reindex handles any minor gaps (listings starting mid-grid, etc.)
    merged = merged.reindex(grid)

    # OI: 1h data → forward-fill to 8h timestamps
    if not oi.empty:
        oi_aligned = oi.reindex(grid, method="ffill")
        merged = merged.join(oi_aligned, how="left")
    else:
        merged["open_interest"] = float("nan")

    return merged


# ---------------------------------------------------------------------------
# Load macro
# ---------------------------------------------------------------------------

def load_macro(grid: pd.DatetimeIndex) -> pd.DataFrame:
    """
    Load VIX, SPY close, and FRED risk-free rate.
    Daily data is forward-filled to the 8h settlement grid.
    """
    vix = load_macro_csv(RAW_MACRO / "vix_daily.csv")
    # Bloomberg sometimes saves the column as 'VIX Index' or 'vix_close'
    vix_col = [c for c in vix.columns if "vix" in c.lower() or "VIX" in c][0]
    vix = vix[[vix_col]].rename(columns={vix_col: "vix_close"})

    spy = load_macro_csv(RAW_MACRO / "spy_daily.csv")
    spy = spy[["close"]].rename(columns={"close": "spy_close"})

    rfr = load_macro_csv(RAW_MACRO / "fred_rfr.csv")
    rfr = rfr[["rfr_daily_decimal"]]

    macro = vix.join(spy, how="outer").join(rfr, how="outer")

    # Fill any NaN rows from market holidays (Bloomberg includes the date but no value).
    # Then forward-fill daily data to the 8h settlement grid.
    macro = macro.ffill().reindex(grid, method="ffill")

    return macro


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def build() -> None:
    OUT.parent.mkdir(parents=True, exist_ok=True)

    print("Building 8h settlement grid from funding rate timestamps...")
    grid = build_8h_grid()
    print(f"  {len(grid)} timestamps  |  {grid[0].date()} → {grid[-1].date()}\n")

    # --- Per-symbol ---
    sym_frames = []
    for sym in SYMBOLS:
        print(f"  Loading {sym}...")
        df = load_symbol(sym, grid)
        df.index = pd.MultiIndex.from_arrays(
            [df.index, [sym] * len(df)],
            names=["datetime", "symbol"],
        )
        sym_frames.append(df)

    panel = pd.concat(sym_frames).sort_index()
    print(f"\nSymbol data merged: {panel.shape[0]:,} rows x {panel.shape[1]} cols")

    # Drop off-grid timestamps: Binance funding history includes ~1,600 settlements
    # at non-standard hours (newly listed symbols, corrections, etc.).
    # Keep only standard 8h settlement times: 00:00, 08:00, 16:00 UTC.
    dt_level = panel.index.get_level_values("datetime")
    on_grid = (
        dt_level.hour.isin([0, 8, 16])
        & (dt_level.minute == 0)
        & (dt_level.second == 0)
    )
    n_before = len(panel)
    panel = panel[on_grid]
    print(f"Dropped {n_before - len(panel):,} off-grid rows → {len(panel):,} rows remain")

    # --- Macro ---
    print("\nLoading macro data...")
    macro = load_macro(grid)
    print(f"  VIX : {macro['vix_close'].notna().sum():>5} non-null rows")
    print(f"  SPY : {macro['spy_close'].notna().sum():>5} non-null rows")
    print(f"  RFR : {macro['rfr_daily_decimal'].notna().sum():>5} non-null rows")

    # Broadcast macro onto panel via datetime level of the MultiIndex
    panel = panel.join(macro, on="datetime")

    # --- Summary ---
    print(f"\nFinal panel shape: {panel.shape}")
    print(f"Columns: {list(panel.columns)}")
    null_counts = panel.isnull().sum()
    if null_counts.any():
        print(f"\nNull counts (non-zero only):")
        print(null_counts[null_counts > 0].to_string())

    panel.to_parquet(OUT, index=True)
    print(f"\nSaved → {OUT}")


if __name__ == "__main__":
    build()
