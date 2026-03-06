"""
strategy_cross.py
-----------------
Cross-exchange delta-neutral spot-vs-perp cash-and-carry strategy.

Trade structure per asset, per 8h period:
  LONG spot (Binance) + SHORT perp (exchange with highest funding rate)
    → collect elevated funding from the best perp market.
  LONG perp (exchange with most-negative FR) + SHORT spot (Binance)
    → collect negative funding (short pays us). Rare in practice.

Signal:
  Rolling z-score of the BEST available funding rate across Binance, GateIO,
  and Hyperliquid for each asset. "Best" = the exchange with the highest |FR|
  at each period, i.e., the most profitable carry opportunity available.

Funding rate alignment:
  Binance  — settles every 8h; raw 8h rate used directly.
  GateIO   — settles every 1h; 8 × 1h rates summed → 8h equivalent.
  HL       — settles every 1h; 8 × 1h rates summed → 8h equivalent.

Fee model:
  Two-legged trade: taker fee on the perp leg + taker fee on the spot leg
  at every turnover event (entry and exit).

Data requirements:
  Binance : data/processed/master_panel.parquet  (already built)
  GateIO  : data/{ASSET}/gateio_{ASSET}_*.parquet
  HL      : data/{ASSET}/hyperliquid_{ASSET}_*.parquet

Usage
-----
    from strategy_cross import run_backtest_cross
    import pandas as pd

    panel = pd.read_parquet("data/processed/master_panel.parquet")
    result = run_backtest_cross(panel)

    print(result["metrics"])
    result["cum_returns"].plot()
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from strategy import (
    compute_metrics,
    funding_zscore,
    macro_gate,
    oi_filter,
    PERIODS_PER_YEAR,  # noqa: F401  (exported for callers)
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DATA_DIR = Path(__file__).resolve().parent / "data"

# 7 assets available on both GateIO and Hyperliquid
CROSS_UNIVERSE: list[str] = [
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT",
    "BNBUSDT",
    "XRPUSDT",
    "DOGEUSDT",
    "AVAXUSDT",
]

# Maps master-panel symbol → data/{ASSET}/ folder name
ASSET_MAP: dict[str, str] = {
    "BTCUSDT":  "BTC",
    "ETHUSDT":  "ETH",
    "SOLUSDT":  "SOL",
    "BNBUSDT":  "BNB",
    "XRPUSDT":  "XRP",
    "DOGEUSDT": "DOGE",
    "AVAXUSDT": "AVAX",
}

# ---------------------------------------------------------------------------
# Default parameters
# ---------------------------------------------------------------------------

DEFAULTS_CROSS: dict = {
    "z_lookback":     270,      # periods (270 × 8h ≈ 90 days)
    "z_entry":        2.0,      # |z| threshold to enter a position
    "z_exit":         0.0,      # |z| threshold to EXIT a position (hysteresis band).
    #   Must be ≤ z_entry.  Setting z_exit = z_entry replicates the old
    #   period-by-period behaviour (exit as soon as z drops below entry).
    #   Setting z_exit = 0.0 keeps a position until the z-score fully reverts,
    #   dramatically reducing turnover and fee drag.
    "min_hold":       3,        # minimum number of 8h periods to hold before exiting.
    #   Prevents immediate exit after entry even if z reverts quickly.
    #   Works in addition to z_exit: both conditions must allow exit.
    "oi_lookback":    270,      # periods for OI rolling mean
    "oi_min_ratio":   0.5,      # OI must be ≥ 50 % of rolling mean
    "vix_gate":       30.0,     # VIX level above which we go flat
    "spy_dd_window":  15,       # periods for SPY drawdown look-back
    "spy_dd_gate":    0.05,     # SPY drawdown threshold (5 %)
    # Fees — two-legged trade (perp + spot)
    "perp_taker_fee": 0.00045,  # blended perp taker fee  (BN 4bps / GI 5bps / HL 3.5bps)
    "spot_taker_fee": 0.001,    # Binance spot taker fee  (10bps)
    # Position sizing
    # "equal_invested" : 1/N where N = active positions each period.
    #   Portfolio is always fully invested, but resizing whenever any asset
    #   enters/exits generates constant cross-rebalancing turnover and fees.
    # "fixed"          : 1/universe_size per active asset (no cross-rebalancing).
    #   Turnover fires only at entry/exit of each asset independently.
    #   Preferred for cash-and-carry — each leg is self-contained.
    "weight_scheme":  "fixed",
    "universe":       None,     # None → CROSS_UNIVERSE
    "data_dir":       None,     # None → DATA_DIR (project root / data)
}

# ---------------------------------------------------------------------------
# Data loading: non-Binance exchanges
# ---------------------------------------------------------------------------


def _load_gateio_fr(data_dir: Path, universe: list[str]) -> pd.DataFrame:
    """
    Load GateIO 1h funding rates and resample to the 8h Binance grid.

    GateIO settles funding every 1h, so 8 consecutive 1h rates are summed
    to produce the total funding collected over an 8h window.

    Returns
    -------
    pd.DataFrame (T × N), DatetimeIndex UTC aligned to 00:00 / 08:00 / 16:00.
    """
    frames: dict[str, pd.Series] = {}
    for sym in universe:
        asset = ASSET_MAP.get(sym)
        if not asset:
            continue
        paths = sorted(data_dir.glob(f"{asset}/gateio_{asset}_*.parquet"))
        if not paths:
            continue
        raw = pd.read_parquet(paths[-1])
        ts = pd.to_datetime(raw["Timestamp"]).dt.tz_convert("UTC")
        fr = pd.Series(raw["funding_rate_raw_gateio"].values, index=ts, name=sym)
        frames[sym] = fr.resample("8h").sum()

    return pd.DataFrame(frames) if frames else pd.DataFrame()


def _load_hl_fr(data_dir: Path, universe: list[str]) -> pd.DataFrame:
    """
    Load Hyperliquid 1h funding rates and resample to the 8h Binance grid.

    HL settles every 1h, so 8 consecutive 1h rates are summed to produce
    the total funding collected over an 8h window.

    Returns
    -------
    pd.DataFrame (T × N), DatetimeIndex UTC aligned to 00:00 / 08:00 / 16:00.
    """
    frames: dict[str, pd.Series] = {}
    for sym in universe:
        asset = ASSET_MAP.get(sym)
        if not asset:
            continue
        paths = sorted(data_dir.glob(f"{asset}/hyperliquid_{asset}_*.parquet"))
        if not paths:
            continue
        raw = pd.read_parquet(paths[-1])
        fr = raw["funding_rate_raw_hyperliquid"].copy()
        if fr.index.tz is None:
            fr.index = fr.index.tz_localize("UTC")
        else:
            fr.index = fr.index.tz_convert("UTC")
        frames[sym] = fr.resample("8h").sum()

    return pd.DataFrame(frames) if frames else pd.DataFrame()


# ---------------------------------------------------------------------------
# Best funding rate: cross-exchange max FR selection (short leg only)
# ---------------------------------------------------------------------------


def compute_best_fr(
    fr_bn: pd.DataFrame,
    fr_gi: pd.DataFrame,
    fr_hl: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    For each (period, asset), identify the exchange offering the HIGHEST
    funding rate — the most attractive short-perp / long-spot opportunity.

    Using max(FRs) rather than max(|FRs|) ensures the signal is directionally
    correct: a high positive z-score means the perp is expensive relative to
    spot on some exchange, which is the cash-and-carry short signal.  Going
    long (z < -entry) is intentionally disabled in run_backtest_cross because
    shorting spot is out of scope; see DEFAULTS_CROSS["short_only"].

    Before GateIO / HL data is available, falls back gracefully to the
    Binance-only value (Binance always has data from 2020).

    Parameters
    ----------
    fr_bn : pd.DataFrame (T × N)  — Binance 8h funding rates (master grid index)
    fr_gi : pd.DataFrame (T × N)  — GateIO 8h-equivalent funding rates
    fr_hl : pd.DataFrame (T × N)  — Hyperliquid 8h-equivalent funding rates

    Returns
    -------
    best_fr   : pd.DataFrame (T × N)  — max FR across exchanges per (period, asset)
    best_exch : pd.DataFrame (T × N) of str  — exchange with the highest FR
    """
    idx = fr_bn.index
    gi  = fr_gi.reindex(idx) if not fr_gi.empty else pd.DataFrame(index=idx)
    hl  = fr_hl.reindex(idx) if not fr_hl.empty else pd.DataFrame(index=idx)

    exchange_data: list[tuple[str, pd.DataFrame]] = [
        ("binance",     fr_bn),
        ("gateio",      gi),
        ("hyperliquid", hl),
    ]

    best_fr   = pd.DataFrame(np.nan, index=idx, columns=fr_bn.columns)
    best_exch = pd.DataFrame("",    index=idx, columns=fr_bn.columns)

    for sym in fr_bn.columns:
        # Build (T × 3) DataFrame: one column per exchange, NaN where unavailable
        sym_cols: dict[str, pd.Series] = {}
        for name, df in exchange_data:
            if sym in df.columns:
                sym_cols[name] = df[sym]
            else:
                sym_cols[name] = pd.Series(np.nan, index=idx)
        sym_df = pd.DataFrame(sym_cols)

        # max(FRs): highest FR across exchanges = best short-perp opportunity.
        # .max(axis=1) is NaN-safe (returns NaN only when ALL columns are NaN).
        # .idxmax raises on all-NaN rows in this pandas version, so fill with -inf first.
        all_nan = sym_df.isna().all(axis=1)
        best_fr[sym]   = sym_df.max(axis=1)
        idx_max        = sym_df.fillna(-np.inf).idxmax(axis=1)
        idx_max[all_nan] = ""
        best_exch[sym] = idx_max.values

    return best_fr, best_exch


# ---------------------------------------------------------------------------
# P&L: two-legged spot-vs-perp
# ---------------------------------------------------------------------------


def compute_pnl_cross(
    positions:      pd.DataFrame,
    best_fr:        pd.DataFrame,
    rfr:            pd.Series,
    perp_taker_fee: float,
    spot_taker_fee: float,
) -> pd.DataFrame:
    """
    Period-by-period P&L for the cross-exchange spot-vs-perp strategy.

    P&L sign convention (same as strategy.py):
      funding_pnl = −position(t) × best_fr(t)
        position = −1 (SHORT perp, LONG spot): pnl = +best_fr  (collect)
        position = +1 (LONG  perp, SHORT spot): pnl = +|best_fr|  (collect neg FR)

    Components
    ----------
    funding_pnl : collect from the perp short leg on the best exchange.
    fee_pnl     : taker cost on BOTH legs at every turnover event.
                  fee = |Δposition| × (perp_taker_fee + spot_taker_fee)
    rfr_pnl     : risk-free interest credit on deployed notional.
    spot_pnl    : ≈ 0 — the long spot and short perp are delta-neutral,
                  so mark-to-market changes cancel across legs.

    Parameters
    ----------
    positions       : pd.DataFrame (T × N)
    best_fr         : pd.DataFrame (T × N) — FR from the best exchange each period
    rfr             : pd.Series (T,)       — daily decimal risk-free rate
    perp_taker_fee  : float
    spot_taker_fee  : float

    Returns
    -------
    pd.DataFrame with columns: funding_pnl, fee_pnl, rfr_pnl, total_pnl
    """
    pos_lagged = positions.shift(1)

    funding_pnl = (-pos_lagged * best_fr.reindex_like(pos_lagged)).sum(axis=1)

    turnover = pos_lagged.diff().abs().sum(axis=1)
    fee_pnl  = -turnover * (perp_taker_fee + spot_taker_fee)

    rfr_pp  = rfr.reindex(positions.index).fillna(0.0)
    rfr_pnl = (pos_lagged.abs() * rfr_pp.values.reshape(-1, 1)).sum(axis=1)

    total_pnl = funding_pnl + fee_pnl + rfr_pnl

    return pd.DataFrame({
        "funding_pnl": funding_pnl,
        "fee_pnl":     fee_pnl,
        "rfr_pnl":     rfr_pnl,
        "total_pnl":   total_pnl,
    })


# ---------------------------------------------------------------------------
# Full backtest pipeline
# ---------------------------------------------------------------------------


def run_backtest_cross(
    panel:    pd.DataFrame,
    data_dir: str | Path = "data",
    **kwargs,
) -> dict:
    """
    End-to-end cross-exchange spot-vs-perp cash-and-carry backtest.

    Parameters
    ----------
    panel : pd.DataFrame
        MultiIndex (datetime, symbol) master panel from master_panel.parquet.
        Required columns: funding_rate, open_interest, vix_close, spy_close,
        rfr_daily_decimal.
    data_dir : str or Path
        Root data directory containing {ASSET}/gateio_*.parquet and
        {ASSET}/hyperliquid_*.parquet sub-folders.
    **kwargs : override any key in DEFAULTS_CROSS

    Returns
    -------
    dict with keys:
        params          — parameter dict used for the run
        zscore          — DataFrame (T, N) z-scores of best-exchange FR
        best_fr         — DataFrame (T, N) signed FR from the best exchange
        best_exchange   — DataFrame (T, N) str exchange label per period/asset
        oi_mask         — DataFrame (T, N) bool OI liquidity filter
        risk_on         — Series  (T,) macro gate (True = risk-on)
        positions       — DataFrame (T, N) final 1/N-scaled positions
        pnl             — DataFrame with funding_pnl, fee_pnl, rfr_pnl, total_pnl
        metrics         — dict of annualised performance statistics
        cum_returns     — Series (T,) cumulative P&L (geometric)
    """
    params   = {**DEFAULTS_CROSS, **kwargs}
    data_dir = Path(params["data_dir"] or data_dir)
    universe = params["universe"] or CROSS_UNIVERSE

    # --- Filter master panel to cross universe ---
    panel = panel[panel.index.get_level_values("symbol").isin(universe)]

    # --- Binance series from master panel (8h grid) ---
    fr_bn = panel["funding_rate"].unstack("symbol")
    oi    = panel["open_interest"].unstack("symbol")
    vix   = panel["vix_close"].unstack("symbol").bfill(axis=1).iloc[:, 0]
    spy   = panel["spy_close"].unstack("symbol").bfill(axis=1).iloc[:, 0]
    rfr   = panel["rfr_daily_decimal"].unstack("symbol").bfill(axis=1).iloc[:, 0]

    # --- Load alternative-exchange funding rates (resampled to 8h) ---
    fr_gi = _load_gateio_fr(data_dir, universe)
    fr_hl = _load_hl_fr(data_dir, universe)

    # --- Best exchange per (period, asset) ---
    best_fr, best_exch = compute_best_fr(fr_bn, fr_gi, fr_hl)

    # --- Align macro / OI series to the same 8h index ---
    oi_aligned  = oi.reindex(best_fr.index)
    vix_aligned = vix.reindex(best_fr.index)
    spy_aligned = spy.reindex(best_fr.index)
    rfr_aligned = rfr.reindex(best_fr.index)

    # --- Signal: z-score of the best-available funding rate ---
    zscore = funding_zscore(best_fr, params["z_lookback"])

    # --- Filters ---
    oi_mask = oi_filter(oi_aligned, params["oi_lookback"], params["oi_min_ratio"])
    risk_on = macro_gate(
        vix_aligned, spy_aligned,
        params["vix_gate"],
        params["spy_dd_window"],
        params["spy_dd_gate"],
    )

    # --- Positions (hysteresis entry/exit band) ---
    # Direction is constrained by the SIGN of best_fr to guarantee we always
    # collect the funding rate rather than pay it:
    #   best_fr > 0 (contango)    → only SHORT allowed (z > +z_entry)
    #   best_fr < 0 (backwardation) → only LONG  allowed (z < -z_entry)
    #
    # Hysteresis: enter when |z| crosses z_entry, hold until |z| drops below
    # z_exit AND min_hold periods have elapsed.  This prevents the old behaviour
    # where any single-period dip below z_entry immediately closed the trade,
    # generating constant turnover and fee drag on very short-lived positions.
    z_entry  = params["z_entry"]
    z_exit   = params["z_exit"]
    min_hold = params["min_hold"]

    z_arr  = zscore.values          # (T, N)
    fr_arr = best_fr.reindex(zscore.index).values  # (T, N)
    raw_arr = np.zeros_like(z_arr)
    hold_arr = np.zeros(z_arr.shape[1], dtype=int)  # periods held per asset

    for t in range(1, len(z_arr)):
        for n in range(z_arr.shape[1]):
            prev   = raw_arr[t - 1, n]
            zv     = z_arr[t, n]
            frv    = fr_arr[t, n]
            hc     = hold_arr[n]

            if np.isnan(zv) or np.isnan(frv):
                raw_arr[t, n] = 0.0
                hold_arr[n]   = 0
                continue

            if prev == 0.0:
                # No position — check entry conditions
                if zv > z_entry and frv > 0:
                    raw_arr[t, n] = -1.0   # contango → short perp
                    hold_arr[n]   = 1
                elif zv < -z_entry and frv < 0:
                    raw_arr[t, n] = 1.0    # backwardation → long perp
                    hold_arr[n]   = 1
                else:
                    raw_arr[t, n] = 0.0
                    hold_arr[n]   = 0
            else:
                # In position — exit only if min_hold elapsed AND z has reverted.
                # Direction-aware: short exits when z < z_exit (contango unwound),
                # long exits when z > -z_exit (backwardation unwound).
                if prev < 0:
                    z_reverted = zv < z_exit
                else:
                    z_reverted = zv > -z_exit
                can_exit = hc >= min_hold and z_reverted
                if can_exit:
                    raw_arr[t, n] = 0.0
                    hold_arr[n]   = 0
                else:
                    raw_arr[t, n] = prev
                    hold_arr[n]   = hc + 1

    raw = pd.DataFrame(raw_arr, index=zscore.index, columns=zscore.columns)

    # Apply OI and macro filters — force flat when either fires
    raw = raw * oi_mask.reindex_like(raw).fillna(True).astype(float)
    raw = raw.multiply(risk_on.reindex(raw.index).fillna(True).astype(float), axis=0)

    # Position sizing
    if params["weight_scheme"] == "fixed":
        # Each asset gets a fixed 1/universe_size weight.
        # Turnover fires only when an individual asset enters or exits —
        # no cross-rebalancing when the count of active positions changes.
        positions = raw / len(universe)
    else:
        # "equal_invested": always fully deployed, 1/N where N is active count.
        # Simpler conceptually, but every asset entry/exit resizes all others.
        n_active  = (raw != 0).sum(axis=1).replace(0, np.nan)
        positions = raw.divide(n_active, axis=0).fillna(0.0)

    # --- P&L ---
    pnl_df = compute_pnl_cross(
        positions,
        best_fr,
        rfr_aligned,
        params["perp_taker_fee"],
        params["spot_taker_fee"],
    )

    # --- Metrics ---
    valid       = pnl_df["total_pnl"].dropna()
    metrics     = compute_metrics(valid, rfr_aligned)
    cum_returns = (1 + pnl_df["total_pnl"].fillna(0)).cumprod() - 1

    return {
        "params":        params,
        "zscore":        zscore,
        "best_fr":       best_fr,
        "best_exchange": best_exch,
        "oi_mask":       oi_mask,
        "risk_on":       risk_on,
        "positions":     positions,
        "pnl":           pnl_df,
        "metrics":       metrics,
        "cum_returns":   cum_returns,
    }
