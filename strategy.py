"""
strategy.py
-----------
Bidirectional delta-neutral funding-rate carry strategy on crypto perpetual futures.

Signal:   Rolling z-score of each symbol's own funding rate.
          z > +z_entry  →  SHORT  (−1): funding elevated, likely to revert → collect funding
          z < −z_entry  →  LONG   (+1): funding depressed/negative → collect negative funding
Filter:   Open interest ≥ oi_min_ratio × 90-day rolling mean (liquidity gate).
Gate:     Flat when VIX > vix_gate OR SPY 5-day drawdown > spy_dd_gate.
Sizing:   Equal weight across active positions (1/N per period).

P&L sign convention
-------------------
  funding_pnl  = −position × funding_rate

  If position = −1 (SHORT perp) and funding_rate > 0:
      long pays short → pnl = −(−1) × (+) = + (we collect)
  If position = +1 (LONG perp) and funding_rate < 0:
      short pays long → pnl = −(+1) × (−) = + (we collect)

Usage
-----
    from strategy import run_backtest
    import pandas as pd

    panel = pd.read_parquet("data/processed/master_panel.parquet")
    result = run_backtest(panel)

    print(result["metrics"])
    result["pnl"].cumsum().plot()
"""

from __future__ import annotations

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Default parameters
# ---------------------------------------------------------------------------

DEFAULTS: dict = {
    "z_lookback":     270,   # periods (270 × 8h ≈ 90 days)
    "z_entry":        1.5,   # |z| threshold to enter a position
    "oi_lookback":    270,   # periods for OI rolling mean
    "oi_min_ratio":   0.5,   # OI must be ≥ 50% of rolling mean
    "vix_gate":       30.0,  # VIX level above which we go flat
    "spy_dd_window":  15,    # periods for SPY drawdown look-back (15 × 8h = 5 days)
    "spy_dd_gate":    0.05,  # SPY drawdown threshold (5%)
    "taker_fee":      0.0004,# Binance taker fee per side (4 bps)
}

PERIODS_PER_YEAR: int = 3 * 365  # 8h periods in a year


# ---------------------------------------------------------------------------
# Signal: funding z-score
# ---------------------------------------------------------------------------

def funding_zscore(
    funding: pd.DataFrame,
    lookback: int,
) -> pd.DataFrame:
    """
    Rolling z-score of each symbol's funding rate relative to its own history.

    Parameters
    ----------
    funding : pd.DataFrame, shape (T, N)
        Funding rates; index = datetime (8h UTC), columns = symbols.
    lookback : int
        Rolling window in periods.

    Returns
    -------
    pd.DataFrame, same shape as funding.
        NaN where fewer than lookback // 2 observations are available.
    """
    min_obs = max(lookback // 2, 2)
    roll = funding.rolling(lookback, min_periods=min_obs)
    mu  = roll.mean()
    std = roll.std()
    z   = (funding - mu) / std.replace(0, np.nan)
    return z


# ---------------------------------------------------------------------------
# Filter: open interest liquidity gate
# ---------------------------------------------------------------------------

def oi_filter(
    oi: pd.DataFrame,
    lookback: int,
    min_ratio: float,
) -> pd.DataFrame:
    """
    Boolean mask: True when a symbol's OI is healthy relative to recent history.

    Parameters
    ----------
    oi : pd.DataFrame, shape (T, N)
        Open interest in base currency.
    lookback : int
        Rolling window for the baseline OI mean.
    min_ratio : float
        Current OI must be ≥ min_ratio × rolling mean to be tradeable.

    Returns
    -------
    pd.DataFrame of bool, same shape as oi.
        True = liquid enough to trade.  NaN OI rows default to True (no filter applied).
    """
    min_obs = max(lookback // 2, 2)
    roll_mean = oi.rolling(lookback, min_periods=min_obs).mean()
    mask = (oi >= min_ratio * roll_mean) | oi.isna()
    return mask.fillna(True)


# ---------------------------------------------------------------------------
# Gate: macro risk filter
# ---------------------------------------------------------------------------

def macro_gate(
    vix: pd.Series,
    spy: pd.Series,
    vix_threshold: float,
    spy_dd_window: int,
    spy_dd_threshold: float,
) -> pd.Series:
    """
    Boolean series: True = risk-on (normal trading), False = go flat.

    Flat when EITHER:
      • VIX close > vix_threshold (e.g. 30), OR
      • SPY rolling drawdown over spy_dd_window periods > spy_dd_threshold (e.g. 5%)

    Parameters
    ----------
    vix : pd.Series   — VIX daily close, aligned to 8h grid.
    spy : pd.Series   — SPY daily close, aligned to 8h grid.
    vix_threshold : float
    spy_dd_window : int    — look-back in 8h periods (15 ≈ 5 calendar days)
    spy_dd_threshold : float

    Returns
    -------
    pd.Series of bool (True = risk-on).
    """
    high_vix = vix > vix_threshold

    spy_rolling_max = spy.rolling(spy_dd_window, min_periods=1).max()
    spy_drawdown    = (spy_rolling_max - spy) / spy_rolling_max
    bad_spy         = spy_drawdown > spy_dd_threshold

    risk_on = ~(high_vix | bad_spy)

    # Where macro data is missing, default to risk-on (no spurious flats)
    risk_on = risk_on.fillna(True)
    return risk_on


# ---------------------------------------------------------------------------
# Position sizing
# ---------------------------------------------------------------------------

def compute_positions(
    zscore: pd.DataFrame,
    oi_mask: pd.DataFrame,
    risk_on: pd.Series,
    z_entry: float,
) -> pd.DataFrame:
    """
    Compute equal-weight positions across active signals.

    Signal logic per symbol:
      z > +z_entry  →  raw = −1  (SHORT: collect elevated positive funding)
      z < −z_entry  →  raw = +1  (LONG:  collect negative/depressed funding)
      else          →  raw =  0  (flat)

    Active = raw != 0 AND oi_mask AND risk_on.
    Each active position sized at 1 / (number of active positions in that period).

    Parameters
    ----------
    zscore : pd.DataFrame, shape (T, N)
    oi_mask : pd.DataFrame, shape (T, N)  — bool
    risk_on : pd.Series, shape (T,)       — bool
    z_entry : float

    Returns
    -------
    pd.DataFrame of float, same shape as zscore.
        Values in (−1, 0, +1) scaled by 1/N.
    """
    raw = pd.DataFrame(0.0, index=zscore.index, columns=zscore.columns)
    raw[zscore >  z_entry] = -1.0
    raw[zscore < -z_entry] =  1.0

    # Apply liquidity filter
    raw = raw * oi_mask.reindex_like(raw).fillna(True).astype(float)

    # Apply macro gate (broadcast across symbols)
    raw = raw.multiply(risk_on.reindex(raw.index).fillna(True).astype(float), axis=0)

    # Equal-weight: scale by 1/N where N = number of active positions per row
    n_active = (raw != 0).sum(axis=1).replace(0, np.nan)
    positions = raw.divide(n_active, axis=0).fillna(0.0)

    return positions


# ---------------------------------------------------------------------------
# P&L computation
# ---------------------------------------------------------------------------

def compute_pnl(
    positions: pd.DataFrame,
    funding: pd.DataFrame,
    rfr: pd.Series,
    taker_fee: float,
) -> pd.DataFrame:
    """
    Compute period-by-period P&L components for each symbol.

    Three components per period:
      funding_pnl  = −position(t) × funding_rate(t)
      fee_pnl      = −|Δposition(t)| × taker_fee   (charged on turnover)
      rfr_pnl      = position(t) × rfr(t)           (interest credit on notional)

    Note: positions are shifted forward by 1 period so we enter at t and
    collect the funding that settles at t (the signal is formed from data
    known just before t, via the z-score which uses funding up to t-1 in
    rolling sense; but Binance posts the next funding rate before settlement).
    In practice positions are lagged by 1 to avoid look-ahead.

    Parameters
    ----------
    positions : pd.DataFrame, shape (T, N)
    funding   : pd.DataFrame, shape (T, N)
    rfr       : pd.Series, shape (T,)   — daily decimal, aligned to 8h grid
    taker_fee : float

    Returns
    -------
    pd.DataFrame with columns:
        funding_pnl, fee_pnl, rfr_pnl, total_pnl
    """
    # Lag positions by 1 period: decision at t-1, collect at t
    pos_lagged = positions.shift(1)

    funding_pnl = (-pos_lagged * funding).sum(axis=1)

    turnover    = pos_lagged.diff().abs().sum(axis=1)
    fee_pnl     = -turnover * taker_fee

    # RFR credit: only on deployed (non-zero) notional, per-period (daily rate / 1)
    rfr_per_period = rfr.reindex(positions.index).fillna(0.0)
    rfr_pnl        = (pos_lagged.abs() * rfr_per_period.values.reshape(-1, 1)).sum(axis=1)

    total_pnl = funding_pnl + fee_pnl + rfr_pnl

    return pd.DataFrame({
        "funding_pnl": funding_pnl,
        "fee_pnl":     fee_pnl,
        "rfr_pnl":     rfr_pnl,
        "total_pnl":   total_pnl,
    })


# ---------------------------------------------------------------------------
# Performance metrics
# ---------------------------------------------------------------------------

def compute_metrics(pnl: pd.Series, rfr: pd.Series) -> dict:
    """
    Compute strategy performance metrics.

    Parameters
    ----------
    pnl : pd.Series   — period total P&L (not cumulative)
    rfr : pd.Series   — risk-free rate per period (daily decimal aligned to 8h)

    Returns
    -------
    dict with keys:
        cagr, ann_vol, sharpe, sortino, max_drawdown,
        hit_rate, avg_win, avg_loss, profit_factor,
        n_periods, n_years
    """
    pnl  = pnl.dropna()
    rfr_ = rfr.reindex(pnl.index).fillna(0.0)

    n_periods = len(pnl)
    n_years   = n_periods / PERIODS_PER_YEAR

    # CAGR
    total_return = (1 + pnl).prod() - 1
    cagr = (1 + total_return) ** (1 / max(n_years, 1e-9)) - 1

    # Volatility (annualised)
    ann_vol = pnl.std() * np.sqrt(PERIODS_PER_YEAR)

    # Sharpe (excess over RFR)
    excess     = pnl - rfr_
    sharpe     = (excess.mean() / excess.std() * np.sqrt(PERIODS_PER_YEAR)
                  if excess.std() > 0 else np.nan)

    # Sortino (downside deviation)
    downside   = excess[excess < 0]
    down_std   = downside.std() * np.sqrt(PERIODS_PER_YEAR) if len(downside) > 1 else np.nan
    sortino    = (excess.mean() * PERIODS_PER_YEAR / down_std
                  if (down_std and down_std > 0) else np.nan)

    # Maximum drawdown
    cum        = (1 + pnl).cumprod()
    roll_max   = cum.cummax()
    drawdown   = (roll_max - cum) / roll_max
    max_dd     = drawdown.max()

    # Win/loss stats
    wins       = pnl[pnl > 0]
    losses     = pnl[pnl < 0]
    hit_rate   = len(wins) / n_periods if n_periods > 0 else np.nan
    avg_win    = wins.mean()  if len(wins)   > 0 else np.nan
    avg_loss   = losses.mean() if len(losses) > 0 else np.nan
    pf_denom   = losses.abs().sum()
    profit_factor = wins.sum() / pf_denom if pf_denom > 0 else np.nan

    return {
        "cagr":           round(cagr,         4),
        "ann_vol":        round(ann_vol,       4),
        "sharpe":         round(sharpe,        3) if not np.isnan(sharpe)   else np.nan,
        "sortino":        round(sortino,       3) if not np.isnan(sortino)  else np.nan,
        "max_drawdown":   round(max_dd,        4),
        "hit_rate":       round(hit_rate,      4) if not np.isnan(hit_rate) else np.nan,
        "avg_win":        round(avg_win,       6) if not np.isnan(avg_win)  else np.nan,
        "avg_loss":       round(avg_loss,      6) if not np.isnan(avg_loss) else np.nan,
        "profit_factor":  round(profit_factor, 3) if not np.isnan(profit_factor) else np.nan,
        "n_periods":      n_periods,
        "n_years":        round(n_years,       2),
    }


# ---------------------------------------------------------------------------
# Full backtest pipeline
# ---------------------------------------------------------------------------

def run_backtest(panel: pd.DataFrame, **kwargs) -> dict:
    """
    End-to-end backtest on the master panel.

    Parameters
    ----------
    panel : pd.DataFrame
        MultiIndex (datetime, symbol) with columns:
        funding_rate, perp_close, spot_close, open_interest,
        vix_close, spy_close, rfr_daily_decimal
    **kwargs : override any key in DEFAULTS

    Returns
    -------
    dict with keys:
        params       — parameter dict used
        zscore       — DataFrame (T, N) funding z-scores
        oi_mask      — DataFrame (T, N) bool liquidity filter
        risk_on      — Series (T,) macro gate
        positions    — DataFrame (T, N) final positions
        pnl          — DataFrame with funding_pnl, fee_pnl, rfr_pnl, total_pnl
        metrics      — dict of performance statistics
        cum_returns  — Series (T,) cumulative P&L
    """
    params = {**DEFAULTS, **kwargs}

    # --- Unstack to wide format (T × N) ---
    funding = panel["funding_rate"].unstack("symbol")
    oi      = panel["open_interest"].unstack("symbol")

    # Macro series — same value across all symbols, take first non-null
    vix = panel["vix_close"].unstack("symbol").bfill(axis=1).iloc[:, 0]
    spy = panel["spy_close"].unstack("symbol").bfill(axis=1).iloc[:, 0]
    rfr = panel["rfr_daily_decimal"].unstack("symbol").bfill(axis=1).iloc[:, 0]

    # --- Signal ---
    zscore = funding_zscore(funding, params["z_lookback"])

    # --- Filters ---
    oi_mask = oi_filter(oi, params["oi_lookback"], params["oi_min_ratio"])
    risk_on = macro_gate(
        vix, spy,
        params["vix_gate"],
        params["spy_dd_window"],
        params["spy_dd_gate"],
    )

    # --- Positions ---
    positions = compute_positions(zscore, oi_mask, risk_on, params["z_entry"])

    # --- P&L ---
    pnl_df = compute_pnl(positions, funding, rfr, params["taker_fee"])

    # --- Metrics (drop burn-in NaN rows) ---
    valid = pnl_df["total_pnl"].dropna()
    metrics = compute_metrics(valid, rfr)

    cum_returns = (1 + pnl_df["total_pnl"].fillna(0)).cumprod() - 1

    return {
        "params":      params,
        "zscore":      zscore,
        "oi_mask":     oi_mask,
        "risk_on":     risk_on,
        "positions":   positions,
        "pnl":         pnl_df,
        "metrics":     metrics,
        "cum_returns": cum_returns,
    }
