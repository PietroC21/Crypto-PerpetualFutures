#!/usr/bin/env python3
"""
OKX CSV → Standardised Hourly Parquet Converter
=================================================

Reads raw CSVs produced by the OKX download pipeline and writes one
Parquet file per coin with a fixed hourly schema:

    timestamp                   (UTC datetime, hourly)
    open_perp_okx               (float64, from trade-aggregated hourly bars)
    high_perp_okx               (float64, from trade-aggregated hourly bars)
    low_perp_okx                (float64, from trade-aggregated hourly bars)
    close_perp_okx              (float64, from trade bars or candle close)
    volume_perp_okx             (float64, from trade-aggregated hourly bars)
    vwap_perp_okx               (float64, volume-weighted average price)
    open_spot_okx               (float64, from trade-aggregated hourly bars)
    high_spot_okx               (float64, from trade-aggregated hourly bars)
    low_spot_okx                (float64, from trade-aggregated hourly bars)
    close_spot_okx              (float64, from trade bars or candle close)
    volume_spot_okx             (float64, from trade-aggregated hourly bars)
    vwap_spot_okx               (float64, volume-weighted average price)
    mark_price_okx              (float64, from mark_price_candles)
    funding_rate_raw_okx        (float64, forward-filled from 8 h events)
    open_interest_usd_okx       (float64, from open_interest download)

If trade-aggregated bar files exist, they provide full OHLCV + VWAP.
Otherwise, only close is populated from candle data (OHLV + VWAP = null).

Usage:
    python csv_to_parquet_standardizer.py [--overwrite]

OKX-specific notes:
  - All OKX timestamps are in MILLISECONDS.
  - Merged funding: {COIN}_USDT_SWAP_funding.csv
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import polars as pl

# ── Paths ────────────────────────────────────────────────────────────────────

PIPELINE_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PIPELINE_ROOT / "data" / "okx" / "raw"
FUNDING_DIR = RAW_DIR / "funding_rates"
PERP_CANDLE_DIR = RAW_DIR / "candles_perp"
SPOT_CANDLE_DIR = RAW_DIR / "candles_spot"
TRADES_PERP_DIR = RAW_DIR / "trades_perp"
TRADES_SPOT_DIR = RAW_DIR / "trades_spot"
MARK_PRICE_DIR = RAW_DIR / "mark_price_candles"
OI_DIR = RAW_DIR / "open_interest"
OUTPUT_DIR = PIPELINE_ROOT / "data" / "okx" / "processed" / "parquet"

EXCHANGE = "okx"

# ── Standardised column names ────────────────────────────────────────────────

SCHEMA_COLS = [
    "timestamp",
    # OHLCV + VWAP for perp
    f"open_perp_{EXCHANGE}",
    f"high_perp_{EXCHANGE}",
    f"low_perp_{EXCHANGE}",
    f"close_perp_{EXCHANGE}",
    f"volume_perp_{EXCHANGE}",
    f"vwap_perp_{EXCHANGE}",
    # OHLCV + VWAP for spot
    f"open_spot_{EXCHANGE}",
    f"high_spot_{EXCHANGE}",
    f"low_spot_{EXCHANGE}",
    f"close_spot_{EXCHANGE}",
    f"volume_spot_{EXCHANGE}",
    f"vwap_spot_{EXCHANGE}",
    # Other columns
    f"mark_price_{EXCHANGE}",
    f"funding_rate_raw_{EXCHANGE}",
    f"open_interest_usd_{EXCHANGE}",
]


# ═════════════════════════════════════════════════════════════════════════════
#  Helpers — coin discovery
# ═════════════════════════════════════════════════════════════════════════════

def discover_coins() -> list[str]:
    """Scan raw directories for all coins present."""
    coins: set[str] = set()

    for p in FUNDING_DIR.glob("*_USDT_SWAP_funding.csv"):
        m = re.match(r"^([A-Z]+)_USDT_SWAP_funding\.csv$", p.name)
        if m:
            coins.add(m.group(1))

    for d in [PERP_CANDLE_DIR, SPOT_CANDLE_DIR]:
        if d.exists():
            for p in d.glob("*_candles_1h.csv"):
                m = re.match(r"^([A-Z]+)_USDT", p.name)
                if m:
                    coins.add(m.group(1))

    for d in [TRADES_PERP_DIR, TRADES_SPOT_DIR]:
        if d.exists():
            for p in d.glob("*_trades_1h.csv"):
                m = re.match(r"^([A-Z]+)_USDT", p.name)
                if m:
                    coins.add(m.group(1))

    return sorted(coins)


# ═════════════════════════════════════════════════════════════════════════════
#  Helpers — parsing
# ═════════════════════════════════════════════════════════════════════════════

def parse_funding(coin: str) -> pl.DataFrame | None:
    """
    Read the merged OKX funding CSV for *coin*.
    Returns: [timestamp (UTC datetime), funding_rate (f64)]
    """
    path = FUNDING_DIR / f"{coin}_USDT_SWAP_funding.csv"
    if not path.exists():
        return None

    df = pl.read_csv(path)
    df = df.with_columns(
        pl.from_epoch(pl.col("timestamp"), time_unit="ms")
        .cast(pl.Datetime("us"))
        .alias("timestamp"),
        pl.col("funding_rate").cast(pl.Float64),
    ).select(["timestamp", "funding_rate"])

    return df.sort("timestamp").unique(subset=["timestamp"], keep="last")


def parse_candle_file(path: Path, ts_unit: str = "ms") -> pl.DataFrame | None:
    """
    Read an hourly candle CSV.
    Returns: [timestamp (datetime), open, high, low, close, volume]
    """
    if not path.exists():
        return None

    df = pl.read_csv(path)
    df = df.with_columns(
        pl.from_epoch(pl.col("timestamp"), time_unit=ts_unit)
        .cast(pl.Datetime("us"))
        .alias("timestamp"),
        pl.col("open").cast(pl.Float64),
        pl.col("high").cast(pl.Float64),
        pl.col("low").cast(pl.Float64),
        pl.col("close").cast(pl.Float64),
        pl.col("volume").cast(pl.Float64),
    ).select(["timestamp", "open", "high", "low", "close", "volume"])

    return df.sort("timestamp").unique(subset=["timestamp"], keep="last")


def parse_trade_ohlcv_file(path: Path, ts_unit: str = "ms") -> pl.DataFrame | None:
    """
    Read an hourly trade-aggregated OHLCV + VWAP CSV.
    Returns: [timestamp (datetime), open, high, low, close, volume, vwap]
    """
    if not path.exists():
        return None

    df = pl.read_csv(path)
    df = df.with_columns(
        pl.from_epoch(pl.col("timestamp"), time_unit=ts_unit)
        .cast(pl.Datetime("us"))
        .alias("timestamp"),
        pl.col("open").cast(pl.Float64),
        pl.col("high").cast(pl.Float64),
        pl.col("low").cast(pl.Float64),
        pl.col("close").cast(pl.Float64),
        pl.col("volume").cast(pl.Float64),
        pl.col("vwap").cast(pl.Float64),
    ).select(["timestamp", "open", "high", "low", "close", "volume", "vwap"])

    return df.sort("timestamp").unique(subset=["timestamp"], keep="last")


def parse_perp_candles(coin: str) -> pl.DataFrame | None:
    """Parse perp candle file for *coin*."""
    path = PERP_CANDLE_DIR / f"{coin}_USDT_SWAP_perp_candles_1h.csv"
    return parse_candle_file(path, ts_unit="ms")


def parse_spot_candles(coin: str) -> pl.DataFrame | None:
    """Parse spot candle file for *coin*."""
    path = SPOT_CANDLE_DIR / f"{coin}_USDT_spot_candles_1h.csv"
    return parse_candle_file(path, ts_unit="ms")


def parse_perp_trades(coin: str) -> pl.DataFrame | None:
    """Parse perp trade-aggregated bar file for *coin*."""
    path = TRADES_PERP_DIR / f"{coin}_USDT_SWAP_perp_trades_1h.csv"
    return parse_trade_ohlcv_file(path, ts_unit="ms")


def parse_spot_trades(coin: str) -> pl.DataFrame | None:
    """Parse spot trade-aggregated bar file for *coin*."""
    path = TRADES_SPOT_DIR / f"{coin}_USDT_spot_trades_1h.csv"
    return parse_trade_ohlcv_file(path, ts_unit="ms")


def parse_mark_price(coin: str) -> pl.DataFrame | None:
    """
    Read mark price candle CSV.
    Returns: [timestamp (datetime), mark_price (f64)]
    """
    path = MARK_PRICE_DIR / f"{coin}_USDT_SWAP_mark_price_1h.csv"
    if not path.exists():
        return None

    df = pl.read_csv(path)
    df = df.with_columns(
        pl.from_epoch(pl.col("timestamp"), time_unit="ms")
        .cast(pl.Datetime("us"))
        .alias("timestamp"),
        pl.col("close").cast(pl.Float64).alias("mark_price"),
    ).select(["timestamp", "mark_price"])

    return df.sort("timestamp").unique(subset=["timestamp"], keep="last")


def parse_open_interest(coin: str) -> pl.DataFrame | None:
    """
    Read OKX open interest CSV.
    Returns: [timestamp (datetime), oi_usd (f64)]
    """
    path = OI_DIR / f"{coin}_USDT_SWAP_open_interest_1h.csv"
    if not path.exists():
        return None

    df = pl.read_csv(path)
    df = df.with_columns(
        pl.from_epoch(pl.col("timestamp"), time_unit="ms")
        .cast(pl.Datetime("us"))
        .alias("timestamp"),
        pl.col("oi_usd").cast(pl.Float64),
    ).select(["timestamp", "oi_usd"])

    return df.sort("timestamp").unique(subset=["timestamp"], keep="last")


# ═════════════════════════════════════════════════════════════════════════════
#  Helpers — hourly alignment
# ═════════════════════════════════════════════════════════════════════════════

def floor_to_hour(col: str = "timestamp") -> pl.Expr:
    """Truncate a datetime column to the hour."""
    return pl.col(col).dt.truncate("1h")


def align_funding_hourly(df: pl.DataFrame) -> pl.DataFrame:
    """
    Convert per-event funding rates (every 8 h) to an hourly series
    via forward-fill.
    """
    df = df.with_columns(floor_to_hour().alias("hour"))

    per_hour = (
        df.sort("timestamp")
        .group_by("hour")
        .agg(pl.col("funding_rate").last())
    )

    ts_min = per_hour["hour"].min()
    ts_max = per_hour["hour"].max()
    spine = pl.DataFrame({
        "hour": pl.datetime_range(ts_min, ts_max, interval="1h", eager=True),
    })

    aligned = spine.join(per_hour, on="hour", how="left")
    aligned = aligned.with_columns(pl.col("funding_rate").forward_fill())
    return aligned.rename({"hour": "timestamp"})


def align_single_col_hourly(
    df: pl.DataFrame,
    value_col: str,
    out_col: str,
) -> pl.DataFrame:
    """
    Floor timestamp to hour, keep last value per hour.
    Returns: [timestamp, out_col]
    """
    df = df.with_columns(floor_to_hour().alias("hour"))
    per_hour = (
        df.sort("timestamp")
        .group_by("hour")
        .agg(pl.col(value_col).last().alias(out_col))
    )
    return per_hour.rename({"hour": "timestamp"})


def align_ohlcv_hourly(
    df: pl.DataFrame,
    suffix: str,
) -> pl.DataFrame:
    """
    Floor timestamp to hour and aggregate OHLCV + VWAP.

    For proper OHLCV: open=first, high=max, low=min, close=last, volume=sum.
    For VWAP: if present in input, re-aggregate via pv_sum; otherwise set to close.

    Renames columns with _{suffix}_{EXCHANGE} suffix.
    Returns: [timestamp, open_{suffix}_{EXCHANGE}, high_..., close_..., volume_..., vwap_...]
    """
    has_vwap = "vwap" in df.columns

    if has_vwap:
        df = df.with_columns(
            floor_to_hour().alias("hour"),
            (pl.col("vwap") * pl.col("volume")).alias("pv_sum"),
        )
    else:
        # No VWAP column — use close as proxy
        df = df.with_columns(
            floor_to_hour().alias("hour"),
            (pl.col("close") * pl.col("volume")).alias("pv_sum"),
        )

    per_hour = (
        df.sort("timestamp")
        .group_by("hour")
        .agg([
            pl.col("open").first().alias(f"open_{suffix}_{EXCHANGE}"),
            pl.col("high").max().alias(f"high_{suffix}_{EXCHANGE}"),
            pl.col("low").min().alias(f"low_{suffix}_{EXCHANGE}"),
            pl.col("close").last().alias(f"close_{suffix}_{EXCHANGE}"),
            pl.col("volume").sum().alias(f"volume_{suffix}_{EXCHANGE}"),
            pl.col("pv_sum").sum().alias("_pv_sum"),
            pl.col("volume").sum().alias("_vol_sum"),
        ])
    )

    per_hour = per_hour.with_columns(
        pl.when(pl.col("_vol_sum") > 0)
        .then(pl.col("_pv_sum") / pl.col("_vol_sum"))
        .otherwise(pl.col(f"close_{suffix}_{EXCHANGE}"))
        .alias(f"vwap_{suffix}_{EXCHANGE}")
    ).drop(["_pv_sum", "_vol_sum"])

    return per_hour.rename({"hour": "timestamp"})


# ═════════════════════════════════════════════════════════════════════════════
#  Core — process one coin
# ═════════════════════════════════════════════════════════════════════════════

def process_coin(coin: str) -> tuple[pl.DataFrame, list[str]]:
    """
    Build the standardised hourly DataFrame for *coin*.

    Uses trade-aggregated OHLCV + VWAP bars if available (preferred),
    otherwise falls back to candle close prices for close only.
    Also includes mark price candles, forward-filled funding rate,
    and open interest.
    """
    funding_raw = parse_funding(coin)
    perp_candle_raw = parse_perp_candles(coin)
    spot_candle_raw = parse_spot_candles(coin)
    perp_trades_raw = parse_perp_trades(coin)
    spot_trades_raw = parse_spot_trades(coin)
    mark_raw = parse_mark_price(coin)
    oi_raw = parse_open_interest(coin)

    source_tags: list[str] = []

    # ── Funding (forward-filled to hourly) ────────────────────────────
    funding_hourly: pl.DataFrame | None = None
    if funding_raw is not None and len(funding_raw) > 0:
        funding_hourly = align_funding_hourly(funding_raw)
        funding_hourly = funding_hourly.rename({
            "funding_rate": f"funding_rate_raw_{EXCHANGE}",
        })
        source_tags.append("funding")

    # ── Perp OHLCV + VWAP (trade bars preferred, candle OHLCV fallback) ─
    perp_hourly: pl.DataFrame | None = None
    if perp_trades_raw is not None and len(perp_trades_raw) > 0:
        # Trade data exists → full OHLCV + real VWAP
        perp_hourly = align_ohlcv_hourly(perp_trades_raw, "perp")
        source_tags.append("perp_trades")

        # Coalesce with candle data for broader time coverage
        if perp_candle_raw is not None and len(perp_candle_raw) > 0:
            candle_ohlcv = align_ohlcv_hourly(perp_candle_raw, "perp")
            candle_renamed = candle_ohlcv.rename({
                c: f"_candle_{c}" for c in candle_ohlcv.columns if c != "timestamp"
            })
            perp_hourly = perp_hourly.join(
                candle_renamed, on="timestamp", how="full", coalesce=True,
            )
            for col in [f"open_perp_{EXCHANGE}", f"high_perp_{EXCHANGE}",
                        f"low_perp_{EXCHANGE}", f"close_perp_{EXCHANGE}",
                        f"volume_perp_{EXCHANGE}", f"vwap_perp_{EXCHANGE}"]:
                candle_col = f"_candle_{col}"
                if candle_col in perp_hourly.columns:
                    perp_hourly = perp_hourly.with_columns(
                        pl.coalesce([pl.col(col), pl.col(candle_col)]).alias(col)
                    )
            drop_cols = [c for c in perp_hourly.columns if c.startswith("_candle_")]
            perp_hourly = perp_hourly.drop(drop_cols)
            source_tags.append("perp_candles")
    elif perp_candle_raw is not None and len(perp_candle_raw) > 0:
        # No trade data — use candle OHLCV (vwap ≈ close)
        perp_hourly = align_ohlcv_hourly(perp_candle_raw, "perp")
        source_tags.append("perp_candles")

    # ── Spot OHLCV + VWAP (same logic as perp) ──────────────────────────
    spot_hourly: pl.DataFrame | None = None
    if spot_trades_raw is not None and len(spot_trades_raw) > 0:
        spot_hourly = align_ohlcv_hourly(spot_trades_raw, "spot")
        source_tags.append("spot_trades")

        if spot_candle_raw is not None and len(spot_candle_raw) > 0:
            candle_ohlcv = align_ohlcv_hourly(spot_candle_raw, "spot")
            candle_renamed = candle_ohlcv.rename({
                c: f"_candle_{c}" for c in candle_ohlcv.columns if c != "timestamp"
            })
            spot_hourly = spot_hourly.join(
                candle_renamed, on="timestamp", how="full", coalesce=True,
            )
            for col in [f"open_spot_{EXCHANGE}", f"high_spot_{EXCHANGE}",
                        f"low_spot_{EXCHANGE}", f"close_spot_{EXCHANGE}",
                        f"volume_spot_{EXCHANGE}", f"vwap_spot_{EXCHANGE}"]:
                candle_col = f"_candle_{col}"
                if candle_col in spot_hourly.columns:
                    spot_hourly = spot_hourly.with_columns(
                        pl.coalesce([pl.col(col), pl.col(candle_col)]).alias(col)
                    )
            drop_cols = [c for c in spot_hourly.columns if c.startswith("_candle_")]
            spot_hourly = spot_hourly.drop(drop_cols)
            source_tags.append("spot_candles")
    elif spot_candle_raw is not None and len(spot_candle_raw) > 0:
        spot_hourly = align_ohlcv_hourly(spot_candle_raw, "spot")
        source_tags.append("spot_candles")

    # ── Mark price (OKX has full history) ─────────────────────────────
    mark_hourly: pl.DataFrame | None = None
    if mark_raw is not None and len(mark_raw) > 0:
        mark_hourly = align_single_col_hourly(
            mark_raw, "mark_price", f"mark_price_{EXCHANGE}"
        )
        source_tags.append("mark_price")

    # ── Open interest ─────────────────────────────────────────────────
    oi_hourly: pl.DataFrame | None = None
    if oi_raw is not None and len(oi_raw) > 0:
        oi_hourly = align_single_col_hourly(
            oi_raw, "oi_usd", f"open_interest_usd_{EXCHANGE}"
        )
        source_tags.append("open_interest")

    # ── Merge on timestamp ────────────────────────────────────────────
    parts = [
        df for df in (
            funding_hourly, perp_hourly, spot_hourly,
            mark_hourly, oi_hourly,
        )
        if df is not None
    ]

    if not parts:
        raise ValueError(f"No data found for {coin}")

    merged = parts[0]
    for p in parts[1:]:
        merged = merged.join(p, on="timestamp", how="full", coalesce=True)

    # ── Add missing columns as null ───────────────────────────────────
    for col in SCHEMA_COLS:
        if col not in merged.columns and col != "timestamp":
            merged = merged.with_columns(
                pl.lit(None).cast(pl.Float64).alias(col)
            )

    # ── Reorder & sort ────────────────────────────────────────────────
    merged = merged.select(SCHEMA_COLS).sort("timestamp").unique(
        subset=["timestamp"], keep="last"
    )

    return merged, source_tags


# ═════════════════════════════════════════════════════════════════════════════
#  I/O — write parquet
# ═════════════════════════════════════════════════════════════════════════════

def write_parquet(
    df: pl.DataFrame,
    coin: str,
    overwrite: bool = False,
) -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    ts_min = df["timestamp"].min()
    ts_max = df["timestamp"].max()

    start_str = ts_min.strftime("%Y-%m-%d")
    end_str = ts_max.strftime("%Y-%m-%d")

    fname = f"{EXCHANGE}_{coin}_{start_str}_{end_str}.parquet"
    out_path = OUTPUT_DIR / fname

    if out_path.exists() and not overwrite:
        print(f"  SKIP  {fname} already exists (use --overwrite)")
        return out_path

    df.write_parquet(out_path, compression="zstd")
    return out_path


# ═════════════════════════════════════════════════════════════════════════════
#  Main
# ═════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Convert OKX raw CSVs to standardised hourly Parquet."
    )
    parser.add_argument(
        "--overwrite", action="store_true",
        help="Overwrite existing parquet files.",
    )
    parser.add_argument(
        "--coins", nargs="*", default=None,
        help="Process only these coins (default: auto-discover all).",
    )
    args = parser.parse_args()

    coins = args.coins or discover_coins()
    if not coins:
        print("ERROR: no coins found in raw directories.")
        sys.exit(1)

    print(f"Discovered coins: {coins}")
    print(f"Output directory: {OUTPUT_DIR}")
    print("=" * 72)

    results = []

    for coin in coins:
        try:
            df, sources = process_coin(coin)
            out_path = write_parquet(df, coin, overwrite=args.overwrite)

            null_cols = [
                c for c in SCHEMA_COLS
                if c != "timestamp" and df[c].is_null().all()
            ]

            ts_min = df["timestamp"].min().strftime("%Y-%m-%d")
            ts_max = df["timestamp"].max().strftime("%Y-%m-%d")
            size_kb = out_path.stat().st_size / 1024

            results.append({
                "coin": coin, "rows": len(df),
                "start": ts_min, "end": ts_max,
                "sources": ", ".join(sources),
                "null_cols": ", ".join(null_cols) if null_cols else "none",
                "file": out_path.name, "size_kb": size_kb,
            })

            print(
                f"  {coin:5s}  "
                f"{len(df):>7,} rows  "
                f"{ts_min} -> {ts_max}  "
                f"sources=[{', '.join(sources)}]  "
                f"null_cols=[{', '.join(null_cols) if null_cols else 'none'}]  "
                f"{size_kb:>7.1f} KB"
            )

        except Exception as exc:
            print(f"  {coin:5s}  ERROR: {exc}")

    print("=" * 72)
    total_rows = sum(r["rows"] for r in results)
    total_kb = sum(r["size_kb"] for r in results)
    print(f"  TOTAL  {len(results)} coins, {total_rows:,} rows, {total_kb:.1f} KB")


if __name__ == "__main__":
    main()
