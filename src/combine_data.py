import os
import pandas as pd
from glob import glob

def combine_exchange_data(coin, exchanges, data_dir="./data", start="2020-01-01", end="2026-03-01"):
    """
    Combine all exchange parquet files for a given coin into a master panel.
    Handles both 'date' column and index as timestamp.
    """
    coin_dir = os.path.join(data_dir, coin.upper())
    file_pattern = os.path.join(coin_dir, f"*_{coin.upper()}_*.parquet")
    files = glob(file_pattern)
    
    # Build master hourly index
    idx = pd.date_range(start=start, end=end, freq="1h", tz="UTC")
    master = pd.DataFrame(index=idx)
    
    keep_patterns = [
        "funding_rate_1h_{ex}", "open_interest_{ex}",
        "vwap_perp_{ex}", "vwap_spot_{ex}", "mark_price_{ex}"
    ]
    
    for ex in exchanges:
        ex_files = [f for f in files if f"/{ex}_" in f.lower()]
        if not ex_files:
            continue
        df = pd.read_parquet(ex_files[0])
        

        # Use 'timestamp' column as index if present, else 'date', else index
        if "timestamp" in df.columns:
            df.index = pd.to_datetime(df["timestamp"])
            df = df.drop(columns=["timestamp"])
        elif "date" in df.columns:
            df.index = pd.to_datetime(df["date"])
            df = df.drop(columns=["date"])
        else:
            df.index = pd.to_datetime(df.index)

        # Floor to hour BEFORE deduplication
        df.index = df.index.tz_localize("UTC") if df.index.tz is None else df.index.tz_convert("UTC")
        df.index = df.index.floor("h")
        df = df[~df.index.duplicated(keep="first")]

        print(f"\n[{ex.upper()}] First 5 index values after flooring:")
        print(df.index[:5])
        print(f"[{ex.upper()}] Columns: {list(df.columns)}")

        keep_cols = []
        for pat in keep_patterns:
            col = pat.format(ex=ex)
            if col in df.columns:
                keep_cols.append(col)
        if not keep_cols:
            continue

        df_sub = df[keep_cols].copy()
        # Index is already UTC and floored
        master = master.join(df_sub, how="left")
    
    out_path = os.path.join(coin_dir, f"{coin.upper()}_master.parquet")
    master.to_parquet(out_path)
    print(f"✅ Saved {out_path} ({master.shape[0]} rows, {master.shape[1]} columns)")
    return master