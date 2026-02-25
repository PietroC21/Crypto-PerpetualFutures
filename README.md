# Crypto-PerpetualFutures
We build a fully bidirectional, delta-neutral carry strategy on crypto perpetual futures. We harvest funding rate dislocations in both directions across 15 liquid pairs, using a z-score signal that identifies anomalies relative to each asset's own funding history.


# Get Started

### 1. Create and activate the conda environment

```bash
conda create -n qts-perp python=3.11 -y
conda activate qts-perp
pip install ccxt pandas pyarrow requests tqdm
```

### 2. Select the interpreter in VSCode

`Cmd+Shift+P` → **Python: Select Interpreter** → pick `qts-perp`

### 3. Connect VPN (required from US IPs)

Binance blocks US-based IPs (HTTP 451). Connect your VPN to a non-US server before running any data fetch.

### 4. Fetch Binance data

```bash
# Test pull — BTC + ETH, last year only
python src/fetch_binance.py --symbols BTC ETH --start 2024-01-01

# Full pull — all 15 pairs, Jan 2020 → today (~30 min)
python src/fetch_binance.py
```

Output lands in `data/raw/binance/` as Parquet files (one per symbol per series).
Re-running is safe — the script resumes from the last saved timestamp.

### 5. Fetch Open Interest (Bybit — no VPN needed)

Binance's OI endpoint is capped at ~30 days. Bybit provides free OI history back to Dec 2020.

```bash
# Full pull — all 15 pairs (~30-45 min)
python src/fetch_bybit_oi.py

# Subset
python src/fetch_bybit_oi.py --symbols BTC ETH SOL
```

Output lands in `data/raw/binance/open_interest/` — same directory as the rest.

---

# Proposed Structure

```
Crypto-PerpetualFutures/
│
├── data/
│   ├── raw/
│   │   ├── binance/
│   │   │   ├── funding_rates/       # per-asset parquet files
│   │   │   ├── ohlcv_perp/          # mark price 8h OHLCV
│   │   │   ├── ohlcv_spot/          # spot index price
│   │   │   └── open_interest/       # OI history
│   │   ├── macro/
│   │   │   ├── vix_daily.csv        # Bloomberg
│   │   │   ├── spy_daily.csv        # Bloomberg / Databento
│   │   │   └── fred_rfr.csv         # 3m T-bill
│   │   └── databento/
│   │       └── spy_intraday.parquet # 5-day rolling drawdown source
│   │
│   └── processed/
│       └── master_panel.parquet     # clean merged dataset
│
├── src/
│   ├── fetch_binance.py     # funding rates, OI, OHLCV
│   ├── fetch_macro.py       # Bloomberg + FRED
│   ├── fetch_databento.py   # Databento SDK
│   ├── validate.py          # data checks & assertions
│   └── build_panel.py       # merges everything → master_panel
│
├── strategy.py              # importable trading logic
└── notebook.ipynb           # research notebook 
```