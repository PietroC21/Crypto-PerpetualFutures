# Crypto-PerpetualFutures

Fully bidirectional, delta-neutral carry strategy on crypto perpetual futures. Harvests funding rate dislocations in both directions across 15 liquid pairs using a z-score signal that identifies anomalies relative to each asset's own funding history.

---

# Get Started

### 1. Create and activate the conda environment

```bash
conda create -n qts-perp python=3.11 -y
conda activate qts-perp
pip install ccxt pandas pyarrow requests tqdm
```

> **Important:** always use the `qts-perp` environment. Reading parquet files written by a different Python/PyArrow version will fail with a histogram mismatch error.

### 2. Select the interpreter in VSCode

`Cmd+Shift+P` → **Python: Select Interpreter** → pick `qts-perp`

For notebooks: `Cmd+Shift+P` → **Notebook: Select Notebook Kernel** → pick `qts-perp`

---

# Data Pipeline

Run the steps below in order. All scripts are idempotent — re-running resumes from the last saved timestamp.

### Step 1 — Fetch Binance data (requires VPN)

Binance blocks US-based IPs (HTTP 451). Connect your VPN to a non-US server first.

```bash
# Test pull — BTC + ETH, last year only
python src/fetch_binance.py --symbols BTC ETH --start 2024-01-01

# Full pull — all 15 pairs, Jan 2020 → today (~30 min)
python src/fetch_binance.py
```

Saves to `data/raw/binance/` (one Parquet file per symbol per series):
- `funding_rates/` — 8h funding rates
- `ohlcv_perp/` — 8h mark-price OHLCV
- `ohlcv_spot/` — 8h spot index OHLCV

### Step 2 — Fetch Open Interest from Bybit (VPN may be needed)

Binance's OI endpoint is hard-capped at ~30 days of history. Bybit provides free OI going back to Dec 2020.

> **Note:** Bybit also blocks US IPs via CloudFront. If you get a 403, switch your VPN to a different server.

```bash
# Full pull — all 15 pairs (~30–45 min)
python src/fetch_bybit_oi.py

# Subset
python src/fetch_bybit_oi.py --symbols BTC ETH SOL
```

Saves 1h OI to `data/raw/binance/open_interest/` (same directory as Binance files).

### Step 3 — Fetch macro data

**FRED (3m T-bill) — runs anywhere:**
```bash
python src/fetch_macro.py --fred-only
```

**VIX + SPY — requires Bloomberg Terminal (run on university computer):**
```bash
# Install on the Bloomberg computer first:
pip install pdblp pandas requests

python src/fetch_macro.py --bloomberg-only
```

Then copy `vix_daily.csv` and `spy_daily.csv` from the Bloomberg computer's `src/macro_output/` folder to `data/raw/macro/` on your main machine.

Saves to `data/raw/macro/`:
- `fred_rfr.csv` — 3m T-bill annualised % + daily decimal
- `vix_daily.csv` — VIX daily close
- `spy_daily.csv` — SPY daily OHLCV

### Step 4 — Build master panel

```bash
python src/build_panel.py
```

Merges all raw data into `data/processed/master_panel.parquet`. Takes ~10 seconds.

---

# Master Panel

`data/processed/master_panel.parquet`

| Property | Value |
|---|---|
| Shape | (101,115 × 11) |
| Index | `MultiIndex(datetime [UTC, 8h], symbol)` |
| Timestamps | 6,741 standard settlement times (00:00, 08:00, 16:00 UTC) |
| Symbols | 15 (see universe below) |

**Columns:**

| Column | Description |
|---|---|
| `funding_rate` | Funding rate at each 8h settlement |
| `perp_open/high/low/close/volume` | Mark-price 8h OHLCV |
| `spot_close` | Spot index 8h close |
| `open_interest` | OI in base currency (Bybit, 1h → aligned to 8h) |
| `vix_close` | VIX daily close (forward-filled to 8h grid) |
| `spy_close` | SPY daily close (forward-filled to 8h grid) |
| `rfr_daily_decimal` | 3m T-bill daily rate (forward-filled, 100% complete) |

**Coverage:**

| Column | Non-null | Notes |
|---|---|---|
| `funding_rate` | 87.5% | ARB/POL only from 2023 |
| `perp_close` | 87.4% | Same reason |
| `open_interest` | 73.5% | Bybit OI starts Dec 2020 |
| `vix_close` | 97.7% | Null only on US market holidays |
| `rfr_daily_decimal` | 100% | FRED covers every calendar day |

**How to load:**
```python
import pandas as pd

panel = pd.read_parquet("data/processed/master_panel.parquet")

# Single symbol time series
btc = panel.xs("BTCUSDT", level="symbol").dropna(subset=["funding_rate"])

# Funding rate matrix — shape (6741, 15)
funding = panel["funding_rate"].unstack("symbol")

# Cross-section at a specific timestamp
panel.loc[pd.Timestamp("2024-01-01 00:00", tz="UTC")]
```

---

# Universe (15 symbols)

| Symbol | Notes |
|---|---|
| BTCUSDT | Full history from Jan 2020 |
| ETHUSDT | Full history from Jan 2020 |
| SOLUSDT | |
| BNBUSDT | |
| XRPUSDT | |
| DOGEUSDT | |
| AVAXUSDT | |
| POLUSDT | MATIC rebranded → POL on Binance/Bybit in Sept 2023; data from ~Sept 2024 on Binance perps |
| LINKUSDT | |
| ADAUSDT | |
| DOTUSDT | |
| ATOMUSDT | |
| LTCUSDT | |
| UNIUSDT | |
| ARBUSDT | Listed ~Sept 2023 |

---

# Known Data Quirks

- **Binance funding timestamps** occasionally have sub-millisecond offsets (e.g. `00:00:00.002`). `build_panel.py` rounds all timestamps to the nearest second before aligning.
- **Off-grid funding events** (~1,600 Binance settlements at non-standard hours for newly listed symbols) are dropped — the panel keeps only standard 00:00/08:00/16:00 UTC timestamps.
- **Binance OI history** is hard-capped at ~30 days regardless of timeframe. Use `fetch_bybit_oi.py` instead.
- **VPN required** for both Binance (HTTP 451) and Bybit (CloudFront 403) from US IPs.

---

# Project Structure

```
Crypto-PerpetualFutures/
│
├── data/
│   ├── raw/
│   │   ├── binance/
│   │   │   ├── funding_rates/       # per-symbol parquet, 8h
│   │   │   ├── ohlcv_perp/          # mark-price 8h OHLCV
│   │   │   ├── ohlcv_spot/          # spot index 8h OHLCV
│   │   │   └── open_interest/       # Bybit 1h OI (stored here for convenience)
│   │   └── macro/
│   │       ├── vix_daily.csv        # Bloomberg Terminal
│   │       ├── spy_daily.csv        # Bloomberg Terminal
│   │       └── fred_rfr.csv         # FRED public API
│   │
│   └── processed/
│       └── master_panel.parquet     # merged panel, ready for notebook
│
├── src/
│   ├── fetch_binance.py     # funding rates, perp/spot OHLCV (Step 1)
│   ├── fetch_bybit_oi.py    # open interest from Bybit (Step 2)
│   ├── fetch_macro.py       # Bloomberg + FRED macro data (Step 3)
│   ├── build_panel.py       # merges everything → master_panel (Step 4)
│   └── validate.py          # data quality checks (optional)
│
├── strategy.py              # importable signal + backtest logic
└── notebook.ipynb           # research notebook
```
