# Crypto-PerpetualFutures
We build a fully bidirectional, delta-neutral carry strategy on crypto perpetual futures. We harvest funding rate dislocations in both directions across 15 liquid pairs, using a z-score signal that identifies anomalies relative to each asset's own funding history.


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