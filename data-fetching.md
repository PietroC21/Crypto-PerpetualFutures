# Data Fetching Specification

## Overview

For each asset (e.g. BTC, ETH, SOL), we fetch **hourly data from January 1st 2020 to present**
across multiple exchange.

Each exchange gets its own set of columns (suffixed `_binance`, `_bybit`, `_hyperliquid`).
The final dataframe is wide-format: one row per UTC hour, one asset at a time.

---

## 1: Fetch column List (for one given exchange)

| Column | Description |
|---|---|
| `timestamp` | UTC timestamp, hourly frequency, shared index across all exchanges |
| `funding_rate_raw_{exchange}` | Raw funding rate as published by the exchange (e.g. 0.0001 = 0.01%) |
| `best_bid_{exchange}` | Best bid on the **perp** market at that timestamp |
| `best_ask_{exchange}` | Best ask on the **perp** market at that timestamp |
| `mark_price_{exchange}` | Official mark price used by the exchange for funding & PnL calculation |
| `open_interest_usd_{exchange}` | Total open interest in USD on the perp market |
| `spot_best_bid_{exchange}` | Best bid on the **spot** market |
| `spot_best_ask_{exchange}` | Best ask on the **spot** market |

> Everything else (mid price, funding rate normalized to hourly, spread, net edge, z-score, OI ratio)
> is **computed from these columns** — do not fetch it, we will derive it.



**Important Notes: Some exchange don't have native spot market (like hyperliquid)**
`spot_best_bid_hyperliquid` and `spot_best_ask_hyperliquid` will always be `NaN`.
Do not attempt to fetch spot data there, it doesn't exist .

### Sample Dataframe — BTC, 3 rows (Binance only shown for readability)

| timestamp (UTC) | funding_rate_raw_binance | best_bid_binance | best_ask_binance | mark_price_binance | open_interest_usd_binance | spot_best_bid_binance | spot_best_ask_binance |
|---|---|---|---|---|---|---|---|
| 2024-01-15 00:00:00 | 0.000100 | 42,850.10 | 42,851.30 | 42,849.90 | 8,412,300,000 | 42,848.00 | 42,849.50 |
| 2024-01-15 01:00:00 | 0.000100 | 42,901.50 | 42,902.80 | 42,901.30 | 8,430,150,000 | 42,899.20 | 42,900.60 |
| 2024-01-15 02:00:00 | 0.000100 | 42,880.00 | 42,881.50 | 42,879.80 | 8,398,700,000 | 42,877.50 | 42,879.00 |

> Note: `funding_rate_raw_binance` stays flat at 0.000100 across rows 00:00–07:00 because
> Binance only updates at the 8h settlement. It will change at 08:00 UTC.
> Bybit and Hyperliquid columns follow the exact same structure with their own suffixes.


---

## 2: Fetch Exchange Constants

These are fixed per exchange and should be stored separately — **not as dataframe columns**.

```python
EXCHANGE_CONSTANTS = {
    "binance":     {"funding_interval_hours": 8,  "perp_taker_fee_bps": 5.0,  "spot_taker_fee_bps": 10.0},
    "bybit":       {"funding_interval_hours": 8,  "perp_taker_fee_bps": 5.5,  "spot_taker_fee_bps": 10.0},
    "hyperliquid": {"funding_interval_hours": 1,  "perp_taker_fee_bps": 2.5,  "spot_taker_fee_bps": None},
}
```


---

## Time Range

- **Start**: 2020-01-01 00:00:00 UTC
- **End**: present (or latest available)
- **Frequency**: 1 hour
- **Coverage note**: Binance BTC funding history goes back to late 2019.
  Most altcoin perps on all three exchanges only start from 2021–2022.
  Expect `NaN` for earlier rows on non-BTC assets.



