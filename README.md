# binance-collector 📈

A zero-infrastructure OHLCV candle collector for Binance, powered entirely by **GitHub Actions**. Every 5 minutes, a scheduled workflow fetches the latest ETH/USDT candlestick data from the Binance public API and commits it directly to this repository as a CSV — no server, no database, no cloud costs.

---

## What it does

- **Pulls 5-minute OHLCV candles** for `ETHUSDT` from the [Binance Vision public API](https://data-api.binance.vision) (no API key required)
- **Backfills 60 days** of historical data on the first run
- **Appends incrementally** on every subsequent run — only fetching candles newer than the last saved one
- **Commits the data** straight to `data/ETHUSDT_5m.csv` in the repo, making it instantly accessible via GitHub's raw file URLs
- **Writes a heartbeat** timestamp to `state/heartbeat_utc.txt` on every run so you can tell the schedule is alive at a glance

---

## Repository structure

```
binance-collector/
├── .github/workflows/
│   ├── collect.yml       # Scheduled every 5 min — runs collector.py and commits data
│   └── migrate_csv.yml   # One-time migration workflow (adds open_time_utc column)
├── collector.py          # Main collection script
├── migrate_inline.py     # One-time CSV migration utility
├── requirements.txt      # Python deps (just: requests)
├── data/
│   └── ETHUSDT_5m.csv    # The collected candle data lives here
└── state/
    ├── last_open_time_ETHUSDT_5m.txt  # Cursor for incremental updates
    └── heartbeat_utc.txt              # Timestamp of last successful run
```

---

## CSV format

Each row in `data/ETHUSDT_5m.csv` represents one 5-minute candle:

| Column | Description |
|---|---|
| `open_time_ms` | Candle open time (Unix milliseconds) |
| `open_time_utc` | Candle open time (human-readable UTC string) |
| `open` | Opening price |
| `high` | High price |
| `low` | Low price |
| `close` | Closing price |
| `volume` | Base asset volume (ETH) |
| `close_time_ms` | Candle close time (Unix milliseconds) |
| `quote_asset_volume` | Quote asset volume (USDT) |
| `num_trades` | Number of trades in the candle |
| `taker_buy_base` | Taker buy base asset volume |
| `taker_buy_quote` | Taker buy quote asset volume |

---

## Setup

### 1. Fork or clone this repo

No additional infrastructure needed — GitHub Actions does everything.

### 2. Enable Actions

Make sure GitHub Actions is enabled on your fork (`Settings → Actions → Allow all actions`).

### 3. Let it run

The `collect.yml` workflow is scheduled to run every 5 minutes automatically. On first run it will backfill the last 60 days of candles, then switch to incremental mode.

You can also trigger it manually at any time from the **Actions** tab → **Collect ETHUSDT 5m Candles** → **Run workflow**.

---

## Changing the symbol or interval

Edit the constants at the top of `collector.py`:

```python
SYMBOL        = "ETHUSDT"   # e.g. "BTCUSDT", "SOLUSDT"
INTERVAL      = "5m"        # e.g. "1m", "15m", "1h", "1d"
DAYS_BACKFILL = 60          # how far back to go on first run
```

Also update the workflow name and any file path references in `collect.yml` to match.

---

## Accessing the data

Once the workflow has run, the CSV is available as a raw file:

```
https://raw.githubusercontent.com/<your-username>/binance-collector/main/data/ETHUSDT_5m.csv
```

You can fetch it directly in Python:

```python
import pandas as pd

url = "https://raw.githubusercontent.com/<your-username>/binance-collector/main/data/ETHUSDT_5m.csv"
df = pd.read_csv(url, parse_dates=["open_time_utc"])
print(df.tail())
```

---

## One-time CSV migration

If you have an older version of the CSV without the `open_time_utc` column, run the migration workflow once from the **Actions** tab → **One-time CSV migration** → **Run workflow**. It will add the human-readable timestamp column in place and commit the result.

---

## How the state tracking works

After each successful run, the collector writes the `open_time_ms` of the most recently collected candle to `state/last_open_time_ETHUSDT_5m.txt`. On the next run, it reads this file and fetches only candles with a timestamp newer than that — so there's no duplication and no gaps.

If the state file is missing or the CSV is empty, the collector automatically falls back to a full backfill.

---

## Requirements

- Python 3.11+
- `requests==2.32.3` (the only dependency)

---

## License

Copyright (No license)
