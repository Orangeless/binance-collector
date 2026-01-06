import csv
import os
import time
import requests

BASE = "https://data-api.binance.vision"
SYMBOL = "ETHUSDT"
INTERVAL = "5m"
LIMIT = 1000

DATA_DIR = "data"
STATE_DIR = "state"

CSV_PATH = os.path.join(DATA_DIR, f"{SYMBOL}_{INTERVAL}.csv")
STATE_PATH = os.path.join(STATE_DIR, f"last_open_time_{SYMBOL}_{INTERVAL}.txt")

CSV_HEADER = [
    "open_time_ms",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time_ms",
    "quote_asset_volume",
    "num_trades",
    "taker_buy_base",
    "taker_buy_quote",
]

def ensure_dirs() -> None:
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(STATE_DIR, exist_ok=True)

def ensure_csv_header() -> None:
    if os.path.exists(CSV_PATH) and os.path.getsize(CSV_PATH) > 0:
        return
    with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(CSV_HEADER)

def read_last_open_time() -> int | None:
    if not os.path.exists(STATE_PATH):
        return None
    with open(STATE_PATH, "r", encoding="utf-8") as f:
        s = f.read().strip()
    return int(s) if s else None

def write_last_open_time(ms: int) -> None:
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        f.write(str(ms))

def fetch_klines(start_ms: int | None, limit: int = LIMIT):
    params = {"symbol": SYMBOL, "interval": INTERVAL, "limit": limit}
    if start_ms is not None:
        params["startTime"] = start_ms
    r = requests.get(f"{BASE}/api/v3/klines", params=params, timeout=20)
    r.raise_for_status()
    return r.json()

def kline_to_row(k):
    # kline format:
    # [ openTime, open, high, low, close, volume, closeTime,
    #   quoteAssetVolume, numberOfTrades, takerBuyBaseAssetVolume,
    #   takerBuyQuoteAssetVolume, ignore ]
    return [
        str(int(k[0])),
        k[1], k[2], k[3], k[4], k[5],
        str(int(k[6])),
        k[7],
        str(int(k[8])),
        k[9],
        k[10],
    ]

def append_rows(rows) -> None:
    with open(CSV_PATH, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerows(rows)

def main():
    ensure_dirs()
    ensure_csv_header()

    last_open = read_last_open_time()

    # Fetch from watermark+1 so we only grab new candles
    start = (last_open + 1) if last_open is not None else None

    klines = fetch_klines(start_ms=start, limit=LIMIT)
    now_ms = int(time.time() * 1000)

    # ONLY closed candles (stable for training)
    closed = [k for k in klines if int(k[6]) <= now_ms]

    if not closed:
        print("No new closed candles.")
        return

    rows = [kline_to_row(k) for k in closed]
    append_rows(rows)

    newest_open = int(closed[-1][0])
    write_last_open_time(newest_open)

    print(f"Appended {len(rows)} candles. New watermark open_time_ms={newest_open}")

if __name__ == "__main__":
    main()
