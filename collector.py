import csv
import os
import time
import requests
from typing import Optional, List, Any
from datetime import datetime, timezone

# IMPORTANT: GitHub runners often get 451 from api.binance.com
BASE = "https://data-api.binance.vision"

SYMBOL = "ETHUSDT"
INTERVAL = "5m"
LIMIT = 1000
DAYS_BACKFILL = 60

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

# ---------- TIME HELPERS ----------

def ms_to_utc_str(ms: int) -> str:
    dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S UTC")

def utc_str_to_ms(s: str) -> int:
    # Expected: "YYYY-MM-DD HH:MM:SS UTC"
    dt = datetime.strptime(s.replace(" UTC", ""), "%Y-%m-%d %H:%M:%S")
    dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)

# ---------------------------------

def ensure_dirs() -> None:
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(STATE_DIR, exist_ok=True)

def csv_is_empty_or_missing() -> bool:
    return (not os.path.exists(CSV_PATH)) or os.path.getsize(CSV_PATH) == 0

def ensure_csv_header() -> None:
    if not csv_is_empty_or_missing():
        return
    with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow(CSV_HEADER)

def read_last_open_time_ms() -> Optional[int]:
    """
    Backward compatible:
    - If state file is digits only => treat as ms (old format)
    - Else => treat as UTC string (new format)
    """
    if not os.path.exists(STATE_PATH):
        return None

    with open(STATE_PATH, "r", encoding="utf-8") as f:
        s = f.read().strip()

    if not s:
        return None

    if s.isdigit():
        return int(s)

    return utc_str_to_ms(s)

def write_last_open_time_ms(ms: int) -> None:
    # Always write new format (UTC string)
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        f.write(ms_to_utc_str(ms))

def fetch_klines(start_ms: Optional[int], limit: int = LIMIT) -> List[List[Any]]:
    params = {"symbol": SYMBOL, "interval": INTERVAL, "limit": limit}
    if start_ms is not None:
        params["startTime"] = start_ms
    r = requests.get(f"{BASE}/api/v3/klines", params=params, timeout=20)
    r.raise_for_status()
    return r.json()

def kline_to_row(k: List[Any]) -> List[str]:
    # [ openTime, open, high, low, close, volume, closeTime,
    #   quoteAssetVolume, numberOfTrades, takerBuyBaseAssetVolume,
    #   takerBuyQuoteAssetVolume, ignore ]
    return [
        str(int(k[0])),
        str(k[1]), str(k[2]), str(k[3]), str(k[4]), str(k[5]),
        str(int(k[6])),
        str(k[7]),
        str(int(k[8])),
        str(k[9]),
        str(k[10]),
    ]

def append_rows(rows: List[List[str]]) -> None:
    if not rows:
        return
    with open(CSV_PATH, "a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerows(rows)

def filter_closed(klines: List[List[Any]], now_ms: int) -> List[List[Any]]:
    # Keep only candles whose close time is <= now (closed candles)
    return [k for k in klines if int(k[6]) <= now_ms]

def backfill_last_n_days() -> Optional[int]:
    """
    Backfills last DAYS_BACKFILL days of 5m candles.
    Returns newest open_time_ms inserted, or None if nothing inserted.
    """
    now_ms = int(time.time() * 1000)
    start_ms = now_ms - DAYS_BACKFILL * 24 * 60 * 60 * 1000

    cursor = start_ms
    newest_open = None
    total_rows = 0

    while True:
        kl = fetch_klines(start_ms=cursor, limit=LIMIT)
        if not kl:
            break

        closed = filter_closed(kl, now_ms)
        if closed:
            rows = [kline_to_row(k) for k in closed]
            append_rows(rows)
            newest_open = int(closed[-1][0])
            total_rows += len(rows)

        last_open = int(kl[-1][0])
        if last_open <= cursor:
            break
        cursor = last_open + 1

        if cursor >= now_ms:
            break

        time.sleep(0.2)

    print(f"Backfill done. Added ~{total_rows} rows.")
    return newest_open

def update_incremental() -> Optional[int]:
    """
    Appends only NEW closed candles after the watermark.
    Returns newest open_time_ms appended, or None if nothing new.
    """
    now_ms = int(time.time() * 1000)
    last_open = read_last_open_time_ms()
    start = (last_open + 1) if last_open is not None else None

    klines = fetch_klines(start_ms=start, limit=LIMIT)
    closed = filter_closed(klines, now_ms)

    if not closed:
        print("No new closed candles.")
        return None

    rows = [kline_to_row(k) for k in closed]
    append_rows(rows)

    newest_open = int(closed[-1][0])
    print(f"Incremental update. Appended {len(rows)} rows.")
    return newest_open

def main():
    ensure_dirs()
    ensure_csv_header()

    last_open = read_last_open_time_ms()

    # First run / empty CSV -> backfill 60 days
    if last_open is None or csv_is_empty_or_missing():
        print("No watermark / empty CSV detected -> running 60-day backfill...")
        newest = backfill_last_n_days()
        if newest is not None:
            write_last_open_time_ms(newest)

    # Always do incremental after backfill
    newest2 = update_incremental()
    if newest2 is not None:
        write_last_open_time_ms(newest2)

if __name__ == "__main__":
    main()
