import csv
import os
from datetime import datetime, timezone

CSV_PATH = os.path.join("data", "ETHUSDT_5m.csv")
TMP_PATH = os.path.join("data", "ETHUSDT_5m.csv.tmp")

def ms_to_utc_str(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S UTC"
    )

if not os.path.exists(CSV_PATH):
    raise SystemExit(f"CSV not found: {CSV_PATH}")

with open(CSV_PATH, "r", newline="", encoding="utf-8") as fin:
    reader = csv.reader(fin)
    header = next(reader)

    if "open_time_utc" in header:
        print("CSV already migrated.")
        raise SystemExit(0)

    new_header = header[:1] + ["open_time_utc"] + header[1:]

    with open(TMP_PATH, "w", newline="", encoding="utf-8") as fout:
        writer = csv.writer(fout)
        writer.writerow(new_header)

        rows = 0
        for row in reader:
            open_ms = int(row[0])
            writer.writerow(row[:1] + [ms_to_utc_str(open_ms)] + row[1:])
            rows += 1

os.replace(TMP_PATH, CSV_PATH)
print(f"Migrated {rows} rows.")
