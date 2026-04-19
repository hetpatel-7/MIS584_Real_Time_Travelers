"""
Standalone test for the consumer preprocessing pipeline.

Reads raw rows directly from data.db (exactly what the Kafka producer sends),
runs each row through the full preprocessing pipeline, and prints both the
raw INPUT and the processed OUTPUT so you can verify the merge is working.

Run from the kafka_coding/ directory:
    python test_preprocessing.py

No Kafka or Docker needed.
"""

import os
import sys
import sqlite3
import pprint

# Make sure tl_preprocessing can be found when run from kafka_coding/
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from tl_preprocessing import (
    load_weather_data,
    preprocess_sensor_message,
    lookup_weather,
    HourlyAggregator,
    WEATHER_COLS_OUT,
)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
_HERE          = os.path.dirname(os.path.abspath(__file__))
DB_PATH        = os.path.join(_HERE, "..", "data.db")
WEATHER_PATH   = os.path.join(_HERE, "..", "talinn_weather_data_raw.csv")

# Producer start date and a 3-hour window (gives ~323 rows across 3 hourly buckets)
START_TS = "2023-01-09T15:00:00+02:00"
END_TS   = "2023-01-09T18:10:00+02:00"

SEP  = "-" * 80
SEP2 = "=" * 80

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def fmt_dict(d: dict, indent: int = 4) -> str:
    return pprint.pformat(d, indent=indent, width=100)


def print_section(title: str):
    print(f"\n{SEP2}")
    print(f"  {title}")
    print(SEP2)


# ---------------------------------------------------------------------------
# Main test
# ---------------------------------------------------------------------------
def main():
    # ── 1. Load weather data ──────────────────────────────────────────────
    print_section("STEP 0 — Loading weather data")
    weather_df = load_weather_data(WEATHER_PATH)
    print(f"  Rows loaded : {len(weather_df)}")
    print(f"  Time range  : {weather_df['time'].min()}  →  {weather_df['time'].max()}")
    print(f"  Columns     : {list(weather_df.columns)}")

    # ── 2. Fetch raw rows from data.db ────────────────────────────────────
    print_section(f"STEP 1 — Raw sensor rows from data.db  ({START_TS} → {END_TS})")
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    raw_rows = cursor.execute(f"""
        SELECT id, andurid_id, name, ts, pir, in_count, out_count, humidity, temp
        FROM TallinnData
        WHERE ts > '{START_TS}' AND ts <= '{END_TS}'
        ORDER BY ts
    """).fetchall()
    conn.close()

    print(f"  Total raw rows fetched: {len(raw_rows)}\n")
    print(f"  First 3 raw messages (this is what the Kafka producer sends):\n")
    for i, row in enumerate(raw_rows[:3]):
        d = dict(row)
        print(f"  [{i+1}] {fmt_dict(d)}\n")

    # ── 3. Run through the full pipeline ─────────────────────────────────
    print_section("STEP 2 + 3 — Preprocessing + Weather Merge + Hourly Aggregation")

    aggregator   = HourlyAggregator()
    hourly_rows  = []

    discarded    = 0
    processed    = 0
    shown_detail = 0   # show detailed input→output for the first 5 valid messages

    print(f"  Processing {len(raw_rows)} messages...\n")
    print(f"  {'─'*60}")
    print(f"  Showing first 5 valid messages in detail:")
    print(f"  {'─'*60}\n")

    for raw_row in raw_rows:
        raw_msg = dict(raw_row)

        # ── Step 1: sensor preprocessing ──────────────────────────────
        sensor_record = preprocess_sensor_message(raw_msg)

        if sensor_record is None:
            discarded += 1
            continue

        # ── Step 2: weather lookup ─────────────────────────────────────
        weather_record = lookup_weather(sensor_record["ts_utc"], weather_df)

        # ── Step 3: hourly aggregation ────────────────────────────────
        hourly_row = aggregator.add(sensor_record, weather_record)
        if hourly_row is not None:
            hourly_rows.append(hourly_row)

        processed += 1

        # Print detail for first 5 valid messages
        if shown_detail < 5:
            shown_detail += 1
            print(f"  ┌─ RAW INPUT (message {shown_detail}) ─────────────────────────────")
            print(f"  │  name      : {raw_msg['name']}")
            print(f"  │  ts        : {raw_msg['ts']}")
            print(f"  │  pir       : {raw_msg['pir']}")
            print(f"  │  in_count  : {raw_msg['in_count']}")
            print(f"  │  out_count : {raw_msg['out_count']}")
            print(f"  │  humidity  : {raw_msg['humidity']}  (sensor reading — will be replaced)")
            print(f"  │  temp      : {raw_msg['temp']}  (sensor reading — will be replaced)")
            print(f"  ├─ AFTER SENSOR PREPROCESSING ──────────────────────────────")
            print(f"  │  device      : {sensor_record['device']}   ← [0]/[1] prefix stripped")
            print(f"  │  ts_utc      : {sensor_record['ts_utc']}   ← converted to UTC")
            print(f"  │  ts_hour     : {sensor_record['ts_hour']}  ← floored to hour (aggregation bucket)")
            print(f"  │  total_count : {sensor_record['total_count']}   ← in + out")
            print(f"  │  date        : {sensor_record['date']}")
            print(f"  │  day_of_week : {sensor_record['day_of_week']}  (0=Mon)")
            print(f"  │  year/month  : {sensor_record['year']} / {sensor_record['month']}")
            print(f"  ├─ AFTER WEATHER LOOKUP (merge_asof backward) ───────────────")
            for col in WEATHER_COLS_OUT:
                val = weather_record.get(col)
                print(f"  │  {col:<20}: {val}")
            print(f"  └─────────────────────────────────────────────────────────────\n")

    # Flush any remaining partial hour
    final_row = aggregator.flush()
    if final_row is not None:
        hourly_rows.append(final_row)

    # ── 4. Print hourly aggregated results ────────────────────────────────
    print_section("STEP 3 OUTPUT — Hourly Aggregated Rows (matches ptc_weather.csv format)")
    print(f"  Raw messages  : {len(raw_rows)}")
    print(f"  Discarded     : {discarded}  (corrupt ts / wrong sensor / pre-2021)")
    print(f"  Processed     : {processed}")
    print(f"  Hourly rows   : {len(hourly_rows)}\n")

    for i, row in enumerate(hourly_rows):
        print(f"  ┌─ HOURLY ROW {i+1} ──────────────────────────────────────────────")
        print(f"  │  unique_id  : {row['unique_id']}")
        print(f"  │  ds         : {row['ds']}  ← hourly timestamp (UTC)")
        print(f"  │  y          : {int(row['y'])}  ← total_count SUM across all sensors")
        print(f"  │  in_sum     : {int(row['in_sum'])}")
        print(f"  │  out_sum    : {int(row['out_sum'])}")
        print(f"  │  year       : {row['year']}  month: {row['month']}  day: {row['day']}")
        print(f"  │  hour       : {row['hour']}  day_of_week: {row['day_of_week']}")
        print(f"  ├─ Weather (merged) ─────────────────────────────────────────────")
        print(f"  │  temp            : {row.get('temp', 'N/A'):.2f} °C  ← MEAN across readings")
        print(f"  │  humidity        : {row.get('humidity', 'N/A'):.2f} %   ← MEAN across readings")
        print(f"  │  rain            : {row.get('rain', 'N/A')} mm")
        print(f"  │  snowfall        : {row.get('snowfall', 'N/A')} cm")
        print(f"  │  snow_depth      : {row.get('snow_depth', 'N/A')} m")
        print(f"  │  wind_speed      : {row.get('wind_speed', 'N/A')} km/h")
        print(f"  │  cloud_cover     : {row.get('cloud_cover', 'N/A')} %")
        print(f"  │  cloud_cover_low : {row.get('cloud_cover_low', 'N/A')} %")
        print(f"  │  cloud_cover_mid : {row.get('cloud_cover_mid', 'N/A')} %")
        print(f"  │  cloud_cover_high: {row.get('cloud_cover_high', 'N/A')} %")
        print(f"  └─────────────────────────────────────────────────────────────────\n")

    # ── 5. Cross-check against TallinnData_Refined.csv ───────────────────
    print_section("STEP 4 — Cross-check vs TallinnData_Refined.csv")
    try:
        import pandas as pd
        refined_path = os.path.join(_HERE, "..", "TallinnData_Refined.csv")
        refined = pd.read_csv(refined_path)
        refined["utc timestamp"] = pd.to_datetime(refined["utc timestamp"], utc=True)

        # Get refined rows in same window
        window_start = pd.Timestamp("2023-01-09T13:00:00", tz="UTC")  # 15:00 Tallinn = 13:00 UTC
        window_end   = pd.Timestamp("2023-01-09T16:00:00", tz="UTC")
        refined_window = refined[
            (refined["utc timestamp"] >= window_start) &
            (refined["utc timestamp"] <  window_end)
        ]

        print(f"  TallinnData_Refined rows in same window: {len(refined_window)}")
        if not refined_window.empty:
            print(f"\n  Sample from TallinnData_Refined.csv (first 3 rows):")
            cols_to_show = ["utc timestamp", "device", "in", "out", "temp", "humidity", "rain", "wind_speed"]
            cols_present = [c for c in cols_to_show if c in refined_window.columns]
            print(refined_window[cols_present].head(3).to_string(index=False))
            print(f"\n  Refined total 'in' for window : {refined_window['in'].sum():.0f}")
            print(f"  Our pipeline in_sum for hour  : {hourly_rows[0]['in_sum'] if hourly_rows else 'N/A'}")
            print(f"\n  Refined weather (rain sample) : {refined_window['rain'].iloc[0] if 'rain' in refined_window.columns else 'N/A'}")
            print(f"  Our pipeline rain for hour    : {hourly_rows[0].get('rain', 'N/A') if hourly_rows else 'N/A'}")
    except Exception as e:
        print(f"  Could not load TallinnData_Refined.csv for cross-check: {e}")

    print(f"\n{SEP2}")
    print("  Test complete.")
    print(SEP2)


if __name__ == "__main__":
    main()
