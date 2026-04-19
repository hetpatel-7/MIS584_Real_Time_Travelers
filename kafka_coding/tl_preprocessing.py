"""
Preprocessing pipeline for the Tallinn Gate Sensor consumer.

Steps reproduced from two source notebooks:
  - old_town_hypotheses_testing.ipynb  : weather loading + sensor cleaning + merge_asof join
  - model_training_with_evaluation_metrics.ipynb : camera filter, total_count, hourly aggregation
"""

import re
import os
import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# 1. Camera filter
#    Same 19 sensors the producer publishes for (tl_starter.py).
#    Source: model_training_with_evaluation_metrics.ipynb cell 13
# ---------------------------------------------------------------------------
CAMERAS_CONSIDERED = {
    "[0] Harju 13",
    "[0] Nunne/Suur-Kloostri",
    "[1] Nunne/Suur-Kloostri",
    "[0] Pikk 72 - Suur Rannavärav",
    "[0] Suur-Karja 18",
    "[1] Suur-Karja 18",
    "[0] Suur-Karja 20-22",
    "[1] Suur-Karja 20-22",
    "[0] Suurtüki/Laboratooriumi",
    "[0] Suurtüki/Laboratooriumi ",  # trailing-space variant kept intentionally
    "[0] Toompea/Falgi tee/Komandandi tee",
    "[1] Toompea/Falgi tee/Komandandi tee",
    "[0] Uus - Väike Rannavärav",
    "[0] Valli 4",
    "[0] Vana-Viru 12",
    "[1] Vana-Viru 12",
    "[0] Viru tänav 27",
    "[1] Viru tänav 27",
    "[0] Väike-Karja 12",
}

# ---------------------------------------------------------------------------
# 2. Weather column rename map
#    Open-Meteo embeds units in the header; we strip them to plain names.
# ---------------------------------------------------------------------------
WEATHER_COLS_OUT = [
    "temp", "humidity", "precipitation",
    "rain", "snowfall", "snow_depth",
    "wind_speed", "cloud_cover",
    "cloud_cover_low", "cloud_cover_mid", "cloud_cover_high",
]

# Maps a normalised key (ASCII-only, lowercase) : target column name
_WEATHER_KEY_TO_TARGET = {
    "temperature_2m c":         "temp",
    "relative_humidity_2m":     "humidity",
    "precipitation mm":         "precipitation",
    "rain mm":                  "rain",
    "snowfall cm":              "snowfall",
    "snow_depth m":             "snow_depth",
    "wind_speed_10m kmh":       "wind_speed",
    "cloud_cover":              "cloud_cover",
    "cloud_cover_low":          "cloud_cover_low",
    "cloud_cover_mid":          "cloud_cover_mid",
    "cloud_cover_high":         "cloud_cover_high",
}


def _normalise_col(col: str) -> str:
    """Strip units, degree symbols and punctuation, lowercase — for fuzzy matching."""
    col = col.replace("\ufffd", "").replace("°", "").replace("%", "").replace("/", "")
    col = re.sub(r"[().]", "", col)
    col = re.sub(r"\s+", " ", col).strip().lower()
    return col


def _build_rename_map(columns) -> dict:
    rename = {}
    for col in columns:
        key = _normalise_col(col)
        if key in _WEATHER_KEY_TO_TARGET:
            rename[col] = _WEATHER_KEY_TO_TARGET[key]
    return rename


# ---------------------------------------------------------------------------
# 3. Weather loader
# ---------------------------------------------------------------------------
def load_weather_data(csv_path: str) -> pd.DataFrame:
    """Load the raw Open-Meteo hourly weather CSV and return a clean DataFrame.

    The file has 2 metadata rows before the header (skiprows=2).
    Returns a UTC-aware DataFrame sorted by 'time', ready for merge_asof lookups.
    """
    weather_df = pd.read_csv(csv_path, skiprows=2)

    rename_map = _build_rename_map(weather_df.columns)
    weather_df = weather_df.rename(columns=rename_map)

    # Parse time as UTC. The weather CSV has no timezone info in its timestamps;
    # utc=True attaches UTC so all timestamps across the pipeline are UTC-aware.
    weather_df["time"] = pd.to_datetime(weather_df["time"], utc=True)

    # Keep only the columns we need
    keep = ["time"] + [c for c in WEATHER_COLS_OUT if c in weather_df.columns]
    weather_df = weather_df[keep].sort_values("time").reset_index(drop=True)

    return weather_df


# ---------------------------------------------------------------------------
# 4. Sensor message preprocessor
#    Applied to every decoded Avro message arriving from Kafka.
#
#    Steps (sources noted inline):
#      a) Normalise trailing-space sensor name variant
#         (model_training_with_evaluation_metrics.ipynb cell 9)
#      b) Validate sensor is in cameras_considered
#         (model_training_with_evaluation_metrics.ipynb cell 15)
#      c) Parse ts → UTC-aware datetime
#         (old_town_hypotheses_testing.ipynb cell 1)
#      d) Filter corrupt historical timestamps (year < 2021)
#         (old_town_hypotheses_testing.ipynb cell 32)
#      e) Strip [0]/[1] direction prefix from name → device
#         (old_town_hypotheses_testing.ipynb cell 1)
#      f) Compute total_count = in_count + out_count
#         (model_training_with_evaluation_metrics.ipynb cell 15)
#      g) Extract time features: date, year, month, day, hour, day_of_week
#         (old_town_hypotheses_testing.ipynb cell 1 + model_training cell 15)
# ---------------------------------------------------------------------------
def preprocess_sensor_message(msg: dict) -> "dict | None":
    """Return a cleaned sensor record dict, or None if the message should be dropped."""

    name = msg.get("name", "")

    # (a) Normalise trailing-space variant
    if name == "[0] Suurtüki/Laboratooriumi ":
        name = "[0] Suurtüki/Laboratooriumi"

    # (b) Drop messages from sensors outside our 19-camera set
    if name not in CAMERAS_CONSIDERED:
        return None

    # (c) Parse timestamp to UTC-aware datetime
    ts_raw = msg.get("ts", "")
    try:
        ts_utc = pd.to_datetime(ts_raw, utc=True)
    except Exception:
        return None

    # (d) Drop corrupt historical records (data.db has rows dating back to 1906)
    if ts_utc.year < 2021:
        return None

    # (e) Strip direction prefix [0] / [1] — multiple cameras share a gate name
    device = re.sub(r"^\[\d+\]\s*", "", name)

    # (f) Directional counts + total
    in_count  = int(msg.get("in_count",  0) or 0)
    out_count = int(msg.get("out_count", 0) or 0)
    total_count = in_count + out_count

    # (g) Time features
    ts_hour = ts_utc.floor("h")  # rounded-down hour used as the aggregation bucket key

    return {
        "device":       device,
        "ts_utc":       ts_utc,
        "ts_hour":      ts_hour,          # bucket key for HourlyAggregator
        "pir":          int(msg.get("pir", 0) or 0),
        "in_count":     in_count,
        "out_count":    out_count,
        "total_count":  total_count,
        "date":         ts_utc.date(),
        "year":         ts_utc.year,
        "month":        ts_utc.month,
        "day":          ts_utc.day,
        "hour":         ts_utc.hour,
        "day_of_week":  ts_utc.dayofweek,  # 0 = Monday … 6 = Sunday
    }


# ---------------------------------------------------------------------------
# 5. Weather lookup (per-message)
#    Replicates merge_asof(..., direction='backward'):
#    finds the most recent hourly weather row at or before the sensor timestamp.
# ---------------------------------------------------------------------------
def lookup_weather(ts_utc: pd.Timestamp, weather_df: pd.DataFrame) -> dict:
    """Return the weather dict for the hour containing ts_utc.

    Uses numpy searchsorted on the underlying int64 values for timezone-safe
    binary search — equivalent to merge_asof backward direction.
    """
    # Work in nanoseconds (int64) to avoid timezone comparison issues
    weather_ns = weather_df["time"].values.astype("int64")
    ts_ns = np.int64(ts_utc.value)

    idx = int(np.searchsorted(weather_ns, ts_ns, side="right")) - 1

    if idx < 0:
        # Sensor timestamp is before the weather data starts — return NaNs
        return {col: np.nan for col in WEATHER_COLS_OUT}

    row = weather_df.iloc[idx]
    return {col: row[col] if col in weather_df.columns else np.nan
            for col in WEATHER_COLS_OUT}


# ---------------------------------------------------------------------------
# 6. Hourly aggregator
#    Buffers individual 10-minute sensor readings and emits one row per
#    complete hour, matching the ptc_weather.csv format consumed by
#    strm_retraining.ipynb.
#
#    Aggregation rules (model_training_with_evaluation_metrics.ipynb cell 17):
#      - y (total_count_sum) : SUM across all readings in the hour
#      - in_sum              : SUM of in_count
#      - out_sum             : SUM of out_count
#      - temp                : MEAN (same weather value every 10-min slot; mean
#                              is used to stay consistent with the notebook)
#      - humidity            : MEAN (same reason)
#      - all other weather   : FIRST value (constant within an hour)
#
#    Output columns (matching strm_retraining.ipynb ptc_weather usage):
#      unique_id, ds, y, in_sum, out_sum,
#      year, month, day, hour, day_of_week,
#      temp, humidity, precipitation, rain, snowfall, snow_depth,
#      wind_speed, cloud_cover, cloud_cover_low, cloud_cover_mid, cloud_cover_high
# ---------------------------------------------------------------------------
class HourlyAggregator:
    """Accumulates sensor+weather records and emits one row per completed hour."""

    def __init__(self):
        self._current_hour: "pd.Timestamp | None" = None
        self._buffer: "list[dict]" = []

    def add(self, sensor_record: dict, weather_record: dict) -> "dict | None":
        """Add one merged sensor+weather record.

        Returns a completed hourly row dict when the hour bucket rolls over,
        otherwise returns None.
        """
        hour_bucket = sensor_record["ts_hour"]
        merged = {**sensor_record, **weather_record}

        completed_row = None

        if self._current_hour is None:
            # First message ever
            self._current_hour = hour_bucket

        elif hour_bucket > self._current_hour:
            # New hour arrived — aggregate and flush the previous bucket
            completed_row = self._aggregate()
            self._buffer = []
            self._current_hour = hour_bucket

        # Messages from a past hour (out-of-order) are silently dropped to keep
        # the aggregation simple; Kafka generally delivers in order here.
        elif hour_bucket < self._current_hour:
            return None

        self._buffer.append(merged)
        return completed_row

    def flush(self) -> "dict | None":
        """Flush any partially accumulated hour (call when consumer is stopping)."""
        if self._buffer:
            row = self._aggregate()
            self._buffer = []
            self._current_hour = None
            return row
        return None

    def _aggregate(self) -> dict:
        df = pd.DataFrame(self._buffer)

        row: dict = {
            "unique_id":   "all_sensors",
            "ds":          self._current_hour,
            "y":           df["total_count"].sum(),
            "in_sum":      df["in_count"].sum(),
            "out_sum":     df["out_count"].sum(),
            "year":        self._current_hour.year,
            "month":       self._current_hour.month,
            "day":         self._current_hour.day,
            "hour":        self._current_hour.hour,
            "day_of_week": self._current_hour.dayofweek,
        }

        # Weather aggregation
        for col in WEATHER_COLS_OUT:
            if col not in df.columns:
                row[col] = np.nan
            elif col in ("temp", "humidity"):
                row[col] = df[col].mean()   # mean — consistent with notebook cell 17
            else:
                row[col] = df[col].iloc[0]  # constant within the hour

        return row
