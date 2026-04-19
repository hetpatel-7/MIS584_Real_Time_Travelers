
import os
import kafka
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import io
import avro.schema
import avro.io
from avro.io import DatumReader
import asyncio
from asyncio import Task
from tl_utils.constants import IntervalUnit_to_Multiplier, SCHEMA
from datetime import datetime, timedelta
from typing import Union, Literal
import time

from tl_preprocessing import (
    load_weather_data,
    preprocess_sensor_message,
    lookup_weather,
    HourlyAggregator,
)

# Weather CSV sits one level above kafka_coding/ in the project root
_HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_WEATHER_CSV = os.path.join(_HERE, "..", "talinn_weather_data_raw.csv")


class TallinnGateSensorConsumer():
    """A single process that consumes data from the topic every X time."""

    def __init__(
            self,
            server_address : str,
            alias : str,
            objective : Union[Literal["forecast_acc"], Literal["per_sensor_acc"], Literal["latest_sensor_reads"]],
            group_id : str,
            task_created : Union[Task, None] = None,
            topic : str = "gate_data",
            interval_number : float = 2,
            interval_unit : IntervalUnit_to_Multiplier = "minutes",
            weather_csv_path : str = DEFAULT_WEATHER_CSV,
        ):
        ### KAFKA SETUP
        self.server_address = server_address
        self.topic = topic
        self.group_id = group_id
        self.consumer = kafka.KafkaConsumer(
            bootstrap_servers=[self.server_address],
            group_id=self.group_id,
            auto_offset_reset="earliest"
        )
        self.consumer.subscribe(topics=[self.topic])

        ### TIMING
        self.alias = alias
        self.total_consuming_time = interval_number * IntervalUnit_to_Multiplier[interval_unit].value
        self.consuming_time_start = time.time()
        self.consuming_datetime_start = datetime.now()
        self.when_to_stop = self.consuming_datetime_start + timedelta(seconds=self.total_consuming_time)
        self.when_to_stop_seconds = self.when_to_stop.timestamp()

        ### OBJECTIVE + STATE
        self.objective = objective
        self.is_consuming = False
        self.task_created = task_created

        ### PREPROCESSING — loaded once at startup so every message lookup is O(log n)
        print(f"[{self.alias}] Loading weather data from:\n  {weather_csv_path}")
        self.weather_df = load_weather_data(weather_csv_path)
        self.aggregator = HourlyAggregator()
        print(f"[{self.alias}] Weather data ready — {len(self.weather_df)} hourly rows "
              f"({self.weather_df['time'].min()} → {self.weather_df['time'].max()})")

    # ------------------------------------------------------------------
    async def start(self):
        print(f"Starting '{self.alias}' (CONS)")
        self.is_consuming = True
        await self.consume()

    async def stop(self):
        print(f"Stopping '{self.alias}' (CONS)")
        self.is_consuming = False
        # TODO: self.task_created.cancel() can be used for external stop button

    # ------------------------------------------------------------------
    async def consume(self):
        try:
            for raw_kafka_msg in self.consumer:

                # ── Decode Avro bytes ──────────────────────────────────
                bytes_reader = io.BytesIO(raw_kafka_msg.value)
                decoder = avro.io.BinaryDecoder(bytes_reader)
                reader = DatumReader(SCHEMA)
                raw_msg = reader.read(decoder)

                # ── Route by objective ─────────────────────────────────
                if self.objective == "forecast_acc":
                    self._handle_forecast_acc(raw_msg)

                elif self.objective == "per_sensor_acc":
                    pass  # future: per-sensor level predictions

                elif self.objective == "latest_sensor_reads":
                    pass  # future: live dashboard feed

                else:
                    print(f"Subscriber '{self.alias}' objective not implemented ({self.objective})")

                # ── Stop conditions (unchanged from original) ──────────
                if not self.is_consuming:
                    break
                if time.time() >= self.when_to_stop_seconds:
                    # Flush any partial hour before shutting down
                    final_row = self.aggregator.flush()
                    if final_row is not None:
                        self._on_hourly_row(final_row)
                    await self.stop()
                    break

        except asyncio.CancelledError:
            self.consumer.close()
            print("Stopped because task was cancelled. Was it on purpose?")

    # ------------------------------------------------------------------
    def _handle_forecast_acc(self, raw_msg: dict):
        """
        Full preprocessing pipeline for the forecast_acc objective.

        Step 1 — Sensor preprocessing (old_town_hypotheses_testing.ipynb +
                  model_training_with_evaluation_metrics.ipynb):
          • normalise sensor name
          • validate camera is in the 19-sensor set
          • parse ts → UTC-aware datetime
          • drop corrupt rows (year < 2021)
          • strip [0]/[1] direction prefix → device name
          • compute total_count = in_count + out_count
          • extract date, year, month, day, hour, day_of_week

        Step 2 — Weather lookup (old_town_hypotheses_testing.ipynb cell 32):
          • backward merge_asof equivalent: find the hourly weather row
            at or before the sensor timestamp

        Step 3 — Hourly aggregation (model_training_with_evaluation_metrics.ipynb cell 17):
          • accumulate readings into hour buckets
          • on hour roll-over: emit aggregated row with
              y = total_count SUM, in_sum, out_sum,
              temp/humidity MEAN, all other weather cols = first value
        """
        # Step 1: sensor preprocessing
        sensor_record = preprocess_sensor_message(raw_msg)
        if sensor_record is None:
            print(f"  [CONSUMER] DROPPED  | name={raw_msg.get('name')} ts={raw_msg.get('ts')}")
            return

        # Step 2: weather lookup
        weather_record = lookup_weather(sensor_record["ts_utc"], self.weather_df)

        print(
            f"  [CONSUMER] MSG      | device={sensor_record['device']:<35}"
            f" ts_utc={sensor_record['ts_utc']}"
            f" total={sensor_record['total_count']:>3}"
            f" | weather temp={weather_record.get('temp')}C"
            f" rain={weather_record.get('rain')}mm"
        )

        # Step 3: feed into hourly aggregator
        hourly_row = self.aggregator.add(sensor_record, weather_record)
        if hourly_row is not None:
            self._on_hourly_row(hourly_row)

    # ------------------------------------------------------------------
    def _on_hourly_row(self, row: dict):
        """Called each time a complete hourly aggregated row is ready.

        Currently prints a summary line. Replace / extend this to write to a
        database, feed into the MSTL retraining loop, etc.
        """
        temp     = row.get("temp")
        rain     = row.get("rain")
        wind     = row.get("wind_speed")
        print(
            f"[HOURLY] ds={row['ds']} | y={int(row['y']):>5} people "
            f"| temp={temp:.1f}°C | rain={rain:.2f}mm | wind={wind:.1f}km/h"
            f"  (in={int(row['in_sum'])}, out={int(row['out_sum'])})"
        )
