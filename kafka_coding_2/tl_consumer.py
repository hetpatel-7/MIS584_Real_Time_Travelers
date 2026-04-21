
import json
import kafka
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import io
import avro.schema
from avro.io import DatumWriter
import asyncio
from asyncio import Task
import sqlite3
from tl_utils.constants import IntervalUnit_to_Multiplier, SCHEMA
from datetime import datetime, timedelta
from typing import Union, Literal, Any
import time
import pandas as pd
from collections import deque




class TallinnGateSensorConsumer():
    """ A single process that consumes data from the topic every X time...
    """    
    def __init__(
            self,
            server_address : str,           # The Kafka Server Address
            alias : str,                    # Display name to use in our code for this consumer
            #objective : Union[Literal["forecast_acc"], Literal["per_sensor_acc"], Literal["latest_sensor_reads"]],
            group_id : str,
            dashboard : Any,
            task_created : Union[Task, None] = None,            # The asyncio Task, stored here for easy access
            topic : str = "gate_data",      # The topic where we publish
            interval_number : float = 2 ,   # Maybe for how much time this should run
            interval_unit : IntervalUnit_to_Multiplier = "minutes",  # Do we wait 3 millis, seconds, or minutes?
        ):
        """ Initialize the Tallinn Gate Sensor to start generating data
        """
        ### KAFKA SETUP
        self.server_address = server_address
        self.topic = topic
        self.group_id = group_id
        self.consumer = kafka.KafkaConsumer(
            bootstrap_servers=[self.server_address],
            group_id = self.group_id,
            auto_offset_reset = "earliest"
        )
        self.consumer.subscribe(topics=[self.topic])
        ### INTERNAL VALUES OF THE SENSOR
        self.alias = alias
        self.total_consuming_time = interval_number * IntervalUnit_to_Multiplier[interval_unit].value
        self.consuming_time_start = time.time()
        self.consuming_datetime_start = datetime.now()
        self.when_to_stop = self.consuming_datetime_start + timedelta(seconds=self.total_consuming_time)
        self.when_to_stop_seconds = self.when_to_stop.timestamp()
        ### CONSUMER FUNCTION TO DO
        #self.objective = objective
        ### CONSUMER STATE
        self.is_consuming = False  # Whether the sensor's publishing
        self.task_created = task_created
        ### DASHBOARD REFERENCE
        self.dashboard = dashboard
    
    async def start(self):
        print(f"Starting '{self.alias}' (CONS)")
        self.is_consuming = True
        
        await self.consume()

    async def stop(self):
        print(f"Stopping '{self.alias}' (CONS)")
        self.is_consuming = False
        ## TODO: Can run self.task_created.cancel() here to stop it instead. That allows for an external button.

    async def consume(self):
        try:
            for sensor_reading in self.consumer:
                bytes_reader = io.BytesIO(sensor_reading.value)
                decoder = avro.io.BinaryDecoder(bytes_reader)
                reader = avro.io.DatumReader(SCHEMA)
                sensor_data = reader.read(decoder)

                self.dashboard.accumulate_entry(sensor_data)
                self.dashboard.update_latest_reading(sensor_data)

                # How to stop consumer from Mohit (2017) https://stackoverflow.com/a/45430054
                if self.is_consuming == False:
                    break
                if time.time() >= self.when_to_stop_seconds:
                    self.stop() #Stop consuming
                    break
                #else:
                #    print(f"{time.time()} | Stop at: {self.when_to_stop_seconds}")
        except asyncio.CancelledError:
            self.consumer.close()
            print("Stopped because tasked was cancelled. Was it on purpose?")


