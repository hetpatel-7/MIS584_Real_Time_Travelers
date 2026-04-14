import json
import kafka
from kafka import KafkaProducer
from kafka.errors import KafkaError
import io
import avro.schema
from avro.io import DatumWriter
import asyncio
from asyncio import Task
import sqlite3
from tl_utils.constants import IntervalUnit_to_Multiplier, SCHEMA, SOCKET_SERVER_HOST, SOCKET_SERVER_PORT, SocketIO_Client_Events, SOCKET_SERVER_NAMESPACE
from datetime import datetime, timedelta
from typing import Union, Literal
import socketio
from tl_sio_client import PublisherSIOClient

class TallinnGateSensorPublisher():
    """ A sensor that will publish data every X units of time.
    """    
    def __init__(
            self,
            server_address : str,           # The Kafka Server Address
            sensor_name : str,              # The sensor name used in the API entries.
            alias : str,                    # Display name to use in our code, maybe in the dashboard too
            socket_number : int,
            start_date : datetime,          # From what time should we start pulling data?
            task_created : Union[Task, None] = None,            # The asyncio Task, stored here for easy access
            topic : str = "gate_data",      # The topic where we publish
            interval_number : float = 2 ,   # How much time to wait (just the number part)
            interval_unit : IntervalUnit_to_Multiplier = "seconds",  # Do we wait 3 millis, seconds, or minutes?
            max_iterations : Union[Literal["forever"], int] = 10,    # How many times this should run 
            enable_socket : bool = False
        ):
        """ Initialize the Tallinn Gate Sensor to start generating data
        """
        ### KAFKA SETUP
        self.server_address = server_address
        self.topic = topic
        self.producer = kafka.KafkaProducer(
            bootstrap_servers=[self.server_address]
        )
        ### DB SENSOR NAME TO SEARCH AND DATETIME TO START ON
        self.sensor_name = sensor_name
        self.start_date = start_date
        ### INTERNAL VALUES OF THE SENSOR
        self.alias = alias
        self.waiting_time = interval_number * IntervalUnit_to_Multiplier[interval_unit].value
        ### SENSOR STATE
        self.is_publishing = False  # Whether the sensor's publishing
        self.last_date = start_date # Increment this by 10 minutes each time.
        self.task_created = task_created
        self.max_iterations = max_iterations
        self.run_iterations = 0
        ### SOCKET SETUP
        self.enable_socket = enable_socket
        if self.enable_socket:
            self.socket_number = socket_number
            self.sio = socketio.AsyncClient()
            print(f"Connecting {self.sensor_name} to '{SOCKET_SERVER_NAMESPACE}'")
            self.sio.register_namespace(
                PublisherSIOClient(SOCKET_SERVER_NAMESPACE, self)
            )
    
    async def start(self):
        print(f"Starting '{self.alias}' (PROD)")
        self.db_connection = sqlite3.connect("./data.db")
        # From tijko (2024) https://stackoverflow.com/a/57806886
        with self.db_connection:
            self.db_connection.row_factory = sqlite3.Row
        self.is_publishing = True
        if self.enable_socket:
            await self.sio.connect(f"http://localhost:{SOCKET_SERVER_PORT}")
        await self.publish()

    async def stop(self):
        print(f"Stopping '{self.alias}' (PROD)")
        self.is_publishing = False
        if self.enable_socket:
            await self.sio.disconnect()

    def get_sensor_entries(self):
        cursor = self.db_connection.cursor()
        if self.sensor_name == "all": #In case we want to run just one process due to performance issues?
            sensor_entries = cursor.execute(f"""
                SELECT id, andurid_id, name, ts, pir,
                    in_count, out_count, humidity, temp
                FROM TallinnData
                WHERE ts > '{(self.last_date - timedelta(minutes=10)).isoformat()}' 
                    AND ts <= '{self.last_date.isoformat()}'
                ORDER BY ts
            """)
        elif isinstance(self.sensor_name, list):
            placeholders = ",".join("?", len(self.sensor_name))
            sensor_entries = cursor.execute(f"""
                SELECT id, andurid_id, name, ts, pir,
                    in_count, out_count, humidity, temp
                FROM TallinnData
                WHERE ts > '{(self.last_date - timedelta(minutes=10)).isoformat()}' 
                    AND ts <= '{self.last_date.isoformat()}' 
                    AND name IN ({placeholders})
                ORDER BY ts
            """, self.sensor_name)
        else:
            sensor_entries = cursor.execute(f"""
                SELECT id, andurid_id, name, ts, pir,
                    in_count, out_count, humidity, temp
                FROM TallinnData
                WHERE ts > '{(self.last_date - timedelta(minutes=10)).isoformat()}' 
                    AND ts <= '{self.last_date.isoformat()}' 
                    AND name = '{self.sensor_name}'
                ORDER BY ts
            """)
        return sensor_entries.fetchall()

    def generate_bytes(self, entry):
        bytes_writer = io.BytesIO()
        encoder = avro.io.BinaryEncoder(bytes_writer)
        writer = DatumWriter(SCHEMA)
        data = {
            "andurid_id":entry["andurid_id"],
            "name":entry["name"],
            "ts":entry["ts"],
            "pir":entry["pir"],
            "in_count":entry["in_count"],
            "out_count":entry["out_count"],
            "humidity":entry["humidity"],
            "temp":entry["temp"],
        }
        writer.write(data, encoder)
        raw_bytes = bytes_writer.getvalue()
        return raw_bytes
    
    async def send_info_via_socket(self, entry):
        await self.sio.emit(
            SocketIO_Client_Events.COUNT.value,
            self.sensor_name,
            namespace=SOCKET_SERVER_NAMESPACE
        )
        await self.sio.emit(
            SocketIO_Client_Events.SENSOR_READING.value,
            {
                "andurid_id":entry["andurid_id"],
                "name":entry["name"],
                "ts":entry["ts"],
                "pir":entry["pir"],
                "in_count":entry["in_count"],
                "out_count":entry["out_count"],
                "humidity":entry["humidity"],
                "temp":entry["temp"],
            },
            namespace=SOCKET_SERVER_NAMESPACE
        )

    async def publish(self):
        while self.is_publishing:
            sensor_entries = self.get_sensor_entries()
            print(f"{self.sensor_name} | {len(sensor_entries)} entries to publish")
            for entry in sensor_entries:
                if self.enable_socket:
                    await self.send_info_via_socket(entry)
                raw_bytes = self.generate_bytes(entry)
                self.producer.send(
                    self.topic,
                    raw_bytes
                )
            self.producer.flush()
            # From JDOaktown (2022) https://stackoverflow.com/a/6205529
            self.last_date = self.last_date + timedelta(minutes=10) #Next time, send data that happened in the next 10 mins.
            # Check if we should stop running!
            self.run_iterations += 1
            if self.max_iterations != "forever" and self.run_iterations >= self.max_iterations:
                await self.stop()
            await asyncio.sleep(self.waiting_time)
        self.db_connection.close() # If it stopped publishing, the connection should be closed.
        print(f"Connection Closed for sensor '{self.alias}' ")

