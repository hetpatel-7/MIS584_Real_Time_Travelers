
from enum import Enum
import avro.schema
from datetime import datetime

class IntervalUnit_to_Multiplier(Enum):
    millis = 0.001
    seconds = 1
    minutes = 60

SCHEMA_PATH = "./tallinn_sensor_schema.avro"
SCHEMA = avro.schema.parse(open(SCHEMA_PATH).read())

SOCKET_SERVER_HOST = "127.0.0.1"
SOCKET_SERVER_PORT = 3507
SOCKET_SERVER_NAMESPACE = "/sensor_socket_server"

SERVER_ADDRESS = "localhost:9092"
TOPIC          = "gate_data"

SENSOR_START_DATE = datetime.fromisoformat("2023-01-09T15:00:00+02:00")

# What can a client do?
#  send messages, as in, the literal message it's adding to kafka   ("send_message")
#  or communicate how many entries it's adding to kafka             ("update_count")
#  or be told to stop                                               ("stop_call")

class SocketIO_Client_Events(Enum):
    COUNT = "count"
    SENSOR_READING = "sensor_reading"
    STOP_CALL = "stop_call"