
from enum import Enum
import avro.schema
from datetime import datetime

class IntervalUnit_to_Multiplier(Enum):
    millis = 0.001
    seconds = 1
    minutes = 60

SCHEMA_PATH = "./tallinn_sensor_schema.avro"
SCHEMA = avro.schema.parse(open(SCHEMA_PATH).read())

SOCKET_SERVER_HOST = "0.0.0.0"
SOCKET_SERVER_PORT = 3507
SOCKET_SERVER_NAMESPACE = "/sensor_socket_server"

KAFKA_SERVER_ADDRESS = "127.0.0.1:9092"
TOPIC = "gate_data"

START_DATE = datetime.fromisoformat("2023-01-09T15:00:00+02:00")

CAMERAS_CONSIDERED_SET = set([
    "[0] Harju 13",
    "[0] Nunne/Suur-Kloostri",
    "[1] Nunne/Suur-Kloostri",
    "[0] Pikk 72 - Suur Rannavärav",
    "[0] Suur-Karja 18",
    "[1] Suur-Karja 18",
    "[0] Suur-Karja 20-22",
    "[1] Suur-Karja 20-22",
    "[0] Suurtüki/Laboratooriumi",
    "[0] Suurtüki/Laboratooriumi ", #The same one but with a space.
    "[0] Toompea/Falgi tee/Komandandi tee",
    "[1] Toompea/Falgi tee/Komandandi tee",
    "[0] Uus - Väike Rannavärav",
    "[0] Valli 4",
    "[0] Vana-Viru 12",
    "[1] Vana-Viru 12",
    "[0] Viru tänav 27",
    "[1] Viru tänav 27",
    "[0] Väike-Karja 12"
])

# What can a client do?
#  send messages, as in, the literal message it's adding to kafka   ("send_message")
#  or communicate how many entries it's adding to kafka             ("update_count")
#  or be told to stop                                               ("stop_call")

class SocketIO_Client_Events(Enum):
    COUNT = "count"
    SENSOR_READING = "sensor_reading"
    STOP_CALL = "stop_call"
    SENSOR_STATE = "sensor_state"