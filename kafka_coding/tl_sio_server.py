import socketio
from aiohttp import web
from tl_utils.constants import SOCKET_SERVER_PORT, SOCKET_SERVER_NAMESPACE

class TallinnSocketServer(socketio.AsyncNamespace):
    def __init__(self, namespace):
        super().__init__(namespace)
        self.sensor_data_dict = {}
        self.sensor_count_dict = {}

    def on_connect(self, sid, environ):
        print(f"Connected: {sid}")
        self.sensor_data_dict[sid] = [] 

    def on_disconnect(self, sid, reason):
        print(f"Disconnected: {sid}, {reason}")

    # SENSOR_READING
    async def on_sensor_reading(self, sid, data):
        if sid not in self.sensor_data_dict:
            self.sensor_data_dict[sid] = []
        self.sensor_data_dict[sid].append(data)

    async def on_count(self, sid, data):
        #if sensor_name not in self.sensor_count_dict:
        #    self.sensor_count_dict[sensor_name] = 0
        #else:
        #    self.sensor_count_dict[sensor_name] += 1
        pass

    # This is called by Panel, we can click on a button to say "stop this one"
    async def stop_client(self, socket_id):
        await self.emit("stop_call", socket_id)


async def start_socket_server(sio : socketio.AsyncServer):
    print(f"Starting Socket Server '{SOCKET_SERVER_NAMESPACE}'")
    app = web.Application()
    sio.attach(app)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(
        runner,
        host="localhost", port=SOCKET_SERVER_PORT
    )
    await site.start()
    print("Awaited Site.Start()")
