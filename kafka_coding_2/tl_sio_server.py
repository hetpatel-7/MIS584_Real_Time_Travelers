import socketio
from aiohttp import web
from tl_utils.constants import SOCKET_SERVER_PORT, SOCKET_SERVER_NAMESPACE

class TallinnSocketServer(socketio.AsyncNamespace):
    def __init__(self, namespace):
        super().__init__(namespace)
        self.dashboard = None

    def on_connect(self, sid, environ):
        print(f"Connected: {sid}")

    def on_disconnect(self, sid, reason):
        print(f"Disconnected: {sid}, {reason}")

    # SENSOR_READING
    async def on_sensor_reading(self, sid, data):
        pass

    async def on_count(self, sid, data):
        if self.dashboard:
            self.dashboard.add_count(data["sensor_name"])

    async def on_sensor_state(self, sid, data):
        if self.dashboard:
            self.dashboard.update_state(data["sensor_name"], data["sensor_state"])

    # This is called by Panel, we can click on a button to say "stop this one"
    async def stop_client(self, socket_id):
        await self.emit("stop_call", socket_id)

    # Called by Panel server, pauses sensors from sending data.
    async def pause_clients(self):
        await self.emit("pause_call")

    # Called by Panel server, allows sensors to resume sending data.
    async def resume_clients(self):
        await self.emit("resume_call")


async def start_socket_server(sio : socketio.AsyncServer):
    print(f"Starting Socket Server '{SOCKET_SERVER_NAMESPACE}'")
    app = web.Application()
    sio.attach(app)
    runner = web.AppRunner(app)
    await runner.setup()
    # host="localhost" can resolve to IPv6 ::1 on Windows, causing connection refusal
    # host="localhost", port=SOCKET_SERVER_PORT
    site = web.TCPSite(
        runner,
        host="0.0.0.0", port=SOCKET_SERVER_PORT
    )
    await site.start()
    print("Awaited Site.Start()")
