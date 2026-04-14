from __future__ import annotations
import socketio
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tl_producer import TallinnGateSensorPublisher

class PublisherSIOClient(socketio.AsyncClientNamespace):
    def __init__(self, namespace, sensor_publisher : TallinnGateSensorPublisher):
        super().__init__(namespace)
        self.sensor_publisher = sensor_publisher

    async def on_connect(self):
        print("Connection Established")

    async def on_connect_error(self, data):
        print("The connection failed!")

    async def on_disconnect(self, reason):
        print('disconnected from server')
        if reason == "client disconnect":
            print("Reason: Client Disconnected")
        elif reason == "server disconnect":
            print("Reason: Server Disconnected the Client")
        else:
            print("Reason: ", reason)

    async def on_stop_call(self, data):
        if data == self.sid:
            await self.sensor_publisher.stop()