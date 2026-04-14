import panel as pn
import holoviews as hv
import asyncio
import socketio
from tl_sio_server import start_socket_server, TallinnSocketServer
from tl_utils.constants import SOCKET_SERVER_NAMESPACE
import threading

hv.extension("bokeh")
pn.extension(
    design="material",
    sizing_mode="stretch_width"
)

sio = socketio.AsyncServer(async_mode="aiohttp")
socket_server = TallinnSocketServer(SOCKET_SERVER_NAMESPACE)
sio.register_namespace(socket_server)

def create_sensor_display(sensor_name):
    """Just show one panel and what it does"""
    return f"""
        <div>
            {sensor_name}
        </div>
    """

template = pn.template.MaterialTemplate(
    site="Panel",
    title='Old Town Tallinn Foot Traffic Dashboard',
    sidebar=[],
    main=[]
)
def start_panel():
    pn.serve(template, port=5006)

async def main():
    panel_thread = threading.Thread(target=start_panel)
    panel_thread.daemon = True
    panel_thread.start()
    await start_socket_server(sio)
    await asyncio.Event().wait()

asyncio.run(main())
