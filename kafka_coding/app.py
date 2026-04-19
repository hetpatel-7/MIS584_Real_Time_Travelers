import panel as pn
import holoviews as hv
import pandas as pd
import asyncio
import socketio
import threading

from tl_sio_server import start_socket_server, TallinnSocketServer
from tl_utils.constants import SOCKET_SERVER_NAMESPACE

hv.extension("bokeh")
pn.extension(design="material", sizing_mode="stretch_width")

# 1. Define all cameras
cameras_considered = [
    "[0] Harju 13", "[0] Nunne/Suur-Kloostri", "[1] Nunne/Suur-Kloostri",
    "[0] Pikk 72 - Suur Rannavärav", "[0] Suur-Karja 18", "[1] Suur-Karja 18",
    "[0] Suur-Karja 20-22", "[1] Suur-Karja 20-22", "[0] Suurtüki/Laboratooriumi",
    "[0] Suurtüki/Laboratooriumi ", "[0] Toompea/Falgi tee/Komandandi tee",
    "[1] Toompea/Falgi tee/Komandandi tee", "[0] Uus - Väike Rannavärav",
    "[0] Valli 4", "[0] Vana-Viru 12", "[1] Vana-Viru 12",
    "[0] Viru tänav 27", "[1] Viru tänav 27", "[0] Väike-Karja 12"
]

# 2. Setup a distinct Streaming Buffer for EACH sensor 
empty_df = pd.DataFrame({"ts": [], "in_count": [], "out_count": []})
buffers = {camera: hv.streams.Buffer(empty_df, length=100, index=False) for camera in cameras_considered}

# 3. Create the UI Selector for the Sidebar
sensor_select = pn.widgets.Select(
    name="Select Camera Location", 
    options=cameras_considered,
    value="[0] Harju 13"
)

# 4. Plotting logic
def plot_traffic(data):
    if not data.empty and not pd.api.types.is_datetime64_any_dtype(data['ts']):
        data['ts'] = pd.to_datetime(data['ts'])
        
    in_curve = hv.Curve(data, kdims=['ts'], vdims=['in_count'], label='Inbound').opts(
        color="#13A076", line_width=2
    )
    out_curve = hv.Curve(data, kdims=['ts'], vdims=['out_count'], label='Outbound').opts(
        color="#D55F03", line_width=2
    )
    
    return (in_curve * out_curve).opts(
        title="Live Foot Traffic",
        width=800, 
        height=400,
        show_grid=True,
        legend_position='top_left',
        xlabel="Time",
        ylabel="Pedestrian Count",
        responsive=True
    )

# 5. Reactive Hook: Switches the active data stream when the dropdown changes
@pn.depends(sensor_select)
def view_chart(selected_sensor):
    return hv.DynamicMap(plot_traffic, streams=[buffers[selected_sensor]])

# 6. Socket Server routing data to the correct sensor buffer
class UIAwareSocketServer(TallinnSocketServer):
    async def on_sensor_reading(self, sid, data):
        await super().on_sensor_reading(sid, data)
        
        sensor_name = data.get("name")
        if sensor_name in buffers:
            new_reading = pd.DataFrame([{
                "ts": data["ts"], 
                "in_count": data["in_count"],
                "out_count": data["out_count"]
            }])
            # Send the reading ONLY to the buffer that matches the sensor name
            pn.state.execute(lambda: buffers[sensor_name].send(new_reading))

# 7. Assemble the Dashboard Template
template = pn.template.MaterialTemplate(
    site="Tallinn Monitors",
    title='Old Town Tallinn Foot Traffic Dashboard',
)

# Append to sidebar
template.sidebar.append(pn.Column("### Controls", sensor_select))

# Append to main viewport
template.main.append(
    pn.Column(
        pn.pane.Markdown("### Real-Time Gate Sensors"),
        view_chart
    )
)

# 8. Server Initialization
sio = socketio.AsyncServer(async_mode="aiohttp", cors_allowed_origins='*')
socket_server = UIAwareSocketServer(SOCKET_SERVER_NAMESPACE)
sio.register_namespace(socket_server)

def start_panel():
    pn.serve(template, port=5006, show=False)

async def main():
    panel_thread = threading.Thread(target=start_panel)
    panel_thread.daemon = True
    panel_thread.start()
    
    await start_socket_server(sio)
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())