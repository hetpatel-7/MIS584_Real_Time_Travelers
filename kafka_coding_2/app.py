from collections import deque

import panel as pn
import holoviews as hv
import param
import asyncio
import socketio

from forecaster import Forecaster
from tl_consumer import TallinnGateSensorConsumer
from tl_sio_server import start_socket_server, TallinnSocketServer
from tl_utils.constants import KAFKA_SERVER_ADDRESS, SOCKET_SERVER_NAMESPACE, TOPIC, CAMERAS_CONSIDERED_SET, START_DATE
import threading
import pandas as pd

hv.extension("bokeh")
pn.extension(
    design="material",
    sizing_mode="stretch_width"
)

# ======================================================
#  SOCKET SERVER (need reference to pass on to Dashboard)
# ======================================================
sio = socketio.AsyncServer(async_mode="aiohttp")
socket_server = TallinnSocketServer(SOCKET_SERVER_NAMESPACE)
sio.register_namespace(socket_server)
print("1. Created Socket Server")
# ======================================================

# ======================================================
#  Create Dashboard
# ======================================================
class TallinnDashboard(param.Parameterized):
    readings = param.Dict(default={})

    def __init__(self, socket_server : TallinnSocketServer, **params):
        super().__init__(**params)
        self.indicators = {}
        self.sensor_readings_df = pd.DataFrame({
            "andurid_id": [],
            "name": [],
            "ts":[],
            "pir":[],
            "total_count":[],
            "humidity":[],
            "temp":[]
        })
        self.socket_server = socket_server
        self.socket_server.dashboard = self
        self.forecast_plot = pn.pane.Matplotlib(
            None,
            sizing_mode="stretch_both"
        )
        self.forecaster = Forecaster(START_DATE)
        for camera in CAMERAS_CONSIDERED_SET:
            # Let's create the entries in the watched dict as well to avoid collisions
            self.readings[camera] = {
                "last_count_reading": 0,
                "last_temp_reading": 0,
                "last_humid_reading": 0,
                "msg_processed_count": 0,
                "msg_emitted_count": 0,
                "sensor_state": "Off"
            }
            self.indicators[camera] = {
                ### SECTION: EVERY TIME A CONSUMER READS
                "last_count_reading": pn.indicators.Number(
                    name="Last Count", value=0, format="{value:.2f}",
                    sizing_mode="fixed",
                    width=100, height=50,
                    font_size="12pt",
                    title_size="8pt"
                ),
                #"last_3_avg": pn.indicators.Number(
                #    name="last_3_avg", value=0, format="{value:.2f}"
                #),
                "last_temp_reading": pn.indicators.Number(
                    name="Last Temperature", value=0, format="{value:.2f}",
                    sizing_mode="fixed",
                    width=150, height=80,
                    font_size="12pt",
                    title_size="8pt"
                ),
                "last_humid_reading": pn.indicators.Number(
                    name="Last Humidity", value=0, format="{value:.2f}",
                    sizing_mode="fixed",
                    width=150, height=80,
                    font_size="12pt",
                    title_size="8pt"
                ),
                "msg_processed_count": pn.indicators.Number(
                    name="Msg Received", value=0, format="{value}",
                    sizing_mode="fixed",
                    width=150, height=80,
                    font_size="12pt",
                    title_size="8pt"
                ),
                ### SECTION: WEBSOCKET UPDATES FOR DEBUGGING
                "msg_emitted_count": pn.indicators.Number(
                    name="Msg Emitted", value=0, format="{value}",
                    sizing_mode="fixed",
                    width=150, height=80,
                    font_size="12pt",
                    title_size="8pt"
                ),
                "sensor_state": pn.widgets.StaticText(
                    name="Sensor State", value="Off",
                    sizing_mode="fixed",
                    width=150, height=80,
                    styles={
                        "font-size":"12pt"
                    }
                )
            }

    async def update_forecast(self):
        """ Tells sensors to pause for a moment, aggregates sensor data,
        then trains a forecasting model, and lastly, tells sensors to begin again.
        """
        # STEP 1: Tell sensors to pause.
        if self.socket_server:
            await self.socket_server.pause_clients()
        # STEP 2: Aggregate sensor data + weather data
        self.forecaster.aggregate_data(self.sensor_readings_df)
        # STEP 3: Restart sensor readings? TODO: Might be dangerous if we drop something!
        self.sensor_readings_df = pd.DataFrame({
            "andurid_id": [],
            "name": [],
            "ts":[],
            "pir":[],
            "total_count":[],
            "humidity":[],
            "temp":[]
        })
        # STEP 3: Train the forecasting model
        # new_plot = self.forecaster.forecast()  ← blocks event loop → socket keepalives fail → producer disconnects
        loop = asyncio.get_event_loop()
        new_plot = await loop.run_in_executor(None, self.forecaster.forecast)
        # STEP 4: Update the Forecast plot(s)
        self.update_forecast_plot(new_plot)
        # STEP 5: Tell sensors to resume
        if self.socket_server:
           await self.socket_server.resume_clients()

    def update_forecast_plot(self, new_plot):
        self.forecast_plot.object = new_plot

    def add_count(self, sensor_name):
        """ Whenever the socket informs us that a producer indeed sent a message to Kafka
        """
        if sensor_name in self.readings:
            self.readings[sensor_name]["msg_emitted_count"]+= 1
            self.param.trigger('readings')

    def accumulate_entry(self, sensor_data):
        self.sensor_readings_df = pd.concat([
            self.sensor_readings_df,
            pd.DataFrame({
                "andurid_id": [sensor_data["andurid_id"]],
                "name": [sensor_data["name"]],
                "ts":[sensor_data["ts"]],
                "pir":[sensor_data["pir"]],
                "total_count":[sensor_data["in_count"] + sensor_data["out_count"]],
                "humidity":[sensor_data["humidity"]],
                "temp":[sensor_data["temp"]]
            })
        ])

    def update_latest_reading(self, sensor_data):
        sname = sensor_data["name"]
        if sname in self.readings:
            self.readings[sname][
                "last_count_reading"
            ] = sensor_data["in_count"] + sensor_data["out_count"]
            self.readings[sname][
                "last_temp_reading"
            ] = sensor_data["temp"]
            self.readings[sname][
                "last_humid_reading"
            ] = sensor_data["humidity"]
            self.readings[sname][
                "msg_processed_count"
            ] += 1
        else:
            self.readings[sname] = {
                "last_count_reading": sensor_data["in_count"] + sensor_data["out_count"],
                "last_temp_reading": sensor_data["temp"],
                "last_humid_reading": sensor_data["humidity"],
                "msg_processed_count": 1,
                "msg_emitted_count": 0,
                "sensor_status": "Emitting"
            }
        self.param.trigger('readings')

    def update_state(self, sensor_name, sensor_state):
        """ Whenever the socket informs us that a producer's state has changed.
        """
        if sensor_name in self.readings:
            if self.readings[sensor_name]["sensor_state"] != sensor_state:
                self.readings[sensor_name]["sensor_state"] = sensor_state
                self.param.trigger('readings')
    
    @param.depends("readings", watch=True)
    def _on_readings_update(self):
        # For each camera
        for key in self.readings.keys():
            entry_dict = self.readings[key]
            # Update each indicator in that camera
            for indicator_key in self.indicators[key].keys():
                self.indicators[key][indicator_key].value = entry_dict[indicator_key]

    @param.depends("readings")
    def view(self):
        indicators_panel = pn.FlexBox(
            *[
                pn.FlexBox(
                    pn.pane.Markdown(f"#### {camera_name}"),
                    pn.FlexBox(
                        indicator["last_count_reading"],
                        indicator["last_temp_reading"],
                        indicator["last_humid_reading"],
                        flex_direction="row",
                        align_items="center",
                        justify_content="flex-start",
                        styles={"gap": "4px"}
                    ),
                    pn.FlexBox(
                        indicator["sensor_state"],
                        indicator["msg_processed_count"],
                        indicator["msg_emitted_count"],
                        flex_direction="row",
                        align_items="center",
                        justify_content="flex-start",
                        styles={"gap": "4px"}
                    ),
                    flex_direction="column",
                    flex_wrap="wrap",
                    align_items="center",
                    justify_content="flex-start",
                    styles={
                        "gap": "4px",
                        "height":"fit-content"
                    }
                )
                for camera_name, indicator in self.indicators.items()
            ],
            flex_direction="row",
            flex_wrap="wrap",
            align_items="flex-start",
            styles={
                "width":"50%",
                "height":"100%",
            }
        )

        forecast_panel = pn.FlexBox(
            self.forecast_plot,
            flex_direction="column",
            align_items="start",
            styles={
                "width":"50%",
                "height":"100%"
            }
        )

        return pn.FlexBox(
            indicators_panel,
            forecast_panel,
            flex_direction="row",
            align_items="stretch",
            styles={
                "width":"100%",
                "height":"100%",
                "margin":"0",
                "padding":"0"
            }
        )
        
dashboard = TallinnDashboard(socket_server)
print("2. Created Dashboard object")

# _main_loop = None  # Set in main() so Tornado thread can schedule coroutines on the right loop

# Every 2 seconds, 10 mins pass, hence 6 times that is an hour. 4 times that is 4 hours.
# Total: 48 seconds.
# pn.state.add_periodic_callback(dashboard.update_forecast, period=2*6*4*1000)  ← async fn deadlocks on Panel's Tornado thread
# def _trigger_forecast():                                                        ← run_coroutine_threadsafe also unreliable across Panel/asyncio thread boundary
#     if _main_loop and _main_loop.is_running():
#         asyncio.run_coroutine_threadsafe(dashboard.update_forecast(), _main_loop)
# pn.state.add_periodic_callback(_trigger_forecast, period=2*6*4*1000)
print("3. Added dashboard update callback")

template = pn.template.MaterialTemplate(
    site="Panel",
    title='Old Town Tallinn Foot Traffic Dashboard',
    sidebar=[],
    main=[dashboard.view()]
)
print("3. Created Panel template")
# ====================================================================================

# ======================================================
#  CREATING THE CONSUMER
# ======================================================
consumer = TallinnGateSensorConsumer(
    server_address=KAFKA_SERVER_ADDRESS,
    alias="forecasting_cn",
    #objective="forecast_acc",
    group_id="forecast_acc",
    topic=TOPIC,
    dashboard = dashboard,
    interval_number=60,   # run for 60 minutes then flush and stop
    interval_unit="minutes"
)
print("4. Created Consumer")
# ======================================================

def start_panel():
    pn.serve(template, port=5006)

async def periodic_forecast():
    # Runs on the main asyncio loop — same loop as the socket server, so pause/resume emit correctly
    forecast_interval_seconds = 2 * 6 * 4  # 48s wall-clock = 4h simulated (24 iterations × 10min each)
    while True:
        await asyncio.sleep(forecast_interval_seconds)
        print("PERIODIC: Triggering forecast...")
        await dashboard.update_forecast()

async def main():
    # global _main_loop               ← no longer needed; forecast runs on this loop directly
    # _main_loop = asyncio.get_event_loop()
    panel_thread = threading.Thread(target=start_panel)
    panel_thread.daemon = True
    panel_thread.start()
    print("5. Started Panel Thread")
    await start_socket_server(sio)
    print("6. Started Socket Server")
    # await dashboard.update_forecast()
    await asyncio.gather(
        consumer.start(),
        periodic_forecast(),
        asyncio.Event().wait()
    )
    

asyncio.run(main())
