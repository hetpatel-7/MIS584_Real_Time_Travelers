import asyncio
from tl_producer import TallinnGateSensorPublisher
from tl_consumer import TallinnGateSensorConsumer
from datetime import datetime

cameras_considered = [
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
]

SERVER_ADDRESS = "localhost:9092"
TOPIC = "gate_data"

async def start_publishers():
    publishers = []
    
    for index, name in enumerate(cameras_considered):
        publisher = TallinnGateSensorPublisher(
            server_address = SERVER_ADDRESS,
            sensor_name = name,
            alias = name,
            socket_number = index,
            start_date = datetime.fromisoformat("2023-01-09T15:00:00+02:00"),
            task_created = None,
            topic=TOPIC,
            interval_number=2,
            interval_unit="seconds",
            max_iterations="forever", # Set to run continuously!
            enable_socket=False       # set True only when app.py socket server is running
        )
        publisher_task = asyncio.create_task(
            publisher.start()
        )
        publisher.task_created = publisher_task
        publishers.append(publisher)

    subscriber = TallinnGateSensorConsumer(
        server_address=SERVER_ADDRESS,
        alias="forecasting_cn",
        objective="forecast_acc",
        group_id="forecast_acc",
        task_created = None,
        topic=TOPIC,
        interval_number=60,           # run consumer for 60 minutes
        interval_unit="minutes"
    )
    subscriber_task = asyncio.create_task(subscriber.start())
    subscriber.task_created = subscriber_task

    await asyncio.gather(
        *[x.task_created for x in publishers],
        subscriber_task
    )

asyncio.run(start_publishers())