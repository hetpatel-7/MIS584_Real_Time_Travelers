import asyncio

import socketio

from tl_producer import TallinnGateSensorPublisher
from tl_consumer import TallinnGateSensorConsumer
from datetime import datetime, timedelta

from tl_sio_client import PublisherSIOClient
from tl_utils.constants import KAFKA_SERVER_ADDRESS, SOCKET_SERVER_NAMESPACE, SOCKET_SERVER_PORT, START_DATE, TOPIC

cameras_considered = set(["all"])

# set([
# "[0] Harju 13",
# #"[0] Nunne/Suur-Kloostri",
# #"[1] Nunne/Suur-Kloostri",
# #"[0] Pikk 72 - Suur Rannavärav",
# #"[0] Suur-Karja 18",
# #"[1] Suur-Karja 18",
# #"[0] Suur-Karja 20-22",
# #"[1] Suur-Karja 20-22",
# #"[0] Suurtüki/Laboratooriumi",
# #"[0] Suurtüki/Laboratooriumi ", #The same one but with a space.
# #"[0] Toompea/Falgi tee/Komandandi tee",
# #"[1] Toompea/Falgi tee/Komandandi tee",
# #"[0] Uus - Väike Rannavärav",
# #"[0] Valli 4",
# #"[0] Vana-Viru 12",
# #"[1] Vana-Viru 12",
# #"[0] Viru tänav 27",
# #"[1] Viru tänav 27",
# #"[0] Väike-Karja 12"
# ])



async def start_publishers():
    pb_created = []
    publishers = []
    
    for index, name in enumerate(cameras_considered):
        publisher = TallinnGateSensorPublisher(
            server_address = KAFKA_SERVER_ADDRESS,
            sensor_name = name,
            alias = name,
            socket_number = index,
            start_date = START_DATE,
            task_created = None,
            topic=TOPIC,
            interval_number=2,
            interval_unit="seconds",
            # max_iterations=12, # Two hours' worth of data — wrong, 4h cycle needs 24
            max_iterations=144, # 6 forecast cycles × 24 iterations per cycle (24 × 10min = 4h)
            enable_socket=True
        )
        pb_created.append(publisher)
        
    # Connect all first, sequentially
    for publisher in pb_created:
        publisher.sio = socketio.AsyncClient()
        print(f"Connecting {publisher.sensor_name} to '{SOCKET_SERVER_NAMESPACE}'")
        publisher.sio.register_namespace(
            PublisherSIOClient(SOCKET_SERVER_NAMESPACE, publisher)
        )
        await publisher.sio.connect(
            f"http://127.0.0.1:{SOCKET_SERVER_PORT}",
            namespaces=[SOCKET_SERVER_NAMESPACE]
        )
        
        publisher_task = asyncio.create_task(
            publisher.start()
        )
        publisher.task_created = publisher_task
        publishers.append(publisher)


    await asyncio.gather(*[x.task_created for x in publishers])

asyncio.run(start_publishers())