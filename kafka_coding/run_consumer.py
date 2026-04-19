"""
Standalone consumer runner — use this in a separate terminal while tl_starter.py runs.

    Terminal 1 (Kafka):   cd kafka_coding/docker && docker-compose up -d
    Terminal 2 (Consumer): cd kafka_coding && python run_consumer.py
    Terminal 3 (Producer): cd kafka_coding && python tl_starter.py
"""

import asyncio
from tl_consumer import TallinnGateSensorConsumer

SERVER_ADDRESS = "localhost:9092"
TOPIC          = "gate_data"

async def main():
    consumer = TallinnGateSensorConsumer(
        server_address=SERVER_ADDRESS,
        alias="forecasting_cn",
        objective="forecast_acc",
        group_id="forecast_acc",
        topic=TOPIC,
        interval_number=60,   # run for 60 minutes then flush and stop
        interval_unit="minutes",
    )
    await consumer.start()

asyncio.run(main())
