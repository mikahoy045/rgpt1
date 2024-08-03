import sys
print(f"Python version: {sys.version}")
print(f"Python executable: {sys.executable}")

from fastapi import FastAPI
from contextlib import asynccontextmanager
import asyncio
from api.dprovider import router as dprovider_router
from database.mongodb import connect_to_mongo, close_mongo_connection
from queuemq.broker import rabbitmq_broker
from dotenv import load_dotenv
from event.provider_consumer import start_consuming

load_dotenv()

consumer_task = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global consumer_task
    connect_to_mongo()
    try:
        await rabbitmq_broker.connect()
        consumer_task = asyncio.create_task(start_consuming())
        print("RabbitMQ consumer started in the background")
    except Exception as e:
        print(f"Failed to connect to RabbitMQ: {str(e)}")
        print("The application will continue without RabbitMQ connection.")
    
    yield
    
    close_mongo_connection()
    await rabbitmq_broker.close()
    if consumer_task:
        consumer_task.cancel()
        try:
            await consumer_task
        except asyncio.CancelledError:
            print("Consumer task cancelled")

app = FastAPI(lifespan=lifespan)

app.include_router(dprovider_router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)