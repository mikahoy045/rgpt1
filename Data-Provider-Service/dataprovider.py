import sys
print(f"Python version: {sys.version}")
print(f"Python executable: {sys.executable}")

from fastapi import FastAPI
from contextlib import asynccontextmanager
import asyncio
import os
from api.dprovider import router as dprovider_router
from database.mongodb import connect_to_mongo, close_mongo_connection, db
from queuemq.broker import rabbitmq_broker
from dotenv import load_dotenv
from event.provider_consumer import start_consuming
from pymongo import ASCENDING

load_dotenv()

consumer_task = None

async def setup_mongodb():
    mongodb_db = os.getenv("MONGODB_DB")
    mongodb_collection = os.getenv("MONGODB_COLLECTION")
    
    if mongodb_db not in db.client.list_database_names():
        print(f"Creating database: {mongodb_db}")
    
    if mongodb_collection not in db.db.list_collection_names():
        print(f"Creating collection: {mongodb_collection}")
        db.db.create_collection(mongodb_collection)
    
    collection = db.db[mongodb_collection]
    
    indexes_to_create = [
        ("hotel_id", ASCENDING),
        ("timestamp", ASCENDING),
        ("rpg_status", ASCENDING),
        ("room_id", ASCENDING),
        ("night_of_stay", ASCENDING)
    ]
    
    existing_indexes = collection.index_information()
    
    for field, direction in indexes_to_create:
        index_name = f"{field}_1"
        if index_name not in existing_indexes:
            print(f"Creating index for field: {field}")
            collection.create_index([(field, direction)])
        else:
            print(f"Index already exists for field: {field}")

async def setup_rabbitmq():
    exchange_name = os.getenv("RABBITMQ_EXCHANGE")
    routing_key = os.getenv("RABBITMQ_ROUTING_KEY")
    queue_name = os.getenv("RABBITMQ_QUEUE")

    channel = await rabbitmq_broker.connection.channel()
    
    exchange = await channel.declare_exchange(
        exchange_name, 
        type="direct", 
        durable=True
    )
    print(f"Declared exchange: {exchange_name}")

    queue = await channel.declare_queue(queue_name, durable=True)
    print(f"Declared queue: {queue_name}")

    await queue.bind(exchange, routing_key)
    print(f"Bound queue {queue_name} to exchange {exchange_name} with routing key {routing_key}")

    await channel.close()

@asynccontextmanager
async def lifespan(app: FastAPI):
    global consumer_task
    connect_to_mongo()
    await setup_mongodb()
    try:
        await rabbitmq_broker.connect()
        await setup_rabbitmq()
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