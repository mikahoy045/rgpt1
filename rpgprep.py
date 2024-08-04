import asyncio
import os
from pymongo import MongoClient
import aio_pika
from dotenv import load_dotenv

load_dotenv(override=True)

# MongoDB configuration
MONGODB_URL = os.getenv("MONGODB_URL", "mongodb://localhost:27017")
DATA_PROVIDER_DB = os.getenv("MONGODB_DB", "rgpt")
DASHBOARD_DB = os.getenv("MONGODB_DB", "rgpt")
DATA_PROVIDER_COLLECTION = os.getenv("MONGODB_COLLECTION", "providers")
DASHBOARD_COLLECTION = os.getenv("MONGODB_COLLECTION_DASHBOARD", "dashboard")

print(f"MONGODB_URL: {MONGODB_URL}")
print(f"DATA_PROVIDER_DB: {DATA_PROVIDER_DB}")
print(f"DASHBOARD_DB: {DASHBOARD_DB}")
print(f"DATA_PROVIDER_COLLECTION: {DATA_PROVIDER_COLLECTION}")
print(f"DASHBOARD_COLLECTION: {DASHBOARD_COLLECTION}")

# RabbitMQ configuration
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "guest")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", 5672))
RABBITMQ_EXCHANGE = os.getenv("RABBITMQ_EXCHANGE", "blankon_exchange")
RABBITMQ_ROUTING_KEY = os.getenv("RABBITMQ_ROUTING_KEY", "blankon_key")
RABBITMQ_QUEUE = os.getenv("RABBITMQ_QUEUE", "blankon_queue")

async def setup_mongodb():
    client = MongoClient(MONGODB_URL)
    
    # Set up Data Provider database and collection
    dp_db = client[DATA_PROVIDER_DB]
    if DATA_PROVIDER_COLLECTION not in dp_db.list_collection_names():
        dp_db.create_collection(DATA_PROVIDER_COLLECTION)
    print(f"Created collection: {DATA_PROVIDER_COLLECTION} in database: {DATA_PROVIDER_DB}")
    
    # Set up Dashboard database and collection
    dash_db = client[DASHBOARD_DB]
    if DASHBOARD_COLLECTION not in dash_db.list_collection_names():
        dash_db.create_collection(DASHBOARD_COLLECTION)
    print(f"Created collection: {DASHBOARD_COLLECTION} in database: {DASHBOARD_DB}")
    
    client.close()

async def setup_rabbitmq():
    connection = await aio_pika.connect_robust(
        f"amqp://{RABBITMQ_USER}:{RABBITMQ_PASS}@{RABBITMQ_HOST}:{RABBITMQ_PORT}/"
    )

    async with connection:
        channel = await connection.channel()

        # Declare exchange
        exchange = await channel.declare_exchange(
            RABBITMQ_EXCHANGE, aio_pika.ExchangeType.DIRECT, durable=True
        )
        print(f"Declared exchange: {RABBITMQ_EXCHANGE}")

        # Declare queue
        queue = await channel.declare_queue(RABBITMQ_QUEUE, durable=True)
        print(f"Declared queue: {RABBITMQ_QUEUE}")

        # Bind queue to exchange with routing key
        await queue.bind(exchange, routing_key=RABBITMQ_ROUTING_KEY)
        print(f"Bound queue {RABBITMQ_QUEUE} to exchange {RABBITMQ_EXCHANGE} with routing key {RABBITMQ_ROUTING_KEY}")

async def main():
    print("Setting up MongoDB...")
    await setup_mongodb()
    
    print("\nSetting up RabbitMQ...")
    await setup_rabbitmq()
    
    print("\nSetup complete!")

if __name__ == "__main__":
    asyncio.run(main())
