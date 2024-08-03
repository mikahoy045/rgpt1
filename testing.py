import asyncio
import aiormq
from aio_pika import connect_robust
import os
import ssl
from dotenv import load_dotenv
from urllib.parse import quote_plus

load_dotenv()

async def main():
    host = os.getenv("RABBITMQ_HOST")
    user = os.getenv("RABBITMQ_USER")
    password = os.getenv("RABBITMQ_PASS")
    port = "5672"
    exchange = os.getenv("RABBITMQ_EXCHANGE")
    routing_key = os.getenv("RABBITMQ_ROUTING_KEY")
    queue = os.getenv("RABBITMQ_QUEUE")

    print(f"user {user} and password {password}")

    encoded_user = quote_plus(user)
    encoded_pass = quote_plus(password)
    url = f"amqp://{encoded_user}:{encoded_pass}@{host}:{port}/"

    # ssl_context = ssl.create_default_context()
    # ssl_context.check_hostname = False
    # ssl_context.verify_mode = ssl.CERT_NONE

    # connection = await aio_pika.connect_robust(url, ssl_options=ssl_context)
    connection = await connect_robust(
        url
    )
    channel = await connection.channel()
    # Get all queues available
    # Note: We can't directly list all queues using aio_pika
    # We'll just check if we can declare our specific queue

    try:
        queue = await channel.declare_queue(os.getenv("RABBITMQ_QUEUE"), durable=True)
        print(f"Successfully declared queue: {queue.name}")
    except Exception as e:
        print(f"Error declaring queue: {str(e)}")

    # Check if exchange exists
    try:
        exchange = await channel.declare_exchange(
            os.getenv("RABBITMQ_EXCHANGE"),
            type="direct",
            durable=True
        )
        print(f"Successfully declared exchange: {exchange.name}")
    except Exception as e:
        print(f"Error declaring exchange: {str(e)}")

    print("Connected successfully")
    await connection.close()

asyncio.run(main())