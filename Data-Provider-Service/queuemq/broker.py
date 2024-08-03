import asyncio
import os
import logging
import json
from aio_pika import connect_robust, Message, ExchangeType
from dotenv import load_dotenv
from urllib.parse import quote_plus

load_dotenv()

# logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class RabbitMQBroker:
    def __init__(self):
        self.host = os.getenv("RABBITMQ_HOST")
        self.user = os.getenv("RABBITMQ_USER")
        self.password = os.getenv("RABBITMQ_PASS")
        self.port = "5672"
        self.exchange_name = os.getenv("RABBITMQ_EXCHANGE")
        self.routing_key = os.getenv("RABBITMQ_ROUTING_KEY")
        self.queue_name = os.getenv("RABBITMQ_QUEUE")
        self.connection = None
        self.channel = None
        self.exchange = None
        self.queue = None

        encoded_user = quote_plus(self.user)
        encoded_pass = quote_plus(self.password)
        self.url = f"amqp://{encoded_user}:{encoded_pass}@{self.host}:{self.port}/"

        # logger.debug(f"RabbitMQ URL: amqp://{encoded_user}:******@{self.host}:{self.port}/")

    async def connect(self):
        try:
            logger.info("Attempting to connect to RabbitMQ...")
            self.connection = await connect_robust(self.url)
            self.channel = await self.connection.channel()
            
            self.exchange = await self.channel.declare_exchange(
                self.exchange_name, 
                ExchangeType.DIRECT,
                durable=True
            )
            
            self.queue = await self.channel.declare_queue(self.queue_name, durable=True)
            await self.queue.bind(self.exchange, routing_key=self.routing_key)
            
            logger.info(f"Successfully connected to RabbitMQ at {self.host}:{self.port}")
        except Exception as e:
            logger.error(f"Failed to connect to RabbitMQ: {str(e)}", exc_info=True)
            raise

    async def publish(self, message: str, correlation_id: str = None):
        if not self.connection or self.connection.is_closed:
            await self.connect()
        
        await self.exchange.publish(
            Message(
                body=message.encode(),
                correlation_id=correlation_id,
                delivery_mode=2
            ),
            routing_key=self.routing_key
        )
        logger.info(f"Sent message to {self.routing_key} with correlation_id: {correlation_id}")

    async def consume(self, callback):
        if not self.connection or self.connection.is_closed:
            await self.connect()

        async with self.queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    await callback(message.body)

    async def close(self):
        if self.connection and not self.connection.is_closed:
            await self.connection.close()
            logger.info("RabbitMQ connection closed")

rabbitmq_broker = RabbitMQBroker()