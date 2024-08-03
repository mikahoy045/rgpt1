import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import asyncio
from dotenv import load_dotenv
from database.mongodb import connect_to_mongo, close_mongo_connection, insert_one
from model.data_provider_model import Event
from datetime import date
from queuemq.broker import rabbitmq_broker

load_dotenv()

async def callback(message):
    event_dict = json.loads(message)
    
    event_dict["night_of_stay"] = date.fromisoformat(event_dict["night_of_stay"])
    
    event = Event(**event_dict)
    
    collection = os.getenv("MONGODB_COLLECTION")
    result = await insert_one(collection, event_dict)
    print(f"Saved event with ID: {result.inserted_id}")

async def start_consuming():
    connect_to_mongo()
    
    try:
        await rabbitmq_broker.connect()
        await rabbitmq_broker.consume(callback)
    except Exception as e:
        print(f"Error in consumer: {str(e)}")
    finally:
        close_mongo_connection()
        await rabbitmq_broker.close()

if __name__ == "__main__":
    asyncio.run(start_consuming())