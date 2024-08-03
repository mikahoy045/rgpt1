import json
import asyncio
import os
import sys
import logging
from dotenv import load_dotenv
from datetime import datetime, date

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.mongodb import connect_to_mongo, close_mongo_connection, insert_one
from model.data_provider_model import Event
from queuemq.broker import rabbitmq_broker

load_dotenv()
logger = logging.getLogger(__name__)

class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        return json.JSONEncoder.default(self, obj)

async def callback(message):
    event_dict = json.loads(message)
    
    if isinstance(event_dict["night_of_stay"], str):
        event_dict["night_of_stay"] = date.fromisoformat(event_dict["night_of_stay"])
    
    event = Event(**event_dict)
    
    event_dict = json.loads(json.dumps(event.dict(), cls=DateTimeEncoder))
    
    collection = os.getenv("MONGODB_COLLECTION")
    max_retries = 5
    retry_delay = 1  # seconds

    for attempt in range(max_retries):
        try:
            result = await asyncio.wait_for(insert_one(collection, event_dict), timeout=5.0)
            logger.info(f"Saved event with ID: {result.inserted_id}")
            print(f"Saved event with ID: {result.inserted_id}")
            break
        except asyncio.TimeoutError:
            if attempt < max_retries - 1:
                logger.warning(f"Timeout occurred. Retrying... (Attempt {attempt + 1}/{max_retries})")
                await asyncio.sleep(retry_delay)
            else:
                logger.error("Max retries reached. Failed to save event.")
                print("Failed to save event due to timeout.")
        except Exception as e:
            logger.error(f"Error saving event: {str(e)}")
            print(f"Error saving event: {str(e)}")
            break

async def start_consuming():
    try:
        await rabbitmq_broker.consume(callback)
    except asyncio.CancelledError:
        logger.error("Consumer was cancelled")
    except Exception as e:
        logger.error(f"Error in consumer: {str(e)}")

if __name__ == "__main__":
    connect_to_mongo()
    try:
        asyncio.run(start_consuming())
    finally:
        close_mongo_connection()