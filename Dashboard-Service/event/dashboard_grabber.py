import asyncio
import httpx
from datetime import datetime, timedelta
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import UpdateOne
from pymongo.errors import BulkWriteError
import os
from dotenv import load_dotenv
import logging
import traceback

load_dotenv()

DATA_PROVIDER_URL = os.getenv("DATA_PROVIDER_URL", "http://localhost:8000")
MONGODB_URL = os.getenv("MONGODB_URL", "mongodb://localhost:27017")
MONGODB_DB = os.getenv("MONGODB_DB", "dashboard_db")
MONGODB_COLLECTION = os.getenv("MONGODB_COLLECTION_DASHBOARD", "bookings")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def fetch_events(start_date, end_date):
    async with httpx.AsyncClient() as client:
        params = {
            "night_of_stay__gte": start_date.strftime("%Y-%m-%d"),
            "night_of_stay__lte": end_date.strftime("%Y-%m-%d"),
            "rpg_status": 1
        }
        response = await client.get(f"{DATA_PROVIDER_URL}/events", params=params)
        response.raise_for_status()
        return response.json()

async def update_database(client, events, year):
    db = client[MONGODB_DB]
    collection = db[MONGODB_COLLECTION]
    
    # Only process the current year
    # if year == datetime.utcnow().year:
    #     await collection.delete_many({"year": year})

    # Re process all years
    await collection.delete_many({"year": year})
    
    daily_bookings = {}
    monthly_bookings = {}
    
    for event in events:
        event_date = datetime.fromisoformat(event['night_of_stay'].replace('Z', '+00:00'))
        hotel_id = event['hotel_id']
        date_key = event_date.strftime("%Y-%m-%d")
        month_key = event_date.strftime("%Y-%m")
        
        if hotel_id not in daily_bookings:
            daily_bookings[hotel_id] = {}
        if hotel_id not in monthly_bookings:
            monthly_bookings[hotel_id] = {}
        
        if date_key not in daily_bookings[hotel_id]:
            daily_bookings[hotel_id][date_key] = {"count": 0, "details": []}
        daily_bookings[hotel_id][date_key]["count"] += 1
        daily_bookings[hotel_id][date_key]["details"].append({
            "_id": str(event['id']),
            "room_id": event['room_id']
        })
        
        monthly_bookings[hotel_id][month_key] = monthly_bookings[hotel_id].get(month_key, 0) + 1
    
    operations = []
    for hotel_id in daily_bookings:
        for date, data in daily_bookings[hotel_id].items():
            operations.append(
                UpdateOne(
                    {"hotel_id": hotel_id, "date": date, "type": "daily"},
                    {"$set": {
                        "count": data["count"],
                        "year": year,
                        "details": data["details"]
                    }},
                    upsert=True
                )
            )
        for month, count in monthly_bookings[hotel_id].items():
            operations.append(
                UpdateOne(
                    {"hotel_id": hotel_id, "date": month, "type": "monthly"},
                    {"$set": {"count": count, "year": year}},
                    upsert=True
                )
            )
    
    if operations:
        try:
            result = await collection.bulk_write(operations, ordered=False)
            logger.info(f"Bulk write result for year {year}: {result.bulk_api_result}")
        except BulkWriteError as bwe:
            logger.error(f"Bulk write error for year {year}: {bwe.details}")
    else:
        logger.info(f"No operations to perform for year {year}")

async def get_years_to_process(collection):
    current_year = datetime.utcnow().year
    
    # Only process the current year
    # years_to_process = [current_year]  
    # for year in range(current_year - 1, current_year - 5, -1):
    #     count = await collection.count_documents({"year": year})
    #     if count == 0:
    #         years_to_process.append(year)

    years_to_process = []
    
    for year in range(current_year, current_year - 5, -1):
        years_to_process.append(year)
    
    return sorted(years_to_process)

async def dashboard_grabber():
    client = AsyncIOMotorClient(MONGODB_URL)
    try:
        while True:
            now = datetime.utcnow()
            db = client[MONGODB_DB]
            collection = db[MONGODB_COLLECTION]
            
            years_to_process = await get_years_to_process(collection)
            
            for year in years_to_process:
                start_date = datetime(year, 1, 1)
                end_date = min(datetime(year, 12, 31, 23, 59, 59), now)
                
                logger.info(f"Fetching events for year {year} from {start_date} to {end_date}")
                
                try:
                    events = await fetch_events(start_date, end_date)
                    if events:
                        await update_database(client, events, year)
                        logger.info(f"Updated database with {len(events)} events for year {year}")
                    else:
                        logger.info(f"No events to update for year {year}")
                except httpx.HTTPStatusError as e:
                    logger.error(f"HTTP error occurred: {e}")
                    logger.error(f"Response text: {e.response.text}")
                except Exception as e:
                    logger.error(f"Error during fetch or update: {str(e)}")
                    logger.error(f"Traceback: {traceback.format_exc()}")
            
            await asyncio.sleep(60)  # Sleep for 60 seconds
    except Exception as e:
        logger.error(f"An error occurred in the main loop: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
    finally:
        client.close()

async def start_dashboard_grabber():
    while True:
        try:
            await dashboard_grabber()
        except Exception as e:
            logger.error(f"Error in dashboard_grabber: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
        await asyncio.sleep(3600)  # Run every hour