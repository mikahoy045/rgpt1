import asyncio
import httpx
from datetime import datetime, timedelta
from motor.motor_asyncio import AsyncIOMotorClient
import os
from dotenv import load_dotenv

load_dotenv()

DATA_PROVIDER_URL = os.getenv("DATA_PROVIDER_URL", "http://localhost:8000")
MONGODB_URL = os.getenv("MONGODB_URL", "mongodb://localhost:27017")
MONGODB_DB = os.getenv("MONGODB_DB", "dashboard_db")
MONGODB_COLLECTION = os.getenv("MONGODB_COLLECTION", "bookings")

async def fetch_events(start_date, end_date):
    async with httpx.AsyncClient() as client:
        params = {
            "hotel_id": "1",  # Changed to string
            "updated__gte": start_date.strftime("%Y-%m-%dT%H:%M:%S"),
            "updated__lte": end_date.strftime("%Y-%m-%dT%H:%M:%S") + "Z"
        }
        response = await client.get(f"{DATA_PROVIDER_URL}/events", params=params)
        response.raise_for_status()
        return response.json()

async def update_database(client, events):
    db = client[MONGODB_DB]
    collection = db[MONGODB_COLLECTION]
    for event in events:
        event_date = datetime.fromisoformat(event['night_of_stay'].replace('Z', '+00:00'))
        doc = {
            "hotel_id": event['hotel_id'],
            "year": event_date.year,
            "date": event_date.date().isoformat(),
            "timestamp": event_date
        }
        await collection.update_one(
            {"hotel_id": doc["hotel_id"], "timestamp": doc["timestamp"]},
            {"$setOnInsert": doc},
            upsert=True
        )

async def dashboard_grabber():
    client = AsyncIOMotorClient(MONGODB_URL)
    try:
        while True:
            now = datetime.utcnow()
            db = client[MONGODB_DB]
            collection = db[MONGODB_COLLECTION]
            latest_doc = await collection.find_one(sort=[("timestamp", -1)])
            
            start_date = latest_doc['timestamp'] if latest_doc else now - timedelta(days=30)
            end_date = now
            
            print(f"Fetching events from {start_date} to {end_date}")
            
            try:
                events = await fetch_events(start_date, end_date)
                if events:
                    await update_database(client, events)
                    print(f"Updated database with {len(events)} events")
                else:
                    print("No events to update")
            except httpx.HTTPStatusError as e:
                print(f"HTTP error occurred: {e}")
                print(f"Response text: {e.response.text}")
            except Exception as e:
                print(f"Error during fetch or update: {str(e)}")
            
            await asyncio.sleep(60)
    except Exception as e:
        print(f"An error occurred in the main loop: {str(e)}")
    finally:
        client.close()

def main():
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(dashboard_grabber())
    except KeyboardInterrupt:
        print("Dashboard grabber stopped by user")
    finally:
        loop.close()

if __name__ == "__main__":
    main()