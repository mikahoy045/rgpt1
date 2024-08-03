from pymongo import MongoClient
from dotenv import load_dotenv
import os
import asyncio
from typing import List
from model.dashboard_model import DashboardResponse, BookingData

load_dotenv()

class MongoDB:
    client: MongoClient = None
    db = None

db = MongoDB()

def connect_to_mongo():
    try:
        db.client = MongoClient(os.getenv("MONGODB_URL"))
        db.db = db.client[os.getenv("MONGODB_DB")]
        print("Successfully connected to MongoDB")
    except Exception as e:
        print(f"Failed to connect to MongoDB: {str(e)}")
        raise

def close_mongo_connection():
    if db.client:
        db.client.close()

async def run_mongo_task(func):
    return await asyncio.to_thread(func)

async def insert_one(collection, document):
    return await run_mongo_task(lambda: db.db[collection].insert_one(document))

async def find_one(collection, query):
    return await run_mongo_task(lambda: db.db[collection].find_one(query))

async def find(collection, query):
    return await run_mongo_task(lambda: list(db.db[collection].find(query)))

async def get_dashboard_data(hotel_id: str, period: str, year: int) -> DashboardResponse:
    collection = os.getenv("MONGODB_COLLECTION")
    
    pipeline = [
        {"$match": {"hotel_id": hotel_id, "year": year}},
        {"$group": {
            "_id": "$date" if period == "day" else {"$substr": ["$date", 0, 7]},
            "count": {"$sum": 1}
        }},
        {"$sort": {"_id": 1}}
    ]

    result = await run_mongo_task(lambda: list(db.db[collection].aggregate(pipeline)))

    bookings: List[BookingData] = [
        BookingData(date=item["_id"], count=item["count"])
        for item in result
    ]

    return DashboardResponse(
        hotel_id=hotel_id,
        period=period,
        year=year,
        bookings=bookings
    )