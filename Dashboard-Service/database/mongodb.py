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

async def get_dashboard_data(hotel_id: int, period: str, year: int) -> DashboardResponse:
    collection = os.getenv("MONGODB_COLLECTION_DASHBOARD")
    
    query = {
        "hotel_id": hotel_id,
        "year": year,
        "rpg_status": 1
    }

    daily_pipeline = [
        {"$match": query},
        {"$group": {
            "_id": "$night_of_stay",
            "total": {"$sum": 1},
            "detail": {"$push": {"id": "$id", "room_id": "$room_id", "night_of_stay": "$night_of_stay"}}
        }},
        {"$sort": {"_id": 1}}
    ]

    monthly_pipeline = [
        {"$match": query},
        {"$group": {
            "_id": {"$substr": ["$night_of_stay", 0, 7]},
            "total": {"$sum": 1},
            "detail": {"$push": {"id": "$id", "room_id": "$room_id", "night_of_stay": "$night_of_stay"}}
        }},
        {"$sort": {"_id": 1}}
    ]

    daily_result = await run_mongo_task(lambda: list(db.db[collection].aggregate(daily_pipeline)))
    monthly_result = await run_mongo_task(lambda: list(db.db[collection].aggregate(monthly_pipeline)))

    daily_bookings = {
        item["_id"]: BookingData(total=item["total"], detail=item["detail"])
        for item in daily_result
    }

    monthly_bookings = {
        item["_id"]: BookingData(total=item["total"], detail=item["detail"])
        for item in monthly_result
    }

    if period == "day":
        return DashboardResponse(
            hotel_id=hotel_id,
            period="daily",
            year=year,
            detail=daily_bookings
        )
    elif period == "month":
        return DashboardResponse(
            hotel_id=hotel_id,
            period="monthly",
            year=year,
            detail=monthly_bookings
        )
    else:  # period == "day+month"
        return DashboardResponse(
            hotel_id=hotel_id,
            period="daily+monthly",
            year=year,
            detail_daily=daily_bookings,
            detail_monthly=monthly_bookings
        )