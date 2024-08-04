from pymongo import MongoClient
from dotenv import load_dotenv
import os
import asyncio
from typing import List
from model.dashboard_model import DashboardResponse, BookingData
from bson import ObjectId
from model.dashboard_model import EventDetail

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
        "type": "daily"
    }

    pipeline = [
        {"$match": query},
        {"$unwind": "$details"},
        {"$group": {
            "_id": "$date",
            "total": {"$sum": "$count"},
            "detail": {"$push": {
                "id": {"$toString": "$details._id"},
                "room_id": "$details.room_id",
                "night_of_stay": "$date"
            }}
        }},
        {"$sort": {"_id": 1}}
    ]

    result = await run_mongo_task(lambda: list(db.db[collection].aggregate(pipeline)))

    bookings = {
        item["_id"]: BookingData(total=item["total"], detail=[EventDetail(**detail) for detail in item["detail"]])
        for item in result
    }

    if period == "day":
        return DashboardResponse(
            hotel_id=hotel_id,
            period="daily",
            year=year,
            detail_daily=bookings
        )
    elif period == "month":
        monthly_pipeline = [
            {"$match": query},
            {"$unwind": "$details"},
            {"$group": {
                "_id": {"$substr": ["$date", 0, 7]},  # Group by YYYY-MM
                "total": {"$sum": "$count"},
                "detail": {"$push": {
                    "id": {"$toString": "$details._id"},
                    "room_id": "$details.room_id",
                    "night_of_stay": "$date"
                }}
            }},
            {"$sort": {"_id": 1}}
        ]
        
        monthly_result = await run_mongo_task(lambda: list(db.db[collection].aggregate(monthly_pipeline)))
        
        monthly_bookings = {
            item["_id"]: BookingData(
                total=item["total"], 
                detail=[EventDetail(**detail) for detail in item["detail"]]
            ) 
            for item in monthly_result
        }
        
        return DashboardResponse(
            hotel_id=hotel_id,
            period="monthly",
            year=year,
            detail_monthly=monthly_bookings
        )
    else:  # period == "day+month"
        monthly_pipeline = [
            {"$match": query},
            {"$unwind": "$details"},
            {"$group": {
                "_id": {"$substr": ["$date", 0, 7]},  # Group by YYYY-MM
                "total": {"$sum": "$count"},
                "detail": {"$push": {
                    "id": {"$toString": "$details._id"},
                    "room_id": "$details.room_id",
                    "night_of_stay": "$date"
                }}
            }},
            {"$sort": {"_id": 1}}
        ]
        
        monthly_result = await run_mongo_task(lambda: list(db.db[collection].aggregate(monthly_pipeline)))
        
        monthly_bookings = {
            item["_id"]: BookingData(
                total=item["total"], 
                detail=[EventDetail(**detail) for detail in item["detail"]]
            ) 
            for item in monthly_result
        }
        
        return DashboardResponse(
            hotel_id=hotel_id,
            period="daily+monthly",
            year=year,
            detail={
                "daily": bookings,
                "monthly": monthly_bookings
            }
        )