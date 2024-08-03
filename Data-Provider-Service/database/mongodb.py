from pymongo import MongoClient
from dotenv import load_dotenv
import os
import asyncio

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