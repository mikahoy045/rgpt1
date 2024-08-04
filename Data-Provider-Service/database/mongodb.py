from pymongo import MongoClient
from dotenv import load_dotenv
import time
import os
import asyncio

load_dotenv(override=True)

class MongoDB:
    client: MongoClient = None
    db = None

db = MongoDB()

def connect_to_mongo():
    max_retries = 5
    retry_delay = 5  # seconds

    for attempt in range(max_retries):
        try:
            mongodb_url = os.getenv("MONGODB_URL")
            mongodb_db = os.getenv("MONGODB_DB")
            print(f"MONGODB_URL: {mongodb_url}")
            print(f"MONGODB_DB: {mongodb_db}")
            print(f"Attempting to connect to MongoDB with URL: {mongodb_url}")
            db.client = MongoClient(mongodb_url, serverSelectionTimeoutMS=5000)
            print("Initial connection successful, attempting to ping...")
            db.client.admin.command('ping')
            print("Ping successful")
            db.db = db.client[mongodb_db]
            print("Successfully connected to MongoDB and selected database")
            break
        except Exception as e:
            print(f"Failed to connect to MongoDB (attempt {attempt + 1}/{max_retries})")
            print(f"Error type: {type(e).__name__}")
            print(f"Error message: {str(e)}")
            print(f"Error details: {e.args}")
            if attempt < max_retries - 1:
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                print("Max retries reached. Unable to connect to MongoDB.")
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