import sys
print(f"Python version: {sys.version}")
print(f"Python executable: {sys.executable}")

from fastapi import FastAPI
from contextlib import asynccontextmanager
import os
from api.dash import router as dash_router
from database.mongodb import connect_to_mongo, close_mongo_connection, db
from dotenv import load_dotenv
from pymongo import ASCENDING

load_dotenv()

async def setup_mongodb():
    mongodb_db = os.getenv("MONGODB_DB")
    mongodb_collection = os.getenv("MONGODB_COLLECTION")
    
    if mongodb_db not in db.client.list_database_names():
        print(f"Creating database: {mongodb_db}")
    
    if mongodb_collection not in db.db.list_collection_names():
        print(f"Creating collection: {mongodb_collection}")
        db.db.create_collection(mongodb_collection)
    
    collection = db.db[mongodb_collection]
    
    indexes_to_create = [
        ("hotel_id", ASCENDING),
        ("year", ASCENDING),
        ("date", ASCENDING)
    ]
    
    existing_indexes = collection.index_information()
    
    for field, direction in indexes_to_create:
        index_name = f"{field}_1"
        if index_name not in existing_indexes:
            print(f"Creating index for field: {field}")
            collection.create_index([(field, direction)])
        else:
            print(f"Index already exists for field: {field}")

@asynccontextmanager
async def lifespan(app: FastAPI):
    connect_to_mongo()
    await setup_mongodb()
    
    yield
    
    close_mongo_connection()

app = FastAPI(lifespan=lifespan)

app.include_router(dash_router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=7000)