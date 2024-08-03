import sys
print(f"Python version: {sys.version}")
print(f"Python executable: {sys.executable}")

from fastapi import FastAPI
import asyncio
from api.dprovider import router as dprovider_router
from database.mongodb import connect_to_mongo, close_mongo_connection
from queuemq.broker import rabbitmq_broker
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

@app.on_event("startup")
async def startup_event():
    connect_to_mongo()
    try:
        await rabbitmq_broker.connect()
    except Exception as e:
        print(f"Failed to connect to RabbitMQ: {str(e)}")
        print("The application will continue without RabbitMQ connection.")

@app.on_event("shutdown")
async def shutdown_event():
    close_mongo_connection()
    await rabbitmq_broker.close()

app.include_router(dprovider_router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)