from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
from datetime import datetime, date
from model.data_provider_model import Event
from database.mongodb import find
from queuemq.broker import rabbitmq_broker
import json
import os

router = APIRouter()

@router.get("/")
def read_root():
    return {"Hello": "Data Provider"}

@router.get("/events", response_model=List[Event])
async def get_events(
    hotel_id: Optional[int] = Query(None),
    updated__gte: Optional[datetime] = Query(None),
    updated__lte: Optional[datetime] = Query(None),
    rpg_status: Optional[int] = Query(None),
    room_id: Optional[str] = Query(None),
    night_of_stay__gte: Optional[date] = Query(None),
    night_of_stay__lte: Optional[date] = Query(None)
):
    query = {}

    if hotel_id is not None:
        query["hotel_id"] = hotel_id

    if updated__gte or updated__lte:
        query["timestamp"] = {}
        if updated__gte:
            query["timestamp"]["$gte"] = updated__gte.isoformat()
        if updated__lte:
            query["timestamp"]["$lte"] = updated__lte.isoformat()

    if rpg_status is not None:
        query["rpg_status"] = rpg_status

    if room_id is not None:
        query["room_id"] = room_id

    if night_of_stay__gte or night_of_stay__lte:
        query["night_of_stay"] = {}
        if night_of_stay__gte:
            query["night_of_stay"]["$gte"] = night_of_stay__gte.isoformat()
        if night_of_stay__lte:
            query["night_of_stay"]["$lte"] = night_of_stay__lte.isoformat()

    collection = os.getenv("MONGODB_COLLECTION")
    events = await find(collection, query)
    
    print(f"Query: {query}")
    print(f"Number of events found: {len(events)}")

    return [Event(**event) for event in events]

class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        return json.JSONEncoder.default(self, obj)

@router.post("/events", response_model=Event)
async def create_event(event: Event):
    try:
        event_dict = event.dict()
        
        event_json = json.dumps(event_dict, cls=DateTimeEncoder)
        
        try:
            await rabbitmq_broker.publish(event_json, str(event_dict["id"]))
            return event
        except Exception as rabbitmq_error:
            raise HTTPException(status_code=500, detail=f"Failed to publish to RabbitMQ: {str(rabbitmq_error)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create event: {str(e)}")