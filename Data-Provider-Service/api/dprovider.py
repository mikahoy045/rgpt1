from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
from datetime import datetime, date
from model.data_provider_model import Event
from queuemq.broker import rabbitmq_broker
from database.mongodb import find
import os
import json

router = APIRouter()

@router.get("/")
def read_root():
    return {"Hello": "Data Provider"}

@router.get("/events", response_model=List[Event])
async def get_events(
    hotel_id: int,
    updated__gte: Optional[datetime] = Query(None),
    updated__lte: Optional[datetime] = Query(None),
    rpg_status: Optional[int] = Query(None),
    room_id: Optional[int] = Query(None),
    night_of_stay__gte: Optional[str] = Query(None),
    night_of_stay__lte: Optional[str] = Query(None)
):
    query = {"hotel_id": hotel_id}

    if updated__gte or updated__lte:
        query["timestamp"] = {}
        if updated__gte:
            query["timestamp"]["$gte"] = updated__gte
        if updated__lte:
            query["timestamp"]["$lte"] = updated__lte

    if rpg_status is not None:
        query["rpg_status"] = rpg_status

    if room_id is not None:
        query["room_id"] = room_id

    if night_of_stay__gte or night_of_stay__lte:
        query["night_of_stay"] = {}
        if night_of_stay__gte:
            query["night_of_stay"]["$gte"] = night_of_stay__gte
        if night_of_stay__lte:
            query["night_of_stay"]["$lte"] = night_of_stay__lte

    collection = os.getenv("MONGODB_COLLECTION")
    events = await find(collection, query)
    for event in events:
        event["night_of_stay"] = date.fromisoformat(event["night_of_stay"])
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