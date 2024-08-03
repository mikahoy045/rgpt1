import pytest
import sys
import os
import random
import asyncio
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from httpx import AsyncClient, ASGITransport
from datetime import datetime, timedelta
from dataprovider import app
from model.data_provider_model import Event
from queuemq.broker import rabbitmq_broker
from database.mongodb import connect_to_mongo, close_mongo_connection
from datetime import date


@pytest.fixture(scope="module")
async def client():
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test"
    ) as ac:
        yield ac

@pytest.fixture(scope="module", autouse=True)
async def setup_teardown():
    connect_to_mongo()
    await rabbitmq_broker.connect()
    yield
    await rabbitmq_broker.close()
    close_mongo_connection()

@pytest.fixture
def sample_event():
    timestamp = datetime.now() + timedelta(days=random.randint(0, 7))
    return {
        "hotel_id": random.randint(1, 2),
        "timestamp": timestamp.isoformat(),
        "rpg_status": random.randint(1, 2),
        "room_id": random.randint(100, 105),
        "night_of_stay": timestamp.date().isoformat()
    }

def is_sorted_by_timestamp(events):
    return all(events[i]["timestamp"] <= events[i+1]["timestamp"] for i in range(len(events)-1))

@pytest.mark.asyncio
async def test_create_multi_event_success(client):
    async for c in client:
        for _ in range(30):
            sample_event = {
                "hotel_id": random.randint(1, 2),
                "timestamp": (datetime.now() + timedelta(days=random.randint(0, 7))).isoformat(),
                "rpg_status": random.randint(1, 2),
                "room_id": random.randint(100, 105),
                "night_of_stay": (date.today() + timedelta(days=random.randint(0, 7))).isoformat()
            }
            
            response = await c.post("/events", json=sample_event)
            assert response.status_code == 200
            assert response.json()["hotel_id"] == sample_event["hotel_id"]
        
            # Add more assertions to verify the response
            response_data = response.json()
            assert "id" in response_data
            assert response_data["timestamp"] == sample_event["timestamp"]
            assert response_data["rpg_status"] == sample_event["rpg_status"]
            assert response_data["room_id"] == str(sample_event["room_id"])
            assert response_data["night_of_stay"] == sample_event["night_of_stay"]
        
        break 

@pytest.mark.asyncio
async def test_create_event_success(sample_event, client):
    async for c in client:
        response = await c.post("/events", json=sample_event)
        assert response.status_code == 200
        assert response.json()["hotel_id"] == sample_event["hotel_id"]

@pytest.mark.asyncio
async def test_create_event_invalid_rpg_status(sample_event, client):
    async for c in client:
        sample_event["rpg_status"] = 3  # Invalid rpg_status
        response = await c.post("/events", json=sample_event)
        assert response.status_code == 422

@pytest.mark.asyncio
async def test_create_event_missing_field(sample_event, client):
    async for c in client:
        del sample_event["room_id"]
        response = await c.post("/events", json=sample_event)
        assert response.status_code == 422

@pytest.mark.asyncio
async def test_create_event_invalid_date_format(sample_event, client):
    async for c in client:
        sample_event["night_of_stay"] = "2023/05/15"  # Invalid date format
        response = await c.post("/events", json=sample_event)
        assert response.status_code == 422

@pytest.mark.asyncio
@pytest.mark.parametrize("hotel_id", [1, 2])
async def test_get_events_by_hotel_id(hotel_id, client):
    async for c in client:
        response = c.get(f"/events?hotel_id={hotel_id}")
        assert response.status_code == 200
        events = response.json()
        assert all(event["hotel_id"] == hotel_id for event in events)
        assert is_sorted_by_timestamp(events)

@pytest.mark.asyncio
async def test_get_events_with_date_range(client):
    async for c in client:
        start_date = date.today() - timedelta(days=7)
        end_date = date.today()
        response = c.get(f"/events?hotel_id=1&night_of_stay__gte={start_date}&night_of_stay__lte={end_date}")
        assert response.status_code == 200
        events = response.json()
        assert all(start_date <= date.fromisoformat(event["night_of_stay"]) <= end_date for event in events)
        assert is_sorted_by_timestamp(events)

@pytest.mark.asyncio
async def test_get_events_with_rpg_status(client):
    async for c in client:
        rpg_status = 1
        response = c.get(f"/events?hotel_id=1&rpg_status={rpg_status}")
        assert response.status_code == 200
        events = response.json()
        assert all(event["rpg_status"] == rpg_status for event in events)
        assert is_sorted_by_timestamp(events)

@pytest.mark.asyncio
async def test_get_events_with_room_id(client):
    async for c in client:
        room_id = 101
        response = c.get(f"/events?hotel_id=1&room_id={room_id}")
        assert response.status_code == 200
        events = response.json()
        assert all(event["room_id"] == room_id for event in events)
        assert is_sorted_by_timestamp(events)

@pytest.mark.asyncio
async def test_get_events_with_timestamp_range(client):
    async for c in client:
        start_time = datetime.now() - timedelta(days=1)
        end_time = datetime.now()
        response = c.get(f"/events?hotel_id=1&updated__gte={start_time.isoformat()}&updated__lte={end_time.isoformat()}")
        assert response.status_code == 200
        events = response.json()
        assert all(start_time <= datetime.fromisoformat(event["timestamp"]) <= end_time for event in events)
        assert is_sorted_by_timestamp(events)

@pytest.mark.asyncio
async def test_get_events_with_all_parameters(client):
    async for c in client:
        hotel_id = 1
        rpg_status = 1
        room_id = 101
        start_date = date.today() - timedelta(days=7)
        end_date = date.today()
        start_time = datetime.now() - timedelta(days=1)
        end_time = datetime.now()
    
        response = c.get(f"/events?hotel_id={hotel_id}&rpg_status={rpg_status}&room_id={room_id}&"
                          f"night_of_stay__gte={start_date}&night_of_stay__lte={end_date}&"
                          f"updated__gte={start_time.isoformat()}&updated__lte={end_time.isoformat()}")
    
        assert response.status_code == 200
        events = response.json()
        assert all(
            event["hotel_id"] == hotel_id and
            event["rpg_status"] == rpg_status and
            event["room_id"] == room_id and
            start_date <= date.fromisoformat(event["night_of_stay"]) <= end_date and
            start_time <= datetime.fromisoformat(event["timestamp"]) <= end_time
            for event in events
        )
        assert is_sorted_by_timestamp(events)

if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])