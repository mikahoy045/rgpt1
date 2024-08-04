from pydantic import BaseModel, Field
from typing import List, Dict, Optional, Any
from datetime import datetime, date

class EventDetail(BaseModel):
    id: str
    room_id: str
    night_of_stay: str

class BookingData(BaseModel):
    total: int
    detail: List[EventDetail]

class DashboardResponse(BaseModel):
    hotel_id: int
    period: str
    year: int
    detail: Dict[str, Dict[str, BookingData]]

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat(),
            date: lambda v: v.isoformat(),
        }