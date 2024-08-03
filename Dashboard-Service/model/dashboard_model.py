from pydantic import BaseModel
from typing import List, Dict

class BookingData(BaseModel):
    date: str
    count: int

class DashboardResponse(BaseModel):
    hotel_id: str
    period: str
    year: int
    bookings: List[BookingData]
