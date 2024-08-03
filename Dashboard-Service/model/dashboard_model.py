from pydantic import BaseModel
from typing import List, Dict, Optional

class EventDetail(BaseModel):
    id: int
    room_id: str
    night_of_stay: str

class BookingData(BaseModel):
    total: int
    detail: List[EventDetail]

class DashboardResponse(BaseModel):
    hotel_id: int
    period: str
    year: int
    detail: Dict[str, BookingData]
    detail_daily: Optional[Dict[str, BookingData]]
    detail_monthly: Optional[Dict[str, BookingData]]