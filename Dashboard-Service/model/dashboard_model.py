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
    # detail: Optional[Dict[str, BookingData]] = None
    detail: Optional[Dict[str, Dict[str, BookingData]]] = None
    detail_daily: Optional[Dict[str, BookingData]] = None
    detail_monthly: Optional[Dict[str, BookingData]] = None

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat(),
            date: lambda v: v.isoformat(),
        }

    def dict(self, *args, **kwargs):
        d = super().dict(*args, **kwargs)
        return {k: v for k, v in d.items() if v is not None and k != "detail_daily" and k != "detail_monthly"}