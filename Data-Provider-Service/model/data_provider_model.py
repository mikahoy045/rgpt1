from pydantic import BaseModel, Field, field_validator
from datetime import datetime, date
from typing import Optional

class Event(BaseModel):
    id: Optional[int] = Field(default_factory=lambda: int(datetime.now().timestamp()))
    hotel_id: int
    timestamp: datetime
    rpg_status: int = Field(..., ge=1, le=2)
    room_id: str
    night_of_stay: date

    @field_validator('timestamp', mode='before')
    @classmethod
    def parse_timestamp(cls, v):
        if isinstance(v, str):
            return datetime.fromisoformat(v.rstrip('Z'))
        return v

    @field_validator('night_of_stay', mode='before')
    @classmethod
    def parse_date(cls, v):
        if isinstance(v, str):
            return date.fromisoformat(v)
        return v

    @field_validator('room_id', mode='before')
    @classmethod
    def convert_room_id_to_string(cls, v):
        return str(v)

    class ConfigDict:
        json_encoders = {
            date: lambda v: v.isoformat(),
            datetime: lambda v: v.isoformat() + "Z",
        }
        json_schema_extra = {
            "example": {
                "hotel_id": 1,
                "timestamp": "2020-01-01T00:00:00Z",
                "rpg_status": 1,
                "room_id": "1",
                "night_of_stay": "2020-01-01",
            }
        }

    def dict(self, *args, **kwargs):
        return self.model_dump(*args, **kwargs)