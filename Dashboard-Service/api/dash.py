from fastapi import APIRouter, HTTPException, Query
from typing import Literal
from model.dashboard_model import DashboardResponse
from database.mongodb import get_dashboard_data

router = APIRouter()

@router.get("/")
def read_root():
    return {"Hello": "Data Provider"}

@router.get("/dashboard", response_model=DashboardResponse)
async def get_dashboard(
    hotel_id: int = Query(..., description="The ID of the hotel"),
    period: str = Query(..., description="The period of the dashboard (day, month, or day+month)"),
    year: int = Query(..., description="The year of the dashboard")
):
    valid_periods = ["day", "month", "day+month"]
    normalized_period = period.replace(" ", "+")
    
    if normalized_period not in valid_periods:
        raise HTTPException(status_code=400, detail=f"Invalid period. Must be one of: {', '.join(valid_periods)}")
    
    try:
        dashboard_data = await get_dashboard_data(hotel_id, normalized_period, year)
        return dashboard_data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))