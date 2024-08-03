from fastapi import APIRouter, HTTPException, Query
from typing import Optional
from datetime import date
from model.dashboard_model import DashboardResponse
from database.mongodb import get_dashboard_data

router = APIRouter()

@router.get("/")
def read_root():
    return {"Hello": "Dashboard Service"}

@router.get("/dashboard", response_model=DashboardResponse)
async def get_dashboard(
    hotel_id: str = Query(..., description="The ID of the hotel"),
    period: str = Query(..., description="The period of the dashboard (month or day)"),
    year: int = Query(..., description="The year of the dashboard")
):
    if period not in ["month", "day"]:
        raise HTTPException(status_code=400, detail="Invalid period. Must be 'month' or 'day'")

    try:
        dashboard_data = await get_dashboard_data(hotel_id, period, year)
        return dashboard_data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

