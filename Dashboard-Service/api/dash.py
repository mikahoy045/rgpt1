from fastapi import APIRouter, HTTPException, Query
from typing import Literal
from model.dashboard_model import DashboardResponse
from database.mongodb import get_dashboard_data

router = APIRouter()

@router.get("/dashboard", response_model=DashboardResponse)
async def get_dashboard(
    hotel_id: int = Query(..., description="The ID of the hotel"),
    period: Literal["day", "month", "day+month"] = Query(..., description="The period of the dashboard"),
    year: int = Query(..., description="The year of the dashboard")
):
    try:
        dashboard_data = await get_dashboard_data(hotel_id, period, year)
        return dashboard_data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))