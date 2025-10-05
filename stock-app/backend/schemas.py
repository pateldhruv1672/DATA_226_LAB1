# backend/schemas.py
from pydantic import BaseModel
from typing import List


class Company(BaseModel):
    name: str
    ticker: str


class TimeSeriesPoint(BaseModel):
    date: str
    close: float | None = None
    close_predicted: float | None = None


class GraphResponse(BaseModel):
    ticker: str
    points: List[TimeSeriesPoint]


class StatusResponse(BaseModel):
    table_exists: bool