from datetime import datetime

from pydantic import BaseModel

from app.models import StationType


class TempRHMeasurement(BaseModel):
    measured_on: datetime
    relative_humidity: float | None
    air_temperature: float | None


class Station(BaseModel):
    name: str
    latitude: float
    longitude: float
    altitude: float
    station_type: StationType
