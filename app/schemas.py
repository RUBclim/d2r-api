from datetime import datetime
from enum import StrEnum

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


class PublicParams(StrEnum):
    absolute_humidity = 'absolute_humidity'
    atmospheric_pressure = 'air_pressure'
    air_temperature = 'air_temperature'
    dew_point = 'dew_point'
    heat_index = 'heat_index'
    lightning_average_distance = 'lightning_avg_distance'
    lightning_strike_count = 'lightning_strike_count'
    mrt = 'mrt'
    pet = 'pet'
    pet_category = 'pet_categoryy'
    precipitation_sum = 'precipitation_sum'
    relative_humidity = 'relative_humidity'
    solar_radiation = 'solar_radiation'
    utci = 'utci'
    utci_category = 'utci_category'
    vapor_pressure = 'vapor_pressure'
    wet_bulb_temperature = 'wet_bulb_temperature'
    wind_direction = 'wind_direction'
    wind_speed = 'wind_speed'
    wind_speed_max = 'wind_speed_max'


class OneParam(BaseModel):
    name: str
    long_name: str
    latitude: float
    longitude: float
    altitude: float
    measured_at: datetime
    value: float | str | None
