from datetime import datetime
from enum import StrEnum
from typing import Generic
from typing import Literal
from typing import TypeVar

from pydantic import BaseModel
from pydantic import Field

from app.models import HeatStressCategories
from app.models import StationType


LCZClass = Literal[
    '1', '2', '3', '4', '5', '6', '7', '8', '9', '10',
    'A', 'B', 'C', 'D', 'E', 'F', 'G',
]


class Units(StrEnum):
    g_m3 = 'g/m³'
    hpa = 'hPa'
    deg_c = '°C'
    km = 'km',
    mm = 'mm'
    wm2 = 'w/m²'
    deg = '°'
    ms = 'm/s'
    perc = '%'
    unitless = '-'


# this includes every param of PublicParams
UNIT_MAPPING: dict[str, Units] = {
    'absolute_humidity': Units.g_m3,
    'atmospheric_pressure': Units.hpa,
    'atmospheric_pressure_reduced': Units.hpa,
    'air_temperature': Units.deg_c,
    'dew_point': Units.deg_c,
    'heat_index': Units.deg_c,
    'lightning_average_distance': Units.km,
    'lightning_strike_count': Units.unitless,
    'mrt': Units.deg_c,
    'pet': Units.deg_c,
    'pet_category': Units.unitless,
    'precipitation_sum': Units.mm,
    'relative_humidity': Units.perc,
    'solar_radiation': Units.wm2,
    'utci': Units.deg_c,
    'utci_category': Units.unitless,
    'vapor_pressure': Units.hpa,
    'wet_bulb_temperature': Units.deg_c,
    'wind_direction': Units.deg,
    'wind_speed': Units.ms,
    'wind_speed_max': Units.ms,
}


class PublicParams(StrEnum):
    absolute_humidity = 'absolute_humidity'
    atmospheric_pressure = 'atmospheric_pressure'
    atmospheric_pressure_reduced = 'atmospheric_pressure_reduced'
    air_temperature = 'air_temperature'
    dew_point = 'dew_point'
    heat_index = 'heat_index'
    lightning_average_distance = 'lightning_avg_distance'
    lightning_strike_count = 'lightning_strike_count'
    mrt = 'mrt'
    pet = 'pet'
    pet_category = 'pet_category'
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


T = TypeVar('T')


class GenericReturn(BaseModel, Generic[T]):
    """Generic structure of an API response."""
    data: T = Field(
        description='array or object containing the requested data',
    )


class StationMetadata(BaseModel):
    name: str = Field(
        examples=['DEC005476'],
        description='The unique identifier of the station',
    )
    long_name: str = Field(
        examples=['Friedensplatz'],
        description='A longer, more descriptive name of the station',
    )
    latitude: float = Field(
        examples=[51.51116],
        description='The latitude of the station in **°** (WGS 84)',
    )
    longitude: float = Field(
        examples=[7.46607],
        description='The longitude of the station in **°** (WGS 84)',
    )
    altitude: float = Field(
        examples=[110.5],
        description='The altitude above sea level of the station in **m**',
    )
    district: str | None = Field(
        examples=['Innenstadt'],
        description='The name of the city district, that station is located in',
    )
    lcz: LCZClass | None = Field(
        examples=[2],
        description='The abbreviated Local Climate Zone Class',
    )
    station_type: StationType = Field(
        examples=[StationType.biomet],
        description=(
            'The type of the station. Depending on the station type, a different set '
            'of parameters is available.'
        ),
    )


class Parameters(BaseModel):
    absolute_humidity: float | None = Field(
        None,
        examples=[11.5],
        description='Absolute humidity in **g/m³**',
    )
    atmospheric_pressure: float | None = Field(
        None,
        examples=[1013.5],
        description='The atmospheric pressure at the station in **hPa**',
    )
    atmospheric_pressure_reduced: float | None = Field(
        None,
        examples=[1020.5],
        description='The atmospheric pressure reduced to sea-level in **hPa**',
    )
    air_temperature: float | None = Field(
        None,
        examples=[12.5],
        description='The air temperature in **°C**',
    )
    dew_point: float | None = Field(
        None,
        examples=[7.5],
        description='The dew point temperature in **°C**',
    )
    heat_index: float | None = Field(
        None,
        examples=[25.6],
        description=(
            'The heat index derived from relative humidity and air temperature '
            'in **°C**'
        ),
    )
    lightning_average_distance: float | None = Field(
        None,
        examples=[3.5],
        description='The average distance of lightning strikes in **km**',
    )
    lightning_strike_count: int | None = Field(
        None,
        examples=[11],
        description='The number of counted lightning strikes **-**',
    )
    mrt: float | None = Field(
        None,
        examples=[64.5],
        description='The mean radiant temperature in **°C**',
    )
    pet: float | None = Field(
        None,
        examples=[35.5],
        description='The physiological equivalent temperature in **°C**',
    )
    pet_category: HeatStressCategories | None = Field(
        None,
        examples=['Moderate heat stress'],
        description='The grade of physiological stress',
    )
    precipitation_sum: float | None = Field(
        None,
        ge=0,
        examples=[8.9],
        description='The precipitation sum since the last log-interval in **mm**',
    )
    relative_humidity: float | None = Field(
        None,
        ge=0,
        le=100,
        examples=[73.6],
        description='The relative humidity in **%**',
    )
    solar_radiation: float | None = Field(
        None,
        examples=[860.5],
        ge=0,
        description='The incoming shortwave solar radiation in **W/m²**',
    )
    utci: float | None = Field(
        None,
        examples=[38.5],
        description='The universal thermal climate index in **°C**',
    )
    utci_category: HeatStressCategories | None = Field(
        None,
        examples=['Strong heat stress'],
        description='The grade of physiological stress',
    )
    vapor_pressure: float | None = Field(
        None,
        examples=[19.5],
        description='The vapor pressure in **hPa**',
    )
    wet_bulb_temperature: float | None = Field(
        None,
        examples=[13.4],
        description='The wet bulb temperature in **°C*',
    )
    wind_direction: float | None = Field(
        None,
        examples=[270.4],
        description='The wind direction in **°**',
        gt=0,
        le=360,
    )
    wind_speed: float | None = Field(
        None,
        examples=[3.8],
        description='The wind speed in **m/s**',
        ge=0,
    )
    wind_speed_max: float | None = Field(
        None,
        examples=[8.5],
        description=(
            'The maximum of the wind speed (gust-strength), since the last '
            'log-interval in **m/s**'
        ),
        ge=0,
    )


class StationParams(StationMetadata, Parameters):
    measured_at: datetime = Field(
        examples=[datetime(2024, 8, 28, 18, 50, 13, 169)],
        description='The exact time the value was measured in **UTC**',
    )


class DistrictParams(Parameters):
    district: str = Field(
        examples=['Innenstadt'],
        description='The name of the city district, that station is located in',
    )


class StationData(Parameters):
    measured_at: datetime = Field(
        examples=[datetime(2024, 8, 28, 18, 50, 13, 169)],
        description='The exact time the value was measured in **UTC**',
    )
