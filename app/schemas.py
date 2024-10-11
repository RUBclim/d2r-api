import subprocess
from datetime import datetime
from datetime import timezone
from enum import StrEnum
from functools import lru_cache
from typing import Generic
from typing import Literal
from typing import TypeVar

from pydantic import BaseModel
from pydantic import Field
from pydantic import RootModel

from app.models import HeatStressCategories
from app.models import StationType


LCZClass = Literal[
    '1', '2', '3', '4', '5', '6', '7', '8', '9', '10',
    'A', 'B', 'C', 'D', 'E', 'F', 'G',
]


class Units(StrEnum):
    """Common units used for data provided by this API"""
    g_m3 = 'g/m³'
    hpa = 'hPa'
    deg_c = '°C'
    km = 'km',
    mm = 'mm'
    wm2 = 'W/m²'
    deg = '°'
    ms = 'm/s'
    perc = '%'
    unitless = '-'


# this includes every param of PublicParams
UNIT_MAPPING: dict[str, Units] = {
    'absolute_humidity': Units.g_m3,
    'absolute_humidity_max': Units.g_m3,
    'absolute_humidity_min': Units.g_m3,
    'atmospheric_pressure': Units.hpa,
    'atmospheric_pressure_max': Units.hpa,
    'atmospheric_pressure_min': Units.hpa,
    'atmospheric_pressure_reduced': Units.hpa,
    'atmospheric_pressure_reduced_max': Units.hpa,
    'atmospheric_pressure_reduced_min': Units.hpa,
    'air_temperature': Units.deg_c,
    'air_temperature_max': Units.deg_c,
    'air_temperature_min': Units.deg_c,
    'dew_point': Units.deg_c,
    'dew_point_max': Units.deg_c,
    'dew_point_min': Units.deg_c,
    'heat_index': Units.deg_c,
    'heat_index_max': Units.deg_c,
    'heat_index_min': Units.deg_c,
    'lightning_average_distance': Units.km,
    'lightning_strike_count': Units.unitless,
    'mrt': Units.deg_c,
    'mrt_max': Units.deg_c,
    'mrt_min': Units.deg_c,
    'pet': Units.deg_c,
    'pet_max': Units.deg_c,
    'pet_min': Units.deg_c,
    'pet_category': Units.unitless,
    'precipitation_sum': Units.mm,
    'relative_humidity': Units.perc,
    'relative_humidity_max': Units.perc,
    'relative_humidity_min': Units.perc,
    'solar_radiation': Units.wm2,
    'solar_radiation_max': Units.wm2,
    'solar_radiation_min': Units.wm2,
    'utci': Units.deg_c,
    'utci_max': Units.deg_c,
    'utci_min': Units.deg_c,
    'utci_category': Units.unitless,
    'vapor_pressure': Units.hpa,
    'vapor_pressure_max': Units.hpa,
    'vapor_pressure_min': Units.hpa,
    'wet_bulb_temperature': Units.deg_c,
    'wet_bulb_temperature_max': Units.deg_c,
    'wet_bulb_temperature_min': Units.deg_c,
    'wind_direction': Units.deg,
    'wind_speed': Units.ms,
    'wind_speed_max': Units.ms,
    'wind_speed_min': Units.ms,
    'maximum_wind_speed': Units.ms,
    'maximum_wind_speed_max': Units.ms,
    'maximum_wind_speed_min': Units.ms,
}


class PublicParams(StrEnum):
    """Parameters that are publicly available and data from those parameters can be
    requested via the API. Not every station supports all of these parameters. Stations
    of type `StationType.biomet` support all parameters, stations of type
    `StationType.temprh` only support a subset of parameters, that can be
    derived from `air_temperature` and `relative_humidity`.
    """
    absolute_humidity = 'absolute_humidity'
    atmospheric_pressure = 'atmospheric_pressure'
    atmospheric_pressure_reduced = 'atmospheric_pressure_reduced'
    air_temperature = 'air_temperature'
    dew_point = 'dew_point'
    heat_index = 'heat_index'
    lightning_average_distance = 'lightning_average_distance'
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
    maximum_wind_speed = 'maximum_wind_speed'


class PublicParamsAggregates(StrEnum):
    """Parameters that are publicly available and data from those parameters can be
    requested via the API. Not every station supports all of these parameters. Stations
    of type `StationType.biomet` support all parameters, stations of type
    `StationType.temprh` only support a subset of parameters, that can be
    derived from `air_temperature` and `relative_humidity`.

    This schema also contains extreme values derived from aggregating instantaneous
    measurements across a time span (e.g. hourly or daily values).
    """
    absolute_humidity = 'absolute_humidity'
    absolute_humidity_max = 'absolute_humidity_max'
    absolute_humidity_min = 'absolute_humidity_min'
    atmospheric_pressure = 'atmospheric_pressure'
    atmospheric_pressure_max = 'atmospheric_pressure_max'
    atmospheric_pressure_min = 'atmospheric_pressure_min'
    atmospheric_pressure_reduced = 'atmospheric_pressure_reduced'
    atmospheric_pressure_reduced_max = 'atmospheric_pressure_reduced_max'
    atmospheric_pressure_reduced_min = 'atmospheric_pressure_reduced_min'
    air_temperature = 'air_temperature'
    air_temperature_max = 'air_temperature_max'
    air_temperature_min = 'air_temperature_min'
    dew_point = 'dew_point'
    dew_point_max = 'dew_point_max'
    dew_point_min = 'dew_point_min'
    heat_index = 'heat_index'
    heat_index_max = 'heat_index_max'
    heat_index_min = 'heat_index_min'
    lightning_average_distance = 'lightning_average_distance'
    lightning_average_distance_max = 'lightning_average_distance_max'
    lightning_average_distance_min = 'lightning_average_distance_min'
    lightning_strike_count = 'lightning_strike_count'
    mrt = 'mrt'
    mrt_max = 'mrt_max'
    mrt_min = 'mrt_min'
    pet = 'pet'
    pet_max = 'pet_max'
    pet_min = 'pet_min'
    pet_category = 'pet_category'
    precipitation_sum = 'precipitation_sum'
    relative_humidity = 'relative_humidity'
    relative_humidity_max = 'relative_humidity_max'
    relative_humidity_min = 'relative_humidity_min'
    solar_radiation = 'solar_radiation'
    solar_radiation_max = 'solar_radiation_max'
    solar_radiation_min = 'solar_radiation_min'
    utci = 'utci'
    utci_max = 'utci_max'
    utci_min = 'utci_min'
    utci_category = 'utci_category'
    vapor_pressure = 'vapor_pressure'
    vapor_pressure_max = 'vapor_pressure_max'
    vapor_pressure_min = 'vapor_pressure_min'
    wet_bulb_temperature = 'wet_bulb_temperature'
    wet_bulb_temperature_max = 'wet_bulb_temperature_max'
    wet_bulb_temperature_min = 'wet_bulb_temperature_min'
    wind_direction = 'wind_direction'
    wind_speed = 'wind_speed'
    wind_speed_max = 'wind_speed_max'
    wind_speed_min = 'wind_speed_min'
    maximum_wind_speed = 'maximum_wind_speed'
    maximum_wind_speed_max = 'maximum_wind_speed_max'
    maximum_wind_speed_min = 'maximum_wind_speed_min'


T = TypeVar('T')


@lru_cache(maxsize=1)
def get_current_version(prefix: str = 'v1') -> str:
    # TODO: we should first try getting the version from pyproject.toml check if this
    # corresponds to a tag or sha, if not, return the git sha, otherwise return the
    # vx.y.z format.
    version = subprocess.check_output(('git', 'rev-parse', '--short', 'HEAD'))
    return f'{prefix}-git+{version.decode().strip()}'


def timestamp() -> int:
    """return the current unix-timestamp (at UTC) in milliseconds"""
    return int(datetime.now(tz=timezone.utc).timestamp() * 1000)


class Response(BaseModel, Generic[T]):
    """Generic structure of an API response."""
    data: T = Field(
        description='array or object containing the requested data',
    )
    version: str = Field(
        default='v1.0.0',
        description=(
            'The current API version in the format of `vx.y.z` or during development '
            'v1-git+<7-digit commit sha>.'
        ),
    )
    timestamp: int = Field(
        default_factory=timestamp,
        examples=['1727319765395'],
        description=(
            'The current time as a unix timestamp in UTC. This provides precise timing '
            'information for the API-response.'
        ),
    )


class StationMetadata(BaseModel):
    """Metadata of a deployed measurement station"""
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
    """Measured or calculated parameters"""
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
        description='The category of physiological stress',
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
        description='The category of physiological stress',
    )
    vapor_pressure: float | None = Field(
        None,
        examples=[19.5],
        description='The vapor pressure in **hPa**',
    )
    wet_bulb_temperature: float | None = Field(
        None,
        examples=[13.4],
        description='The wet bulb temperature in **°C**',
    )
    wind_direction: float | None = Field(
        None,
        examples=[270.4],
        description='The wind direction in **°**',
        ge=0,
        le=360,
    )
    wind_speed: float | None = Field(
        None,
        examples=[3.8],
        description='The wind speed in **m/s**',
        ge=0,
    )
    maximum_wind_speed: float | None = Field(
        None,
        examples=[8.5],
        description=(
            'The maximum of the wind speed (gust-strength), since the last '
            'log-interval in **m/s**'
        ),
        ge=0,
    )

# TODO: we may also generate these at some point from either the enums or the basic
# schema without extreme values


class ParametersAgg(BaseModel):
    """Extreme values aggregated to a lower temporal resolution"""
    absolute_humidity_min: float | None = Field(
        None,
        examples=[11.1],
        description='The minimum absolute humidity in **g/m³**',
    )
    absolute_humidity_max: float | None = Field(
        None,
        examples=[11.9],
        description='The maximum Absolute humidity in **g/m³**',
    )
    atmospheric_pressure_min: float | None = Field(
        None,
        examples=[1013.1],
        description='The minimum atmospheric pressure at the station in **hPa**',
    )
    atmospheric_pressure_max: float | None = Field(
        None,
        examples=[1013.9],
        description='The maximum atmospheric pressure at the station in **hPa**',
    )
    atmospheric_pressure_reduced_min: float | None = Field(
        None,
        examples=[1020.9],
        description='The minimum atmospheric pressure reduced to sea-level in **hPa**',
    )
    atmospheric_pressure_reduced_max: float | None = Field(
        None,
        examples=[1020.9],
        description='The maximum atmospheric pressure reduced to sea-level in **hPa**',
    )
    air_temperature_min: float | None = Field(
        None,
        examples=[12.1],
        description='The minimum air temperature in **°C**',
    )
    air_temperature_max: float | None = Field(
        None,
        examples=[12.9],
        description='The maximum air temperature in **°C**',
    )
    dew_point_min: float | None = Field(
        None,
        examples=[7.1],
        description='The minimum dew point temperature in **°C**',
    )
    dew_point_max: float | None = Field(
        None,
        examples=[7.9],
        description='The maximum dew point temperature in **°C**',
    )
    heat_index_min: float | None = Field(
        None,
        examples=[25.1],
        description=(
            'The minimum heat index derived from relative humidity and air temperature '
            'in **°C**'
        ),
    )
    heat_index_max: float | None = Field(
        None,
        examples=[25.9],
        description=(
            'The maximum heat index derived from relative humidity and air temperature '
            'in **°C**'
        ),
    )
    lightning_average_distance_min: float | None = Field(
        None,
        examples=[3.1],
        description='The minimum distance of lightning strikes in **km**',
    )
    lightning_average_distance_max: float | None = Field(
        None,
        examples=[3.9],
        description='The maximum distance of lightning strikes in **km**',
    )
    mrt_min: float | None = Field(
        None,
        examples=[64.1],
        description='The minimum mean radiant temperature in **°C**',
    )
    mrt_max: float | None = Field(
        None,
        examples=[64.9],
        description='The maximum mean radiant temperature in **°C**',
    )
    pet_min: float | None = Field(
        None,
        examples=[35.1],
        description='The minimum physiological equivalent temperature in **°C**',
    )
    pet_max: float | None = Field(
        None,
        examples=[35.9],
        description='The maximum physiological equivalent temperature in **°C**',
    )
    relative_humidity_min: float | None = Field(
        None,
        ge=0,
        le=100,
        examples=[73.1],
        description='The minimum relative humidity in **%**',
    )
    relative_humidity_max: float | None = Field(
        None,
        ge=0,
        le=100,
        examples=[73.9],
        description='The maximum relative humidity in **%**',
    )
    solar_radiation: float | None = Field(
        None,
        examples=[860.1],
        ge=0,
        description='The minimum incoming shortwave solar radiation in **W/m²**',
    )
    solar_radiation_max: float | None = Field(
        None,
        examples=[860.9],
        ge=0,
        description='The maximum incoming shortwave solar radiation in **W/m²**',
    )
    utci_min: float | None = Field(
        None,
        examples=[38.1],
        description='The minimum universal thermal climate index in **°C**',
    )
    utci_max: float | None = Field(
        None,
        examples=[38.0],
        description='The maximum universal thermal climate index in **°C**',
    )
    vapor_pressure_min: float | None = Field(
        None,
        examples=[19.1],
        description='The minimum vapor pressure in **hPa**',
    )
    vapor_pressure_max: float | None = Field(
        None,
        examples=[19.9],
        description='The maximum vapor pressure in **hPa**',
    )
    wet_bulb_temperature_min: float | None = Field(
        None,
        examples=[13.1],
        description='The minimum wet bulb temperature in **°C**',
    )
    wet_bulb_temperature_max: float | None = Field(
        None,
        examples=[13.9],
        description='The maximum wet bulb temperature in **°C**',
    )
    wind_speed_min: float | None = Field(
        None,
        examples=[3.1],
        description='The minimum average wind speed in **m/s**',
        ge=0,
    )
    wind_speed_max: float | None = Field(
        None,
        examples=[3.9],
        description='The maximum average wind speed in **m/s**',
        ge=0,
    )


class StationParams(StationMetadata, Parameters):
    """Parameters provided by a station"""
    measured_at: datetime = Field(
        examples=[datetime(2024, 8, 28, 18, 50, 13, 169)],
        description='The exact time the value was measured in **UTC**',
    )


class DistrictParams(Parameters):
    """Parameters provided by a district"""
    district: str = Field(
        examples=['Innenstadt'],
        description='The name of the city district',
    )


class StationData(Parameters):
    """Data from a single station"""
    measured_at: datetime = Field(
        examples=[datetime(2024, 8, 28, 18, 50, 13, 169)],
        description='The exact time the value was measured in **UTC**',
    )


class StationDataAgg(ParametersAgg, Parameters):
    """Aggregated data from a single station (this contains `_min` and `_max` values)"""
    measured_at: datetime = Field(
        examples=[datetime(2024, 8, 28, 18, 50, 13, 169)],
        description='The exact time the value was measured in **UTC**',
    )


class TrendValue(RootModel[dict[str, float | datetime | HeatStressCategories | None]]):
    """Key-Value pair where the key is either the station name or the district name.
    Value can be really anything that is stored as data.
    """
    pass


class Trends(BaseModel):
    """Trends for a single or multiple stations/districts"""
    supported_ids: list[str] = Field(
        examples=[['DEC005476']],
        description='Either names of stations or names of districts.',
    )
    unit: Units = Field(
        examples=[Units.wm2],
        description='The corresponding unit of the values shown in trends',
    )
    trends: list[TrendValue] = Field(
        examples=[
            [
                TrendValue(
                    {
                        'DEC005476': 855.1,
                        'measured_at': datetime(2024, 8, 1, 10, 0, tzinfo=timezone.utc),
                    },
                ),
            ],
        ],
    )
