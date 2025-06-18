from __future__ import annotations

from collections.abc import Awaitable
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any
from typing import ClassVar
from typing import Protocol

from psycopg import sql
from sqlalchemy import BigInteger
from sqlalchemy import Computed
from sqlalchemy import Connection
from sqlalchemy import DateTime
from sqlalchemy import desc
from sqlalchemy import event
from sqlalchemy import ForeignKey
from sqlalchemy import Index
from sqlalchemy import Table
from sqlalchemy import Text
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import ENUM
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship

from app.database import Base
from app.database import sessionmanager


class StationType(StrEnum):
    """Enum differentiating between the different types of stations:

    - ``temprh``: station with a SHT35 sensor
    - ``biomet``: station with an ATM41 and a BLG sensor
    - ``double``: station with an ATM41, SHT35, and a BLG sensor
    """
    biomet = 'biomet'
    double = 'double'
    temprh = 'temprh'


class SensorType(StrEnum):
    """Enum differentiating between the different types of sensors:

    - ``atm41``: ATM41 sensor with many parameters
    - ``blg``: BLG sensor with black globe temperature
    - ``sht35``: SHT35 sensor with temperature and relative humidity
    """
    atm41 = 'atm41'
    blg = 'blg'
    sht35 = 'sht35'


class HeatStressCategories(StrEnum):
    """Enum for the different heat stress categories as defined by PET or UTCI."""
    unknown = 'unknown'
    extreme_cold_stress = 'extreme cold stress'
    very_strong_cold_stress = 'very strong cold stress'
    strong_cold_stress = 'strong cold stress'
    moderate_cold_stress = 'moderate cold stress'
    slight_cold_stress = 'slight cold stress'
    no_thermal_stress = 'no thermal stress'
    slight_heat_stress = 'slight heat stress'  # only PET has this?
    moderate_heat_stress = 'moderate heat stress'
    strong_heat_stress = 'strong heat stress'
    very_strong_heat_stress = 'very strong heat stress'
    extreme_heat_stress = 'extreme heat stress'


# this was taken and adapted from PythermalComfort
# https://www.researchgate.net/publication/233759000_Another_kind_of_environmental_stress_Thermal_stress
# http://dx.doi.org/10.1007/s00484-013-0738-8
# https://www.dwd.de/DE/leistungen/klimastatusbericht/publikationen/ksb2009_pdf/artikel11.pdf?__blob=publicationFile&v=1
# TODO (LW): validate this mapping for PET. There might be limitations depending on the
# activity. We have to make sure that this matches the Klima-Michel

PET_STRESS_CATEGORIES: dict[float, HeatStressCategories] = {
    4.0: HeatStressCategories.extreme_cold_stress,
    8.0: HeatStressCategories.strong_cold_stress,
    13.0: HeatStressCategories.moderate_cold_stress,
    18.0: HeatStressCategories.slight_cold_stress,
    23.0: HeatStressCategories.no_thermal_stress,
    29.0: HeatStressCategories.slight_heat_stress,
    35.0: HeatStressCategories.moderate_heat_stress,
    41.0: HeatStressCategories.strong_heat_stress,
    1000.0: HeatStressCategories.extreme_heat_stress,
}

UTCI_STRESS_CATEGORIES: dict[float, HeatStressCategories] = {
    -40.0: HeatStressCategories.extreme_cold_stress,
    -27.0: HeatStressCategories.very_strong_cold_stress,
    -13.0: HeatStressCategories.strong_cold_stress,
    0.0: HeatStressCategories.moderate_cold_stress,
    9.0: HeatStressCategories.slight_cold_stress,
    26.0: HeatStressCategories.no_thermal_stress,
    32.0: HeatStressCategories.moderate_heat_stress,
    38.0: HeatStressCategories.strong_heat_stress,
    46.0: HeatStressCategories.very_strong_heat_stress,
    1000.0: HeatStressCategories.extreme_heat_stress,
}

# we need this for pandas to be able to insert enums via .to_sql
_HeatStressCategories = ENUM(HeatStressCategories)


class _StationAwaitableAttrs(Protocol):
    active_sensors: Awaitable[list[Sensor]]
    former_sensors: Awaitable[list[Sensor]]
    active_deployments: Awaitable[list[SensorDeployment]]
    former_deployments: Awaitable[list[SensorDeployment]]
    deployments: Awaitable[list[SensorDeployment]]


class Station(Base):
    """Representation of a station which has a physical location and sensor(s) attached
    to it."""
    __tablename__ = 'station'

    # IDs
    station_id: Mapped[str] = mapped_column(
        Text,
        primary_key=True,
        index=True,
        doc='id of the station e.g. ``DOBNOM``',
    )
    long_name: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        doc='long name of the station e.g. ``Nordmarkt``',
    )
    station_type: Mapped[StationType] = mapped_column(
        nullable=False,
        doc='type of the station e.g. ``temprh``',
    )

    # geographical position
    latitude: Mapped[float] = mapped_column(
        nullable=False,
        doc='latitude of the station in **decimal degrees**',
    )
    longitude: Mapped[float] = mapped_column(
        nullable=False,
        doc='longitude of the station in **decimal degrees**',
    )
    altitude: Mapped[float] = mapped_column(
        nullable=False,
        doc='altitude of the station in **m a.s.l.**',
    )

    # address information
    street: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        doc='name of the street the station is located at',
    )
    number: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
        doc='if possible, the number of the closest building',
    )
    plz: Mapped[int] = mapped_column(
        nullable=False,
        doc='postal code of the station',
    )
    city: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        doc='name of the city the station is located in',
    )
    country: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        doc='name of the country the station is located in',
    )
    district: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        doc='name of the district the station is located in',
    )

    # siting information
    lcz: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
        doc='local climate zone of the station',
    )
    dominant_land_use: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
        comment='e.g. residential, commercial, industrial, ...',
        doc='dominant land use at the station',
    )
    urban_atlas_class_name: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
        doc='urban atlas class name of the station',
    )
    urban_atlas_class_nr: Mapped[int | None] = mapped_column(
        nullable=True,
        doc='urban atlas class number of the station',
    )
    orographic_setting: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
        comment='e.g. Flat, Hilly',
        doc='orographic setting of the station e.g. flat, hilly, ...',
    )
    svf: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='sky view factor of the station',
    )
    artificial_heat_sources: Mapped[str] = mapped_column(
        Text,
        nullable=True,
        comment='e.g. cars, buildings, ...',
        doc='artificial heat sources at the station',
    )
    proximity_to_building: Mapped[Decimal | None] = mapped_column(
        nullable=True,
        doc='the distance to the closest building in **m**',
    )
    proximity_to_parking: Mapped[Decimal | None] = mapped_column(
        nullable=True,
        doc='the distance to the closest parking lot in **m**',
    )
    proximity_to_tree: Mapped[Decimal | None] = mapped_column(
        nullable=True,
        doc='the distance to the closest tree in **m**',
    )
    surrounding_land_cover_description: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
        comment='a text describing the surrounding land cover',
        doc='a text describing the surrounding land cover',
    )

    # mounting information
    mounting_type: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
        comment='the structure the sensor is mounted to e.g. black mast, building, ...',
        doc='the structure the sensor is mounted to e.g. black mast, building, ...',
    )
    leuchtennummer: Mapped[int] = mapped_column(
        nullable=False,
        doc='the number of the streetlight the sensor is mounted to',
    )
    mounting_structure_material: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
        comment=(
            'The material the structure the sensor is mounted to is made of e.g. '
            'metal, wood, ...'
        ),
        doc=(
            'The material the structure the sensor is mounted to is made of e.g. '
            'metal, wood, ...'
        ),
    )
    mounting_structure_height_agl: Mapped[Decimal | None] = mapped_column(
        nullable=True,
        comment='the total height of the mounting structure above ground level',
        doc='the total height of the mounting structure above ground level',
    )
    mounting_structure_diameter: Mapped[Decimal | None] = mapped_column(
        nullable=True,
        comment='the diameter of the mounting structure at the mounting height',
        doc='the diameter of the mounting structure at the mounting height',
    )
    mounting_structure_light_extension_offset: Mapped[Decimal | None] = mapped_column(
        nullable=True,
        comment='when mounted to a lantern post, the overhang of the lantern',
        doc='when mounted to a lantern post, the overhang of the lantern',
    )
    sensor_height_agl: Mapped[Decimal | None] = mapped_column(
        nullable=True,
        comment=(
            'the mounting height of the main component of the station (ATM41 or SHT35)'
        ),
        doc=(
            'the mounting height of the main component of the station (ATM41 or SHT35)'
        ),
    )
    sensor_distance_from_mounting_structure: Mapped[Decimal | None] = mapped_column(
        Text,
        nullable=True,
        comment=(
            'the distance of the main component of the station (ATM41 or SHT35) '
            'from the mounting structure'
        ),
        doc=(
            'the distance of the main component of the station (ATM41 or SHT35) '
            'from the mounting structure'
        ),
    )
    sensor_orientation: Mapped[Decimal | None] = mapped_column(
        Text,
        nullable=True,
        comment=(
            'the orientation (-angle) of the arm of the main component of the station '
            '(ATM41 or SHT35) from the mounting structure'
        ),
        doc=(
            'the orientation (-angle) of the arm of the main component of the station '
            '(ATM41 or SHT35) from the mounting structure'
        ),
    )
    blg_sensor_height_agl: Mapped[Decimal | None] = mapped_column(
        nullable=True,
        comment='the mounting height of the black globe sensor of the station',
        doc='the mounting height of the black globe sensor of the station',
    )
    blg_sensor_distance_from_mounting_structure: Mapped[Decimal | None] = mapped_column(
        Text,
        nullable=True,
        comment=(
            'the distance of the black globe sensor of the station from the mounting '
            'structure'
        ),
        doc=(
            'the distance of the black globe sensor of the station from the mounting '
            'structure'
        ),
    )
    blg_sensor_orientation: Mapped[Decimal | None] = mapped_column(
        Text,
        nullable=True,
        comment=(
            'the orientation (-angle) of the arm of the black globe sensor of the '
            'station from the mounting structure'
        ),
        doc=(
            'the orientation (-angle) of the arm of the black globe sensor of the '
            'station from the mounting structure'
        ),
    )
    comment: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
        doc='a text describing the station',
    )

    # relationships
    awaitable_attrs: ClassVar[_StationAwaitableAttrs]  # type: ignore[assignment]
    active_sensors: Mapped[list[Sensor]] = relationship(
        'Sensor',
        secondary='sensor_deployment',
        primaryjoin=(
            'and_('
            '    Station.station_id == SensorDeployment.station_id,'
            '    SensorDeployment.teardown_date == None'
            ')'
        ),
        secondaryjoin='Sensor.sensor_id == SensorDeployment.sensor_id',
        viewonly=True,
        lazy=True,
        order_by='SensorDeployment.setup_date',
        doc='list of sensors that are currently deployed at the station',
    )
    former_sensors: Mapped[list[Sensor]] = relationship(
        'Sensor',
        secondary='sensor_deployment',
        primaryjoin=(
            'and_('
            '    Station.station_id == SensorDeployment.station_id,'
            '    SensorDeployment.teardown_date != None'
            ')'
        ),
        secondaryjoin='Sensor.sensor_id == SensorDeployment.sensor_id',
        viewonly=True,
        lazy=True,
        order_by='SensorDeployment.setup_date',
        doc='list of sensors that were previously deployed at the station',
    )
    active_deployments: Mapped[list[SensorDeployment]] = relationship(
        'SensorDeployment',
        primaryjoin=(
            'and_('
            '    Station.station_id == SensorDeployment.station_id,'
            '    SensorDeployment.teardown_date == None'
            ')'
        ),
        viewonly=True,
        lazy=True,
        order_by='SensorDeployment.setup_date',
        doc='list of deployments that are currently active at the station',
    )
    former_deployments: Mapped[list[SensorDeployment]] = relationship(
        'SensorDeployment',
        primaryjoin=(
            'and_('
            '    Station.station_id == SensorDeployment.station_id,'
            '    SensorDeployment.teardown_date != None'
            ')'
        ),
        viewonly=True,
        lazy=True,
        order_by='SensorDeployment.setup_date',
        doc='list of deployments that were previously active at the station',
    )
    deployments: Mapped[list[SensorDeployment]] = relationship(
        back_populates='station',
        lazy=True,
        order_by='SensorDeployment.setup_date, SensorDeployment.deployment_id',
        doc='list of all deployments at the station',
    )

    @property
    def full_address(self) -> str:
        """Returns the full address of the station as a string."""
        address = [
            self.street,
            f' {self.number}' if self.number else '',
            ', ',
            f'{self.plz} ',
            self.city,
            f' {self.district}',
            f', {self.country}',
        ]
        return ''.join(address)

    def __repr__(self) -> str:
        return (
            f'{type(self).__name__}('
            f'station_id={self.station_id!r}, '
            f'long_name={self.long_name!r}, '
            f'station_type={self.station_type!r}, '
            f'latitude={self.latitude!r}, '
            f'longitude={self.longitude!r}, '
            f'altitude={self.altitude!r}, '
            f'street={self.street!r}, '
            f'number={self.number!r}, '
            f'plz={self.plz!r}, '
            f'city={self.city!r}, '
            f'country={self.country!r}, '
            f'district={self.district!r}, '
            f'lcz={self.lcz!r}, '
            f'dominant_land_use={self.dominant_land_use!r}, '
            f'urban_atlas_class_name={self.urban_atlas_class_name!r}, '
            f'urban_atlas_class_nr={self.urban_atlas_class_nr!r}, '
            f'orographic_setting={self.orographic_setting!r}, '
            f'svf={self.svf!r}, '
            f'artificial_heat_sources={self.artificial_heat_sources!r}, '
            f'proximity_to_building={self.proximity_to_building!r}, '
            f'proximity_to_parking={self.proximity_to_parking!r}, '
            f'proximity_to_tree={self.proximity_to_tree!r}, '
            f'surrounding_land_cover_description={self.surrounding_land_cover_description!r}, '  # noqa: E501
            f'mounting_type={self.mounting_type!r}, '
            f'leuchtennummer={self.leuchtennummer!r}, '
            f'mounting_structure_material={self.mounting_structure_material!r}, '
            f'mounting_structure_height_agl={self.mounting_structure_height_agl!r}, '
            f'mounting_structure_diameter={self.mounting_structure_diameter!r}, '
            f'mounting_structure_light_extension_offset={self.mounting_structure_light_extension_offset!r}, '  # noqa: E501
            f'sensor_height_agl={self.sensor_height_agl!r}, '
            f'sensor_distance_from_mounting_structure={self.sensor_distance_from_mounting_structure!r}, '  # noqa: E501
            f'sensor_orientation={self.sensor_orientation!r}, '
            f'blg_sensor_height_agl={self.blg_sensor_height_agl!r}, '
            f'blg_sensor_distance_from_mounting_structure={self.blg_sensor_distance_from_mounting_structure!r}, '  # noqa: E501
            f'blg_sensor_orientation={self.blg_sensor_orientation!r}, '
            f'comment={self.comment!r}'
            f')'
        )


class _SensorDeploymentAwaitableAttrs(Protocol):
    sensor: Awaitable[Sensor]
    station: Awaitable[Station]


class SensorDeployment(Base):
    """Deployment of a sensor at a station"""
    __tablename__ = 'sensor_deployment'

    deployment_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    sensor_id: Mapped[str] = mapped_column(ForeignKey('sensor.sensor_id'))
    station_id: Mapped[str] = mapped_column(ForeignKey('station.station_id'))
    setup_date: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    teardown_date: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    awaitable_attrs: ClassVar[_SensorDeploymentAwaitableAttrs]  # type: ignore[assignment] # noqa: E501
    sensor: Mapped[Sensor] = relationship(
        'Sensor',
        back_populates='deployments',
        lazy=True,
    )
    station: Mapped[Station] = relationship(
        back_populates='deployments',
        lazy=True,
    )

    def __repr__(self) -> str:
        return (
            f'{type(self).__name__}('
            f'deployment_id={self.deployment_id!r}, '
            f'sensor_id={self.sensor_id!r}, '
            f'station_id={self.station_id!r}, '
            f'setup_date={self.setup_date!r}, '
            f'teardown_date={self.teardown_date!r} '
            f')'
        )


class Sensor(Base):
    """Pool of sensors that can be installed at a station"""
    __tablename__ = 'sensor'

    sensor_id: Mapped[str] = mapped_column(
        primary_key=True,
        doc='id of the sensor e.g. ``DEC1234``',
    )
    device_id: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
        doc='device id of the sensor e.g. ``1234567890``',
    )
    sensor_type: Mapped[SensorType] = mapped_column(
        nullable=False,
        doc='type of the sensor e.g. ``biomet``',
    )
    # calibration information
    temp_calib_offset: Mapped[Decimal] = mapped_column(
        nullable=False,
        default=0,
        server_default='0',
        doc=(
            'temperature calibration offset in **°C**. This is the offset that '
            'is applied to the temperature value before it is stored in the database'
        ),
    )
    relhum_calib_offset: Mapped[Decimal] = mapped_column(
        nullable=False,
        default=0,
        server_default='0',
        doc=(
            'relative humidity calibration offset in **%**. This is the offset that '
            'is applied to the relative humidity value before it is stored in the '
            'database'
        ),
    )

    # relationships
    deployments: Mapped[list[SensorDeployment]] = relationship(
        back_populates='sensor',
        lazy=True,
        doc='list of all deployments of the sensor',
    )
    current_station: Mapped[Station | None] = relationship(
        secondary='sensor_deployment',
        primaryjoin=(
            'and_('
            '    Sensor.sensor_id == SensorDeployment.sensor_id,'
            '    SensorDeployment.teardown_date == None'
            ')'
        ),
        secondaryjoin='Station.station_id == SensorDeployment.station_id',
        viewonly=True,
        lazy=True,
        doc='the station the sensor is currently deployed at',
    )
    former_stations: Mapped[list[Station]] = relationship(
        'Station',
        secondary='sensor_deployment',
        primaryjoin=(
            'and_('
            '    Sensor.sensor_id == SensorDeployment.sensor_id,'
            '    SensorDeployment.teardown_date != None'
            ')'
        ),
        secondaryjoin='Station.station_id == SensorDeployment.station_id',
        viewonly=True,
        lazy=True,
        doc='list of stations the sensor was previously deployed at',
    )

    def __repr__(self) -> str:
        return (
            f'{type(self).__name__}('
            f'sensor_id={self.sensor_id!r}, '
            f'device_id={self.device_id!r}, '
            f'sensor_type={self.sensor_type!r}, '
            f'temp_calib_offset={self.temp_calib_offset!r}, '
            f'relhum_calib_offset={self.relhum_calib_offset!r}'
            f')'
        )


class _RawDataAwaitableAttrs(Protocol):
    sensor: Awaitable[Sensor]


class _Data(Base):
    __abstract__ = True

    measured_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        primary_key=True,
        index=True,
        doc='The exact time the value was measured in **UTC**',
    )
    battery_voltage: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='Volts',
        doc='The battery voltage of the sensor in **Volts**',
    )
    protocol_version: Mapped[int] = mapped_column(
        nullable=True,
        doc='The protocol version the data was sent with',
    )


class _SHT35DataRawBase(_Data):
    __abstract__ = True

    air_temperature: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
        doc='air temperature in **°C**',
    )
    relative_humidity: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='%',
        doc='relative humidity in **%**',
    )


class _SHT35DataRawBaseQC(Base):
    __abstract__ = True

    air_temperature_qc_range_check: Mapped[bool] = mapped_column(nullable=True)
    air_temperature_qc_persistence_check: Mapped[bool] = mapped_column(nullable=True)
    air_temperature_qc_spike_dip_check: Mapped[bool] = mapped_column(nullable=True)
    relative_humidity_qc_range_check: Mapped[bool] = mapped_column(nullable=True)
    relative_humidity_qc_persistence_check: Mapped[bool] = mapped_column(nullable=True)
    relative_humidity_qc_spike_dip_check: Mapped[bool] = mapped_column(nullable=True)
    # create a column that unifies all qc checks for restrictive filtering
    qc_flagged: Mapped[bool] = mapped_column(
        Computed(
            '''
            air_temperature_qc_range_check IS TRUE OR
            air_temperature_qc_range_check IS NULL OR
            air_temperature_qc_persistence_check IS TRUE OR
            air_temperature_qc_persistence_check IS NULL OR
            air_temperature_qc_spike_dip_check IS TRUE OR
            air_temperature_qc_spike_dip_check IS NULL OR
            relative_humidity_qc_range_check IS TRUE OR
            relative_humidity_qc_range_check IS NULL OR
            relative_humidity_qc_persistence_check IS TRUE OR
            relative_humidity_qc_persistence_check IS NULL OR
            relative_humidity_qc_spike_dip_check IS TRUE OR
            relative_humidity_qc_spike_dip_check IS NULL
            ''',
            persisted=True,
        ),
    )


class SHT35DataRaw(_SHT35DataRawBase):
    __tablename__ = 'sht35_data_raw'
    sensor_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey('sensor.sensor_id'),
        primary_key=True,
        index=True,
        doc='id of the sensor e.g. ``DEC1234``',
    )
    sensor: Mapped[Sensor] = relationship(
        lazy=True,
        doc='The sensor the data was measured with',
    )
    awaitable_attrs: ClassVar[_RawDataAwaitableAttrs]  # type: ignore[assignment]

    def __repr__(self) -> str:
        return (
            f'{type(self).__name__}('
            f'sensor_id={self.sensor_id!r}, '
            f'measured_at={self.measured_at!r}, '
            f'air_temperature={self.air_temperature!r}, '
            f'relative_humidity={self.relative_humidity!r}, '
            f'battery_voltage={self.battery_voltage!r}, '
            f'protocol_version={self.protocol_version!r}'
            f')'
        )


class _ATM41DataRawBase(_Data):
    __abstract__ = True

    air_temperature: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
        doc='air temperature in **°C**',
    )
    relative_humidity: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='%',
        doc='relative humidity in **%**',
    )
    atmospheric_pressure: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='kPa',
        doc='atmospheric pressure in **kPa**',
    )
    vapor_pressure: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='kPa',
        doc='vapor pressure in **kPa**',
    )
    wind_speed: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='m/s',
        doc='wind speed in **m/s**',
    )
    wind_direction: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°',
        doc='wind direction in **°**',
    )
    u_wind: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='m/s',
        doc='u wind component in **m/s**',
    )
    v_wind: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='m/s',
        doc='v wind component in **m/s**',
    )
    maximum_wind_speed: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='m/s',
        doc='maximum wind speed in **m/s** (gusts)',
    )
    precipitation_sum: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='mm',
        doc='precipitation sum in **mm**',
    )
    solar_radiation: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='W/m2',
        doc='solar radiation in **W/m2**',
    )
    lightning_average_distance: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='km',
        doc='distance of lightning strikes in **km**',
    )
    lightning_strike_count: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='-',
        doc='number of lightning strikes',
    )
    sensor_temperature_internal: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
        doc='internal temperature of the sensor in **°C**',
    )
    x_orientation_angle: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°',
        doc='x-tilt angle of the sensor in **°**',
    )
    y_orientation_angle: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°',
        doc='y-tilt angle of the sensor in **°**',
    )


class _ATM41DataRawBaseQC(Base):
    __abstract__ = True

    air_temperature_qc_range_check: Mapped[bool] = mapped_column(nullable=True)
    air_temperature_qc_persistence_check: Mapped[bool] = mapped_column(nullable=True)
    air_temperature_qc_spike_dip_check: Mapped[bool] = mapped_column(nullable=True)
    relative_humidity_qc_range_check: Mapped[bool] = mapped_column(nullable=True)
    relative_humidity_qc_persistence_check: Mapped[bool] = mapped_column(nullable=True)
    relative_humidity_qc_spike_dip_check: Mapped[bool] = mapped_column(nullable=True)
    atmospheric_pressure_qc_range_check: Mapped[bool] = mapped_column(nullable=True)
    atmospheric_pressure_qc_persistence_check: Mapped[bool] = mapped_column(
        nullable=True,
    )
    atmospheric_pressure_qc_spike_dip_check: Mapped[bool] = mapped_column(nullable=True)
    wind_speed_qc_range_check: Mapped[bool] = mapped_column(nullable=True)
    wind_speed_qc_persistence_check: Mapped[bool] = mapped_column(nullable=True)
    wind_speed_qc_spike_dip_check: Mapped[bool] = mapped_column(nullable=True)
    # wind direction has no spike/dip check, because it is not a continuous value
    wind_direction_qc_range_check: Mapped[bool] = mapped_column(nullable=True)
    wind_direction_qc_persistence_check: Mapped[bool] = mapped_column(nullable=True)
    u_wind_qc_range_check: Mapped[bool] = mapped_column(nullable=True)
    u_wind_qc_persistence_check: Mapped[bool] = mapped_column(nullable=True)
    u_wind_qc_spike_dip_check: Mapped[bool] = mapped_column(nullable=True)
    v_wind_qc_range_check: Mapped[bool] = mapped_column(nullable=True)
    v_wind_qc_persistence_check: Mapped[bool] = mapped_column(nullable=True)
    v_wind_qc_spike_dip_check: Mapped[bool] = mapped_column(nullable=True)
    # maximum wind speed has no spike/dip check, because it is intentionally spiky
    maximum_wind_speed_qc_range_check: Mapped[bool] = mapped_column(nullable=True)
    maximum_wind_speed_qc_persistence_check: Mapped[bool] = mapped_column(nullable=True)
    precipitation_sum_qc_range_check: Mapped[bool] = mapped_column(nullable=True)
    precipitation_sum_qc_persistence_check: Mapped[bool] = mapped_column(nullable=True)
    precipitation_sum_qc_spike_dip_check: Mapped[bool] = mapped_column(nullable=True)
    solar_radiation_qc_range_check: Mapped[bool] = mapped_column(nullable=True)
    solar_radiation_qc_persistence_check: Mapped[bool] = mapped_column(nullable=True)
    solar_radiation_qc_spike_dip_check: Mapped[bool] = mapped_column(nullable=True)
    # lightings strikes appear suddenly hence no spike/dip check
    lightning_average_distance_qc_range_check: Mapped[bool] = mapped_column(
        nullable=True,
    )
    lightning_average_distance_qc_persistence_check: Mapped[bool] = mapped_column(
        nullable=True,
    )
    lightning_strike_count_qc_range_check: Mapped[bool] = mapped_column(nullable=True)
    lightning_strike_count_qc_persistence_check: Mapped[bool] = mapped_column(
        nullable=True,
    )
    # there is no persistence check for the orientation angles, because they are
    # not expected to change over time
    x_orientation_angle_qc_range_check: Mapped[bool] = mapped_column(nullable=True)
    x_orientation_angle_qc_spike_dip_check: Mapped[bool] = mapped_column(nullable=True)
    y_orientation_angle_qc_range_check: Mapped[bool] = mapped_column(nullable=True)
    y_orientation_angle_qc_spike_dip_check: Mapped[bool] = mapped_column(nullable=True)


class ATM41DataRaw(_ATM41DataRawBase):
    __tablename__ = 'atm41_data_raw'
    sensor_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey('sensor.sensor_id'),
        primary_key=True,
        index=True,
        doc='id of the sensor e.g. ``DEC1234``',
    )
    sensor: Mapped[Sensor] = relationship(
        lazy=True,
        doc='The sensor the data was measured with',
    )
    awaitable_attrs: ClassVar[_RawDataAwaitableAttrs]  # type: ignore[assignment]


class _BLGDataRawBase(_Data):
    __abstract__ = True
    black_globe_temperature: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
        doc='black globe temperature in **°C**',
    )
    thermistor_resistance: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='Ohms',
        doc='thermistor resistance in **Ohms**',
    )
    voltage_ratio: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='-',
        doc='voltage ratio of the sensor',
    )


class _BLGDataRawBaseQC(Base):
    __abstract__ = True

    black_globe_temperature_qc_range_check: Mapped[bool] = mapped_column(nullable=True)
    black_globe_temperature_qc_persistence_check: Mapped[bool] = mapped_column(
        nullable=True,
    )
    black_globe_temperature_qc_spike_dip_check: Mapped[bool] = mapped_column(
        nullable=True,
    )


class BLGDataRaw(_BLGDataRawBase):
    __tablename__ = 'blg_data_raw'
    sensor_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey('sensor.sensor_id'),
        primary_key=True,
        index=True,
        doc='id of the sensor e.g. ``DEC1234``',
    )
    awaitable_attrs: ClassVar[_RawDataAwaitableAttrs]  # type: ignore[assignment]
    sensor: Mapped[Sensor] = relationship(
        lazy=True,
        doc='The sensor the data was measured with',
    )


class _TempRHDerivatives(Base):
    __abstract__ = True
    dew_point: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
        doc=(
            'dew point temperature in **°C** calculated using '
            ':func:`thermal_comfort.dew_point`'
        ),
    )
    absolute_humidity: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='g/m3',
        doc=(
            'absolute humidity in **g/m3** calculated using '
            ':func:`thermal_comfort.absolute_humidity`'
        ),
    )
    specific_humidity: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='g/kg',
        doc=(
            'specific humidity in **g/kg** calculated using '
            ':func:`thermal_comfort.specific_humidity`'
        ),
    )
    heat_index: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
        doc=(
            'heat index in **°C** calculated using '
            ':func:`thermal_comfort.heat_index_extended`'
        ),
    )
    wet_bulb_temperature: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
        doc=(
            'wet bulb temperature in **°C** calculated using '
            ':func:`thermal_comfort.wet_bulb_temp`'
        ),
    )


class _BiometDerivatives(Base):
    __abstract__ = True
    blg_time_offset: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='seconds',
        doc=(
            'time offset of the Blackglobe sensor to the corresponding ATM41 sensor '
            'in **seconds**'
        ),
    )
    mrt: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
        doc=(
            'mean radiant temperature in **°C** calculated using '
            ':func:`thermal_comfort.mean_radiant_temp`'
        ),
    )
    utci: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
        doc=(
            'universal thermal climate index in **°C** calculated using '
            ':func:`thermal_comfort.utci_approx`'
        ),
    )
    utci_category: Mapped[HeatStressCategories] = mapped_column(
        nullable=True,
        doc=(
            'universal thermal climate index category derived from '
            ':const:`UTCI_STRESS_CATEGORIES` and applied using '
            ':func:`app.tasks.category_mapping`'
        ),
    )
    pet: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
        doc=(
            'physiological equivalent temperature in **°C** calculated using '
            ':func:`thermal_comfort.pet_static`'
        ),
    )
    pet_category: Mapped[HeatStressCategories] = mapped_column(
        nullable=True,
        doc=(
            'physiological equivalent temperature category derived from '
            ':const:`PET_STRESS_CATEGORIES` and applied using '
            ':func:`app.tasks.category_mapping`'
        ),
    )
    # we've converted it to hPa in the meantime
    atmospheric_pressure: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='hPa',
        doc='atmospheric pressure in **hPa**',
    )
    atmospheric_pressure_reduced: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='hPa',
        doc=(
            'atmospheric pressure reduced to sea level in **hPa** calculated using '
            ':func:`app.tasks.reduce_pressure`'
        ),
    )
    vapor_pressure: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='hPa',
        doc='vapor pressure in **hPa**',
    )
    # we need this as an alias in the big biomet table
    blg_battery_voltage: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='V',
        doc='battery voltage of the black globe sensor in **Volts**',
    )


class _CalibrationDerivatives(Base):
    __abstract__ = True
    air_temperature_raw: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
        doc='raw air temperature in **°C** with no calibration applied',
    )
    relative_humidity_raw: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='%',
        doc='raw relative humidity in **%** with no calibration applied',
    )


class _BiometDataAwaitableAttrs(Protocol):
    station: Awaitable[Station]
    sensor: Awaitable[Sensor]
    blg_sensor: Awaitable[Sensor | None]
    deployments: Awaitable[list[SensorDeployment]]


class BiometData(
    _ATM41DataRawBase, _BLGDataRawBase, _TempRHDerivatives, _BiometDerivatives,
    _ATM41DataRawBaseQC, _BLGDataRawBaseQC,
):
    __tablename__ = 'biomet_data'
    __table_args__ = (
        Index(
            'ix_biomet_data_station_id_measured_at_desc',
            'station_id',
            desc('measured_at'),
        ),
    )
    station_id: Mapped[str] = mapped_column(
        ForeignKey('station.station_id'),
        primary_key=True,
        doc='id of the station these measurements were taken at',
    )
    sensor_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey('sensor.sensor_id'),
        index=True,
        doc='id of the ATM41 sensor these measurements were taken with',
    )
    blg_sensor_id: Mapped[str | None] = mapped_column(
        Text,
        ForeignKey('sensor.sensor_id'),
        index=True,
        # this needs to be nullable, since we may have measurements of the ATM41 sensor
        # that do not have corresponding blackglobe measurements
        nullable=True,
        doc='id of the BLG sensor these measurements were taken with',
    )
    # create a column that unifies all qc checks for restrictive filtering
    qc_flagged: Mapped[bool] = mapped_column(
        Computed(
            '''
            air_temperature_qc_range_check IS TRUE OR
            air_temperature_qc_range_check IS NULL OR
            air_temperature_qc_persistence_check IS TRUE OR
            air_temperature_qc_persistence_check IS NULL OR
            air_temperature_qc_spike_dip_check IS TRUE OR
            air_temperature_qc_spike_dip_check IS NULL OR
            relative_humidity_qc_range_check IS TRUE OR
            relative_humidity_qc_range_check IS NULL OR
            relative_humidity_qc_persistence_check IS TRUE OR
            relative_humidity_qc_persistence_check IS NULL OR
            relative_humidity_qc_spike_dip_check IS TRUE OR
            relative_humidity_qc_spike_dip_check IS NULL OR
            atmospheric_pressure_qc_range_check IS TRUE OR
            atmospheric_pressure_qc_range_check IS NULL OR
            atmospheric_pressure_qc_persistence_check IS TRUE OR
            atmospheric_pressure_qc_persistence_check IS NULL OR
            atmospheric_pressure_qc_spike_dip_check IS TRUE OR
            atmospheric_pressure_qc_spike_dip_check IS NULL OR
            wind_speed_qc_range_check IS TRUE OR
            wind_speed_qc_range_check IS NULL OR
            wind_speed_qc_persistence_check IS TRUE OR
            wind_speed_qc_persistence_check IS NULL OR
            wind_speed_qc_spike_dip_check IS TRUE OR
            wind_speed_qc_spike_dip_check IS NULL OR
            wind_direction_qc_range_check IS TRUE OR
            wind_direction_qc_range_check IS NULL OR
            wind_direction_qc_persistence_check IS TRUE OR
            wind_direction_qc_persistence_check IS NULL OR
            u_wind_qc_range_check IS TRUE OR
            u_wind_qc_range_check IS NULL OR
            u_wind_qc_persistence_check IS TRUE OR
            u_wind_qc_persistence_check IS NULL OR
            u_wind_qc_spike_dip_check IS TRUE OR
            u_wind_qc_spike_dip_check IS NULL OR
            v_wind_qc_range_check IS TRUE OR
            v_wind_qc_range_check IS NULL OR
            v_wind_qc_persistence_check IS TRUE OR
            v_wind_qc_persistence_check IS NULL OR
            v_wind_qc_spike_dip_check IS TRUE OR
            v_wind_qc_spike_dip_check IS NULL OR
            maximum_wind_speed_qc_range_check IS TRUE OR
            maximum_wind_speed_qc_range_check IS NULL OR
            maximum_wind_speed_qc_persistence_check IS TRUE OR
            maximum_wind_speed_qc_persistence_check IS NULL OR
            precipitation_sum_qc_range_check IS TRUE OR
            precipitation_sum_qc_range_check IS NULL OR
            precipitation_sum_qc_persistence_check IS TRUE OR
            precipitation_sum_qc_persistence_check IS NULL OR
            precipitation_sum_qc_spike_dip_check IS TRUE OR
            precipitation_sum_qc_spike_dip_check IS NULL OR
            solar_radiation_qc_range_check IS TRUE OR
            solar_radiation_qc_range_check IS NULL OR
            solar_radiation_qc_persistence_check IS TRUE OR
            solar_radiation_qc_persistence_check IS NULL OR
            solar_radiation_qc_spike_dip_check IS TRUE OR
            solar_radiation_qc_spike_dip_check IS NULL OR
            lightning_average_distance_qc_range_check IS TRUE OR
            lightning_average_distance_qc_range_check IS NULL OR
            lightning_average_distance_qc_persistence_check IS TRUE OR
            lightning_average_distance_qc_persistence_check IS NULL OR
            lightning_strike_count_qc_range_check IS TRUE OR
            lightning_strike_count_qc_range_check IS NULL OR
            lightning_strike_count_qc_persistence_check IS TRUE OR
            lightning_strike_count_qc_persistence_check IS NULL OR
            x_orientation_angle_qc_range_check IS TRUE OR
            x_orientation_angle_qc_range_check IS NULL OR
            x_orientation_angle_qc_spike_dip_check IS TRUE OR
            x_orientation_angle_qc_spike_dip_check IS NULL OR
            y_orientation_angle_qc_range_check IS TRUE OR
            y_orientation_angle_qc_range_check IS NULL OR
            y_orientation_angle_qc_spike_dip_check IS TRUE OR
            y_orientation_angle_qc_spike_dip_check IS NULL OR
            black_globe_temperature_qc_range_check IS TRUE OR
            black_globe_temperature_qc_range_check IS NULL OR
            black_globe_temperature_qc_persistence_check IS TRUE OR
            black_globe_temperature_qc_persistence_check IS NULL OR
            black_globe_temperature_qc_spike_dip_check IS TRUE OR
            black_globe_temperature_qc_spike_dip_check IS NULL
            ''',
            persisted=True,
        ),
    )

    awaitable_attrs: ClassVar[_BiometDataAwaitableAttrs]  # type: ignore[assignment]
    station: Mapped[Station] = relationship(
        lazy=True,
        doc='The station the data was measured at',
    )
    sensor: Mapped[Sensor] = relationship(
        # this should only ever be a biomet sensor, but just to make sure!
        primaryjoin='and_(BiometData.sensor_id == Sensor.sensor_id, Sensor.sensor_type == "atm41")',  # noqa: E501
        viewonly=True,
        lazy=True,
        doc='The sensor the data was measured with',
    )
    blg_sensor: Mapped[Sensor | None] = relationship(
        primaryjoin='and_(BiometData.blg_sensor_id == Sensor.sensor_id, Sensor.sensor_type == "blg")',  # noqa: E501
        viewonly=True,
        lazy=True,
        doc='The black globe sensor the data was measured with',
    )

    deployments: Mapped[list[SensorDeployment]] = relationship(
        SensorDeployment,
        primaryjoin=(
            '(BiometData.station_id == foreign(SensorDeployment.station_id)) &'
            '('
            '    (BiometData.measured_at.between(SensorDeployment.setup_date, SensorDeployment.teardown_date))'  # noqa: E501
            '    |'
            '    ((SensorDeployment.setup_date <= BiometData.measured_at) & SensorDeployment.teardown_date.is_(None))'  # noqa: E501
            ')'
        ),
        order_by=SensorDeployment.deployment_id,
        lazy=True,
        viewonly=True,
        doc='list of deployments that were involved in the measurement of this data',
    )


class _TempRHDataAwaitableAttrs(Protocol):
    station: Awaitable[Station]
    sensor: Awaitable[Sensor]
    deployment: Awaitable[SensorDeployment]


class TempRHData(
    _SHT35DataRawBase, _TempRHDerivatives, _CalibrationDerivatives, _SHT35DataRawBaseQC,
):
    __tablename__ = 'temp_rh_data'
    __table_args__ = (
        Index(
            'ix_temp_rh_data_station_id_measured_at_desc',
            'station_id',
            desc('measured_at'),
        ),
    )
    station_id: Mapped[str] = mapped_column(
        ForeignKey('station.station_id'),
        primary_key=True,
        doc='id of the station these measurements were taken at',
    )
    sensor_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey('sensor.sensor_id'),
        index=True,
        doc='id of the SHT35 sensor these measurements were taken with',
    )
    awaitable_attrs: ClassVar[_TempRHDataAwaitableAttrs]  # type: ignore[assignment]
    station: Mapped[Station] = relationship(
        lazy=True,
        doc='The station the data was measured at',
    )
    sensor: Mapped[Sensor] = relationship(
        lazy=True,
        doc='The sensor the data was measured with',
    )

    deployment: Mapped[SensorDeployment] = relationship(
        SensorDeployment,
        primaryjoin=(
            '(TempRHData.station_id == foreign(SensorDeployment.station_id)) &'
            '('
            '    (TempRHData.measured_at.between(SensorDeployment.setup_date, SensorDeployment.teardown_date))'  # noqa: E501
            '    |'
            '    ((SensorDeployment.setup_date <= TempRHData.measured_at) & SensorDeployment.teardown_date.is_(None))'  # noqa: E501
            ')'
        ),
        lazy=True,
        viewonly=True,
        doc='the deployment that made the measurement of this data',
    )


class _BuddyCheckQcBase(Base):
    __abstract__ = True

    air_temperature_qc_isolated_check: Mapped[bool] = mapped_column(
        nullable=True,
        doc='quality control for the air temperature using an isolation check',
    )
    air_temperature_qc_buddy_check: Mapped[bool] = mapped_column(
        nullable=True,
        doc='quality control for the air temperature using a buddy check',
    )
    relative_humidity_qc_isolated_check: Mapped[bool] = mapped_column(
        nullable=True,
        doc='quality control for the relative humidity using an isolation check',
    )
    relative_humidity_qc_buddy_check: Mapped[bool] = mapped_column(
        nullable=True,
        doc='quality control for the relative humidity using a buddy check',
    )
    atmospheric_pressure_qc_isolated_check: Mapped[bool] = mapped_column(
        nullable=True,
        doc='quality control for the atmospheric pressure using an isolation check',
    )
    atmospheric_pressure_qc_buddy_check: Mapped[bool] = mapped_column(
        nullable=True,
        doc='quality control for the atmospheric pressure using a buddy check',
    )


class BuddyCheckQc(_BuddyCheckQcBase):
    """The quality control flags returned by the buddy check for a station."""
    __tablename__ = 'buddy_check_qc'
    __table_args__ = (
        Index(
            'ix_buddy_check_qc_station_id_measured_at',
            'station_id',
            'measured_at',
            unique=True,
        ),
    )
    measured_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        primary_key=True,
        index=True,
        doc='The exact time the value was measured in **UTC**',
    )
    station_id: Mapped[str] = mapped_column(
        ForeignKey('station.station_id'),
        primary_key=True,
        index=True,
        doc='id of the station these measurements were taken at',
    )


class _ViewAwaitableAttrs(Protocol):
    station: Awaitable[Station]


class MaterializedView(Base):
    """Baseclass for a materialized view"""
    __abstract__ = True
    # is this a timescale continuous aggregate?
    is_continuous_aggregate = False

    station_id: Mapped[str] = mapped_column(
        ForeignKey(
            'station.station_id',
        ),
        primary_key=True,
        index=True,
        doc='id of the station these measurements were taken at',
    )
    awaitable_attrs: ClassVar[_ViewAwaitableAttrs]  # type: ignore[assignment]

    @classmethod
    async def refresh(
            cls,
            *,
            concurrently: bool = True,
            window_start: datetime | None = None,
            window_end: datetime | None = None,
    ) -> None:
        """Refresh the materialized view.

        This takes into account whether the view is a continuous aggregate or a vanilla
        postgres materialized view. This needs to be done outside of a transaction.

        :param concurrently: Whether to refresh the view concurrently. Refreshing
            concurrently is slower, however no exclusive lock is acquired. This way
            reads to the view can still be performed. This only applies to vanilla
            postgres materialized views.
        :param window_start: The start of the window that will be refreshed. This only
            applies to continuous aggregates.
        :param window_end: The end of the window that will be refreshed. This only
            applies to continuous aggregates.
        """
        async with sessionmanager.connect(as_transaction=False) as sess:
            if cls.is_continuous_aggregate is False:
                # vanilla postgres
                if concurrently is True:
                    query = sql.SQL(
                        'REFRESH MATERIALIZED VIEW CONCURRENTLY {name}',
                    ).format(name=sql.Identifier(cls.__tablename__)).as_string()
                else:
                    query = sql.SQL('REFRESH MATERIALIZED VIEW {name}').format(
                        name=sql.Identifier(cls.__tablename__),
                    ).as_string()
            else:
                # timescale
                query = sql.SQL(
                    'CALL refresh_continuous_aggregate({name}, {start}, {end})',
                ).format(
                    name=cls.__tablename__,
                    start=window_start,
                    end=window_end,
                ).as_string()

            await sess.execute(text(query))


class LatestData(
    MaterializedView,
    _ATM41DataRawBase,
    _BLGDataRawBase,
    _TempRHDerivatives,
    _ATM41DataRawBaseQC,
    _BLGDataRawBaseQC,
    _SHT35DataRawBaseQC,
    _BuddyCheckQcBase,
):
    """This is not an actual table, but a materialized view. We simply trick sqlalchemy
    into thinking this was a table. Querying a materialized view does not differ from
    querying a proper table.

    The query for creating this materialized view is saved above.
    """
    __tablename__ = 'latest_data'

    station_id: Mapped[str] = mapped_column(
        ForeignKey('station.station_id'),
        nullable=False,
        unique=True,
        index=True,
        doc=Station.station_id.doc,
    )
    long_name: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        doc=Station.long_name.doc,
    )
    latitude: Mapped[float] = mapped_column(nullable=False, doc=Station.latitude.doc)
    longitude: Mapped[float] = mapped_column(nullable=False, doc=Station.longitude.doc)
    altitude: Mapped[float] = mapped_column(nullable=False, doc=Station.altitude.doc)
    district: Mapped[str] = mapped_column(
        Text,
        nullable=True,
        index=True,
        doc=Station.district.doc,
    )
    lcz: Mapped[str] = mapped_column(Text, nullable=True, doc=Station.lcz.doc)
    station_type: Mapped[StationType] = mapped_column(
        nullable=False,
        doc=Station.station_type.doc,
    )
    mrt: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
        doc=BiometData.mrt.doc,
    )
    utci: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
        doc=BiometData.utci.doc,
    )
    utci_category: Mapped[HeatStressCategories] = mapped_column(
        nullable=True,
        doc=BiometData.utci_category.doc,
    )
    pet: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
        doc=BiometData.pet.doc,
    )
    pet_category: Mapped[HeatStressCategories] = mapped_column(
        nullable=True,
        doc=BiometData.pet_category.doc,
    )
    # we've converted it to hPa in the meantime
    atmospheric_pressure: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='hPa',
        doc=BiometData.atmospheric_pressure.doc,
    )
    atmospheric_pressure_reduced: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='hPa',
        doc=BiometData.atmospheric_pressure_reduced.doc,
    )
    vapor_pressure: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='hPa',
        doc=BiometData.vapor_pressure.doc,
    )

    station: Mapped[Station] = relationship(
        lazy=True,
        doc='The station the data was measured at',
    )

    # we exclude the temprh part of a double station here and only use the biomet part
    creation_sql = text('''\
    CREATE MATERIALIZED VIEW IF NOT EXISTS latest_data AS
    (
        SELECT DISTINCT ON (station_id)
            biomet_data.station_id,
            long_name,
            latitude,
            longitude,
            altitude,
            district,
            lcz,
            station_type,
            biomet_data.measured_at,
            air_temperature,
            relative_humidity,
            dew_point,
            absolute_humidity,
            specific_humidity,
            heat_index,
            wet_bulb_temperature,
            atmospheric_pressure,
            atmospheric_pressure_reduced,
            lightning_average_distance,
            lightning_strike_count,
            mrt,
            pet,
            pet_category,
            precipitation_sum,
            solar_radiation,
            utci,
            utci_category,
            vapor_pressure,
            wind_direction,
            wind_speed,
            maximum_wind_speed,
            u_wind,
            v_wind,
            sensor_temperature_internal,
            x_orientation_angle,
            y_orientation_angle,
            black_globe_temperature,
            thermistor_resistance,
            voltage_ratio,
            air_temperature_qc_range_check,
            air_temperature_qc_persistence_check,
            air_temperature_qc_spike_dip_check,
            relative_humidity_qc_range_check,
            relative_humidity_qc_persistence_check,
            relative_humidity_qc_spike_dip_check,
            atmospheric_pressure_qc_range_check,
            atmospheric_pressure_qc_persistence_check,
            atmospheric_pressure_qc_spike_dip_check,
            wind_speed_qc_range_check,
            wind_speed_qc_persistence_check,
            wind_speed_qc_spike_dip_check,
            wind_direction_qc_range_check,
            wind_direction_qc_persistence_check,
            u_wind_qc_range_check,
            u_wind_qc_persistence_check,
            u_wind_qc_spike_dip_check,
            v_wind_qc_range_check,
            v_wind_qc_persistence_check,
            v_wind_qc_spike_dip_check,
            maximum_wind_speed_qc_range_check,
            maximum_wind_speed_qc_persistence_check,
            precipitation_sum_qc_range_check,
            precipitation_sum_qc_persistence_check,
            precipitation_sum_qc_spike_dip_check,
            solar_radiation_qc_range_check,
            solar_radiation_qc_persistence_check,
            solar_radiation_qc_spike_dip_check,
            lightning_average_distance_qc_range_check,
            lightning_average_distance_qc_persistence_check,
            lightning_strike_count_qc_range_check,
            lightning_strike_count_qc_persistence_check,
            x_orientation_angle_qc_range_check,
            x_orientation_angle_qc_spike_dip_check,
            y_orientation_angle_qc_range_check,
            y_orientation_angle_qc_spike_dip_check,
            black_globe_temperature_qc_range_check,
            black_globe_temperature_qc_persistence_check,
            black_globe_temperature_qc_spike_dip_check,
            qc_flagged,
            air_temperature_qc_isolated_check,
            air_temperature_qc_buddy_check,
            relative_humidity_qc_isolated_check,
            relative_humidity_qc_buddy_check,
            atmospheric_pressure_qc_isolated_check,
            atmospheric_pressure_qc_buddy_check,
            battery_voltage,
            protocol_version
        FROM biomet_data
            INNER JOIN station ON biomet_data.station_id = station.station_id
            LEFT OUTER JOIN buddy_check_qc ON (
                biomet_data.station_id = buddy_check_qc.station_id AND
                biomet_data.measured_at = buddy_check_qc.measured_at
            )
        ORDER BY biomet_data.station_id, biomet_data.measured_at DESC
    )
    UNION ALL
    (
        SELECT DISTINCT ON (station_id)
            temp_rh_data.station_id,
            long_name,
            latitude,
            longitude,
            altitude,
            district,
            lcz,
            station_type,
            temp_rh_data.measured_at,
            air_temperature,
            relative_humidity,
            dew_point,
            absolute_humidity,
            specific_humidity,
            heat_index,
            wet_bulb_temperature,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            air_temperature_qc_range_check,
            air_temperature_qc_persistence_check,
            air_temperature_qc_spike_dip_check,
            relative_humidity_qc_range_check,
            relative_humidity_qc_persistence_check,
            relative_humidity_qc_spike_dip_check,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            qc_flagged,
            air_temperature_qc_isolated_check,
            air_temperature_qc_buddy_check,
            relative_humidity_qc_isolated_check,
            relative_humidity_qc_buddy_check,
            NULL,
            NULL,
            battery_voltage,
            protocol_version
        FROM temp_rh_data
            INNER JOIN station ON temp_rh_data.station_id = station.station_id
            LEFT OUTER JOIN buddy_check_qc ON (
                temp_rh_data.station_id = buddy_check_qc.station_id AND
                temp_rh_data.measured_at = buddy_check_qc.measured_at
            )
        WHERE station.station_type <> 'double'
        ORDER BY temp_rh_data.station_id, temp_rh_data.measured_at DESC
    )
    ''')

    def __repr__(self) -> str:
        return (
            f'{type(self).__name__}('
            f'station_id={self.station_id!r}, '
            f'long_name={self.long_name!r}, '
            f'latitude={self.latitude!r}, '
            f'longitude={self.longitude!r}, '
            f'altitude={self.altitude!r}, '
            f'district={self.district!r}, '
            f'lcz={self.lcz!r}, '
            f'station_type={self.station_type!r}, '
            f'measured_at={self.measured_at!r}, '
            f'air_temperature={self.air_temperature!r}, '
            f'relative_humidity={self.relative_humidity!r}, '
            f'dew_point={self.dew_point!r}, '
            f'absolute_humidity={self.absolute_humidity!r}, '
            f'heat_index={self.heat_index!r}, '
            f'wet_bulb_temperature={self.wet_bulb_temperature!r}, '
            f'atmospheric_pressure={self.atmospheric_pressure!r}, '
            f'atmospheric_pressure_reduced={self.atmospheric_pressure_reduced!r}, '
            f'lightning_average_distance={self.lightning_average_distance!r}, '
            f'lightning_strike_count={self.lightning_strike_count!r}, '
            f'mrt={self.mrt!r}, '
            f'pet={self.pet!r}, '
            f'pet_category={self.pet_category!r}, '
            f'precipitation_sum={self.precipitation_sum!r}, '
            f'solar_radiation={self.solar_radiation!r}, '
            f'utci={self.utci!r}, '
            f'utci_category={self.utci_category!r}, '
            f'vapor_pressure={self.vapor_pressure!r}, '
            f'wind_direction={self.wind_direction!r}, '
            f'wind_speed={self.wind_speed!r}, '
            f'maximum_wind_speed={self.maximum_wind_speed!r}, '
            f'u_wind={self.u_wind!r}, '
            f'v_wind={self.v_wind!r}, '
            f'sensor_temperature_internal={self.sensor_temperature_internal!r}, '
            f'x_orientation_angle={self.x_orientation_angle!r}, '
            f'y_orientation_angle={self.y_orientation_angle!r}, '
            f'black_globe_temperature={self.black_globe_temperature!r}, '
            f'thermistor_resistance={self.thermistor_resistance!r}, '
            f'voltage_ratio={self.voltage_ratio!r}, '
            f'air_temperature_qc_range_check={self.air_temperature_qc_range_check!r}, '
            f'air_temperature_qc_persistence_check={self.air_temperature_qc_persistence_check!r}, '  # noqa: E501
            f'air_temperature_qc_spike_dip_check={self.air_temperature_qc_spike_dip_check!r}, '  # noqa: E501
            f'relative_humidity_qc_range_check={self.relative_humidity_qc_range_check!r}, '  # noqa: E501
            f'relative_humidity_qc_persistence_check={self.relative_humidity_qc_persistence_check!r}, '  # noqa: E501
            f'relative_humidity_qc_spike_dip_check={self.relative_humidity_qc_spike_dip_check!r}, '  # noqa: E501
            f'atmospheric_pressure_qc_range_check={self.atmospheric_pressure_qc_range_check!r}, '  # noqa: E501
            f'atmospheric_pressure_qc_persistence_check={self.atmospheric_pressure_qc_persistence_check!r}, '  # noqa: E501
            f'atmospheric_pressure_qc_spike_dip_check={self.atmospheric_pressure_qc_spike_dip_check!r}, '  # noqa: E501
            f'wind_speed_qc_range_check={self.wind_speed_qc_range_check!r}, '
            f'wind_speed_qc_persistence_check={self.wind_speed_qc_persistence_check!r}, '  # noqa: E501
            f'wind_speed_qc_spike_dip_check={self.wind_speed_qc_spike_dip_check!r}, '
            f'wind_direction_qc_range_check={self.wind_direction_qc_range_check!r}, '
            f'wind_direction_qc_persistence_check={self.wind_direction_qc_persistence_check!r}, '  # noqa: E501
            f'u_wind_qc_range_check={self.u_wind_qc_range_check!r}, '
            f'u_wind_qc_persistence_check={self.u_wind_qc_persistence_check!r}, '
            f'u_wind_qc_spike_dip_check={self.u_wind_qc_spike_dip_check!r}, '
            f'v_wind_qc_range_check={self.v_wind_qc_range_check!r}, '
            f'v_wind_qc_persistence_check={self.v_wind_qc_persistence_check!r}, '
            f'v_wind_qc_spike_dip_check={self.v_wind_qc_spike_dip_check!r}, '
            f'maximum_wind_speed_qc_range_check={self.maximum_wind_speed_qc_range_check!r}, '  # noqa: E501
            f'maximum_wind_speed_qc_persistence_check={self.maximum_wind_speed_qc_persistence_check!r}, '  # noqa: E501
            f'precipitation_sum_qc_range_check={self.precipitation_sum_qc_range_check!r}, '  # noqa: E501
            f'precipitation_sum_qc_persistence_check={self.precipitation_sum_qc_persistence_check!r}, '  # noqa: E501
            f'precipitation_sum_qc_spike_dip_check={self.precipitation_sum_qc_spike_dip_check!r}, '  # noqa: E501
            f'solar_radiation_qc_range_check={self.solar_radiation_qc_range_check!r}, '  # noqa: E501
            f'solar_radiation_qc_persistence_check={self.solar_radiation_qc_persistence_check!r}, '  # noqa: E501
            f'solar_radiation_qc_spike_dip_check={self.solar_radiation_qc_spike_dip_check!r}, '  # noqa: E501
            f'lightning_average_distance_qc_range_check={self.lightning_average_distance_qc_range_check!r}, '  # noqa: E501
            f'lightning_average_distance_qc_persistence_check={self.lightning_average_distance_qc_persistence_check!r}, '  # noqa: E501
            f'lightning_strike_count_qc_range_check={self.lightning_strike_count_qc_range_check!r}, '  # noqa: E501
            f'lightning_strike_count_qc_persistence_check={self.lightning_strike_count_qc_persistence_check!r}, '  # noqa: E501
            f'x_orientation_angle_qc_range_check={self.x_orientation_angle_qc_range_check!r}, '  # noqa: E501
            f'x_orientation_angle_qc_spike_dip_check={self.x_orientation_angle_qc_spike_dip_check!r}, '  # noqa: E501
            f'y_orientation_angle_qc_range_check={self.y_orientation_angle_qc_range_check!r}, '  # noqa: E501
            f'y_orientation_angle_qc_spike_dip_check={self.y_orientation_angle_qc_spike_dip_check!r}, '  # noqa: E501
            f'qc_flagged={self.qc_flagged!r}, '
            f'air_temperature_qc_isolated_check{self.air_temperature_qc_isolated_check!r}, '  # noqa: E501
            f'air_temperature_qc_buddy_check{self.air_temperature_qc_buddy_check!r}, '
            f'relative_humidity_qc_isolated_check{self.relative_humidity_qc_isolated_check!r}, '  # noqa: E501
            f'relative_humidity_qc_buddy_check{self.relative_humidity_qc_buddy_check!r}, '  # noqa: E501
            f'atmospheric_pressure_qc_isolated_check{self.atmospheric_pressure_qc_isolated_check!r}, '  # noqa: E501
            f'atmospheric_pressure_qc_buddy_check{self.atmospheric_pressure_qc_buddy_check!r}, '  # noqa: E501
            f'battery_voltage={self.battery_voltage!r}, '
            f'protocol_version={self.protocol_version!r}'
            f')'
        )


# START_GENERATED
class BiometDataHourly(
    MaterializedView,
    _ATM41DataRawBase,
    _BLGDataRawBase,
    _TempRHDerivatives,
    _BiometDerivatives,
):
    """This is not an actual table, but a materialized view. We simply trick sqlalchemy
    into thinking this was a table. Querying a materialized view does not differ from
    querying a proper table.
    """
    __tablename__ = 'biomet_data_hourly'
    __table_args__ = (
        Index(
            'ix_biomet_data_hourly_station_id_measured_at',
            'station_id',
            'measured_at',
            unique=True,
        ),
    )

    absolute_humidity_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='g/m3',
        doc='minimum of absolute humidity in **g/m3** calculated using :func:`thermal_comfort.absolute_humidity`',  # noqa: E501,
    )
    absolute_humidity_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='g/m3',
        doc='maximum of absolute humidity in **g/m3** calculated using :func:`thermal_comfort.absolute_humidity`',  # noqa: E501,
    )
    air_temperature_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
        doc='minimum of air temperature in **°C**',
    )
    air_temperature_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
        doc='maximum of air temperature in **°C**',
    )
    atmospheric_pressure_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='kPa',
        doc='minimum of atmospheric pressure in **kPa**',
    )
    atmospheric_pressure_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='kPa',
        doc='maximum of atmospheric pressure in **kPa**',
    )
    atmospheric_pressure_reduced_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='hPa',
        doc='minimum of atmospheric pressure reduced to sea level in **hPa** calculated using :func:`app.tasks.reduce_pressure`',  # noqa: E501,
    )
    atmospheric_pressure_reduced_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='hPa',
        doc='maximum of atmospheric pressure reduced to sea level in **hPa** calculated using :func:`app.tasks.reduce_pressure`',  # noqa: E501,
    )
    battery_voltage_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='Volts',
        doc='minimum of The battery voltage of the sensor in **Volts**',
    )
    battery_voltage_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='Volts',
        doc='maximum of The battery voltage of the sensor in **Volts**',
    )
    black_globe_temperature_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
        doc='minimum of black globe temperature in **°C**',
    )
    black_globe_temperature_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
        doc='maximum of black globe temperature in **°C**',
    )
    blg_battery_voltage_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='V',
        doc='minimum of battery voltage of the black globe sensor in **Volts**',
    )
    blg_battery_voltage_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='V',
        doc='maximum of battery voltage of the black globe sensor in **Volts**',
    )
    blg_time_offset_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='seconds',
        doc='minimum of time offset of the Blackglobe sensor to the corresponding ATM41 sensor in **seconds**',  # noqa: E501,
    )
    blg_time_offset_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='seconds',
        doc='maximum of time offset of the Blackglobe sensor to the corresponding ATM41 sensor in **seconds**',  # noqa: E501,
    )
    dew_point_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
        doc='minimum of dew point temperature in **°C** calculated using :func:`thermal_comfort.dew_point`',  # noqa: E501,
    )
    dew_point_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
        doc='maximum of dew point temperature in **°C** calculated using :func:`thermal_comfort.dew_point`',  # noqa: E501,
    )
    heat_index_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
        doc='minimum of heat index in **°C** calculated using :func:`thermal_comfort.heat_index_extended`',  # noqa: E501,
    )
    heat_index_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
        doc='maximum of heat index in **°C** calculated using :func:`thermal_comfort.heat_index_extended`',  # noqa: E501,
    )
    lightning_average_distance_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='km',
        doc='minimum of distance of lightning strikes in **km**',
    )
    lightning_average_distance_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='km',
        doc='maximum of distance of lightning strikes in **km**',
    )
    mrt_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
        doc='minimum of mean radiant temperature in **°C** calculated using :func:`thermal_comfort.mean_radiant_temp`',  # noqa: E501,
    )
    mrt_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
        doc='maximum of mean radiant temperature in **°C** calculated using :func:`thermal_comfort.mean_radiant_temp`',  # noqa: E501,
    )
    pet_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
        doc='minimum of physiological equivalent temperature in **°C** calculated using :func:`thermal_comfort.pet_static`',  # noqa: E501,
    )
    pet_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
        doc='maximum of physiological equivalent temperature in **°C** calculated using :func:`thermal_comfort.pet_static`',  # noqa: E501,
    )
    relative_humidity_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='%',
        doc='minimum of relative humidity in **%**',
    )
    relative_humidity_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='%',
        doc='maximum of relative humidity in **%**',
    )
    sensor_temperature_internal_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
        doc='minimum of internal temperature of the sensor in **°C**',
    )
    sensor_temperature_internal_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
        doc='maximum of internal temperature of the sensor in **°C**',
    )
    solar_radiation_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='W/m2',
        doc='minimum of solar radiation in **W/m2**',
    )
    solar_radiation_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='W/m2',
        doc='maximum of solar radiation in **W/m2**',
    )
    specific_humidity_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='g/kg',
        doc='minimum of specific humidity in **g/kg** calculated using :func:`thermal_comfort.specific_humidity`',  # noqa: E501,
    )
    specific_humidity_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='g/kg',
        doc='maximum of specific humidity in **g/kg** calculated using :func:`thermal_comfort.specific_humidity`',  # noqa: E501,
    )
    thermistor_resistance_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='Ohms',
        doc='minimum of thermistor resistance in **Ohms**',
    )
    thermistor_resistance_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='Ohms',
        doc='maximum of thermistor resistance in **Ohms**',
    )
    u_wind_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='m/s',
        doc='minimum of u wind component in **m/s**',
    )
    u_wind_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='m/s',
        doc='maximum of u wind component in **m/s**',
    )
    utci_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
        doc='minimum of universal thermal climate index in **°C** calculated using :func:`thermal_comfort.utci_approx`',  # noqa: E501,
    )
    utci_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
        doc='maximum of universal thermal climate index in **°C** calculated using :func:`thermal_comfort.utci_approx`',  # noqa: E501,
    )
    v_wind_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='m/s',
        doc='minimum of v wind component in **m/s**',
    )
    v_wind_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='m/s',
        doc='maximum of v wind component in **m/s**',
    )
    vapor_pressure_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='kPa',
        doc='minimum of vapor pressure in **kPa**',
    )
    vapor_pressure_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='kPa',
        doc='maximum of vapor pressure in **kPa**',
    )
    voltage_ratio_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='-',
        doc='minimum of voltage ratio of the sensor',
    )
    voltage_ratio_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='-',
        doc='maximum of voltage ratio of the sensor',
    )
    wet_bulb_temperature_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
        doc='minimum of wet bulb temperature in **°C** calculated using :func:`thermal_comfort.wet_bulb_temp`',  # noqa: E501,
    )
    wet_bulb_temperature_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
        doc='maximum of wet bulb temperature in **°C** calculated using :func:`thermal_comfort.wet_bulb_temp`',  # noqa: E501,
    )
    wind_speed_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='m/s',
        doc='minimum of wind speed in **m/s**',
    )
    wind_speed_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='m/s',
        doc='maximum of wind speed in **m/s**',
    )
    x_orientation_angle_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°',
        doc='minimum of x-tilt angle of the sensor in **°**',
    )
    x_orientation_angle_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°',
        doc='maximum of x-tilt angle of the sensor in **°**',
    )
    y_orientation_angle_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°',
        doc='minimum of y-tilt angle of the sensor in **°**',
    )
    y_orientation_angle_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°',
        doc='maximum of y-tilt angle of the sensor in **°**',
    )
    station: Mapped[Station] = relationship(
        lazy=True,
        doc='The station the data was measured at',
    )

    def __repr__(self) -> str:
        return (
            f'{type(self).__name__}('
            f'measured_at={self.measured_at!r}, '
            f'absolute_humidity={self.absolute_humidity!r}, '
            f'absolute_humidity_min={self.absolute_humidity_min!r}, '
            f'absolute_humidity_max={self.absolute_humidity_max!r}, '
            f'air_temperature={self.air_temperature!r}, '
            f'air_temperature_min={self.air_temperature_min!r}, '
            f'air_temperature_max={self.air_temperature_max!r}, '
            f'atmospheric_pressure={self.atmospheric_pressure!r}, '
            f'atmospheric_pressure_min={self.atmospheric_pressure_min!r}, '
            f'atmospheric_pressure_max={self.atmospheric_pressure_max!r}, '
            f'atmospheric_pressure_reduced={self.atmospheric_pressure_reduced!r}, '
            f'atmospheric_pressure_reduced_min={self.atmospheric_pressure_reduced_min!r}, '  # noqa: E501
            f'atmospheric_pressure_reduced_max={self.atmospheric_pressure_reduced_max!r}, '  # noqa: E501
            f'battery_voltage={self.battery_voltage!r}, '
            f'battery_voltage_min={self.battery_voltage_min!r}, '
            f'battery_voltage_max={self.battery_voltage_max!r}, '
            f'black_globe_temperature={self.black_globe_temperature!r}, '
            f'black_globe_temperature_min={self.black_globe_temperature_min!r}, '
            f'black_globe_temperature_max={self.black_globe_temperature_max!r}, '
            f'blg_battery_voltage={self.blg_battery_voltage!r}, '
            f'blg_battery_voltage_min={self.blg_battery_voltage_min!r}, '
            f'blg_battery_voltage_max={self.blg_battery_voltage_max!r}, '
            f'blg_time_offset={self.blg_time_offset!r}, '
            f'blg_time_offset_min={self.blg_time_offset_min!r}, '
            f'blg_time_offset_max={self.blg_time_offset_max!r}, '
            f'dew_point={self.dew_point!r}, '
            f'dew_point_min={self.dew_point_min!r}, '
            f'dew_point_max={self.dew_point_max!r}, '
            f'heat_index={self.heat_index!r}, '
            f'heat_index_min={self.heat_index_min!r}, '
            f'heat_index_max={self.heat_index_max!r}, '
            f'lightning_average_distance={self.lightning_average_distance!r}, '
            f'lightning_average_distance_min={self.lightning_average_distance_min!r}, '
            f'lightning_average_distance_max={self.lightning_average_distance_max!r}, '
            f'lightning_strike_count={self.lightning_strike_count!r}, '
            f'maximum_wind_speed={self.maximum_wind_speed!r}, '
            f'mrt={self.mrt!r}, '
            f'mrt_min={self.mrt_min!r}, '
            f'mrt_max={self.mrt_max!r}, '
            f'pet={self.pet!r}, '
            f'pet_min={self.pet_min!r}, '
            f'pet_max={self.pet_max!r}, '
            f'pet_category={self.pet_category!r}, '
            f'precipitation_sum={self.precipitation_sum!r}, '
            f'protocol_version={self.protocol_version!r}, '
            f'relative_humidity={self.relative_humidity!r}, '
            f'relative_humidity_min={self.relative_humidity_min!r}, '
            f'relative_humidity_max={self.relative_humidity_max!r}, '
            f'sensor_temperature_internal={self.sensor_temperature_internal!r}, '
            f'sensor_temperature_internal_min={self.sensor_temperature_internal_min!r}, '  # noqa: E501
            f'sensor_temperature_internal_max={self.sensor_temperature_internal_max!r}, '  # noqa: E501
            f'solar_radiation={self.solar_radiation!r}, '
            f'solar_radiation_min={self.solar_radiation_min!r}, '
            f'solar_radiation_max={self.solar_radiation_max!r}, '
            f'specific_humidity={self.specific_humidity!r}, '
            f'specific_humidity_min={self.specific_humidity_min!r}, '
            f'specific_humidity_max={self.specific_humidity_max!r}, '
            f'thermistor_resistance={self.thermistor_resistance!r}, '
            f'thermistor_resistance_min={self.thermistor_resistance_min!r}, '
            f'thermistor_resistance_max={self.thermistor_resistance_max!r}, '
            f'u_wind={self.u_wind!r}, '
            f'u_wind_min={self.u_wind_min!r}, '
            f'u_wind_max={self.u_wind_max!r}, '
            f'utci={self.utci!r}, '
            f'utci_min={self.utci_min!r}, '
            f'utci_max={self.utci_max!r}, '
            f'utci_category={self.utci_category!r}, '
            f'v_wind={self.v_wind!r}, '
            f'v_wind_min={self.v_wind_min!r}, '
            f'v_wind_max={self.v_wind_max!r}, '
            f'vapor_pressure={self.vapor_pressure!r}, '
            f'vapor_pressure_min={self.vapor_pressure_min!r}, '
            f'vapor_pressure_max={self.vapor_pressure_max!r}, '
            f'voltage_ratio={self.voltage_ratio!r}, '
            f'voltage_ratio_min={self.voltage_ratio_min!r}, '
            f'voltage_ratio_max={self.voltage_ratio_max!r}, '
            f'wet_bulb_temperature={self.wet_bulb_temperature!r}, '
            f'wet_bulb_temperature_min={self.wet_bulb_temperature_min!r}, '
            f'wet_bulb_temperature_max={self.wet_bulb_temperature_max!r}, '
            f'wind_direction={self.wind_direction!r}, '
            f'wind_speed={self.wind_speed!r}, '
            f'wind_speed_min={self.wind_speed_min!r}, '
            f'wind_speed_max={self.wind_speed_max!r}, '
            f'x_orientation_angle={self.x_orientation_angle!r}, '
            f'x_orientation_angle_min={self.x_orientation_angle_min!r}, '
            f'x_orientation_angle_max={self.x_orientation_angle_max!r}, '
            f'y_orientation_angle={self.y_orientation_angle!r}, '
            f'y_orientation_angle_min={self.y_orientation_angle_min!r}, '
            f'y_orientation_angle_max={self.y_orientation_angle_max!r}, '
            f')'
        )

    creation_sql = text('''\
    CREATE MATERIALIZED VIEW IF NOT EXISTS biomet_data_hourly AS
    WITH data_bounds AS (
        SELECT
            station_id,
            MIN(measured_at) AS start_time,
            MAX(measured_at) AS end_time
        FROM biomet_data
        GROUP BY station_id
    ), filling_time_series AS (
        SELECT generate_series(
            DATE_TRUNC('hour', (SELECT MIN(measured_at) FROM biomet_data)),
            DATE_TRUNC('hour', (SELECT MAX(measured_at) FROM biomet_data) + '1 hour'::INTERVAL),
            '1 hour'::INTERVAL
        ) AS measured_at
    ),
    stations_subset AS (
        -- TODO: this could be faster if check the station table by station_type
        SELECT DISTINCT station_id FROM biomet_data
    ),
    time_station_combinations AS (
        SELECT
            measured_at,
            stations_subset.station_id,
            start_time,
            end_time
        FROM filling_time_series
        CROSS JOIN stations_subset
        JOIN data_bounds
            ON data_bounds.station_id = stations_subset.station_id
        WHERE filling_time_series.measured_at >= data_bounds.start_time
        AND filling_time_series.measured_at <= data_bounds.end_time
    ), all_data AS(
        (
            SELECT
                measured_at AS ma,
                station_id,
                NULL AS absolute_humidity,
                NULL AS air_temperature,
                NULL AS atmospheric_pressure,
                NULL AS atmospheric_pressure_reduced,
                NULL AS battery_voltage,
                NULL AS black_globe_temperature,
                NULL AS blg_battery_voltage,
                NULL AS blg_time_offset,
                NULL AS dew_point,
                NULL AS heat_index,
                NULL AS lightning_average_distance,
                NULL AS lightning_strike_count,
                NULL AS maximum_wind_speed,
                NULL AS mrt,
                NULL AS pet,
                NULL AS pet_category,
                NULL AS precipitation_sum,
                NULL AS protocol_version,
                NULL AS relative_humidity,
                NULL AS sensor_temperature_internal,
                NULL AS solar_radiation,
                NULL AS specific_humidity,
                NULL AS thermistor_resistance,
                NULL AS u_wind,
                NULL AS utci,
                NULL AS utci_category,
                NULL AS v_wind,
                NULL AS vapor_pressure,
                NULL AS voltage_ratio,
                NULL AS wet_bulb_temperature,
                NULL AS wind_direction,
                NULL AS wind_speed,
                NULL AS x_orientation_angle,
                NULL AS y_orientation_angle
            FROM time_station_combinations
        )
        UNION ALL
        (
            SELECT
                measured_at AS ma,
                station_id,
                absolute_humidity,
                air_temperature,
                atmospheric_pressure,
                atmospheric_pressure_reduced,
                battery_voltage,
                black_globe_temperature,
                blg_battery_voltage,
                blg_time_offset,
                dew_point,
                heat_index,
                lightning_average_distance,
                lightning_strike_count,
                maximum_wind_speed,
                mrt,
                pet,
                pet_category,
                precipitation_sum,
                protocol_version,
                relative_humidity,
                sensor_temperature_internal,
                solar_radiation,
                specific_humidity,
                thermistor_resistance,
                u_wind,
                utci,
                utci_category,
                v_wind,
                vapor_pressure,
                voltage_ratio,
                wet_bulb_temperature,
                wind_direction,
                wind_speed,
                x_orientation_angle,
                y_orientation_angle
            FROM biomet_data
        )
    ) SELECT
        time_bucket('1 hour', ma) + '1 hour'::INTERVAL AS measured_at,
        station_id,
        avg(absolute_humidity) AS absolute_humidity,
        min(absolute_humidity) AS absolute_humidity_min,
        max(absolute_humidity) AS absolute_humidity_max,
        avg(air_temperature) AS air_temperature,
        min(air_temperature) AS air_temperature_min,
        max(air_temperature) AS air_temperature_max,
        avg(atmospheric_pressure) AS atmospheric_pressure,
        min(atmospheric_pressure) AS atmospheric_pressure_min,
        max(atmospheric_pressure) AS atmospheric_pressure_max,
        avg(atmospheric_pressure_reduced) AS atmospheric_pressure_reduced,
        min(atmospheric_pressure_reduced) AS atmospheric_pressure_reduced_min,
        max(atmospheric_pressure_reduced) AS atmospheric_pressure_reduced_max,
        avg(battery_voltage) AS battery_voltage,
        min(battery_voltage) AS battery_voltage_min,
        max(battery_voltage) AS battery_voltage_max,
        avg(black_globe_temperature) AS black_globe_temperature,
        min(black_globe_temperature) AS black_globe_temperature_min,
        max(black_globe_temperature) AS black_globe_temperature_max,
        avg(blg_battery_voltage) AS blg_battery_voltage,
        min(blg_battery_voltage) AS blg_battery_voltage_min,
        max(blg_battery_voltage) AS blg_battery_voltage_max,
        avg(blg_time_offset) AS blg_time_offset,
        min(blg_time_offset) AS blg_time_offset_min,
        max(blg_time_offset) AS blg_time_offset_max,
        avg(dew_point) AS dew_point,
        min(dew_point) AS dew_point_min,
        max(dew_point) AS dew_point_max,
        avg(heat_index) AS heat_index,
        min(heat_index) AS heat_index_min,
        max(heat_index) AS heat_index_max,
        avg(lightning_average_distance) FILTER (WHERE lightning_average_distance > 0.0) AS lightning_average_distance,
        min(lightning_average_distance) FILTER (WHERE lightning_average_distance > 0.0) AS lightning_average_distance_min,
        max(lightning_average_distance) FILTER (WHERE lightning_average_distance > 0.0) AS lightning_average_distance_max,
        sum(lightning_strike_count) AS lightning_strike_count,
        max(maximum_wind_speed) AS maximum_wind_speed,
        avg(mrt) AS mrt,
        min(mrt) AS mrt_min,
        max(mrt) AS mrt_max,
        avg(pet) AS pet,
        min(pet) AS pet_min,
        max(pet) AS pet_max,
        mode() WITHIN GROUP (ORDER BY pet_category ASC) AS pet_category,
        sum(precipitation_sum) AS precipitation_sum,
        mode() WITHIN GROUP (ORDER BY protocol_version ASC) AS protocol_version,
        avg(relative_humidity) AS relative_humidity,
        min(relative_humidity) AS relative_humidity_min,
        max(relative_humidity) AS relative_humidity_max,
        avg(sensor_temperature_internal) AS sensor_temperature_internal,
        min(sensor_temperature_internal) AS sensor_temperature_internal_min,
        max(sensor_temperature_internal) AS sensor_temperature_internal_max,
        avg(solar_radiation) AS solar_radiation,
        min(solar_radiation) AS solar_radiation_min,
        max(solar_radiation) AS solar_radiation_max,
        avg(specific_humidity) AS specific_humidity,
        min(specific_humidity) AS specific_humidity_min,
        max(specific_humidity) AS specific_humidity_max,
        avg(thermistor_resistance) AS thermistor_resistance,
        min(thermistor_resistance) AS thermistor_resistance_min,
        max(thermistor_resistance) AS thermistor_resistance_max,
        avg(u_wind) AS u_wind,
        min(u_wind) AS u_wind_min,
        max(u_wind) AS u_wind_max,
        avg(utci) AS utci,
        min(utci) AS utci_min,
        max(utci) AS utci_max,
        mode() WITHIN GROUP (ORDER BY utci_category ASC) AS utci_category,
        avg(v_wind) AS v_wind,
        min(v_wind) AS v_wind_min,
        max(v_wind) AS v_wind_max,
        avg(vapor_pressure) AS vapor_pressure,
        min(vapor_pressure) AS vapor_pressure_min,
        max(vapor_pressure) AS vapor_pressure_max,
        avg(voltage_ratio) AS voltage_ratio,
        min(voltage_ratio) AS voltage_ratio_min,
        max(voltage_ratio) AS voltage_ratio_max,
        avg(wet_bulb_temperature) AS wet_bulb_temperature,
        min(wet_bulb_temperature) AS wet_bulb_temperature_min,
        max(wet_bulb_temperature) AS wet_bulb_temperature_max,
        avg_angle(wind_direction) AS wind_direction,
        avg(wind_speed) AS wind_speed,
        min(wind_speed) AS wind_speed_min,
        max(wind_speed) AS wind_speed_max,
        avg(x_orientation_angle) AS x_orientation_angle,
        min(x_orientation_angle) AS x_orientation_angle_min,
        max(x_orientation_angle) AS x_orientation_angle_max,
        avg(y_orientation_angle) AS y_orientation_angle,
        min(y_orientation_angle) AS y_orientation_angle_min,
        max(y_orientation_angle) AS y_orientation_angle_max
    FROM all_data
    GROUP BY measured_at, station_id
    ORDER BY measured_at, station_id
    ''')  # noqa: E501


class TempRHDataHourly(
    MaterializedView,
    _SHT35DataRawBase,
    _TempRHDerivatives,
    _CalibrationDerivatives,
):
    """This is not an actual table, but a materialized view. We simply trick sqlalchemy
    into thinking this was a table. Querying a materialized view does not differ from
    querying a proper table.
    """
    __tablename__ = 'temp_rh_data_hourly'
    __table_args__ = (
        Index(
            'ix_temp_rh_data_hourly_station_id_measured_at',
            'station_id',
            'measured_at',
            unique=True,
        ),
    )

    absolute_humidity_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='g/m3',
        doc='minimum of absolute humidity in **g/m3** calculated using :func:`thermal_comfort.absolute_humidity`',  # noqa: E501,
    )
    absolute_humidity_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='g/m3',
        doc='maximum of absolute humidity in **g/m3** calculated using :func:`thermal_comfort.absolute_humidity`',  # noqa: E501,
    )
    air_temperature_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
        doc='minimum of air temperature in **°C**',
    )
    air_temperature_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
        doc='maximum of air temperature in **°C**',
    )
    air_temperature_raw_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
        doc='minimum of raw air temperature in **°C** with no calibration applied',
    )
    air_temperature_raw_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
        doc='maximum of raw air temperature in **°C** with no calibration applied',
    )
    battery_voltage_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='Volts',
        doc='minimum of The battery voltage of the sensor in **Volts**',
    )
    battery_voltage_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='Volts',
        doc='maximum of The battery voltage of the sensor in **Volts**',
    )
    dew_point_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
        doc='minimum of dew point temperature in **°C** calculated using :func:`thermal_comfort.dew_point`',  # noqa: E501,
    )
    dew_point_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
        doc='maximum of dew point temperature in **°C** calculated using :func:`thermal_comfort.dew_point`',  # noqa: E501,
    )
    heat_index_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
        doc='minimum of heat index in **°C** calculated using :func:`thermal_comfort.heat_index_extended`',  # noqa: E501,
    )
    heat_index_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
        doc='maximum of heat index in **°C** calculated using :func:`thermal_comfort.heat_index_extended`',  # noqa: E501,
    )
    relative_humidity_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='%',
        doc='minimum of relative humidity in **%**',
    )
    relative_humidity_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='%',
        doc='maximum of relative humidity in **%**',
    )
    relative_humidity_raw_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='%',
        doc='minimum of raw relative humidity in **%** with no calibration applied',
    )
    relative_humidity_raw_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='%',
        doc='maximum of raw relative humidity in **%** with no calibration applied',
    )
    specific_humidity_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='g/kg',
        doc='minimum of specific humidity in **g/kg** calculated using :func:`thermal_comfort.specific_humidity`',  # noqa: E501,
    )
    specific_humidity_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='g/kg',
        doc='maximum of specific humidity in **g/kg** calculated using :func:`thermal_comfort.specific_humidity`',  # noqa: E501,
    )
    wet_bulb_temperature_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
        doc='minimum of wet bulb temperature in **°C** calculated using :func:`thermal_comfort.wet_bulb_temp`',  # noqa: E501,
    )
    wet_bulb_temperature_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
        doc='maximum of wet bulb temperature in **°C** calculated using :func:`thermal_comfort.wet_bulb_temp`',  # noqa: E501,
    )
    station: Mapped[Station] = relationship(
        lazy=True,
        doc='The station the data was measured at',
    )

    def __repr__(self) -> str:
        return (
            f'{type(self).__name__}('
            f'measured_at={self.measured_at!r}, '
            f'absolute_humidity={self.absolute_humidity!r}, '
            f'absolute_humidity_min={self.absolute_humidity_min!r}, '
            f'absolute_humidity_max={self.absolute_humidity_max!r}, '
            f'air_temperature={self.air_temperature!r}, '
            f'air_temperature_min={self.air_temperature_min!r}, '
            f'air_temperature_max={self.air_temperature_max!r}, '
            f'air_temperature_raw={self.air_temperature_raw!r}, '
            f'air_temperature_raw_min={self.air_temperature_raw_min!r}, '
            f'air_temperature_raw_max={self.air_temperature_raw_max!r}, '
            f'battery_voltage={self.battery_voltage!r}, '
            f'battery_voltage_min={self.battery_voltage_min!r}, '
            f'battery_voltage_max={self.battery_voltage_max!r}, '
            f'dew_point={self.dew_point!r}, '
            f'dew_point_min={self.dew_point_min!r}, '
            f'dew_point_max={self.dew_point_max!r}, '
            f'heat_index={self.heat_index!r}, '
            f'heat_index_min={self.heat_index_min!r}, '
            f'heat_index_max={self.heat_index_max!r}, '
            f'protocol_version={self.protocol_version!r}, '
            f'relative_humidity={self.relative_humidity!r}, '
            f'relative_humidity_min={self.relative_humidity_min!r}, '
            f'relative_humidity_max={self.relative_humidity_max!r}, '
            f'relative_humidity_raw={self.relative_humidity_raw!r}, '
            f'relative_humidity_raw_min={self.relative_humidity_raw_min!r}, '
            f'relative_humidity_raw_max={self.relative_humidity_raw_max!r}, '
            f'specific_humidity={self.specific_humidity!r}, '
            f'specific_humidity_min={self.specific_humidity_min!r}, '
            f'specific_humidity_max={self.specific_humidity_max!r}, '
            f'wet_bulb_temperature={self.wet_bulb_temperature!r}, '
            f'wet_bulb_temperature_min={self.wet_bulb_temperature_min!r}, '
            f'wet_bulb_temperature_max={self.wet_bulb_temperature_max!r}, '
            f')'
        )

    creation_sql = text('''\
    CREATE MATERIALIZED VIEW IF NOT EXISTS temp_rh_data_hourly AS
    WITH data_bounds AS (
        SELECT
            station_id,
            MIN(measured_at) AS start_time,
            MAX(measured_at) AS end_time
        FROM temp_rh_data
        GROUP BY station_id
    ), filling_time_series AS (
        SELECT generate_series(
            DATE_TRUNC('hour', (SELECT MIN(measured_at) FROM temp_rh_data)),
            DATE_TRUNC('hour', (SELECT MAX(measured_at) FROM temp_rh_data) + '1 hour'::INTERVAL),
            '1 hour'::INTERVAL
        ) AS measured_at
    ),
    stations_subset AS (
        -- TODO: this could be faster if check the station table by station_type
        SELECT DISTINCT station_id FROM temp_rh_data
    ),
    time_station_combinations AS (
        SELECT
            measured_at,
            stations_subset.station_id,
            start_time,
            end_time
        FROM filling_time_series
        CROSS JOIN stations_subset
        JOIN data_bounds
            ON data_bounds.station_id = stations_subset.station_id
        WHERE filling_time_series.measured_at >= data_bounds.start_time
        AND filling_time_series.measured_at <= data_bounds.end_time
    ), all_data AS(
        (
            SELECT
                measured_at AS ma,
                station_id,
                NULL AS absolute_humidity,
                NULL AS air_temperature,
                NULL AS air_temperature_raw,
                NULL AS battery_voltage,
                NULL AS dew_point,
                NULL AS heat_index,
                NULL AS protocol_version,
                NULL AS relative_humidity,
                NULL AS relative_humidity_raw,
                NULL AS specific_humidity,
                NULL AS wet_bulb_temperature
            FROM time_station_combinations
        )
        UNION ALL
        (
            SELECT
                measured_at AS ma,
                station_id,
                absolute_humidity,
                air_temperature,
                air_temperature_raw,
                battery_voltage,
                dew_point,
                heat_index,
                protocol_version,
                relative_humidity,
                relative_humidity_raw,
                specific_humidity,
                wet_bulb_temperature
            FROM temp_rh_data
        )
    ) SELECT
        time_bucket('1 hour', ma) + '1 hour'::INTERVAL AS measured_at,
        station_id,
        avg(absolute_humidity) AS absolute_humidity,
        min(absolute_humidity) AS absolute_humidity_min,
        max(absolute_humidity) AS absolute_humidity_max,
        avg(air_temperature) AS air_temperature,
        min(air_temperature) AS air_temperature_min,
        max(air_temperature) AS air_temperature_max,
        avg(air_temperature_raw) AS air_temperature_raw,
        min(air_temperature_raw) AS air_temperature_raw_min,
        max(air_temperature_raw) AS air_temperature_raw_max,
        avg(battery_voltage) AS battery_voltage,
        min(battery_voltage) AS battery_voltage_min,
        max(battery_voltage) AS battery_voltage_max,
        avg(dew_point) AS dew_point,
        min(dew_point) AS dew_point_min,
        max(dew_point) AS dew_point_max,
        avg(heat_index) AS heat_index,
        min(heat_index) AS heat_index_min,
        max(heat_index) AS heat_index_max,
        mode() WITHIN GROUP (ORDER BY protocol_version ASC) AS protocol_version,
        avg(relative_humidity) AS relative_humidity,
        min(relative_humidity) AS relative_humidity_min,
        max(relative_humidity) AS relative_humidity_max,
        avg(relative_humidity_raw) AS relative_humidity_raw,
        min(relative_humidity_raw) AS relative_humidity_raw_min,
        max(relative_humidity_raw) AS relative_humidity_raw_max,
        avg(specific_humidity) AS specific_humidity,
        min(specific_humidity) AS specific_humidity_min,
        max(specific_humidity) AS specific_humidity_max,
        avg(wet_bulb_temperature) AS wet_bulb_temperature,
        min(wet_bulb_temperature) AS wet_bulb_temperature_min,
        max(wet_bulb_temperature) AS wet_bulb_temperature_max
    FROM all_data
    GROUP BY measured_at, station_id
    ORDER BY measured_at, station_id
    ''')  # noqa: E501


class BiometDataDaily(
    MaterializedView,
    _ATM41DataRawBase,
    _BLGDataRawBase,
    _TempRHDerivatives,
    _BiometDerivatives,
):
    """This is not an actual table, but a materialized view. We simply trick sqlalchemy
    into thinking this was a table. Querying a materialized view does not differ from
    querying a proper table.
    """
    __tablename__ = 'biomet_data_daily'
    __table_args__ = (
        Index(
            'ix_biomet_data_daily_station_id_measured_at',
            'station_id',
            'measured_at',
            unique=True,
        ),
    )

    absolute_humidity_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='g/m3',
        doc='minimum of absolute humidity in **g/m3** calculated using :func:`thermal_comfort.absolute_humidity`',  # noqa: E501,
    )
    absolute_humidity_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='g/m3',
        doc='maximum of absolute humidity in **g/m3** calculated using :func:`thermal_comfort.absolute_humidity`',  # noqa: E501,
    )
    air_temperature_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
        doc='minimum of air temperature in **°C**',
    )
    air_temperature_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
        doc='maximum of air temperature in **°C**',
    )
    atmospheric_pressure_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='kPa',
        doc='minimum of atmospheric pressure in **kPa**',
    )
    atmospheric_pressure_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='kPa',
        doc='maximum of atmospheric pressure in **kPa**',
    )
    atmospheric_pressure_reduced_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='hPa',
        doc='minimum of atmospheric pressure reduced to sea level in **hPa** calculated using :func:`app.tasks.reduce_pressure`',  # noqa: E501,
    )
    atmospheric_pressure_reduced_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='hPa',
        doc='maximum of atmospheric pressure reduced to sea level in **hPa** calculated using :func:`app.tasks.reduce_pressure`',  # noqa: E501,
    )
    battery_voltage_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='Volts',
        doc='minimum of The battery voltage of the sensor in **Volts**',
    )
    battery_voltage_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='Volts',
        doc='maximum of The battery voltage of the sensor in **Volts**',
    )
    black_globe_temperature_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
        doc='minimum of black globe temperature in **°C**',
    )
    black_globe_temperature_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
        doc='maximum of black globe temperature in **°C**',
    )
    blg_battery_voltage_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='V',
        doc='minimum of battery voltage of the black globe sensor in **Volts**',
    )
    blg_battery_voltage_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='V',
        doc='maximum of battery voltage of the black globe sensor in **Volts**',
    )
    blg_time_offset_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='seconds',
        doc='minimum of time offset of the Blackglobe sensor to the corresponding ATM41 sensor in **seconds**',  # noqa: E501,
    )
    blg_time_offset_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='seconds',
        doc='maximum of time offset of the Blackglobe sensor to the corresponding ATM41 sensor in **seconds**',  # noqa: E501,
    )
    dew_point_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
        doc='minimum of dew point temperature in **°C** calculated using :func:`thermal_comfort.dew_point`',  # noqa: E501,
    )
    dew_point_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
        doc='maximum of dew point temperature in **°C** calculated using :func:`thermal_comfort.dew_point`',  # noqa: E501,
    )
    heat_index_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
        doc='minimum of heat index in **°C** calculated using :func:`thermal_comfort.heat_index_extended`',  # noqa: E501,
    )
    heat_index_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
        doc='maximum of heat index in **°C** calculated using :func:`thermal_comfort.heat_index_extended`',  # noqa: E501,
    )
    lightning_average_distance_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='km',
        doc='minimum of distance of lightning strikes in **km**',
    )
    lightning_average_distance_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='km',
        doc='maximum of distance of lightning strikes in **km**',
    )
    mrt_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
        doc='minimum of mean radiant temperature in **°C** calculated using :func:`thermal_comfort.mean_radiant_temp`',  # noqa: E501,
    )
    mrt_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
        doc='maximum of mean radiant temperature in **°C** calculated using :func:`thermal_comfort.mean_radiant_temp`',  # noqa: E501,
    )
    pet_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
        doc='minimum of physiological equivalent temperature in **°C** calculated using :func:`thermal_comfort.pet_static`',  # noqa: E501,
    )
    pet_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
        doc='maximum of physiological equivalent temperature in **°C** calculated using :func:`thermal_comfort.pet_static`',  # noqa: E501,
    )
    relative_humidity_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='%',
        doc='minimum of relative humidity in **%**',
    )
    relative_humidity_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='%',
        doc='maximum of relative humidity in **%**',
    )
    sensor_temperature_internal_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
        doc='minimum of internal temperature of the sensor in **°C**',
    )
    sensor_temperature_internal_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
        doc='maximum of internal temperature of the sensor in **°C**',
    )
    solar_radiation_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='W/m2',
        doc='minimum of solar radiation in **W/m2**',
    )
    solar_radiation_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='W/m2',
        doc='maximum of solar radiation in **W/m2**',
    )
    specific_humidity_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='g/kg',
        doc='minimum of specific humidity in **g/kg** calculated using :func:`thermal_comfort.specific_humidity`',  # noqa: E501,
    )
    specific_humidity_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='g/kg',
        doc='maximum of specific humidity in **g/kg** calculated using :func:`thermal_comfort.specific_humidity`',  # noqa: E501,
    )
    thermistor_resistance_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='Ohms',
        doc='minimum of thermistor resistance in **Ohms**',
    )
    thermistor_resistance_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='Ohms',
        doc='maximum of thermistor resistance in **Ohms**',
    )
    u_wind_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='m/s',
        doc='minimum of u wind component in **m/s**',
    )
    u_wind_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='m/s',
        doc='maximum of u wind component in **m/s**',
    )
    utci_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
        doc='minimum of universal thermal climate index in **°C** calculated using :func:`thermal_comfort.utci_approx`',  # noqa: E501,
    )
    utci_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
        doc='maximum of universal thermal climate index in **°C** calculated using :func:`thermal_comfort.utci_approx`',  # noqa: E501,
    )
    v_wind_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='m/s',
        doc='minimum of v wind component in **m/s**',
    )
    v_wind_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='m/s',
        doc='maximum of v wind component in **m/s**',
    )
    vapor_pressure_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='kPa',
        doc='minimum of vapor pressure in **kPa**',
    )
    vapor_pressure_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='kPa',
        doc='maximum of vapor pressure in **kPa**',
    )
    voltage_ratio_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='-',
        doc='minimum of voltage ratio of the sensor',
    )
    voltage_ratio_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='-',
        doc='maximum of voltage ratio of the sensor',
    )
    wet_bulb_temperature_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
        doc='minimum of wet bulb temperature in **°C** calculated using :func:`thermal_comfort.wet_bulb_temp`',  # noqa: E501,
    )
    wet_bulb_temperature_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
        doc='maximum of wet bulb temperature in **°C** calculated using :func:`thermal_comfort.wet_bulb_temp`',  # noqa: E501,
    )
    wind_speed_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='m/s',
        doc='minimum of wind speed in **m/s**',
    )
    wind_speed_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='m/s',
        doc='maximum of wind speed in **m/s**',
    )
    x_orientation_angle_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°',
        doc='minimum of x-tilt angle of the sensor in **°**',
    )
    x_orientation_angle_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°',
        doc='maximum of x-tilt angle of the sensor in **°**',
    )
    y_orientation_angle_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°',
        doc='minimum of y-tilt angle of the sensor in **°**',
    )
    y_orientation_angle_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°',
        doc='maximum of y-tilt angle of the sensor in **°**',
    )
    station: Mapped[Station] = relationship(
        lazy=True,
        doc='The station the data was measured at',
    )

    def __repr__(self) -> str:
        return (
            f'{type(self).__name__}('
            f'measured_at={self.measured_at!r}, '
            f'absolute_humidity={self.absolute_humidity!r}, '
            f'absolute_humidity_min={self.absolute_humidity_min!r}, '
            f'absolute_humidity_max={self.absolute_humidity_max!r}, '
            f'air_temperature={self.air_temperature!r}, '
            f'air_temperature_min={self.air_temperature_min!r}, '
            f'air_temperature_max={self.air_temperature_max!r}, '
            f'atmospheric_pressure={self.atmospheric_pressure!r}, '
            f'atmospheric_pressure_min={self.atmospheric_pressure_min!r}, '
            f'atmospheric_pressure_max={self.atmospheric_pressure_max!r}, '
            f'atmospheric_pressure_reduced={self.atmospheric_pressure_reduced!r}, '
            f'atmospheric_pressure_reduced_min={self.atmospheric_pressure_reduced_min!r}, '  # noqa: E501
            f'atmospheric_pressure_reduced_max={self.atmospheric_pressure_reduced_max!r}, '  # noqa: E501
            f'battery_voltage={self.battery_voltage!r}, '
            f'battery_voltage_min={self.battery_voltage_min!r}, '
            f'battery_voltage_max={self.battery_voltage_max!r}, '
            f'black_globe_temperature={self.black_globe_temperature!r}, '
            f'black_globe_temperature_min={self.black_globe_temperature_min!r}, '
            f'black_globe_temperature_max={self.black_globe_temperature_max!r}, '
            f'blg_battery_voltage={self.blg_battery_voltage!r}, '
            f'blg_battery_voltage_min={self.blg_battery_voltage_min!r}, '
            f'blg_battery_voltage_max={self.blg_battery_voltage_max!r}, '
            f'blg_time_offset={self.blg_time_offset!r}, '
            f'blg_time_offset_min={self.blg_time_offset_min!r}, '
            f'blg_time_offset_max={self.blg_time_offset_max!r}, '
            f'dew_point={self.dew_point!r}, '
            f'dew_point_min={self.dew_point_min!r}, '
            f'dew_point_max={self.dew_point_max!r}, '
            f'heat_index={self.heat_index!r}, '
            f'heat_index_min={self.heat_index_min!r}, '
            f'heat_index_max={self.heat_index_max!r}, '
            f'lightning_average_distance={self.lightning_average_distance!r}, '
            f'lightning_average_distance_min={self.lightning_average_distance_min!r}, '
            f'lightning_average_distance_max={self.lightning_average_distance_max!r}, '
            f'lightning_strike_count={self.lightning_strike_count!r}, '
            f'maximum_wind_speed={self.maximum_wind_speed!r}, '
            f'mrt={self.mrt!r}, '
            f'mrt_min={self.mrt_min!r}, '
            f'mrt_max={self.mrt_max!r}, '
            f'pet={self.pet!r}, '
            f'pet_min={self.pet_min!r}, '
            f'pet_max={self.pet_max!r}, '
            f'pet_category={self.pet_category!r}, '
            f'precipitation_sum={self.precipitation_sum!r}, '
            f'protocol_version={self.protocol_version!r}, '
            f'relative_humidity={self.relative_humidity!r}, '
            f'relative_humidity_min={self.relative_humidity_min!r}, '
            f'relative_humidity_max={self.relative_humidity_max!r}, '
            f'sensor_temperature_internal={self.sensor_temperature_internal!r}, '
            f'sensor_temperature_internal_min={self.sensor_temperature_internal_min!r}, '  # noqa: E501
            f'sensor_temperature_internal_max={self.sensor_temperature_internal_max!r}, '  # noqa: E501
            f'solar_radiation={self.solar_radiation!r}, '
            f'solar_radiation_min={self.solar_radiation_min!r}, '
            f'solar_radiation_max={self.solar_radiation_max!r}, '
            f'specific_humidity={self.specific_humidity!r}, '
            f'specific_humidity_min={self.specific_humidity_min!r}, '
            f'specific_humidity_max={self.specific_humidity_max!r}, '
            f'thermistor_resistance={self.thermistor_resistance!r}, '
            f'thermistor_resistance_min={self.thermistor_resistance_min!r}, '
            f'thermistor_resistance_max={self.thermistor_resistance_max!r}, '
            f'u_wind={self.u_wind!r}, '
            f'u_wind_min={self.u_wind_min!r}, '
            f'u_wind_max={self.u_wind_max!r}, '
            f'utci={self.utci!r}, '
            f'utci_min={self.utci_min!r}, '
            f'utci_max={self.utci_max!r}, '
            f'utci_category={self.utci_category!r}, '
            f'v_wind={self.v_wind!r}, '
            f'v_wind_min={self.v_wind_min!r}, '
            f'v_wind_max={self.v_wind_max!r}, '
            f'vapor_pressure={self.vapor_pressure!r}, '
            f'vapor_pressure_min={self.vapor_pressure_min!r}, '
            f'vapor_pressure_max={self.vapor_pressure_max!r}, '
            f'voltage_ratio={self.voltage_ratio!r}, '
            f'voltage_ratio_min={self.voltage_ratio_min!r}, '
            f'voltage_ratio_max={self.voltage_ratio_max!r}, '
            f'wet_bulb_temperature={self.wet_bulb_temperature!r}, '
            f'wet_bulb_temperature_min={self.wet_bulb_temperature_min!r}, '
            f'wet_bulb_temperature_max={self.wet_bulb_temperature_max!r}, '
            f'wind_direction={self.wind_direction!r}, '
            f'wind_speed={self.wind_speed!r}, '
            f'wind_speed_min={self.wind_speed_min!r}, '
            f'wind_speed_max={self.wind_speed_max!r}, '
            f'x_orientation_angle={self.x_orientation_angle!r}, '
            f'x_orientation_angle_min={self.x_orientation_angle_min!r}, '
            f'x_orientation_angle_max={self.x_orientation_angle_max!r}, '
            f'y_orientation_angle={self.y_orientation_angle!r}, '
            f'y_orientation_angle_min={self.y_orientation_angle_min!r}, '
            f'y_orientation_angle_max={self.y_orientation_angle_max!r}, '
            f')'
        )

    creation_sql = text('''\
    CREATE MATERIALIZED VIEW IF NOT EXISTS biomet_data_daily AS
    WITH data_bounds AS (
        SELECT
            station_id,
            MIN(measured_at) AS start_time,
            MAX(measured_at) AS end_time
        FROM biomet_data
        GROUP BY station_id
    ), filling_time_series AS (
        SELECT generate_series(
            DATE_TRUNC('hour', (SELECT MIN(measured_at) FROM biomet_data)),
            DATE_TRUNC('hour', (SELECT MAX(measured_at) FROM biomet_data) + '1 hour'::INTERVAL),
            '1 hour'::INTERVAL
        ) AS measured_at
    ),
    stations_subset AS (
        -- TODO: this could be faster if check the station table by station_type
        SELECT DISTINCT station_id FROM biomet_data
    ),
    time_station_combinations AS (
        SELECT
            measured_at,
            stations_subset.station_id,
            start_time,
            end_time
        FROM filling_time_series
        CROSS JOIN stations_subset
        JOIN data_bounds
            ON data_bounds.station_id = stations_subset.station_id
        WHERE filling_time_series.measured_at >= data_bounds.start_time
        AND filling_time_series.measured_at <= data_bounds.end_time
    ), all_data AS(
        (
            SELECT
                measured_at AS ma,
                station_id,
                NULL AS absolute_humidity,
                NULL AS air_temperature,
                NULL AS atmospheric_pressure,
                NULL AS atmospheric_pressure_reduced,
                NULL AS battery_voltage,
                NULL AS black_globe_temperature,
                NULL AS blg_battery_voltage,
                NULL AS blg_time_offset,
                NULL AS dew_point,
                NULL AS heat_index,
                NULL AS lightning_average_distance,
                NULL AS lightning_strike_count,
                NULL AS maximum_wind_speed,
                NULL AS mrt,
                NULL AS pet,
                NULL AS pet_category,
                NULL AS precipitation_sum,
                NULL AS protocol_version,
                NULL AS relative_humidity,
                NULL AS sensor_temperature_internal,
                NULL AS solar_radiation,
                NULL AS specific_humidity,
                NULL AS thermistor_resistance,
                NULL AS u_wind,
                NULL AS utci,
                NULL AS utci_category,
                NULL AS v_wind,
                NULL AS vapor_pressure,
                NULL AS voltage_ratio,
                NULL AS wet_bulb_temperature,
                NULL AS wind_direction,
                NULL AS wind_speed,
                NULL AS x_orientation_angle,
                NULL AS y_orientation_angle
            FROM time_station_combinations
        )
        UNION ALL
        (
            SELECT
                measured_at AS ma,
                station_id,
                absolute_humidity,
                air_temperature,
                atmospheric_pressure,
                atmospheric_pressure_reduced,
                battery_voltage,
                black_globe_temperature,
                blg_battery_voltage,
                blg_time_offset,
                dew_point,
                heat_index,
                lightning_average_distance,
                lightning_strike_count,
                maximum_wind_speed,
                mrt,
                pet,
                pet_category,
                precipitation_sum,
                protocol_version,
                relative_humidity,
                sensor_temperature_internal,
                solar_radiation,
                specific_humidity,
                thermistor_resistance,
                u_wind,
                utci,
                utci_category,
                v_wind,
                vapor_pressure,
                voltage_ratio,
                wet_bulb_temperature,
                wind_direction,
                wind_speed,
                x_orientation_angle,
                y_orientation_angle
            FROM biomet_data
        )
    ) SELECT
        (time_bucket('1day', ma, 'CET') + '1 hour'::INTERVAL)::DATE AS measured_at,
        station_id,
        CASE
            WHEN (count(*) FILTER (
                    WHERE absolute_humidity IS NOT NULL) / 288.0
                ) > 0.7 THEN avg(absolute_humidity)
            ELSE NULL
        END AS absolute_humidity,
        CASE
            WHEN (count(*) FILTER (
                    WHERE absolute_humidity IS NOT NULL) / 288.0
                ) > 0.7 THEN min(absolute_humidity)
            ELSE NULL
        END AS absolute_humidity_min,
        CASE
            WHEN (count(*) FILTER (
                    WHERE absolute_humidity IS NOT NULL) / 288.0
                ) > 0.7 THEN max(absolute_humidity)
            ELSE NULL
        END AS absolute_humidity_max,
        CASE
            WHEN (count(*) FILTER (
                    WHERE air_temperature IS NOT NULL) / 288.0
                ) > 0.7 THEN avg(air_temperature)
            ELSE NULL
        END AS air_temperature,
        CASE
            WHEN (count(*) FILTER (
                    WHERE air_temperature IS NOT NULL) / 288.0
                ) > 0.7 THEN min(air_temperature)
            ELSE NULL
        END AS air_temperature_min,
        CASE
            WHEN (count(*) FILTER (
                    WHERE air_temperature IS NOT NULL) / 288.0
                ) > 0.7 THEN max(air_temperature)
            ELSE NULL
        END AS air_temperature_max,
        CASE
            WHEN (count(*) FILTER (
                    WHERE atmospheric_pressure IS NOT NULL) / 288.0
                ) > 0.7 THEN avg(atmospheric_pressure)
            ELSE NULL
        END AS atmospheric_pressure,
        CASE
            WHEN (count(*) FILTER (
                    WHERE atmospheric_pressure IS NOT NULL) / 288.0
                ) > 0.7 THEN min(atmospheric_pressure)
            ELSE NULL
        END AS atmospheric_pressure_min,
        CASE
            WHEN (count(*) FILTER (
                    WHERE atmospheric_pressure IS NOT NULL) / 288.0
                ) > 0.7 THEN max(atmospheric_pressure)
            ELSE NULL
        END AS atmospheric_pressure_max,
        CASE
            WHEN (count(*) FILTER (
                    WHERE atmospheric_pressure_reduced IS NOT NULL) / 288.0
                ) > 0.7 THEN avg(atmospheric_pressure_reduced)
            ELSE NULL
        END AS atmospheric_pressure_reduced,
        CASE
            WHEN (count(*) FILTER (
                    WHERE atmospheric_pressure_reduced IS NOT NULL) / 288.0
                ) > 0.7 THEN min(atmospheric_pressure_reduced)
            ELSE NULL
        END AS atmospheric_pressure_reduced_min,
        CASE
            WHEN (count(*) FILTER (
                    WHERE atmospheric_pressure_reduced IS NOT NULL) / 288.0
                ) > 0.7 THEN max(atmospheric_pressure_reduced)
            ELSE NULL
        END AS atmospheric_pressure_reduced_max,
        CASE
            WHEN (count(*) FILTER (
                    WHERE battery_voltage IS NOT NULL) / 288.0
                ) > 0.7 THEN avg(battery_voltage)
            ELSE NULL
        END AS battery_voltage,
        CASE
            WHEN (count(*) FILTER (
                    WHERE battery_voltage IS NOT NULL) / 288.0
                ) > 0.7 THEN min(battery_voltage)
            ELSE NULL
        END AS battery_voltage_min,
        CASE
            WHEN (count(*) FILTER (
                    WHERE battery_voltage IS NOT NULL) / 288.0
                ) > 0.7 THEN max(battery_voltage)
            ELSE NULL
        END AS battery_voltage_max,
        CASE
            WHEN (count(*) FILTER (
                    WHERE black_globe_temperature IS NOT NULL) / 288.0
                ) > 0.7 THEN avg(black_globe_temperature)
            ELSE NULL
        END AS black_globe_temperature,
        CASE
            WHEN (count(*) FILTER (
                    WHERE black_globe_temperature IS NOT NULL) / 288.0
                ) > 0.7 THEN min(black_globe_temperature)
            ELSE NULL
        END AS black_globe_temperature_min,
        CASE
            WHEN (count(*) FILTER (
                    WHERE black_globe_temperature IS NOT NULL) / 288.0
                ) > 0.7 THEN max(black_globe_temperature)
            ELSE NULL
        END AS black_globe_temperature_max,
        CASE
            WHEN (count(*) FILTER (
                    WHERE blg_battery_voltage IS NOT NULL) / 288.0
                ) > 0.7 THEN avg(blg_battery_voltage)
            ELSE NULL
        END AS blg_battery_voltage,
        CASE
            WHEN (count(*) FILTER (
                    WHERE blg_battery_voltage IS NOT NULL) / 288.0
                ) > 0.7 THEN min(blg_battery_voltage)
            ELSE NULL
        END AS blg_battery_voltage_min,
        CASE
            WHEN (count(*) FILTER (
                    WHERE blg_battery_voltage IS NOT NULL) / 288.0
                ) > 0.7 THEN max(blg_battery_voltage)
            ELSE NULL
        END AS blg_battery_voltage_max,
        CASE
            WHEN (count(*) FILTER (
                    WHERE blg_time_offset IS NOT NULL) / 288.0
                ) > 0.7 THEN avg(blg_time_offset)
            ELSE NULL
        END AS blg_time_offset,
        CASE
            WHEN (count(*) FILTER (
                    WHERE blg_time_offset IS NOT NULL) / 288.0
                ) > 0.7 THEN min(blg_time_offset)
            ELSE NULL
        END AS blg_time_offset_min,
        CASE
            WHEN (count(*) FILTER (
                    WHERE blg_time_offset IS NOT NULL) / 288.0
                ) > 0.7 THEN max(blg_time_offset)
            ELSE NULL
        END AS blg_time_offset_max,
        CASE
            WHEN (count(*) FILTER (
                    WHERE dew_point IS NOT NULL) / 288.0
                ) > 0.7 THEN avg(dew_point)
            ELSE NULL
        END AS dew_point,
        CASE
            WHEN (count(*) FILTER (
                    WHERE dew_point IS NOT NULL) / 288.0
                ) > 0.7 THEN min(dew_point)
            ELSE NULL
        END AS dew_point_min,
        CASE
            WHEN (count(*) FILTER (
                    WHERE dew_point IS NOT NULL) / 288.0
                ) > 0.7 THEN max(dew_point)
            ELSE NULL
        END AS dew_point_max,
        CASE
            WHEN (count(*) FILTER (
                    WHERE heat_index IS NOT NULL) / 288.0
                ) > 0.7 THEN avg(heat_index)
            ELSE NULL
        END AS heat_index,
        CASE
            WHEN (count(*) FILTER (
                    WHERE heat_index IS NOT NULL) / 288.0
                ) > 0.7 THEN min(heat_index)
            ELSE NULL
        END AS heat_index_min,
        CASE
            WHEN (count(*) FILTER (
                    WHERE heat_index IS NOT NULL) / 288.0
                ) > 0.7 THEN max(heat_index)
            ELSE NULL
        END AS heat_index_max,
        CASE
            WHEN (count(*) FILTER (
                    WHERE lightning_average_distance IS NOT NULL) / 288.0
                ) > 0.7 THEN avg(lightning_average_distance) FILTER (WHERE lightning_average_distance > 0.0)
            ELSE NULL
        END AS lightning_average_distance,
        CASE
            WHEN (count(*) FILTER (
                    WHERE lightning_average_distance IS NOT NULL) / 288.0
                ) > 0.7 THEN min(lightning_average_distance) FILTER (WHERE lightning_average_distance > 0.0)
            ELSE NULL
        END AS lightning_average_distance_min,
        CASE
            WHEN (count(*) FILTER (
                    WHERE lightning_average_distance IS NOT NULL) / 288.0
                ) > 0.7 THEN max(lightning_average_distance) FILTER (WHERE lightning_average_distance > 0.0)
            ELSE NULL
        END AS lightning_average_distance_max,
        CASE
            WHEN (count(*) FILTER (
                    WHERE lightning_strike_count IS NOT NULL) / 288.0
                ) > 0.7 THEN sum(lightning_strike_count)
            ELSE NULL
        END AS lightning_strike_count,
        CASE
            WHEN (count(*) FILTER (
                    WHERE maximum_wind_speed IS NOT NULL) / 288.0
                ) > 0.7 THEN max(maximum_wind_speed)
            ELSE NULL
        END AS maximum_wind_speed,
        CASE
            WHEN (count(*) FILTER (
                    WHERE mrt IS NOT NULL) / 288.0
                ) > 0.7 THEN avg(mrt)
            ELSE NULL
        END AS mrt,
        CASE
            WHEN (count(*) FILTER (
                    WHERE mrt IS NOT NULL) / 288.0
                ) > 0.7 THEN min(mrt)
            ELSE NULL
        END AS mrt_min,
        CASE
            WHEN (count(*) FILTER (
                    WHERE mrt IS NOT NULL) / 288.0
                ) > 0.7 THEN max(mrt)
            ELSE NULL
        END AS mrt_max,
        CASE
            WHEN (count(*) FILTER (
                    WHERE pet IS NOT NULL) / 288.0
                ) > 0.7 THEN avg(pet)
            ELSE NULL
        END AS pet,
        CASE
            WHEN (count(*) FILTER (
                    WHERE pet IS NOT NULL) / 288.0
                ) > 0.7 THEN min(pet)
            ELSE NULL
        END AS pet_min,
        CASE
            WHEN (count(*) FILTER (
                    WHERE pet IS NOT NULL) / 288.0
                ) > 0.7 THEN max(pet)
            ELSE NULL
        END AS pet_max,
        CASE
            WHEN (count(*) FILTER (
                    WHERE pet_category IS NOT NULL) / 288.0
                ) > 0.7 THEN mode() WITHIN GROUP (ORDER BY pet_category ASC)
            ELSE NULL
        END AS pet_category,
        CASE
            WHEN (count(*) FILTER (
                    WHERE precipitation_sum IS NOT NULL) / 288.0
                ) > 0.7 THEN sum(precipitation_sum)
            ELSE NULL
        END AS precipitation_sum,
        CASE
            WHEN (count(*) FILTER (
                    WHERE protocol_version IS NOT NULL) / 288.0
                ) > 0.7 THEN mode() WITHIN GROUP (ORDER BY protocol_version ASC)
            ELSE NULL
        END AS protocol_version,
        CASE
            WHEN (count(*) FILTER (
                    WHERE relative_humidity IS NOT NULL) / 288.0
                ) > 0.7 THEN avg(relative_humidity)
            ELSE NULL
        END AS relative_humidity,
        CASE
            WHEN (count(*) FILTER (
                    WHERE relative_humidity IS NOT NULL) / 288.0
                ) > 0.7 THEN min(relative_humidity)
            ELSE NULL
        END AS relative_humidity_min,
        CASE
            WHEN (count(*) FILTER (
                    WHERE relative_humidity IS NOT NULL) / 288.0
                ) > 0.7 THEN max(relative_humidity)
            ELSE NULL
        END AS relative_humidity_max,
        CASE
            WHEN (count(*) FILTER (
                    WHERE sensor_temperature_internal IS NOT NULL) / 288.0
                ) > 0.7 THEN avg(sensor_temperature_internal)
            ELSE NULL
        END AS sensor_temperature_internal,
        CASE
            WHEN (count(*) FILTER (
                    WHERE sensor_temperature_internal IS NOT NULL) / 288.0
                ) > 0.7 THEN min(sensor_temperature_internal)
            ELSE NULL
        END AS sensor_temperature_internal_min,
        CASE
            WHEN (count(*) FILTER (
                    WHERE sensor_temperature_internal IS NOT NULL) / 288.0
                ) > 0.7 THEN max(sensor_temperature_internal)
            ELSE NULL
        END AS sensor_temperature_internal_max,
        CASE
            WHEN (count(*) FILTER (
                    WHERE solar_radiation IS NOT NULL) / 288.0
                ) > 0.7 THEN avg(solar_radiation)
            ELSE NULL
        END AS solar_radiation,
        CASE
            WHEN (count(*) FILTER (
                    WHERE solar_radiation IS NOT NULL) / 288.0
                ) > 0.7 THEN min(solar_radiation)
            ELSE NULL
        END AS solar_radiation_min,
        CASE
            WHEN (count(*) FILTER (
                    WHERE solar_radiation IS NOT NULL) / 288.0
                ) > 0.7 THEN max(solar_radiation)
            ELSE NULL
        END AS solar_radiation_max,
        CASE
            WHEN (count(*) FILTER (
                    WHERE specific_humidity IS NOT NULL) / 288.0
                ) > 0.7 THEN avg(specific_humidity)
            ELSE NULL
        END AS specific_humidity,
        CASE
            WHEN (count(*) FILTER (
                    WHERE specific_humidity IS NOT NULL) / 288.0
                ) > 0.7 THEN min(specific_humidity)
            ELSE NULL
        END AS specific_humidity_min,
        CASE
            WHEN (count(*) FILTER (
                    WHERE specific_humidity IS NOT NULL) / 288.0
                ) > 0.7 THEN max(specific_humidity)
            ELSE NULL
        END AS specific_humidity_max,
        CASE
            WHEN (count(*) FILTER (
                    WHERE thermistor_resistance IS NOT NULL) / 288.0
                ) > 0.7 THEN avg(thermistor_resistance)
            ELSE NULL
        END AS thermistor_resistance,
        CASE
            WHEN (count(*) FILTER (
                    WHERE thermistor_resistance IS NOT NULL) / 288.0
                ) > 0.7 THEN min(thermistor_resistance)
            ELSE NULL
        END AS thermistor_resistance_min,
        CASE
            WHEN (count(*) FILTER (
                    WHERE thermistor_resistance IS NOT NULL) / 288.0
                ) > 0.7 THEN max(thermistor_resistance)
            ELSE NULL
        END AS thermistor_resistance_max,
        CASE
            WHEN (count(*) FILTER (
                    WHERE u_wind IS NOT NULL) / 288.0
                ) > 0.7 THEN avg(u_wind)
            ELSE NULL
        END AS u_wind,
        CASE
            WHEN (count(*) FILTER (
                    WHERE u_wind IS NOT NULL) / 288.0
                ) > 0.7 THEN min(u_wind)
            ELSE NULL
        END AS u_wind_min,
        CASE
            WHEN (count(*) FILTER (
                    WHERE u_wind IS NOT NULL) / 288.0
                ) > 0.7 THEN max(u_wind)
            ELSE NULL
        END AS u_wind_max,
        CASE
            WHEN (count(*) FILTER (
                    WHERE utci IS NOT NULL) / 288.0
                ) > 0.7 THEN avg(utci)
            ELSE NULL
        END AS utci,
        CASE
            WHEN (count(*) FILTER (
                    WHERE utci IS NOT NULL) / 288.0
                ) > 0.7 THEN min(utci)
            ELSE NULL
        END AS utci_min,
        CASE
            WHEN (count(*) FILTER (
                    WHERE utci IS NOT NULL) / 288.0
                ) > 0.7 THEN max(utci)
            ELSE NULL
        END AS utci_max,
        CASE
            WHEN (count(*) FILTER (
                    WHERE utci_category IS NOT NULL) / 288.0
                ) > 0.7 THEN mode() WITHIN GROUP (ORDER BY utci_category ASC)
            ELSE NULL
        END AS utci_category,
        CASE
            WHEN (count(*) FILTER (
                    WHERE v_wind IS NOT NULL) / 288.0
                ) > 0.7 THEN avg(v_wind)
            ELSE NULL
        END AS v_wind,
        CASE
            WHEN (count(*) FILTER (
                    WHERE v_wind IS NOT NULL) / 288.0
                ) > 0.7 THEN min(v_wind)
            ELSE NULL
        END AS v_wind_min,
        CASE
            WHEN (count(*) FILTER (
                    WHERE v_wind IS NOT NULL) / 288.0
                ) > 0.7 THEN max(v_wind)
            ELSE NULL
        END AS v_wind_max,
        CASE
            WHEN (count(*) FILTER (
                    WHERE vapor_pressure IS NOT NULL) / 288.0
                ) > 0.7 THEN avg(vapor_pressure)
            ELSE NULL
        END AS vapor_pressure,
        CASE
            WHEN (count(*) FILTER (
                    WHERE vapor_pressure IS NOT NULL) / 288.0
                ) > 0.7 THEN min(vapor_pressure)
            ELSE NULL
        END AS vapor_pressure_min,
        CASE
            WHEN (count(*) FILTER (
                    WHERE vapor_pressure IS NOT NULL) / 288.0
                ) > 0.7 THEN max(vapor_pressure)
            ELSE NULL
        END AS vapor_pressure_max,
        CASE
            WHEN (count(*) FILTER (
                    WHERE voltage_ratio IS NOT NULL) / 288.0
                ) > 0.7 THEN avg(voltage_ratio)
            ELSE NULL
        END AS voltage_ratio,
        CASE
            WHEN (count(*) FILTER (
                    WHERE voltage_ratio IS NOT NULL) / 288.0
                ) > 0.7 THEN min(voltage_ratio)
            ELSE NULL
        END AS voltage_ratio_min,
        CASE
            WHEN (count(*) FILTER (
                    WHERE voltage_ratio IS NOT NULL) / 288.0
                ) > 0.7 THEN max(voltage_ratio)
            ELSE NULL
        END AS voltage_ratio_max,
        CASE
            WHEN (count(*) FILTER (
                    WHERE wet_bulb_temperature IS NOT NULL) / 288.0
                ) > 0.7 THEN avg(wet_bulb_temperature)
            ELSE NULL
        END AS wet_bulb_temperature,
        CASE
            WHEN (count(*) FILTER (
                    WHERE wet_bulb_temperature IS NOT NULL) / 288.0
                ) > 0.7 THEN min(wet_bulb_temperature)
            ELSE NULL
        END AS wet_bulb_temperature_min,
        CASE
            WHEN (count(*) FILTER (
                    WHERE wet_bulb_temperature IS NOT NULL) / 288.0
                ) > 0.7 THEN max(wet_bulb_temperature)
            ELSE NULL
        END AS wet_bulb_temperature_max,
        CASE
            WHEN (count(*) FILTER (
                    WHERE wind_direction IS NOT NULL) / 288.0
                ) > 0.7 THEN avg_angle(wind_direction)
            ELSE NULL
        END AS wind_direction,
        CASE
            WHEN (count(*) FILTER (
                    WHERE wind_speed IS NOT NULL) / 288.0
                ) > 0.7 THEN avg(wind_speed)
            ELSE NULL
        END AS wind_speed,
        CASE
            WHEN (count(*) FILTER (
                    WHERE wind_speed IS NOT NULL) / 288.0
                ) > 0.7 THEN min(wind_speed)
            ELSE NULL
        END AS wind_speed_min,
        CASE
            WHEN (count(*) FILTER (
                    WHERE wind_speed IS NOT NULL) / 288.0
                ) > 0.7 THEN max(wind_speed)
            ELSE NULL
        END AS wind_speed_max,
        CASE
            WHEN (count(*) FILTER (
                    WHERE x_orientation_angle IS NOT NULL) / 288.0
                ) > 0.7 THEN avg(x_orientation_angle)
            ELSE NULL
        END AS x_orientation_angle,
        CASE
            WHEN (count(*) FILTER (
                    WHERE x_orientation_angle IS NOT NULL) / 288.0
                ) > 0.7 THEN min(x_orientation_angle)
            ELSE NULL
        END AS x_orientation_angle_min,
        CASE
            WHEN (count(*) FILTER (
                    WHERE x_orientation_angle IS NOT NULL) / 288.0
                ) > 0.7 THEN max(x_orientation_angle)
            ELSE NULL
        END AS x_orientation_angle_max,
        CASE
            WHEN (count(*) FILTER (
                    WHERE y_orientation_angle IS NOT NULL) / 288.0
                ) > 0.7 THEN avg(y_orientation_angle)
            ELSE NULL
        END AS y_orientation_angle,
        CASE
            WHEN (count(*) FILTER (
                    WHERE y_orientation_angle IS NOT NULL) / 288.0
                ) > 0.7 THEN min(y_orientation_angle)
            ELSE NULL
        END AS y_orientation_angle_min,
        CASE
            WHEN (count(*) FILTER (
                    WHERE y_orientation_angle IS NOT NULL) / 288.0
                ) > 0.7 THEN max(y_orientation_angle)
            ELSE NULL
        END AS y_orientation_angle_max
    FROM all_data
    GROUP BY measured_at, station_id
    ORDER BY measured_at, station_id
    ''')  # noqa: E501


class TempRHDataDaily(
    MaterializedView,
    _SHT35DataRawBase,
    _TempRHDerivatives,
    _CalibrationDerivatives,
):
    """This is not an actual table, but a materialized view. We simply trick sqlalchemy
    into thinking this was a table. Querying a materialized view does not differ from
    querying a proper table.
    """
    __tablename__ = 'temp_rh_data_daily'
    __table_args__ = (
        Index(
            'ix_temp_rh_data_daily_station_id_measured_at',
            'station_id',
            'measured_at',
            unique=True,
        ),
    )

    absolute_humidity_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='g/m3',
        doc='minimum of absolute humidity in **g/m3** calculated using :func:`thermal_comfort.absolute_humidity`',  # noqa: E501,
    )
    absolute_humidity_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='g/m3',
        doc='maximum of absolute humidity in **g/m3** calculated using :func:`thermal_comfort.absolute_humidity`',  # noqa: E501,
    )
    air_temperature_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
        doc='minimum of air temperature in **°C**',
    )
    air_temperature_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
        doc='maximum of air temperature in **°C**',
    )
    air_temperature_raw_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
        doc='minimum of raw air temperature in **°C** with no calibration applied',
    )
    air_temperature_raw_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
        doc='maximum of raw air temperature in **°C** with no calibration applied',
    )
    battery_voltage_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='Volts',
        doc='minimum of The battery voltage of the sensor in **Volts**',
    )
    battery_voltage_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='Volts',
        doc='maximum of The battery voltage of the sensor in **Volts**',
    )
    dew_point_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
        doc='minimum of dew point temperature in **°C** calculated using :func:`thermal_comfort.dew_point`',  # noqa: E501,
    )
    dew_point_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
        doc='maximum of dew point temperature in **°C** calculated using :func:`thermal_comfort.dew_point`',  # noqa: E501,
    )
    heat_index_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
        doc='minimum of heat index in **°C** calculated using :func:`thermal_comfort.heat_index_extended`',  # noqa: E501,
    )
    heat_index_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
        doc='maximum of heat index in **°C** calculated using :func:`thermal_comfort.heat_index_extended`',  # noqa: E501,
    )
    relative_humidity_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='%',
        doc='minimum of relative humidity in **%**',
    )
    relative_humidity_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='%',
        doc='maximum of relative humidity in **%**',
    )
    relative_humidity_raw_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='%',
        doc='minimum of raw relative humidity in **%** with no calibration applied',
    )
    relative_humidity_raw_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='%',
        doc='maximum of raw relative humidity in **%** with no calibration applied',
    )
    specific_humidity_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='g/kg',
        doc='minimum of specific humidity in **g/kg** calculated using :func:`thermal_comfort.specific_humidity`',  # noqa: E501,
    )
    specific_humidity_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='g/kg',
        doc='maximum of specific humidity in **g/kg** calculated using :func:`thermal_comfort.specific_humidity`',  # noqa: E501,
    )
    wet_bulb_temperature_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
        doc='minimum of wet bulb temperature in **°C** calculated using :func:`thermal_comfort.wet_bulb_temp`',  # noqa: E501,
    )
    wet_bulb_temperature_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
        doc='maximum of wet bulb temperature in **°C** calculated using :func:`thermal_comfort.wet_bulb_temp`',  # noqa: E501,
    )
    station: Mapped[Station] = relationship(
        lazy=True,
        doc='The station the data was measured at',
    )

    def __repr__(self) -> str:
        return (
            f'{type(self).__name__}('
            f'measured_at={self.measured_at!r}, '
            f'absolute_humidity={self.absolute_humidity!r}, '
            f'absolute_humidity_min={self.absolute_humidity_min!r}, '
            f'absolute_humidity_max={self.absolute_humidity_max!r}, '
            f'air_temperature={self.air_temperature!r}, '
            f'air_temperature_min={self.air_temperature_min!r}, '
            f'air_temperature_max={self.air_temperature_max!r}, '
            f'air_temperature_raw={self.air_temperature_raw!r}, '
            f'air_temperature_raw_min={self.air_temperature_raw_min!r}, '
            f'air_temperature_raw_max={self.air_temperature_raw_max!r}, '
            f'battery_voltage={self.battery_voltage!r}, '
            f'battery_voltage_min={self.battery_voltage_min!r}, '
            f'battery_voltage_max={self.battery_voltage_max!r}, '
            f'dew_point={self.dew_point!r}, '
            f'dew_point_min={self.dew_point_min!r}, '
            f'dew_point_max={self.dew_point_max!r}, '
            f'heat_index={self.heat_index!r}, '
            f'heat_index_min={self.heat_index_min!r}, '
            f'heat_index_max={self.heat_index_max!r}, '
            f'protocol_version={self.protocol_version!r}, '
            f'relative_humidity={self.relative_humidity!r}, '
            f'relative_humidity_min={self.relative_humidity_min!r}, '
            f'relative_humidity_max={self.relative_humidity_max!r}, '
            f'relative_humidity_raw={self.relative_humidity_raw!r}, '
            f'relative_humidity_raw_min={self.relative_humidity_raw_min!r}, '
            f'relative_humidity_raw_max={self.relative_humidity_raw_max!r}, '
            f'specific_humidity={self.specific_humidity!r}, '
            f'specific_humidity_min={self.specific_humidity_min!r}, '
            f'specific_humidity_max={self.specific_humidity_max!r}, '
            f'wet_bulb_temperature={self.wet_bulb_temperature!r}, '
            f'wet_bulb_temperature_min={self.wet_bulb_temperature_min!r}, '
            f'wet_bulb_temperature_max={self.wet_bulb_temperature_max!r}, '
            f')'
        )

    creation_sql = text('''\
    CREATE MATERIALIZED VIEW IF NOT EXISTS temp_rh_data_daily AS
    WITH data_bounds AS (
        SELECT
            station_id,
            MIN(measured_at) AS start_time,
            MAX(measured_at) AS end_time
        FROM temp_rh_data
        GROUP BY station_id
    ), filling_time_series AS (
        SELECT generate_series(
            DATE_TRUNC('hour', (SELECT MIN(measured_at) FROM temp_rh_data)),
            DATE_TRUNC('hour', (SELECT MAX(measured_at) FROM temp_rh_data) + '1 hour'::INTERVAL),
            '1 hour'::INTERVAL
        ) AS measured_at
    ),
    stations_subset AS (
        -- TODO: this could be faster if check the station table by station_type
        SELECT DISTINCT station_id FROM temp_rh_data
    ),
    time_station_combinations AS (
        SELECT
            measured_at,
            stations_subset.station_id,
            start_time,
            end_time
        FROM filling_time_series
        CROSS JOIN stations_subset
        JOIN data_bounds
            ON data_bounds.station_id = stations_subset.station_id
        WHERE filling_time_series.measured_at >= data_bounds.start_time
        AND filling_time_series.measured_at <= data_bounds.end_time
    ), all_data AS(
        (
            SELECT
                measured_at AS ma,
                station_id,
                NULL AS absolute_humidity,
                NULL AS air_temperature,
                NULL AS air_temperature_raw,
                NULL AS battery_voltage,
                NULL AS dew_point,
                NULL AS heat_index,
                NULL AS protocol_version,
                NULL AS relative_humidity,
                NULL AS relative_humidity_raw,
                NULL AS specific_humidity,
                NULL AS wet_bulb_temperature
            FROM time_station_combinations
        )
        UNION ALL
        (
            SELECT
                measured_at AS ma,
                station_id,
                absolute_humidity,
                air_temperature,
                air_temperature_raw,
                battery_voltage,
                dew_point,
                heat_index,
                protocol_version,
                relative_humidity,
                relative_humidity_raw,
                specific_humidity,
                wet_bulb_temperature
            FROM temp_rh_data
        )
    ) SELECT
        (time_bucket('1day', ma, 'CET') + '1 hour'::INTERVAL)::DATE AS measured_at,
        station_id,
        CASE
            WHEN (count(*) FILTER (
                    WHERE absolute_humidity IS NOT NULL) / 288.0
                ) > 0.7 THEN avg(absolute_humidity)
            ELSE NULL
        END AS absolute_humidity,
        CASE
            WHEN (count(*) FILTER (
                    WHERE absolute_humidity IS NOT NULL) / 288.0
                ) > 0.7 THEN min(absolute_humidity)
            ELSE NULL
        END AS absolute_humidity_min,
        CASE
            WHEN (count(*) FILTER (
                    WHERE absolute_humidity IS NOT NULL) / 288.0
                ) > 0.7 THEN max(absolute_humidity)
            ELSE NULL
        END AS absolute_humidity_max,
        CASE
            WHEN (count(*) FILTER (
                    WHERE air_temperature IS NOT NULL) / 288.0
                ) > 0.7 THEN avg(air_temperature)
            ELSE NULL
        END AS air_temperature,
        CASE
            WHEN (count(*) FILTER (
                    WHERE air_temperature IS NOT NULL) / 288.0
                ) > 0.7 THEN min(air_temperature)
            ELSE NULL
        END AS air_temperature_min,
        CASE
            WHEN (count(*) FILTER (
                    WHERE air_temperature IS NOT NULL) / 288.0
                ) > 0.7 THEN max(air_temperature)
            ELSE NULL
        END AS air_temperature_max,
        CASE
            WHEN (count(*) FILTER (
                    WHERE air_temperature_raw IS NOT NULL) / 288.0
                ) > 0.7 THEN avg(air_temperature_raw)
            ELSE NULL
        END AS air_temperature_raw,
        CASE
            WHEN (count(*) FILTER (
                    WHERE air_temperature_raw IS NOT NULL) / 288.0
                ) > 0.7 THEN min(air_temperature_raw)
            ELSE NULL
        END AS air_temperature_raw_min,
        CASE
            WHEN (count(*) FILTER (
                    WHERE air_temperature_raw IS NOT NULL) / 288.0
                ) > 0.7 THEN max(air_temperature_raw)
            ELSE NULL
        END AS air_temperature_raw_max,
        CASE
            WHEN (count(*) FILTER (
                    WHERE battery_voltage IS NOT NULL) / 288.0
                ) > 0.7 THEN avg(battery_voltage)
            ELSE NULL
        END AS battery_voltage,
        CASE
            WHEN (count(*) FILTER (
                    WHERE battery_voltage IS NOT NULL) / 288.0
                ) > 0.7 THEN min(battery_voltage)
            ELSE NULL
        END AS battery_voltage_min,
        CASE
            WHEN (count(*) FILTER (
                    WHERE battery_voltage IS NOT NULL) / 288.0
                ) > 0.7 THEN max(battery_voltage)
            ELSE NULL
        END AS battery_voltage_max,
        CASE
            WHEN (count(*) FILTER (
                    WHERE dew_point IS NOT NULL) / 288.0
                ) > 0.7 THEN avg(dew_point)
            ELSE NULL
        END AS dew_point,
        CASE
            WHEN (count(*) FILTER (
                    WHERE dew_point IS NOT NULL) / 288.0
                ) > 0.7 THEN min(dew_point)
            ELSE NULL
        END AS dew_point_min,
        CASE
            WHEN (count(*) FILTER (
                    WHERE dew_point IS NOT NULL) / 288.0
                ) > 0.7 THEN max(dew_point)
            ELSE NULL
        END AS dew_point_max,
        CASE
            WHEN (count(*) FILTER (
                    WHERE heat_index IS NOT NULL) / 288.0
                ) > 0.7 THEN avg(heat_index)
            ELSE NULL
        END AS heat_index,
        CASE
            WHEN (count(*) FILTER (
                    WHERE heat_index IS NOT NULL) / 288.0
                ) > 0.7 THEN min(heat_index)
            ELSE NULL
        END AS heat_index_min,
        CASE
            WHEN (count(*) FILTER (
                    WHERE heat_index IS NOT NULL) / 288.0
                ) > 0.7 THEN max(heat_index)
            ELSE NULL
        END AS heat_index_max,
        CASE
            WHEN (count(*) FILTER (
                    WHERE protocol_version IS NOT NULL) / 288.0
                ) > 0.7 THEN mode() WITHIN GROUP (ORDER BY protocol_version ASC)
            ELSE NULL
        END AS protocol_version,
        CASE
            WHEN (count(*) FILTER (
                    WHERE relative_humidity IS NOT NULL) / 288.0
                ) > 0.7 THEN avg(relative_humidity)
            ELSE NULL
        END AS relative_humidity,
        CASE
            WHEN (count(*) FILTER (
                    WHERE relative_humidity IS NOT NULL) / 288.0
                ) > 0.7 THEN min(relative_humidity)
            ELSE NULL
        END AS relative_humidity_min,
        CASE
            WHEN (count(*) FILTER (
                    WHERE relative_humidity IS NOT NULL) / 288.0
                ) > 0.7 THEN max(relative_humidity)
            ELSE NULL
        END AS relative_humidity_max,
        CASE
            WHEN (count(*) FILTER (
                    WHERE relative_humidity_raw IS NOT NULL) / 288.0
                ) > 0.7 THEN avg(relative_humidity_raw)
            ELSE NULL
        END AS relative_humidity_raw,
        CASE
            WHEN (count(*) FILTER (
                    WHERE relative_humidity_raw IS NOT NULL) / 288.0
                ) > 0.7 THEN min(relative_humidity_raw)
            ELSE NULL
        END AS relative_humidity_raw_min,
        CASE
            WHEN (count(*) FILTER (
                    WHERE relative_humidity_raw IS NOT NULL) / 288.0
                ) > 0.7 THEN max(relative_humidity_raw)
            ELSE NULL
        END AS relative_humidity_raw_max,
        CASE
            WHEN (count(*) FILTER (
                    WHERE specific_humidity IS NOT NULL) / 288.0
                ) > 0.7 THEN avg(specific_humidity)
            ELSE NULL
        END AS specific_humidity,
        CASE
            WHEN (count(*) FILTER (
                    WHERE specific_humidity IS NOT NULL) / 288.0
                ) > 0.7 THEN min(specific_humidity)
            ELSE NULL
        END AS specific_humidity_min,
        CASE
            WHEN (count(*) FILTER (
                    WHERE specific_humidity IS NOT NULL) / 288.0
                ) > 0.7 THEN max(specific_humidity)
            ELSE NULL
        END AS specific_humidity_max,
        CASE
            WHEN (count(*) FILTER (
                    WHERE wet_bulb_temperature IS NOT NULL) / 288.0
                ) > 0.7 THEN avg(wet_bulb_temperature)
            ELSE NULL
        END AS wet_bulb_temperature,
        CASE
            WHEN (count(*) FILTER (
                    WHERE wet_bulb_temperature IS NOT NULL) / 288.0
                ) > 0.7 THEN min(wet_bulb_temperature)
            ELSE NULL
        END AS wet_bulb_temperature_min,
        CASE
            WHEN (count(*) FILTER (
                    WHERE wet_bulb_temperature IS NOT NULL) / 288.0
                ) > 0.7 THEN max(wet_bulb_temperature)
            ELSE NULL
        END AS wet_bulb_temperature_max
    FROM all_data
    GROUP BY measured_at, station_id
    ORDER BY measured_at, station_id
    ''')  # noqa: E501
# END_GENERATED


@event.listens_for(TempRHData.__table__, 'after_create')
@event.listens_for(BiometData.__table__, 'after_create')
@event.listens_for(ATM41DataRaw.__table__, 'after_create')
@event.listens_for(SHT35DataRaw.__table__, 'after_create')
@event.listens_for(BLGDataRaw.__table__, 'after_create')
@event.listens_for(BuddyCheckQc.__table__, 'after_create')
def create_hypertable(target: Table, connection: Connection, **kwargs: Any) -> None:
    """Create a timescaledb hypertable for the given table if it doesn't exist.

    :param target: The table to create a hypertable for
    :param connection: The database connection to use
    :param kwargs: Additional keyword arguments (which are ignored)
    """
    connection.execute(
        text(
            '''\
            SELECT create_hypertable(
                :table,
                by_range('measured_at', INTERVAL '30 day'),
                if_not_exists => TRUE
            )
            ''',
        ),
        parameters={'table': target.name},
    )
