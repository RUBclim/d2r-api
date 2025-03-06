from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any

from psycopg import sql
from sqlalchemy import BigInteger
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
    temprh = 'temprh'
    biomet = 'biomet'
    double = 'double'


class SensorType(StrEnum):
    atm41 = 'atm41'
    sht35 = 'sht35'
    blg = 'blg'


class HeatStressCategories(StrEnum):
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

# we need this for pandas to be able to insert enums via .to_sql
_HeatStressCategories = ENUM(HeatStressCategories)


class Station(Base):
    """Representation of a station which has a physical location and sensor(s) attached
    to it."""
    __tablename__ = 'station'

    # IDs
    station_id: Mapped[str] = mapped_column(Text, primary_key=True, index=True)
    long_name: Mapped[str] = mapped_column(Text, nullable=False)
    station_type: Mapped[StationType] = mapped_column(nullable=False)

    # geographical position
    latitude: Mapped[float] = mapped_column(nullable=False)
    longitude: Mapped[float] = mapped_column(nullable=False)
    altitude: Mapped[float] = mapped_column(nullable=False)

    # address information
    street: Mapped[str] = mapped_column(Text, nullable=False)
    number: Mapped[str | None] = mapped_column(Text, nullable=True)
    plz: Mapped[int] = mapped_column(nullable=False)
    city: Mapped[str] = mapped_column(Text, nullable=False)
    country: Mapped[str] = mapped_column(Text, nullable=False)
    district: Mapped[str] = mapped_column(Text, nullable=False)

    # siting information
    lcz: Mapped[str | None] = mapped_column(Text, nullable=True)
    dominant_land_use: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
        comment='e.g. residential, commercial, industrial, ...',
    )
    urban_atlas_class_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    urban_atlas_class_nr: Mapped[int | None] = mapped_column(nullable=True)
    orographic_setting: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
        comment='e.g. Flat, Hilly',
    )
    svf: Mapped[Decimal] = mapped_column(nullable=True)
    artificial_heat_sources: Mapped[str] = mapped_column(
        Text,
        nullable=True,
        comment='e.g. cars, buildings, ...',
    )
    proximity_to_building: Mapped[Decimal | None] = mapped_column(nullable=True)
    proximity_to_parking: Mapped[Decimal | None] = mapped_column(nullable=True)
    proximity_to_tree: Mapped[Decimal | None] = mapped_column(nullable=True)
    surrounding_land_cover_description: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
        comment='a text describing the surrounding land cover',
    )

    # mounting information
    mounting_type: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
        comment='the structure the sensor is mounted to e.g. black mast, building, ...',
    )
    leuchtennummer: Mapped[int] = mapped_column(nullable=False)
    mounting_structure_material: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
        comment=(
            'The material the structure the sensor is mounted to is made of e.g. '
            'metal, wood, ...'
        ),
    )
    mounting_structure_height_agl: Mapped[Decimal | None] = mapped_column(
        nullable=True,
        comment='the total height of the mounting structure above ground level',
    )
    mounting_structure_diameter: Mapped[Decimal | None] = mapped_column(
        nullable=True,
        comment='the diameter of the mounting structure at the mounting height',
    )
    mounting_structure_light_extension_offset: Mapped[Decimal | None] = mapped_column(
        nullable=True,
        comment='when mounted to a lantern post, the overhang of the lantern',
    )
    sensor_height_agl: Mapped[Decimal | None] = mapped_column(
        nullable=True,
        comment=(
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
    )
    sensor_orientation: Mapped[Decimal | None] = mapped_column(
        Text,
        nullable=True,
        comment=(
            'the orientation (-angle) of the arm of the main component of the station '
            '(ATM41 or SHT35) from the mounting structure'
        ),
    )
    blg_sensor_height_agl: Mapped[Decimal | None] = mapped_column(
        nullable=True,
        comment=(
            'the mounting height of the black globe sensor of the station'
        ),
    )
    blg_sensor_distance_from_mounting_structure: Mapped[Decimal | None] = mapped_column(
        Text,
        nullable=True,
        comment=(
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
    )
    comment: Mapped[str | None] = mapped_column(Text, nullable=True)

    # relationships
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
        lazy='selectin',
        order_by='SensorDeployment.setup_date',
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
        lazy='selectin',
        order_by='SensorDeployment.setup_date',
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
        lazy='selectin',
        order_by='SensorDeployment.setup_date',
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
        lazy='selectin',
        order_by='SensorDeployment.setup_date',
    )
    deployments: Mapped[list[SensorDeployment]] = relationship(
        back_populates='station',
        lazy='selectin',
        order_by='SensorDeployment.setup_date, SensorDeployment.deployment_id',
    )

    @property
    def full_address(self) -> str:
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

    sensor: Mapped[Sensor] = relationship(
        'Sensor',
        back_populates='deployments',
        lazy='selectin',
    )
    station: Mapped[Station] = relationship(
        back_populates='deployments',
        lazy='selectin',
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

    sensor_id: Mapped[str] = mapped_column(primary_key=True)
    device_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    sensor_type: Mapped[SensorType] = mapped_column(nullable=False)
    # calibration information
    temp_calib_offset: Mapped[Decimal] = mapped_column(
        nullable=False,
        default=0,
        server_default='0',
    )
    relhum_calib_offset: Mapped[Decimal] = mapped_column(
        nullable=False,
        default=0,
        server_default='0',
    )

    # relationships
    deployments: Mapped[list[SensorDeployment]] = relationship(
        back_populates='sensor',
        lazy='selectin',
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
        lazy='selectin',
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
        lazy='selectin',
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


class _Data(Base):
    __abstract__ = True

    measured_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        primary_key=True,
        index=True,
    )
    battery_voltage: Mapped[Decimal] = mapped_column(nullable=True, comment='Volts')
    protocol_version: Mapped[int] = mapped_column(nullable=True)


class _SHT35DataRawBase(_Data):
    __abstract__ = True

    air_temperature: Mapped[Decimal] = mapped_column(nullable=True, comment='°C')
    relative_humidity: Mapped[Decimal] = mapped_column(nullable=True, comment='%')


class SHT35DataRaw(_SHT35DataRawBase):
    __tablename__ = 'sht35_data_raw'
    sensor_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey('sensor.sensor_id'),
        primary_key=True,
        index=True,
    )
    sensor: Mapped[Sensor] = relationship(lazy='selectin')

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

    air_temperature: Mapped[Decimal] = mapped_column(nullable=True, comment='°C')
    relative_humidity: Mapped[Decimal] = mapped_column(nullable=True, comment='%')
    atmospheric_pressure: Mapped[Decimal] = mapped_column(nullable=True, comment='kPa')
    vapor_pressure: Mapped[Decimal] = mapped_column(nullable=True, comment='kPa')
    wind_speed: Mapped[Decimal] = mapped_column(nullable=True, comment='m/s')
    wind_direction: Mapped[Decimal] = mapped_column(nullable=True, comment='°')
    u_wind: Mapped[Decimal] = mapped_column(nullable=True, comment='m/s')
    v_wind: Mapped[Decimal] = mapped_column(nullable=True, comment='m/s')
    maximum_wind_speed: Mapped[Decimal] = mapped_column(nullable=True, comment='m/s')
    precipitation_sum: Mapped[Decimal] = mapped_column(nullable=True, comment='mm')
    solar_radiation: Mapped[Decimal] = mapped_column(nullable=True, comment='W/m2')
    lightning_average_distance: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='km',
    )
    lightning_strike_count: Mapped[Decimal] = mapped_column(nullable=True, comment='-')
    sensor_temperature_internal: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
    )
    x_orientation_angle: Mapped[Decimal] = mapped_column(nullable=True, comment='°')
    y_orientation_angle: Mapped[Decimal] = mapped_column(nullable=True, comment='°')


class ATM41DataRaw(_ATM41DataRawBase):
    __tablename__ = 'atm41_data_raw'
    sensor_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey('sensor.sensor_id'),
        primary_key=True,
        index=True,
    )
    sensor: Mapped[Sensor] = relationship(lazy='selectin')


class _BLGDataRawBase(_Data):
    __abstract__ = True
    black_globe_temperature: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
    )
    thermistor_resistance: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='Ohms',
    )
    voltage_ratio: Mapped[Decimal] = mapped_column(nullable=True, comment='-')


class BLGDataRaw(_BLGDataRawBase):
    __tablename__ = 'blg_data_raw'
    sensor_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey('sensor.sensor_id'),
        primary_key=True,
        index=True,
    )
    sensor: Mapped[Sensor] = relationship(lazy='selectin')


class _TempRHDerivatives(Base):
    __abstract__ = True
    dew_point: Mapped[Decimal] = mapped_column(nullable=True, comment='°C')
    absolute_humidity: Mapped[Decimal] = mapped_column(nullable=True, comment='g/m3')
    heat_index: Mapped[Decimal] = mapped_column(nullable=True, comment='°C')
    wet_bulb_temperature: Mapped[Decimal] = mapped_column(nullable=True, comment='°C')


class _BiometDerivatives(Base):
    __abstract__ = True
    blg_time_offset: Mapped[Decimal] = mapped_column(nullable=True, comment='seconds')
    mrt: Mapped[Decimal] = mapped_column(nullable=True, comment='°C')
    utci: Mapped[Decimal] = mapped_column(nullable=True, comment='°C')
    utci_category: Mapped[HeatStressCategories] = mapped_column(nullable=True)
    pet: Mapped[Decimal] = mapped_column(nullable=True, comment='°C')
    pet_category: Mapped[HeatStressCategories] = mapped_column(nullable=True)
    # we've converted it to hPa in the meantime
    atmospheric_pressure: Mapped[Decimal] = mapped_column(nullable=True, comment='hPa')
    atmospheric_pressure_reduced: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='hPa',
    )
    vapor_pressure: Mapped[Decimal] = mapped_column(nullable=True, comment='hPa')
    # we need this as an alias in the big biomet table
    blg_battery_voltage: Mapped[Decimal] = mapped_column(nullable=True, comment='V')


class _CalibrationDerivatives(Base):
    __abstract__ = True
    air_temperature_raw: Mapped[Decimal] = mapped_column(nullable=True, comment='°C')
    relative_humidity_raw: Mapped[Decimal] = mapped_column(nullable=True, comment='%')


class BiometData(
    _ATM41DataRawBase, _BLGDataRawBase, _TempRHDerivatives, _BiometDerivatives,
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
    )
    sensor_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey('sensor.sensor_id'),
        index=True,
    )
    blg_sensor_id: Mapped[str | None] = mapped_column(
        Text,
        ForeignKey('sensor.sensor_id'),
        index=True,
        # this needs to be nullable, since we may have measurements of the ATM41 sensor
        # that do not have corresponding blackglobe measurements
        nullable=True,
    )

    station: Mapped[Station] = relationship(lazy=True)
    sensor: Mapped[Sensor] = relationship(
        # this should only ever be a biomet sensor, but just to make sure!
        primaryjoin='and_(BiometData.sensor_id == Sensor.sensor_id, Sensor.sensor_type == "atm41")',  # noqa: E501
        viewonly=True,
        lazy='selectin',
    )
    blg_sensor: Mapped[Sensor | None] = relationship(
        primaryjoin='and_(BiometData.blg_sensor_id == Sensor.sensor_id, Sensor.sensor_type == "blg")',  # noqa: E501
        viewonly=True,
        lazy='selectin',
    )


class TempRHData(_SHT35DataRawBase, _TempRHDerivatives, _CalibrationDerivatives):
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
    )
    sensor_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey('sensor.sensor_id'),
        index=True,
    )
    station: Mapped[Station] = relationship(lazy=True)
    sensor: Mapped[Sensor] = relationship(lazy=True)


class MaterializedView(Base):
    """Baseclass for a materialized view"""
    __abstract__ = True
    # is this a timescale continuous aggregate?
    is_continuous_aggregate = False

    station_id: Mapped[str] = mapped_column(
        ForeignKey(
            'station.station_id',
        ), primary_key=True, index=True,
    )

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
):
    """This is not an actual table, but a materialized view. We simply trick sqlalchemy
    into thinking this was a table. Querying a materialized view does not differ from
    querying a proper table.

    The query for creating this materialized view is saved above.
    """
    __tablename__ = 'latest_data'

    station_id: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        unique=True,
        index=True,
    )
    long_name: Mapped[str] = mapped_column(Text, nullable=False)
    latitude: Mapped[float] = mapped_column(nullable=False)
    longitude: Mapped[float] = mapped_column(nullable=False)
    altitude: Mapped[float] = mapped_column(nullable=False)
    district: Mapped[str] = mapped_column(Text, nullable=True, index=True)
    lcz: Mapped[str] = mapped_column(Text, nullable=True)
    station_type: Mapped[StationType] = mapped_column(nullable=False)
    mrt: Mapped[Decimal] = mapped_column(nullable=True, comment='°C')
    utci: Mapped[Decimal] = mapped_column(nullable=True, comment='°C')
    utci_category: Mapped[HeatStressCategories] = mapped_column(nullable=True)
    pet: Mapped[Decimal] = mapped_column(nullable=True, comment='°C')
    pet_category: Mapped[HeatStressCategories] = mapped_column(nullable=True)
    # we've converted it to hPa in the meantime
    atmospheric_pressure: Mapped[Decimal] = mapped_column(nullable=True, comment='hPa')
    atmospheric_pressure_reduced: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='hPa',
    )
    vapor_pressure: Mapped[Decimal] = mapped_column(nullable=True, comment='hPa')

    # we exclude the temprh part of a double station here and only use the biomet part
    creation_sql = text('''\
    CREATE MATERIALIZED VIEW IF NOT EXISTS latest_data AS
    (
        SELECT DISTINCT ON (station_id)
            station_id,
            long_name,
            latitude,
            longitude,
            altitude,
            district,
            lcz,
            station_type,
            measured_at,
            air_temperature,
            relative_humidity,
            dew_point,
            absolute_humidity,
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
            battery_voltage,
            protocol_version
        FROM biomet_data INNER JOIN station USING(station_id)
        ORDER BY station_id, measured_at DESC
    )
    UNION ALL
    (
        SELECT DISTINCT ON (station_id)
            station_id,
            long_name,
            latitude,
            longitude,
            altitude,
            district,
            lcz,
            station_type,
            measured_at,
            air_temperature,
            relative_humidity,
            dew_point,
            absolute_humidity,
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
            battery_voltage,
            protocol_version
        FROM temp_rh_data INNER JOIN station USING(station_id)
        WHERE station.station_type <> 'double'
        ORDER BY station_id, measured_at DESC
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
    )
    absolute_humidity_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='g/m3',
    )
    air_temperature_min: Mapped[Decimal] = mapped_column(nullable=True, comment='°C')
    air_temperature_max: Mapped[Decimal] = mapped_column(nullable=True, comment='°C')
    atmospheric_pressure_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='kPa',
    )
    atmospheric_pressure_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='kPa',
    )
    atmospheric_pressure_reduced_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='hPa',
    )
    atmospheric_pressure_reduced_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='hPa',
    )
    battery_voltage_min: Mapped[Decimal] = mapped_column(nullable=True, comment='Volts')
    battery_voltage_max: Mapped[Decimal] = mapped_column(nullable=True, comment='Volts')
    black_globe_temperature_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
    )
    black_globe_temperature_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
    )
    blg_battery_voltage_min: Mapped[Decimal] = mapped_column(nullable=True, comment='V')
    blg_battery_voltage_max: Mapped[Decimal] = mapped_column(nullable=True, comment='V')
    blg_time_offset_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='seconds',
    )
    blg_time_offset_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='seconds',
    )
    dew_point_min: Mapped[Decimal] = mapped_column(nullable=True, comment='°C')
    dew_point_max: Mapped[Decimal] = mapped_column(nullable=True, comment='°C')
    heat_index_min: Mapped[Decimal] = mapped_column(nullable=True, comment='°C')
    heat_index_max: Mapped[Decimal] = mapped_column(nullable=True, comment='°C')
    lightning_average_distance_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='km',
    )
    lightning_average_distance_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='km',
    )
    mrt_min: Mapped[Decimal] = mapped_column(nullable=True, comment='°C')
    mrt_max: Mapped[Decimal] = mapped_column(nullable=True, comment='°C')
    pet_min: Mapped[Decimal] = mapped_column(nullable=True, comment='°C')
    pet_max: Mapped[Decimal] = mapped_column(nullable=True, comment='°C')
    relative_humidity_min: Mapped[Decimal] = mapped_column(nullable=True, comment='%')
    relative_humidity_max: Mapped[Decimal] = mapped_column(nullable=True, comment='%')
    sensor_temperature_internal_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
    )
    sensor_temperature_internal_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
    )
    solar_radiation_min: Mapped[Decimal] = mapped_column(nullable=True, comment='W/m2')
    solar_radiation_max: Mapped[Decimal] = mapped_column(nullable=True, comment='W/m2')
    thermistor_resistance_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='Ohms',
    )
    thermistor_resistance_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='Ohms',
    )
    u_wind_min: Mapped[Decimal] = mapped_column(nullable=True, comment='m/s')
    u_wind_max: Mapped[Decimal] = mapped_column(nullable=True, comment='m/s')
    utci_min: Mapped[Decimal] = mapped_column(nullable=True, comment='°C')
    utci_max: Mapped[Decimal] = mapped_column(nullable=True, comment='°C')
    v_wind_min: Mapped[Decimal] = mapped_column(nullable=True, comment='m/s')
    v_wind_max: Mapped[Decimal] = mapped_column(nullable=True, comment='m/s')
    vapor_pressure_min: Mapped[Decimal] = mapped_column(nullable=True, comment='kPa')
    vapor_pressure_max: Mapped[Decimal] = mapped_column(nullable=True, comment='kPa')
    voltage_ratio_min: Mapped[Decimal] = mapped_column(nullable=True, comment='-')
    voltage_ratio_max: Mapped[Decimal] = mapped_column(nullable=True, comment='-')
    wet_bulb_temperature_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
    )
    wet_bulb_temperature_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
    )
    wind_speed_min: Mapped[Decimal] = mapped_column(nullable=True, comment='m/s')
    wind_speed_max: Mapped[Decimal] = mapped_column(nullable=True, comment='m/s')
    x_orientation_angle_min: Mapped[Decimal] = mapped_column(nullable=True, comment='°')
    x_orientation_angle_max: Mapped[Decimal] = mapped_column(nullable=True, comment='°')
    y_orientation_angle_min: Mapped[Decimal] = mapped_column(nullable=True, comment='°')
    y_orientation_angle_max: Mapped[Decimal] = mapped_column(nullable=True, comment='°')
    station: Mapped[Station] = relationship(lazy=True)

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
        avg(lightning_average_distance) AS lightning_average_distance,
        min(lightning_average_distance) AS lightning_average_distance_min,
        max(lightning_average_distance) AS lightning_average_distance_max,
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
    )
    absolute_humidity_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='g/m3',
    )
    air_temperature_min: Mapped[Decimal] = mapped_column(nullable=True, comment='°C')
    air_temperature_max: Mapped[Decimal] = mapped_column(nullable=True, comment='°C')
    air_temperature_raw_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
    )
    air_temperature_raw_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
    )
    battery_voltage_min: Mapped[Decimal] = mapped_column(nullable=True, comment='Volts')
    battery_voltage_max: Mapped[Decimal] = mapped_column(nullable=True, comment='Volts')
    dew_point_min: Mapped[Decimal] = mapped_column(nullable=True, comment='°C')
    dew_point_max: Mapped[Decimal] = mapped_column(nullable=True, comment='°C')
    heat_index_min: Mapped[Decimal] = mapped_column(nullable=True, comment='°C')
    heat_index_max: Mapped[Decimal] = mapped_column(nullable=True, comment='°C')
    relative_humidity_min: Mapped[Decimal] = mapped_column(nullable=True, comment='%')
    relative_humidity_max: Mapped[Decimal] = mapped_column(nullable=True, comment='%')
    relative_humidity_raw_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='%',
    )
    relative_humidity_raw_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='%',
    )
    wet_bulb_temperature_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
    )
    wet_bulb_temperature_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
    )
    station: Mapped[Station] = relationship(lazy=True)

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
    )
    absolute_humidity_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='g/m3',
    )
    air_temperature_min: Mapped[Decimal] = mapped_column(nullable=True, comment='°C')
    air_temperature_max: Mapped[Decimal] = mapped_column(nullable=True, comment='°C')
    atmospheric_pressure_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='kPa',
    )
    atmospheric_pressure_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='kPa',
    )
    atmospheric_pressure_reduced_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='hPa',
    )
    atmospheric_pressure_reduced_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='hPa',
    )
    battery_voltage_min: Mapped[Decimal] = mapped_column(nullable=True, comment='Volts')
    battery_voltage_max: Mapped[Decimal] = mapped_column(nullable=True, comment='Volts')
    black_globe_temperature_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
    )
    black_globe_temperature_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
    )
    blg_battery_voltage_min: Mapped[Decimal] = mapped_column(nullable=True, comment='V')
    blg_battery_voltage_max: Mapped[Decimal] = mapped_column(nullable=True, comment='V')
    blg_time_offset_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='seconds',
    )
    blg_time_offset_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='seconds',
    )
    dew_point_min: Mapped[Decimal] = mapped_column(nullable=True, comment='°C')
    dew_point_max: Mapped[Decimal] = mapped_column(nullable=True, comment='°C')
    heat_index_min: Mapped[Decimal] = mapped_column(nullable=True, comment='°C')
    heat_index_max: Mapped[Decimal] = mapped_column(nullable=True, comment='°C')
    lightning_average_distance_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='km',
    )
    lightning_average_distance_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='km',
    )
    mrt_min: Mapped[Decimal] = mapped_column(nullable=True, comment='°C')
    mrt_max: Mapped[Decimal] = mapped_column(nullable=True, comment='°C')
    pet_min: Mapped[Decimal] = mapped_column(nullable=True, comment='°C')
    pet_max: Mapped[Decimal] = mapped_column(nullable=True, comment='°C')
    relative_humidity_min: Mapped[Decimal] = mapped_column(nullable=True, comment='%')
    relative_humidity_max: Mapped[Decimal] = mapped_column(nullable=True, comment='%')
    sensor_temperature_internal_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
    )
    sensor_temperature_internal_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
    )
    solar_radiation_min: Mapped[Decimal] = mapped_column(nullable=True, comment='W/m2')
    solar_radiation_max: Mapped[Decimal] = mapped_column(nullable=True, comment='W/m2')
    thermistor_resistance_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='Ohms',
    )
    thermistor_resistance_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='Ohms',
    )
    u_wind_min: Mapped[Decimal] = mapped_column(nullable=True, comment='m/s')
    u_wind_max: Mapped[Decimal] = mapped_column(nullable=True, comment='m/s')
    utci_min: Mapped[Decimal] = mapped_column(nullable=True, comment='°C')
    utci_max: Mapped[Decimal] = mapped_column(nullable=True, comment='°C')
    v_wind_min: Mapped[Decimal] = mapped_column(nullable=True, comment='m/s')
    v_wind_max: Mapped[Decimal] = mapped_column(nullable=True, comment='m/s')
    vapor_pressure_min: Mapped[Decimal] = mapped_column(nullable=True, comment='kPa')
    vapor_pressure_max: Mapped[Decimal] = mapped_column(nullable=True, comment='kPa')
    voltage_ratio_min: Mapped[Decimal] = mapped_column(nullable=True, comment='-')
    voltage_ratio_max: Mapped[Decimal] = mapped_column(nullable=True, comment='-')
    wet_bulb_temperature_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
    )
    wet_bulb_temperature_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
    )
    wind_speed_min: Mapped[Decimal] = mapped_column(nullable=True, comment='m/s')
    wind_speed_max: Mapped[Decimal] = mapped_column(nullable=True, comment='m/s')
    x_orientation_angle_min: Mapped[Decimal] = mapped_column(nullable=True, comment='°')
    x_orientation_angle_max: Mapped[Decimal] = mapped_column(nullable=True, comment='°')
    y_orientation_angle_min: Mapped[Decimal] = mapped_column(nullable=True, comment='°')
    y_orientation_angle_max: Mapped[Decimal] = mapped_column(nullable=True, comment='°')
    station: Mapped[Station] = relationship(lazy=True)

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
                ) > 0.7 THEN avg(lightning_average_distance)
            ELSE NULL
        END AS lightning_average_distance,
        CASE
            WHEN (count(*) FILTER (
                    WHERE lightning_average_distance IS NOT NULL) / 288.0
                ) > 0.7 THEN min(lightning_average_distance)
            ELSE NULL
        END AS lightning_average_distance_min,
        CASE
            WHEN (count(*) FILTER (
                    WHERE lightning_average_distance IS NOT NULL) / 288.0
                ) > 0.7 THEN max(lightning_average_distance)
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
    )
    absolute_humidity_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='g/m3',
    )
    air_temperature_min: Mapped[Decimal] = mapped_column(nullable=True, comment='°C')
    air_temperature_max: Mapped[Decimal] = mapped_column(nullable=True, comment='°C')
    air_temperature_raw_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
    )
    air_temperature_raw_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
    )
    battery_voltage_min: Mapped[Decimal] = mapped_column(nullable=True, comment='Volts')
    battery_voltage_max: Mapped[Decimal] = mapped_column(nullable=True, comment='Volts')
    dew_point_min: Mapped[Decimal] = mapped_column(nullable=True, comment='°C')
    dew_point_max: Mapped[Decimal] = mapped_column(nullable=True, comment='°C')
    heat_index_min: Mapped[Decimal] = mapped_column(nullable=True, comment='°C')
    heat_index_max: Mapped[Decimal] = mapped_column(nullable=True, comment='°C')
    relative_humidity_min: Mapped[Decimal] = mapped_column(nullable=True, comment='%')
    relative_humidity_max: Mapped[Decimal] = mapped_column(nullable=True, comment='%')
    relative_humidity_raw_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='%',
    )
    relative_humidity_raw_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='%',
    )
    wet_bulb_temperature_min: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
    )
    wet_bulb_temperature_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
    )
    station: Mapped[Station] = relationship(lazy=True)

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
def create_hypertable(target: Table, connection: Connection, **kwargs: Any) -> None:
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
