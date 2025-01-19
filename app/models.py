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
    """Representation of a station"""
    __tablename__ = 'station'

    # IDs
    name: Mapped[str] = mapped_column(Text, primary_key=True, index=True)
    device_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    long_name: Mapped[str] = mapped_column(Text, nullable=False)
    # the biomet stations have two components (ATM41 and BLG)
    blg_name: Mapped[str | None] = mapped_column(Text, unique=True, nullable=True)
    blg_device_id: Mapped[int | None] = mapped_column(
        BigInteger,
        unique=True,
        nullable=True,
    )
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
    setup_date: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True)
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

    # calibration information
    temp_calib_offset: Mapped[Decimal] = mapped_column(
        nullable=False, default=0,
        server_default='0',
    )
    relhum_calib_offset: Mapped[Decimal] = mapped_column(
        nullable=False,
        default=0,
        server_default='0',
    )

    comment: Mapped[str | None] = mapped_column(Text, nullable=True)

    # relationships
    blg_data_raw: Mapped[list[BLGDataRaw]] = relationship(
        back_populates='station',
        lazy=True,
    )
    atm41_data_raw: Mapped[list[ATM41DataRaw]] = relationship(
        back_populates='station',
        lazy=True,
    )
    sht35_data_raw: Mapped[list[SHT35DataRaw]] = relationship(
        back_populates='station',
        lazy=True,
    )
    biomet_data: Mapped[list[BiometData]] = relationship(
        back_populates='station',
        lazy=True,
    )
    temp_rh_data: Mapped[list[TempRHData]] = relationship(
        back_populates='station',
        lazy=True,
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


class _Data(Base):
    __abstract__ = True

    measured_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        primary_key=True,
        index=True,
    )
    name: Mapped[str] = mapped_column(
        Text,
        ForeignKey('station.name'),
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
    station: Mapped[Station] = relationship(back_populates='sht35_data_raw', lazy=True)


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
    station: Mapped[Station] = relationship(back_populates='atm41_data_raw', lazy=True)


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
    name: Mapped[str] = mapped_column(
        Text,
        ForeignKey('station.blg_name'),
        primary_key=True,
    )
    station: Mapped[Station] = relationship(back_populates='blg_data_raw', lazy=True)


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
            'ix_biomet_data_name_measured_at_desc',
            'name',
            desc('measured_at'),
        ),
    )

    # TODO: QC fields?
    station: Mapped[Station] = relationship(back_populates='biomet_data', lazy=True)


class TempRHData(_SHT35DataRawBase, _TempRHDerivatives):
    __tablename__ = 'temp_rh_data'
    __table_args__ = (
        Index(
            'ix_temp_rh_data_name_measured_at_desc',
            'name',
            desc('measured_at'),
        ),
    )

    air_temperature_raw: Mapped[Decimal] = mapped_column(nullable=True, comment='°C')
    relative_humidity_raw: Mapped[Decimal] = mapped_column(nullable=True, comment='%')
    # TODO: QC fields?
    station: Mapped[Station] = relationship(back_populates='temp_rh_data', lazy=True)


class MaterializedView(Base):
    """Baseclass for a materialized view"""
    __abstract__ = True
    # is this a timescale continuous aggregate?
    is_continuous_aggregate = False

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
    name: Mapped[str] = mapped_column(Text, nullable=False, unique=True, index=True)
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

    creation_sql = text('''\
    CREATE MATERIALIZED VIEW IF NOT EXISTS latest_data AS
    (
        SELECT DISTINCT ON (name)
            name,
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
            maximum_wind_speed
        FROM biomet_data INNER JOIN station USING(name)
        ORDER BY name, measured_at DESC
    )
    UNION ALL
    (
        SELECT DISTINCT ON (name)
            name,
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
            NULL
        FROM temp_rh_data INNER JOIN station USING(name)
        ORDER BY name, measured_at DESC
    )
    ''')


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
            'ix_biomet_data_hourly_name_measured_at',
            'name',
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

    creation_sql = text('''\
    CREATE MATERIALIZED VIEW IF NOT EXISTS biomet_data_hourly AS
    WITH data_bounds AS (
        SELECT
            name,
            MIN(measured_at) AS start_time,
            MAX(measured_at) AS end_time
        FROM biomet_data
        GROUP BY name
    ), filling_time_series AS (
        SELECT generate_series(
            DATE_TRUNC('hour', (SELECT min(measured_at) FROM biomet_data)),
            DATE_TRUNC('hour', (SELECT max(measured_at) FROM biomet_data) + '1 hour'::INTERVAL),
            '1 hour'::interval
        ) AS measured_at
    ),
    stations_subset AS (
        -- TODO: this could be faster if check the station table by station_type
        SELECT DISTINCT name FROM biomet_data
    ),
    time_station_combinations AS (
        SELECT
            measured_at,
            stations_subset.name,
            start_time,
            end_time
        FROM filling_time_series
        CROSS JOIN stations_subset
        JOIN data_bounds
            ON data_bounds.name = stations_subset.name
        WHERE filling_time_series.measured_at >= data_bounds.start_time
        AND filling_time_series.measured_at <= data_bounds.end_time
    ), all_data AS(
        (
            SELECT
                measured_at + '1hour'::INTERVAL AS ma,
                name,
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
                measured_at  + '1hour'::INTERVAL AS ma,
                name,
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
        time_bucket('1 hour', ma) AS measured_at,
        name,
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
    GROUP BY measured_at, name
    ORDER BY measured_at, name
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
            'ix_temp_rh_data_hourly_name_measured_at',
            'name',
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

    creation_sql = text('''\
    CREATE MATERIALIZED VIEW IF NOT EXISTS temp_rh_data_hourly AS
    WITH data_bounds AS (
        SELECT
            name,
            MIN(measured_at) AS start_time,
            MAX(measured_at) AS end_time
        FROM temp_rh_data
        GROUP BY name
    ), filling_time_series AS (
        SELECT generate_series(
            DATE_TRUNC('hour', (SELECT min(measured_at) FROM temp_rh_data)),
            DATE_TRUNC('hour', (SELECT max(measured_at) FROM temp_rh_data) + '1 hour'::INTERVAL),
            '1 hour'::interval
        ) AS measured_at
    ),
    stations_subset AS (
        -- TODO: this could be faster if check the station table by station_type
        SELECT DISTINCT name FROM temp_rh_data
    ),
    time_station_combinations AS (
        SELECT
            measured_at,
            stations_subset.name,
            start_time,
            end_time
        FROM filling_time_series
        CROSS JOIN stations_subset
        JOIN data_bounds
            ON data_bounds.name = stations_subset.name
        WHERE filling_time_series.measured_at >= data_bounds.start_time
        AND filling_time_series.measured_at <= data_bounds.end_time
    ), all_data AS(
        (
            SELECT
                measured_at + '1hour'::INTERVAL AS ma,
                name,
                NULL AS absolute_humidity,
                NULL AS air_temperature,
                NULL AS air_temperature_raw,
                NULL AS battery_voltage,
                NULL AS dew_point,
                NULL AS heat_index,
                NULL AS relative_humidity,
                NULL AS relative_humidity_raw,
                NULL AS wet_bulb_temperature
            FROM time_station_combinations
        )
        UNION ALL
        (
            SELECT
                measured_at  + '1hour'::INTERVAL AS ma,
                name,
                absolute_humidity,
                air_temperature,
                air_temperature_raw,
                battery_voltage,
                dew_point,
                heat_index,
                relative_humidity,
                relative_humidity_raw,
                wet_bulb_temperature
            FROM temp_rh_data
        )
    ) SELECT
        time_bucket('1 hour', ma) AS measured_at,
        name,
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
    GROUP BY measured_at, name
    ORDER BY measured_at, name
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
            'ix_biomet_data_daily_name_measured_at',
            'name',
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

    creation_sql = text('''\
    CREATE MATERIALIZED VIEW IF NOT EXISTS biomet_data_daily AS
    SELECT
        (time_bucket('1day', measured_at, 'CET') + '1 hour'::INTERVAL)::DATE as measured_at,
        name,
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
    FROM biomet_data
    GROUP BY (time_bucket('1day', measured_at, 'CET') + '1 hour'::INTERVAL)::DATE, name
    ORDER BY measured_at, name
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
            'ix_temp_rh_data_daily_name_measured_at',
            'name',
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

    creation_sql = text('''\
    CREATE MATERIALIZED VIEW IF NOT EXISTS temp_rh_data_daily AS
    SELECT
        (time_bucket('1day', measured_at, 'CET') + '1 hour'::INTERVAL)::DATE as measured_at,
        name,
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
    FROM temp_rh_data
    GROUP BY (time_bucket('1day', measured_at, 'CET') + '1 hour'::INTERVAL)::DATE, name
    ORDER BY measured_at, name
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
