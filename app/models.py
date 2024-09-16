from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any

from sqlalchemy import BigInteger
from sqlalchemy import Connection
from sqlalchemy import DateTime
from sqlalchemy import event
from sqlalchemy import ForeignKey
from sqlalchemy import Table
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import ENUM
from sqlalchemy.ext.asyncio import AsyncSession
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

    name: Mapped[str] = mapped_column(primary_key=True)
    device_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    long_name: Mapped[str] = mapped_column(nullable=False)
    latitude: Mapped[float] = mapped_column(nullable=False)
    longitude: Mapped[float] = mapped_column(nullable=False)
    altitude: Mapped[float] = mapped_column(nullable=False)
    station_type: Mapped[StationType] = mapped_column(nullable=False)
    # the biomet stations have two components (ATM41 and BLG)
    blg_name: Mapped[str] = mapped_column(unique=True, nullable=True)
    blg_device_id: Mapped[int] = mapped_column(
        BigInteger,
        unique=True,
        nullable=True,
    )
    street: Mapped[str] = mapped_column(nullable=True)
    number: Mapped[str] = mapped_column(nullable=True)
    plz: Mapped[int] = mapped_column(nullable=True)
    leuchtennummer: Mapped[int] = mapped_column(nullable=False)
    district: Mapped[str] = mapped_column(nullable=False)
    comment: Mapped[str] = mapped_column(nullable=True)
    setup_date: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True)
    # TODO: add more station metadata
    lcz: Mapped[str] = mapped_column(nullable=True)
    svf: Mapped[Decimal] = mapped_column(nullable=True)
    temp_calib_offset: Mapped[Decimal] = mapped_column(
        nullable=False, default=0,
        server_default='0',
    )
    relhum_calib_offset: Mapped[Decimal] = mapped_column(
        nullable=False,
        default=0,
        server_default='0',
    )
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


class _Data(Base):
    __abstract__ = True

    measured_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        primary_key=True,
    )
    name: Mapped[str] = mapped_column(
        ForeignKey('station.name'),
        primary_key=True,
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
    name: Mapped[str] = mapped_column(ForeignKey('station.blg_name'), primary_key=True)
    station: Mapped[Station] = relationship(back_populates='blg_data_raw', lazy=True)


class _TempRHDerivatives(Base):
    __abstract__ = True
    dew_point: Mapped[Decimal] = mapped_column(nullable=True, comment='°C')
    absolute_humidity: Mapped[Decimal] = mapped_column(nullable=True, comment='g/m3')
    heat_index: Mapped[Decimal] = mapped_column(nullable=True, comment='°C')
    wet_bulb_temperature: Mapped[Decimal] = mapped_column(nullable=True, comment='°C')


class _BiometDerivatives(Base):
    __abstract__ = True
    blg_time_offset: Mapped[float] = mapped_column(nullable=True, comment='seconds')
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
    # TODO: QC fields?
    station: Mapped[Station] = relationship(back_populates='biomet_data', lazy=True)


class TempRHData(_SHT35DataRawBase, _TempRHDerivatives):
    __tablename__ = 'temp_rh_data'
    air_temperature_raw: Mapped[Decimal] = mapped_column(nullable=True, comment='°C')
    relative_humidity_raw: Mapped[Decimal] = mapped_column(nullable=True, comment='%')
    # TODO: QC fields?
    station: Mapped[Station] = relationship(back_populates='temp_rh_data', lazy=True)


class LatestData(_ATM41DataRawBase, _BLGDataRawBase, _TempRHDerivatives):
    """This is not an actual table, but a materialized view. We simply trick sqlalchemy
    into thinking this was a table. Querying a materialized view does not differ from
    querying a proper table.

    The query for creating this materialized view is saved above.
    """
    __tablename__ = 'latest_data'
    long_name: Mapped[str] = mapped_column(nullable=False)
    latitude: Mapped[float] = mapped_column(nullable=False)
    longitude: Mapped[float] = mapped_column(nullable=False)
    altitude: Mapped[float] = mapped_column(nullable=False)
    district: Mapped[str] = mapped_column(nullable=True)
    lcz: Mapped[str] = mapped_column(nullable=True)
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

    @classmethod
    async def refresh(cls, db: AsyncSession) -> None:
        await db.execute(text('REFRESH MATERIALIZED VIEW latest_data'))

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
    _ATM41DataRawBase, _BLGDataRawBase, _TempRHDerivatives, _BiometDerivatives,
):
    """This is not an actual table, but a materialized view. We simply trick sqlalchemy
    into thinking this was a table. Querying a materialized view does not differ from
    querying a proper table.
    """
    __tablename__ = 'biomet_data_hourly'

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
    blg_time_offset_min: Mapped[float] = mapped_column(nullable=True, comment='seconds')
    blg_time_offset_max: Mapped[float] = mapped_column(nullable=True, comment='seconds')
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

    @classmethod
    async def refresh(cls) -> None:
        async with sessionmanager.connect(as_transaction=False) as sess:
            await sess.execute(
                text("CALL refresh_continuous_aggregate('biomet_data_hourly', NULL, NULL)"),  # noqa: E501
            )

    creation_sql = text('''\
    CREATE MATERIALIZED VIEW IF NOT EXISTS biomet_data_hourly(
        measured_at,
        name,
        absolute_humidity,
        absolute_humidity_min,
        absolute_humidity_max,
        air_temperature,
        air_temperature_min,
        air_temperature_max,
        atmospheric_pressure,
        atmospheric_pressure_min,
        atmospheric_pressure_max,
        atmospheric_pressure_reduced,
        atmospheric_pressure_reduced_min,
        atmospheric_pressure_reduced_max,
        battery_voltage,
        battery_voltage_min,
        battery_voltage_max,
        black_globe_temperature,
        black_globe_temperature_min,
        black_globe_temperature_max,
        blg_battery_voltage,
        blg_battery_voltage_min,
        blg_battery_voltage_max,
        blg_time_offset,
        blg_time_offset_min,
        blg_time_offset_max,
        dew_point,
        dew_point_min,
        dew_point_max,
        heat_index,
        heat_index_min,
        heat_index_max,
        lightning_average_distance,
        lightning_average_distance_min,
        lightning_average_distance_max,
        lightning_strike_count,
        maximum_wind_speed,
        mrt,
        mrt_min,
        mrt_max,
        pet,
        pet_min,
        pet_max,
        pet_category,
        precipitation_sum,
        relative_humidity,
        relative_humidity_min,
        relative_humidity_max,
        sensor_temperature_internal,
        sensor_temperature_internal_min,
        sensor_temperature_internal_max,
        solar_radiation,
        solar_radiation_min,
        solar_radiation_max,
        thermistor_resistance,
        thermistor_resistance_min,
        thermistor_resistance_max,
        u_wind,
        u_wind_min,
        u_wind_max,
        utci,
        utci_min,
        utci_max,
        utci_category,
        v_wind,
        v_wind_min,
        v_wind_max,
        vapor_pressure,
        vapor_pressure_min,
        vapor_pressure_max,
        voltage_ratio,
        voltage_ratio_min,
        voltage_ratio_max,
        wet_bulb_temperature,
        wet_bulb_temperature_min,
        wet_bulb_temperature_max,
        wind_direction,
        wind_speed,
        wind_speed_min,
        wind_speed_max,
        x_orientation_angle,
        x_orientation_angle_min,
        x_orientation_angle_max,
        y_orientation_angle,
        y_orientation_angle_min,
        y_orientation_angle_max
    )
    WITH (timescaledb.continuous, timescaledb.materialized_only = true) AS
        SELECT
            time_bucket('1hour', measured_at) AT TIME ZONE 'UTC' + '1 hour',
            name,
            avg(biomet_data.absolute_humidity) AS absolute_humidity,
            min(biomet_data.absolute_humidity) AS absolute_humidity_min,
            max(biomet_data.absolute_humidity) AS absolute_humidity_max,
            avg(biomet_data.air_temperature) AS air_temperature,
            min(biomet_data.air_temperature) AS air_temperature_min,
            max(biomet_data.air_temperature) AS air_temperature_max,
            avg(biomet_data.atmospheric_pressure) AS atmospheric_pressure,
            min(biomet_data.atmospheric_pressure) AS atmospheric_pressure_min,
            max(biomet_data.atmospheric_pressure) AS atmospheric_pressure_max,
            avg(biomet_data.atmospheric_pressure_reduced) AS atmospheric_pressure_reduced,
            min(biomet_data.atmospheric_pressure_reduced) AS atmospheric_pressure_reduced_min,
            max(biomet_data.atmospheric_pressure_reduced) AS atmospheric_pressure_reduced_max,
            avg(biomet_data.battery_voltage) AS battery_voltage,
            min(biomet_data.battery_voltage) AS battery_voltage_min,
            max(biomet_data.battery_voltage) AS battery_voltage_max,
            avg(biomet_data.black_globe_temperature) AS black_globe_temperature,
            min(biomet_data.black_globe_temperature) AS black_globe_temperature_min,
            max(biomet_data.black_globe_temperature) AS black_globe_temperature_max,
            avg(biomet_data.blg_battery_voltage) AS blg_battery_voltage,
            min(biomet_data.blg_battery_voltage) AS blg_battery_voltage_min,
            max(biomet_data.blg_battery_voltage) AS blg_battery_voltage_max,
            avg(biomet_data.blg_time_offset) AS blg_time_offset,
            min(biomet_data.blg_time_offset) AS blg_time_offset_min,
            max(biomet_data.blg_time_offset) AS blg_time_offset_max,
            avg(biomet_data.dew_point) AS dew_point,
            min(biomet_data.dew_point) AS dew_point_min,
            max(biomet_data.dew_point) AS dew_point_max,
            avg(biomet_data.heat_index) AS heat_index,
            min(biomet_data.heat_index) AS heat_index_min,
            max(biomet_data.heat_index) AS heat_index_max,
            avg(biomet_data.lightning_average_distance) AS lightning_average_distance,
            min(biomet_data.lightning_average_distance) AS lightning_average_distance_min,
            max(biomet_data.lightning_average_distance) AS lightning_average_distance_max,
            sum(biomet_data.lightning_strike_count) AS lightning_strike_count,
            max(biomet_data.maximum_wind_speed) AS maximum_wind_speed,
            avg(biomet_data.mrt) AS mrt,
            min(biomet_data.mrt) AS mrt_min,
            max(biomet_data.mrt) AS mrt_max,
            avg(biomet_data.pet) AS pet,
            min(biomet_data.pet) AS pet_min,
            max(biomet_data.pet) AS pet_max,
            mode() WITHIN GROUP (ORDER BY biomet_data.pet_category ASC) AS pet_category,
            sum(biomet_data.precipitation_sum) AS precipitation_sum,
            avg(biomet_data.relative_humidity) AS relative_humidity,
            min(biomet_data.relative_humidity) AS relative_humidity_min,
            max(biomet_data.relative_humidity) AS relative_humidity_max,
            avg(biomet_data.sensor_temperature_internal) AS sensor_temperature_internal,
            min(biomet_data.sensor_temperature_internal) AS sensor_temperature_internal_min,
            max(biomet_data.sensor_temperature_internal) AS sensor_temperature_internal_max,
            avg(biomet_data.solar_radiation) AS solar_radiation,
            min(biomet_data.solar_radiation) AS solar_radiation_min,
            max(biomet_data.solar_radiation) AS solar_radiation_max,
            avg(biomet_data.thermistor_resistance) AS thermistor_resistance,
            min(biomet_data.thermistor_resistance) AS thermistor_resistance_min,
            max(biomet_data.thermistor_resistance) AS thermistor_resistance_max,
            avg(biomet_data.u_wind) AS u_wind,
            min(biomet_data.u_wind) AS u_wind_min,
            max(biomet_data.u_wind) AS u_wind_max,
            avg(biomet_data.utci) AS utci,
            min(biomet_data.utci) AS utci_min,
            max(biomet_data.utci) AS utci_max,
            mode() WITHIN GROUP (ORDER BY biomet_data.utci_category ASC) AS utci_category,
            avg(biomet_data.v_wind) AS v_wind,
            min(biomet_data.v_wind) AS v_wind_min,
            max(biomet_data.v_wind) AS v_wind_max,
            avg(biomet_data.vapor_pressure) AS vapor_pressure,
            min(biomet_data.vapor_pressure) AS vapor_pressure_min,
            max(biomet_data.vapor_pressure) AS vapor_pressure_max,
            avg(biomet_data.voltage_ratio) AS voltage_ratio,
            min(biomet_data.voltage_ratio) AS voltage_ratio_min,
            max(biomet_data.voltage_ratio) AS voltage_ratio_max,
            avg(biomet_data.wet_bulb_temperature) AS wet_bulb_temperature,
            min(biomet_data.wet_bulb_temperature) AS wet_bulb_temperature_min,
            max(biomet_data.wet_bulb_temperature) AS wet_bulb_temperature_max,
            avg_angle(biomet_data.wind_direction) AS wind_direction,
            avg(biomet_data.wind_speed) AS wind_speed,
            min(biomet_data.wind_speed) AS wind_speed_min,
            max(biomet_data.wind_speed) AS wind_speed_max,
            avg(biomet_data.x_orientation_angle) AS x_orientation_angle,
            min(biomet_data.x_orientation_angle) AS x_orientation_angle_min,
            max(biomet_data.x_orientation_angle) AS x_orientation_angle_max,
            avg(biomet_data.y_orientation_angle) AS y_orientation_angle,
            min(biomet_data.y_orientation_angle) AS y_orientation_angle_min,
            max(biomet_data.y_orientation_angle) AS y_orientation_angle_max
        FROM biomet_data
        GROUP BY time_bucket('1hour', measured_at), name
    ''')  # noqa: E501


class TempRHDataHourly(_SHT35DataRawBase, _TempRHDerivatives, _CalibrationDerivatives):
    """This is not an actual table, but a materialized view. We simply trick sqlalchemy
    into thinking this was a table. Querying a materialized view does not differ from
    querying a proper table.
    """
    __tablename__ = 'temp_rh_data_hourly'

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

    @classmethod
    async def refresh(cls) -> None:
        async with sessionmanager.connect(as_transaction=False) as sess:
            await sess.execute(
                text("CALL refresh_continuous_aggregate('temp_rh_data_hourly', NULL, NULL)"),  # noqa: E501
            )

    creation_sql = text('''\
    CREATE MATERIALIZED VIEW IF NOT EXISTS temp_rh_data_hourly(
        measured_at,
        name,
        absolute_humidity,
        absolute_humidity_min,
        absolute_humidity_max,
        air_temperature,
        air_temperature_min,
        air_temperature_max,
        air_temperature_raw,
        air_temperature_raw_min,
        air_temperature_raw_max,
        battery_voltage,
        battery_voltage_min,
        battery_voltage_max,
        dew_point,
        dew_point_min,
        dew_point_max,
        heat_index,
        heat_index_min,
        heat_index_max,
        relative_humidity,
        relative_humidity_min,
        relative_humidity_max,
        relative_humidity_raw,
        relative_humidity_raw_min,
        relative_humidity_raw_max,
        wet_bulb_temperature,
        wet_bulb_temperature_min,
        wet_bulb_temperature_max
    )
    WITH (timescaledb.continuous, timescaledb.materialized_only = true) AS
        SELECT
            time_bucket('1hour', measured_at) AT TIME ZONE 'UTC' + '1 hour',
            name,
            avg(temp_rh_data.absolute_humidity) AS absolute_humidity,
            min(temp_rh_data.absolute_humidity) AS absolute_humidity_min,
            max(temp_rh_data.absolute_humidity) AS absolute_humidity_max,
            avg(temp_rh_data.air_temperature) AS air_temperature,
            min(temp_rh_data.air_temperature) AS air_temperature_min,
            max(temp_rh_data.air_temperature) AS air_temperature_max,
            avg(temp_rh_data.air_temperature_raw) AS air_temperature_raw,
            min(temp_rh_data.air_temperature_raw) AS air_temperature_raw_min,
            max(temp_rh_data.air_temperature_raw) AS air_temperature_raw_max,
            avg(temp_rh_data.battery_voltage) AS battery_voltage,
            min(temp_rh_data.battery_voltage) AS battery_voltage_min,
            max(temp_rh_data.battery_voltage) AS battery_voltage_max,
            avg(temp_rh_data.dew_point) AS dew_point,
            min(temp_rh_data.dew_point) AS dew_point_min,
            max(temp_rh_data.dew_point) AS dew_point_max,
            avg(temp_rh_data.heat_index) AS heat_index,
            min(temp_rh_data.heat_index) AS heat_index_min,
            max(temp_rh_data.heat_index) AS heat_index_max,
            avg(temp_rh_data.relative_humidity) AS relative_humidity,
            min(temp_rh_data.relative_humidity) AS relative_humidity_min,
            max(temp_rh_data.relative_humidity) AS relative_humidity_max,
            avg(temp_rh_data.relative_humidity_raw) AS relative_humidity_raw,
            min(temp_rh_data.relative_humidity_raw) AS relative_humidity_raw_min,
            max(temp_rh_data.relative_humidity_raw) AS relative_humidity_raw_max,
            avg(temp_rh_data.wet_bulb_temperature) AS wet_bulb_temperature,
            min(temp_rh_data.wet_bulb_temperature) AS wet_bulb_temperature_min,
            max(temp_rh_data.wet_bulb_temperature) AS wet_bulb_temperature_max
        FROM temp_rh_data
        GROUP BY time_bucket('1hour', measured_at), name
    ''')


class BiometDataDaily(
    _ATM41DataRawBase, _BLGDataRawBase, _TempRHDerivatives, _BiometDerivatives,
):
    """This is not an actual table, but a materialized view. We simply trick sqlalchemy
    into thinking this was a table. Querying a materialized view does not differ from
    querying a proper table.
    """
    __tablename__ = 'biomet_data_daily'

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
    blg_time_offset_min: Mapped[float] = mapped_column(nullable=True, comment='seconds')
    blg_time_offset_max: Mapped[float] = mapped_column(nullable=True, comment='seconds')
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

    @classmethod
    async def refresh(cls, db: AsyncSession) -> None:
        await db.execute(text('REFRESH MATERIALIZED VIEW biomet_data_daily'))

    creation_sql = text('''\
    CREATE MATERIALIZED VIEW IF NOT EXISTS biomet_data_daily AS
    SELECT
        (time_bucket('1day', measured_at, 'CET') + '1 hour'::INTERVAL)::DATE as measured_at,
        name,
        CASE
            WHEN (count(*) FILTER (
                    WHERE absolute_humidity IS NOT NULL) / 288
                ) > 0.7 THEN avg(biomet_data.absolute_humidity)
            ELSE NULL
        END AS absolute_humidity,
        CASE
            WHEN (count(*) FILTER (
                    WHERE absolute_humidity IS NOT NULL) / 288
                ) > 0.7 THEN min(biomet_data.absolute_humidity)
            ELSE NULL
        END AS absolute_humidity_min,
        CASE
            WHEN (count(*) FILTER (
                    WHERE absolute_humidity IS NOT NULL) / 288
                ) > 0.7 THEN max(biomet_data.absolute_humidity)
            ELSE NULL
        END AS absolute_humidity_max,
        CASE
            WHEN (count(*) FILTER (
                    WHERE air_temperature IS NOT NULL) / 288
                ) > 0.7 THEN avg(biomet_data.air_temperature)
            ELSE NULL
        END AS air_temperature,
        CASE
            WHEN (count(*) FILTER (
                    WHERE air_temperature IS NOT NULL) / 288
                ) > 0.7 THEN min(biomet_data.air_temperature)
            ELSE NULL
        END AS air_temperature_min,
        CASE
            WHEN (count(*) FILTER (
                    WHERE air_temperature IS NOT NULL) / 288
                ) > 0.7 THEN max(biomet_data.air_temperature)
            ELSE NULL
        END AS air_temperature_max,
        CASE
            WHEN (count(*) FILTER (
                    WHERE atmospheric_pressure IS NOT NULL) / 288
                ) > 0.7 THEN avg(biomet_data.atmospheric_pressure)
            ELSE NULL
        END AS atmospheric_pressure,
        CASE
            WHEN (count(*) FILTER (
                    WHERE atmospheric_pressure IS NOT NULL) / 288
                ) > 0.7 THEN min(biomet_data.atmospheric_pressure)
            ELSE NULL
        END AS atmospheric_pressure_min,
        CASE
            WHEN (count(*) FILTER (
                    WHERE atmospheric_pressure IS NOT NULL) / 288
                ) > 0.7 THEN max(biomet_data.atmospheric_pressure)
            ELSE NULL
        END AS atmospheric_pressure_max,
        CASE
            WHEN (count(*) FILTER (
                    WHERE atmospheric_pressure_reduced IS NOT NULL) / 288
                ) > 0.7 THEN avg(biomet_data.atmospheric_pressure_reduced)
            ELSE NULL
        END AS atmospheric_pressure_reduced,
        CASE
            WHEN (count(*) FILTER (
                    WHERE atmospheric_pressure_reduced IS NOT NULL) / 288
                ) > 0.7 THEN min(biomet_data.atmospheric_pressure_reduced)
            ELSE NULL
        END AS atmospheric_pressure_reduced_min,
        CASE
            WHEN (count(*) FILTER (
                    WHERE atmospheric_pressure_reduced IS NOT NULL) / 288
                ) > 0.7 THEN max(biomet_data.atmospheric_pressure_reduced)
            ELSE NULL
        END AS atmospheric_pressure_reduced_max,
        CASE
            WHEN (count(*) FILTER (
                    WHERE battery_voltage IS NOT NULL) / 288
                ) > 0.7 THEN avg(biomet_data.battery_voltage)
            ELSE NULL
        END AS battery_voltage,
        CASE
            WHEN (count(*) FILTER (
                    WHERE battery_voltage IS NOT NULL) / 288
                ) > 0.7 THEN min(biomet_data.battery_voltage)
            ELSE NULL
        END AS battery_voltage_min,
        CASE
            WHEN (count(*) FILTER (
                    WHERE battery_voltage IS NOT NULL) / 288
                ) > 0.7 THEN max(biomet_data.battery_voltage)
            ELSE NULL
        END AS battery_voltage_max,
        CASE
            WHEN (count(*) FILTER (
                    WHERE black_globe_temperature IS NOT NULL) / 288
                ) > 0.7 THEN avg(biomet_data.black_globe_temperature)
            ELSE NULL
        END AS black_globe_temperature,
        CASE
            WHEN (count(*) FILTER (
                    WHERE black_globe_temperature IS NOT NULL) / 288
                ) > 0.7 THEN min(biomet_data.black_globe_temperature)
            ELSE NULL
        END AS black_globe_temperature_min,
        CASE
            WHEN (count(*) FILTER (
                    WHERE black_globe_temperature IS NOT NULL) / 288
                ) > 0.7 THEN max(biomet_data.black_globe_temperature)
            ELSE NULL
        END AS black_globe_temperature_max,
        CASE
            WHEN (count(*) FILTER (
                    WHERE blg_battery_voltage IS NOT NULL) / 288
                ) > 0.7 THEN avg(biomet_data.blg_battery_voltage)
            ELSE NULL
        END AS blg_battery_voltage,
        CASE
            WHEN (count(*) FILTER (
                    WHERE blg_battery_voltage IS NOT NULL) / 288
                ) > 0.7 THEN min(biomet_data.blg_battery_voltage)
            ELSE NULL
        END AS blg_battery_voltage_min,
        CASE
            WHEN (count(*) FILTER (
                    WHERE blg_battery_voltage IS NOT NULL) / 288
                ) > 0.7 THEN max(biomet_data.blg_battery_voltage)
            ELSE NULL
        END AS blg_battery_voltage_max,
        CASE
            WHEN (count(*) FILTER (
                    WHERE blg_time_offset IS NOT NULL) / 288
                ) > 0.7 THEN avg(biomet_data.blg_time_offset)
            ELSE NULL
        END AS blg_time_offset,
        CASE
            WHEN (count(*) FILTER (
                    WHERE blg_time_offset IS NOT NULL) / 288
                ) > 0.7 THEN min(biomet_data.blg_time_offset)
            ELSE NULL
        END AS blg_time_offset_min,
        CASE
            WHEN (count(*) FILTER (
                    WHERE blg_time_offset IS NOT NULL) / 288
                ) > 0.7 THEN max(biomet_data.blg_time_offset)
            ELSE NULL
        END AS blg_time_offset_max,
        CASE
            WHEN (count(*) FILTER (
                    WHERE dew_point IS NOT NULL) / 288
                ) > 0.7 THEN avg(biomet_data.dew_point)
            ELSE NULL
        END AS dew_point,
        CASE
            WHEN (count(*) FILTER (
                    WHERE dew_point IS NOT NULL) / 288
                ) > 0.7 THEN min(biomet_data.dew_point)
            ELSE NULL
        END AS dew_point_min,
        CASE
            WHEN (count(*) FILTER (
                    WHERE dew_point IS NOT NULL) / 288
                ) > 0.7 THEN max(biomet_data.dew_point)
            ELSE NULL
        END AS dew_point_max,
        CASE
            WHEN (count(*) FILTER (
                    WHERE heat_index IS NOT NULL) / 288
                ) > 0.7 THEN avg(biomet_data.heat_index)
            ELSE NULL
        END AS heat_index,
        CASE
            WHEN (count(*) FILTER (
                    WHERE heat_index IS NOT NULL) / 288
                ) > 0.7 THEN min(biomet_data.heat_index)
            ELSE NULL
        END AS heat_index_min,
        CASE
            WHEN (count(*) FILTER (
                    WHERE heat_index IS NOT NULL) / 288
                ) > 0.7 THEN max(biomet_data.heat_index)
            ELSE NULL
        END AS heat_index_max,
        CASE
            WHEN (count(*) FILTER (
                    WHERE lightning_average_distance IS NOT NULL) / 288
                ) > 0.7 THEN avg(biomet_data.lightning_average_distance)
            ELSE NULL
        END AS lightning_average_distance,
        CASE
            WHEN (count(*) FILTER (
                    WHERE lightning_average_distance IS NOT NULL) / 288
                ) > 0.7 THEN min(biomet_data.lightning_average_distance)
            ELSE NULL
        END AS lightning_average_distance_min,
        CASE
            WHEN (count(*) FILTER (
                    WHERE lightning_average_distance IS NOT NULL) / 288
                ) > 0.7 THEN max(biomet_data.lightning_average_distance)
            ELSE NULL
        END AS lightning_average_distance_max,
        CASE
            WHEN (count(*) FILTER (
                    WHERE lightning_strike_count IS NOT NULL) / 288
                ) > 0.7 THEN sum(biomet_data.lightning_strike_count)
            ELSE NULL
        END AS lightning_strike_count,
        CASE
            WHEN (count(*) FILTER (
                    WHERE maximum_wind_speed IS NOT NULL) / 288
                ) > 0.7 THEN max(biomet_data.maximum_wind_speed)
            ELSE NULL
        END AS maximum_wind_speed,
        CASE
            WHEN (count(*) FILTER (
                    WHERE mrt IS NOT NULL) / 288
                ) > 0.7 THEN avg(biomet_data.mrt)
            ELSE NULL
        END AS mrt,
        CASE
            WHEN (count(*) FILTER (
                    WHERE mrt IS NOT NULL) / 288
                ) > 0.7 THEN min(biomet_data.mrt)
            ELSE NULL
        END AS mrt_min,
        CASE
            WHEN (count(*) FILTER (
                    WHERE mrt IS NOT NULL) / 288
                ) > 0.7 THEN max(biomet_data.mrt)
            ELSE NULL
        END AS mrt_max,
        CASE
            WHEN (count(*) FILTER (
                    WHERE pet IS NOT NULL) / 288
                ) > 0.7 THEN avg(biomet_data.pet)
            ELSE NULL
        END AS pet,
        CASE
            WHEN (count(*) FILTER (
                    WHERE pet IS NOT NULL) / 288
                ) > 0.7 THEN min(biomet_data.pet)
            ELSE NULL
        END AS pet_min,
        CASE
            WHEN (count(*) FILTER (
                    WHERE pet IS NOT NULL) / 288
                ) > 0.7 THEN max(biomet_data.pet)
            ELSE NULL
        END AS pet_max,
        CASE
            WHEN (count(*) FILTER (
                    WHERE pet_category IS NOT NULL) / 288
                ) > 0.7 THEN mode() WITHIN GROUP (ORDER BY biomet_data.pet_category ASC)
            ELSE NULL
        END AS pet_category,
        CASE
            WHEN (count(*) FILTER (
                    WHERE precipitation_sum IS NOT NULL) / 288
                ) > 0.7 THEN sum(biomet_data.precipitation_sum)
            ELSE NULL
        END AS precipitation_sum,
        CASE
            WHEN (count(*) FILTER (
                    WHERE relative_humidity IS NOT NULL) / 288
                ) > 0.7 THEN avg(biomet_data.relative_humidity)
            ELSE NULL
        END AS relative_humidity,
        CASE
            WHEN (count(*) FILTER (
                    WHERE relative_humidity IS NOT NULL) / 288
                ) > 0.7 THEN min(biomet_data.relative_humidity)
            ELSE NULL
        END AS relative_humidity_min,
        CASE
            WHEN (count(*) FILTER (
                    WHERE relative_humidity IS NOT NULL) / 288
                ) > 0.7 THEN max(biomet_data.relative_humidity)
            ELSE NULL
        END AS relative_humidity_max,
        CASE
            WHEN (count(*) FILTER (
                    WHERE sensor_temperature_internal IS NOT NULL) / 288
                ) > 0.7 THEN avg(biomet_data.sensor_temperature_internal)
            ELSE NULL
        END AS sensor_temperature_internal,
        CASE
            WHEN (count(*) FILTER (
                    WHERE sensor_temperature_internal IS NOT NULL) / 288
                ) > 0.7 THEN min(biomet_data.sensor_temperature_internal)
            ELSE NULL
        END AS sensor_temperature_internal_min,
        CASE
            WHEN (count(*) FILTER (
                    WHERE sensor_temperature_internal IS NOT NULL) / 288
                ) > 0.7 THEN max(biomet_data.sensor_temperature_internal)
            ELSE NULL
        END AS sensor_temperature_internal_max,
        CASE
            WHEN (count(*) FILTER (
                    WHERE solar_radiation IS NOT NULL) / 288
                ) > 0.7 THEN avg(biomet_data.solar_radiation)
            ELSE NULL
        END AS solar_radiation,
        CASE
            WHEN (count(*) FILTER (
                    WHERE solar_radiation IS NOT NULL) / 288
                ) > 0.7 THEN min(biomet_data.solar_radiation)
            ELSE NULL
        END AS solar_radiation_min,
        CASE
            WHEN (count(*) FILTER (
                    WHERE solar_radiation IS NOT NULL) / 288
                ) > 0.7 THEN max(biomet_data.solar_radiation)
            ELSE NULL
        END AS solar_radiation_max,
        CASE
            WHEN (count(*) FILTER (
                    WHERE thermistor_resistance IS NOT NULL) / 288
                ) > 0.7 THEN avg(biomet_data.thermistor_resistance)
            ELSE NULL
        END AS thermistor_resistance,
        CASE
            WHEN (count(*) FILTER (
                    WHERE thermistor_resistance IS NOT NULL) / 288
                ) > 0.7 THEN min(biomet_data.thermistor_resistance)
            ELSE NULL
        END AS thermistor_resistance_min,
        CASE
            WHEN (count(*) FILTER (
                    WHERE thermistor_resistance IS NOT NULL) / 288
                ) > 0.7 THEN max(biomet_data.thermistor_resistance)
            ELSE NULL
        END AS thermistor_resistance_max,
        CASE
            WHEN (count(*) FILTER (
                    WHERE u_wind IS NOT NULL) / 288
                ) > 0.7 THEN avg(biomet_data.u_wind)
            ELSE NULL
        END AS u_wind,
        CASE
            WHEN (count(*) FILTER (
                    WHERE u_wind IS NOT NULL) / 288
                ) > 0.7 THEN min(biomet_data.u_wind)
            ELSE NULL
        END AS u_wind_min,
        CASE
            WHEN (count(*) FILTER (
                    WHERE u_wind IS NOT NULL) / 288
                ) > 0.7 THEN max(biomet_data.u_wind)
            ELSE NULL
        END AS u_wind_max,
        CASE
            WHEN (count(*) FILTER (
                    WHERE utci IS NOT NULL) / 288
                ) > 0.7 THEN avg(biomet_data.utci)
            ELSE NULL
        END AS utci,
        CASE
            WHEN (count(*) FILTER (
                    WHERE utci IS NOT NULL) / 288
                ) > 0.7 THEN min(biomet_data.utci)
            ELSE NULL
        END AS utci_min,
        CASE
            WHEN (count(*) FILTER (
                    WHERE utci IS NOT NULL) / 288
                ) > 0.7 THEN max(biomet_data.utci)
            ELSE NULL
        END AS utci_max,
        CASE
            WHEN (count(*) FILTER (
                    WHERE utci_category IS NOT NULL) / 288
                ) > 0.7 THEN mode() WITHIN GROUP (ORDER BY biomet_data.utci_category ASC)
            ELSE NULL
        END AS utci_category,
        CASE
            WHEN (count(*) FILTER (
                    WHERE v_wind IS NOT NULL) / 288
                ) > 0.7 THEN avg(biomet_data.v_wind)
            ELSE NULL
        END AS v_wind,
        CASE
            WHEN (count(*) FILTER (
                    WHERE v_wind IS NOT NULL) / 288
                ) > 0.7 THEN min(biomet_data.v_wind)
            ELSE NULL
        END AS v_wind_min,
        CASE
            WHEN (count(*) FILTER (
                    WHERE v_wind IS NOT NULL) / 288
                ) > 0.7 THEN max(biomet_data.v_wind)
            ELSE NULL
        END AS v_wind_max,
        CASE
            WHEN (count(*) FILTER (
                    WHERE vapor_pressure IS NOT NULL) / 288
                ) > 0.7 THEN avg(biomet_data.vapor_pressure)
            ELSE NULL
        END AS vapor_pressure,
        CASE
            WHEN (count(*) FILTER (
                    WHERE vapor_pressure IS NOT NULL) / 288
                ) > 0.7 THEN min(biomet_data.vapor_pressure)
            ELSE NULL
        END AS vapor_pressure_min,
        CASE
            WHEN (count(*) FILTER (
                    WHERE vapor_pressure IS NOT NULL) / 288
                ) > 0.7 THEN max(biomet_data.vapor_pressure)
            ELSE NULL
        END AS vapor_pressure_max,
        CASE
            WHEN (count(*) FILTER (
                    WHERE voltage_ratio IS NOT NULL) / 288
                ) > 0.7 THEN avg(biomet_data.voltage_ratio)
            ELSE NULL
        END AS voltage_ratio,
        CASE
            WHEN (count(*) FILTER (
                    WHERE voltage_ratio IS NOT NULL) / 288
                ) > 0.7 THEN min(biomet_data.voltage_ratio)
            ELSE NULL
        END AS voltage_ratio_min,
        CASE
            WHEN (count(*) FILTER (
                    WHERE voltage_ratio IS NOT NULL) / 288
                ) > 0.7 THEN max(biomet_data.voltage_ratio)
            ELSE NULL
        END AS voltage_ratio_max,
        CASE
            WHEN (count(*) FILTER (
                    WHERE wet_bulb_temperature IS NOT NULL) / 288
                ) > 0.7 THEN avg(biomet_data.wet_bulb_temperature)
            ELSE NULL
        END AS wet_bulb_temperature,
        CASE
            WHEN (count(*) FILTER (
                    WHERE wet_bulb_temperature IS NOT NULL) / 288
                ) > 0.7 THEN min(biomet_data.wet_bulb_temperature)
            ELSE NULL
        END AS wet_bulb_temperature_min,
        CASE
            WHEN (count(*) FILTER (
                    WHERE wet_bulb_temperature IS NOT NULL) / 288
                ) > 0.7 THEN max(biomet_data.wet_bulb_temperature)
            ELSE NULL
        END AS wet_bulb_temperature_max,
        CASE
            WHEN (count(*) FILTER (
                    WHERE wind_direction IS NOT NULL) / 288
                ) > 0.7 THEN avg_angle(biomet_data.wind_direction)
            ELSE NULL
        END AS wind_direction,
        CASE
            WHEN (count(*) FILTER (
                    WHERE wind_speed IS NOT NULL) / 288
                ) > 0.7 THEN avg(biomet_data.wind_speed)
            ELSE NULL
        END AS wind_speed,
        CASE
            WHEN (count(*) FILTER (
                    WHERE wind_speed IS NOT NULL) / 288
                ) > 0.7 THEN min(biomet_data.wind_speed)
            ELSE NULL
        END AS wind_speed_min,
        CASE
            WHEN (count(*) FILTER (
                    WHERE wind_speed IS NOT NULL) / 288
                ) > 0.7 THEN max(biomet_data.wind_speed)
            ELSE NULL
        END AS wind_speed_max,
        CASE
            WHEN (count(*) FILTER (
                    WHERE x_orientation_angle IS NOT NULL) / 288
                ) > 0.7 THEN avg(biomet_data.x_orientation_angle)
            ELSE NULL
        END AS x_orientation_angle,
        CASE
            WHEN (count(*) FILTER (
                    WHERE x_orientation_angle IS NOT NULL) / 288
                ) > 0.7 THEN min(biomet_data.x_orientation_angle)
            ELSE NULL
        END AS x_orientation_angle_min,
        CASE
            WHEN (count(*) FILTER (
                    WHERE x_orientation_angle IS NOT NULL) / 288
                ) > 0.7 THEN max(biomet_data.x_orientation_angle)
            ELSE NULL
        END AS x_orientation_angle_max,
        CASE
            WHEN (count(*) FILTER (
                    WHERE y_orientation_angle IS NOT NULL) / 288
                ) > 0.7 THEN avg(biomet_data.y_orientation_angle)
            ELSE NULL
        END AS y_orientation_angle,
        CASE
            WHEN (count(*) FILTER (
                    WHERE y_orientation_angle IS NOT NULL) / 288
                ) > 0.7 THEN min(biomet_data.y_orientation_angle)
            ELSE NULL
        END AS y_orientation_angle_min,
        CASE
            WHEN (count(*) FILTER (
                    WHERE y_orientation_angle IS NOT NULL) / 288
                ) > 0.7 THEN max(biomet_data.y_orientation_angle)
            ELSE NULL
        END AS y_orientation_angle_max
    FROM biomet_data
    GROUP BY (time_bucket('1day', measured_at, 'CET') + '1 hour'::INTERVAL)::DATE, name
    ORDER BY measured_at, name
    ''')  # noqa: E501


class TempRHDataDaily(_SHT35DataRawBase, _TempRHDerivatives, _CalibrationDerivatives):
    """This is not an actual table, but a materialized view. We simply trick sqlalchemy
    into thinking this was a table. Querying a materialized view does not differ from
    querying a proper table.
    """
    __tablename__ = 'temp_rh_data_daily'

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

    @classmethod
    async def refresh(cls, db: AsyncSession) -> None:
        await db.execute(text('REFRESH MATERIALIZED VIEW temp_rh_data_daily'))

    creation_sql = text('''\
    CREATE MATERIALIZED VIEW IF NOT EXISTS temp_rh_data_daily AS
    SELECT
        (time_bucket('1day', measured_at, 'CET') + '1 hour'::INTERVAL)::DATE as measured_at,
        name,
        CASE
            WHEN (count(*) FILTER (
                    WHERE absolute_humidity IS NOT NULL) / 288
                ) > 0.7 THEN avg(temp_rh_data.absolute_humidity)
            ELSE NULL
        END AS absolute_humidity,
        CASE
            WHEN (count(*) FILTER (
                    WHERE absolute_humidity IS NOT NULL) / 288
                ) > 0.7 THEN min(temp_rh_data.absolute_humidity)
            ELSE NULL
        END AS absolute_humidity_min,
        CASE
            WHEN (count(*) FILTER (
                    WHERE absolute_humidity IS NOT NULL) / 288
                ) > 0.7 THEN max(temp_rh_data.absolute_humidity)
            ELSE NULL
        END AS absolute_humidity_max,
        CASE
            WHEN (count(*) FILTER (
                    WHERE air_temperature IS NOT NULL) / 288
                ) > 0.7 THEN avg(temp_rh_data.air_temperature)
            ELSE NULL
        END AS air_temperature,
        CASE
            WHEN (count(*) FILTER (
                    WHERE air_temperature IS NOT NULL) / 288
                ) > 0.7 THEN min(temp_rh_data.air_temperature)
            ELSE NULL
        END AS air_temperature_min,
        CASE
            WHEN (count(*) FILTER (
                    WHERE air_temperature IS NOT NULL) / 288
                ) > 0.7 THEN max(temp_rh_data.air_temperature)
            ELSE NULL
        END AS air_temperature_max,
        CASE
            WHEN (count(*) FILTER (
                    WHERE air_temperature_raw IS NOT NULL) / 288
                ) > 0.7 THEN avg(temp_rh_data.air_temperature_raw)
            ELSE NULL
        END AS air_temperature_raw,
        CASE
            WHEN (count(*) FILTER (
                    WHERE air_temperature_raw IS NOT NULL) / 288
                ) > 0.7 THEN min(temp_rh_data.air_temperature_raw)
            ELSE NULL
        END AS air_temperature_raw_min,
        CASE
            WHEN (count(*) FILTER (
                    WHERE air_temperature_raw IS NOT NULL) / 288
                ) > 0.7 THEN max(temp_rh_data.air_temperature_raw)
            ELSE NULL
        END AS air_temperature_raw_max,
        CASE
            WHEN (count(*) FILTER (
                    WHERE battery_voltage IS NOT NULL) / 288
                ) > 0.7 THEN avg(temp_rh_data.battery_voltage)
            ELSE NULL
        END AS battery_voltage,
        CASE
            WHEN (count(*) FILTER (
                    WHERE battery_voltage IS NOT NULL) / 288
                ) > 0.7 THEN min(temp_rh_data.battery_voltage)
            ELSE NULL
        END AS battery_voltage_min,
        CASE
            WHEN (count(*) FILTER (
                    WHERE battery_voltage IS NOT NULL) / 288
                ) > 0.7 THEN max(temp_rh_data.battery_voltage)
            ELSE NULL
        END AS battery_voltage_max,
        CASE
            WHEN (count(*) FILTER (
                    WHERE dew_point IS NOT NULL) / 288
                ) > 0.7 THEN avg(temp_rh_data.dew_point)
            ELSE NULL
        END AS dew_point,
        CASE
            WHEN (count(*) FILTER (
                    WHERE dew_point IS NOT NULL) / 288
                ) > 0.7 THEN min(temp_rh_data.dew_point)
            ELSE NULL
        END AS dew_point_min,
        CASE
            WHEN (count(*) FILTER (
                    WHERE dew_point IS NOT NULL) / 288
                ) > 0.7 THEN max(temp_rh_data.dew_point)
            ELSE NULL
        END AS dew_point_max,
        CASE
            WHEN (count(*) FILTER (
                    WHERE heat_index IS NOT NULL) / 288
                ) > 0.7 THEN avg(temp_rh_data.heat_index)
            ELSE NULL
        END AS heat_index,
        CASE
            WHEN (count(*) FILTER (
                    WHERE heat_index IS NOT NULL) / 288
                ) > 0.7 THEN min(temp_rh_data.heat_index)
            ELSE NULL
        END AS heat_index_min,
        CASE
            WHEN (count(*) FILTER (
                    WHERE heat_index IS NOT NULL) / 288
                ) > 0.7 THEN max(temp_rh_data.heat_index)
            ELSE NULL
        END AS heat_index_max,
        CASE
            WHEN (count(*) FILTER (
                    WHERE relative_humidity IS NOT NULL) / 288
                ) > 0.7 THEN avg(temp_rh_data.relative_humidity)
            ELSE NULL
        END AS relative_humidity,
        CASE
            WHEN (count(*) FILTER (
                    WHERE relative_humidity IS NOT NULL) / 288
                ) > 0.7 THEN min(temp_rh_data.relative_humidity)
            ELSE NULL
        END AS relative_humidity_min,
        CASE
            WHEN (count(*) FILTER (
                    WHERE relative_humidity IS NOT NULL) / 288
                ) > 0.7 THEN max(temp_rh_data.relative_humidity)
            ELSE NULL
        END AS relative_humidity_max,
        CASE
            WHEN (count(*) FILTER (
                    WHERE relative_humidity_raw IS NOT NULL) / 288
                ) > 0.7 THEN avg(temp_rh_data.relative_humidity_raw)
            ELSE NULL
        END AS relative_humidity_raw,
        CASE
            WHEN (count(*) FILTER (
                    WHERE relative_humidity_raw IS NOT NULL) / 288
                ) > 0.7 THEN min(temp_rh_data.relative_humidity_raw)
            ELSE NULL
        END AS relative_humidity_raw_min,
        CASE
            WHEN (count(*) FILTER (
                    WHERE relative_humidity_raw IS NOT NULL) / 288
                ) > 0.7 THEN max(temp_rh_data.relative_humidity_raw)
            ELSE NULL
        END AS relative_humidity_raw_max,
        CASE
            WHEN (count(*) FILTER (
                    WHERE wet_bulb_temperature IS NOT NULL) / 288
                ) > 0.7 THEN avg(temp_rh_data.wet_bulb_temperature)
            ELSE NULL
        END AS wet_bulb_temperature,
        CASE
            WHEN (count(*) FILTER (
                    WHERE wet_bulb_temperature IS NOT NULL) / 288
                ) > 0.7 THEN min(temp_rh_data.wet_bulb_temperature)
            ELSE NULL
        END AS wet_bulb_temperature_min,
        CASE
            WHEN (count(*) FILTER (
                    WHERE wet_bulb_temperature IS NOT NULL) / 288
                ) > 0.7 THEN max(temp_rh_data.wet_bulb_temperature)
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
