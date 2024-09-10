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
    district: Mapped[str] = mapped_column(nullable=True)
    comment: Mapped[str] = mapped_column(nullable=True)
    setup_date: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    # TODO: add more station metadata
    lcz: Mapped[str] = mapped_column(nullable=True)
    svf: Mapped[Decimal] = mapped_column(nullable=True)
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
    battery_voltage: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='Volts',
    )
    protocol_version: Mapped[int] = mapped_column(nullable=True)


class _SHT35DataRawBase(_Data):
    __abstract__ = True

    air_temperature: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
    )
    relative_humidity: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='%',
    )


class SHT35DataRaw(_SHT35DataRawBase):
    __tablename__ = 'sht35_data_raw'
    station: Mapped[Station] = relationship(
        back_populates='sht35_data_raw',
        lazy=True,
    )


class _ATM41DataRawBase(_Data):
    __abstract__ = True

    air_temperature: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
    )
    relative_humidity: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='%',
    )
    atmospheric_pressure: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='kPa',
    )
    vapor_pressure: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='kPa',
    )
    wind_speed: Mapped[Decimal] = mapped_column(nullable=True, comment='m/s')
    wind_direction: Mapped[Decimal] = mapped_column(nullable=True, comment='°')
    u_wind: Mapped[Decimal] = mapped_column(nullable=True, comment='m/s')
    v_wind: Mapped[Decimal] = mapped_column(nullable=True, comment='m/s')
    wind_speed_max: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='m/s',
    )
    precipitation_sum: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='mm',
    )
    solar_radiation: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='W/m2',
    )
    lightning_average_distance: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='km',
    )
    lightning_strike_count: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='-',
    )
    sensor_temperature_internal: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
    )
    x_orientation_angle: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°',
    )
    y_orientation_angle: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°',
    )


class ATM41DataRaw(_ATM41DataRawBase):
    __tablename__ = 'atm41_data_raw'
    station: Mapped[Station] = relationship(
        back_populates='atm41_data_raw',
        lazy=True,
    )


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
        ForeignKey('station.blg_name'), primary_key=True,
    )
    station: Mapped[Station] = relationship(
        back_populates='blg_data_raw',
        lazy=True,
    )


class _TempRHDerivatives(Base):
    __abstract__ = True
    dew_point: Mapped[Decimal] = mapped_column(nullable=True, comment='°C')
    absolute_humidity: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='g/m3',
    )
    heat_index: Mapped[Decimal] = mapped_column(nullable=True, comment='°C')
    wet_bulb_temperature: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
    )


class BiometData(_ATM41DataRawBase, _BLGDataRawBase, _TempRHDerivatives):
    __tablename__ = 'biomet_data'
    blg_time_offset: Mapped[float] = mapped_column(
        nullable=True,
        comment='seconds',
    )
    mrt: Mapped[Decimal] = mapped_column(nullable=True, comment='°C')
    utci: Mapped[Decimal] = mapped_column(nullable=True, comment='°C')
    utci_category: Mapped[HeatStressCategories] = mapped_column(nullable=True)
    pet: Mapped[Decimal] = mapped_column(nullable=True, comment='°C')
    pet_category: Mapped[HeatStressCategories] = mapped_column(nullable=True)
    atmospheric_pressure: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='hPa',  # we've converted it to hPa in the meantime
    )
    atmospheric_pressure_reduced: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='hPa',
    )
    vapor_pressure: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='hPa',
    )
    # we need this as an alias in the big biomet table
    blg_battery_voltage: Mapped[Decimal] = mapped_column(nullable=True, comment='V')
    # TODO: QC fields?
    station: Mapped[Station] = relationship(
        back_populates='biomet_data',
        lazy=True,
    )


class TempRHData(_SHT35DataRawBase, _TempRHDerivatives):
    __tablename__ = 'temp_rh_data'
    air_temperature_raw: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
    )
    relative_humidity_raw: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='%',
    )
    # TODO: QC fields?
    station: Mapped[Station] = relationship(
        back_populates='temp_rh_data',
        lazy=True,
    )


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
    atmospheric_pressure: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='hPa',  # we've converted it to hPa in the meantime
    )
    atmospheric_pressure_reduced: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='hPa',
    )
    vapor_pressure: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='hPa',
    )

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
            wind_speed_max
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


class BiometDataHourly(_ATM41DataRawBase, _BLGDataRawBase, _TempRHDerivatives):
    """This is not an actual table, but a materialized view. We simply trick sqlalchemy
    into thinking this was a table. Querying a materialized view does not differ from
    querying a proper table.
    """
    __tablename__ = 'biomet_data_hourly'

    mrt: Mapped[Decimal] = mapped_column(nullable=True, comment='°C')
    utci: Mapped[Decimal] = mapped_column(nullable=True, comment='°C')
    utci_category: Mapped[HeatStressCategories] = mapped_column(nullable=True)
    pet: Mapped[Decimal] = mapped_column(nullable=True, comment='°C')
    pet_category: Mapped[HeatStressCategories] = mapped_column(nullable=True)
    atmospheric_pressure: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='hPa',  # we've converted it to hPa in the meantime
    )
    atmospheric_pressure_reduced: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='hPa',
    )
    vapor_pressure: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='hPa',
    )
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
            mrt,
            utci,
            utci_category,
            pet,
            pet_category,
            atmospheric_pressure,
            atmospheric_pressure_reduced,
            vapor_pressure,
            air_temperature,
            relative_humidity,
            wind_speed,
            wind_direction,
            u_wind,
            v_wind,
            wind_speed_max,
            precipitation_sum,
            solar_radiation,
            lightning_average_distance,
            lightning_strike_count,
            sensor_temperature_internal,
            x_orientation_angle,
            y_orientation_angle,
            black_globe_temperature,
            thermistor_resistance,
            voltage_ratio,
            battery_voltage,
            dew_point,
            absolute_humidity,
            heat_index,
            wet_bulb_temperature,
            blg_battery_voltage
        )
        WITH (timescaledb.continuous, timescaledb.materialized_only = true) AS
            SELECT
                time_bucket('1hour', measured_at) AT TIME ZONE 'UTC' + '1 hour',
                name,
                AVG(mrt),
                AVG(utci),
                mode() WITHIN GROUP (ORDER BY utci_category),
                AVG(pet),
                mode() WITHIN GROUP (ORDER BY pet_category),
                AVG(atmospheric_pressure),
                AVG(atmospheric_pressure_reduced),
                AVG(vapor_pressure),
                AVG(air_temperature),
                AVG(relative_humidity),
                AVG(wind_speed),
                avg_angle(wind_direction),
                AVG(u_wind),
                AVG(v_wind),
                MAX(wind_speed_max),
                SUM(precipitation_sum),
                AVG(solar_radiation),
                AVG(lightning_average_distance),
                SUM(lightning_strike_count),
                AVG(sensor_temperature_internal),
                AVG(x_orientation_angle),
                AVG(y_orientation_angle),
                AVG(black_globe_temperature),
                AVG(thermistor_resistance),
                AVG(voltage_ratio),
                AVG(battery_voltage),
                AVG(dew_point),
                AVG(absolute_humidity),
                AVG(heat_index),
                AVG(wet_bulb_temperature),
                AVG(blg_battery_voltage)
            FROM biomet_data
            GROUP BY time_bucket('1hour', measured_at), name
    ''')


class TempRHDataHourly(_SHT35DataRawBase, _TempRHDerivatives):
    """This is not an actual table, but a materialized view. We simply trick sqlalchemy
    into thinking this was a table. Querying a materialized view does not differ from
    querying a proper table.
    """
    __tablename__ = 'temp_rh_data_hourly'
    air_temperature_raw: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='°C',
    )
    relative_humidity_raw: Mapped[Decimal] = mapped_column(
        nullable=True,
        comment='%',
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
            air_temperature_raw,
            relative_humidity_raw,
            air_temperature,
            relative_humidity,
            battery_voltage,
            dew_point,
            absolute_humidity,
            heat_index,
            wet_bulb_temperature
        )
        WITH (timescaledb.continuous, timescaledb.materialized_only = true) AS
            SELECT
                time_bucket('1hour', measured_at) AT TIME ZONE 'UTC' + '1 hour',
                name,
                AVG(air_temperature_raw),
                AVG(relative_humidity_raw),
                AVG(air_temperature),
                AVG(relative_humidity),
                AVG(battery_voltage),
                AVG(dew_point),
                AVG(absolute_humidity),
                AVG(heat_index),
                AVG(wet_bulb_temperature)
            FROM temp_rh_data
            GROUP BY time_bucket('1hour', measured_at), name
    ''')


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
