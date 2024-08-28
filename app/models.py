from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from enum import StrEnum

from sqlalchemy import BigInteger
from sqlalchemy import DateTime
from sqlalchemy import ForeignKey
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship

from app.database import Base


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
    moderate_heat_stress = 'moderate heat stress'
    strong_heat_stress = 'strong heat stress'
    very_strong_heat_stress = 'very strong heat stress'
    extreme_heat_stress = 'extreme heat stress'


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
    # TODO: this should become an Enum
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
    # TODO: QC fields?
    station: Mapped[Station] = relationship(
        back_populates='biomet_data',
        lazy=True,
    )


class TempRHData(_SHT35DataRawBase, _TempRHDerivatives):
    __tablename__ = 'temp_rh_data'
    # TODO: QC fields?
    station: Mapped[Station] = relationship(
        back_populates='temp_rh_data',
        lazy=True,
    )


async def refresh_views(db: AsyncSession) -> None:
    await db.execute(text('REFRESH MATERIALIZED VIEW latest_data'))


latest_data_view = '''\
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
'''
