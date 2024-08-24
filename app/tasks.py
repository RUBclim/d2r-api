import os
from datetime import timedelta
from typing import Any

from celery.schedules import crontab
from element import ElementApi
from sqlalchemy import func
from sqlalchemy import select

from app.celery import async_task
from app.celery import celery_app
from app.database import sessionmanager
from app.models import ATM41DataRaw
from app.models import BLGDataRaw
from app.models import SHT35DataRaw
from app.models import Station
from app.models import StationType


@celery_app.on_after_configure.connect
def setup_periodic_tasks(sender: Any, **kwargs: dict[str, Any]) -> None:
    sender.add_periodic_task(
        crontab(minute='*/5'),
        _sync_data_wrapper.s(),
        name='download-data-periodic',
    )


api = ElementApi(
    api_location='https://dew21.element-iot.com/api/v1/',
    api_key=os.environ['ELEMENT_API_KEY'],
)


RENAMER = {
    'air_humidity': 'relative_humidity',
    'east_wind_speed': 'u_wind',
    'north_wind_speed': 'v_wind',
    'maximum_wind_speed': 'wind_speed_max',
    'precipitation': 'precipitation_sum',
    'temperature': 'black_globe_temperature',
}


@async_task(
    app=celery_app,
    name='download_temp_rh_data',
)
async def download_temp_rh_data(name: str) -> None:
    async with sessionmanager.connect() as con:
        start_date = (
            await con.execute(
                select(
                    func.max(SHT35DataRaw.measured_at).label('newest_data'),
                ).where(SHT35DataRaw.name == name),
            )
        ).scalar_one_or_none()
        if start_date is not None:
            # the API request is inclusive, so we need to move forward one tick
            start_date += timedelta(microseconds=1)
        data = api.get_readings(
            device_name=name,
            sort='measured_at',
            sort_direction='asc',
            start=start_date,
            as_dataframe=True,
        )
        if data.empty:
            return

        data['name'] = name
        data = data.rename(columns=RENAMER)
        subset = [
            'name', 'battery_voltage', 'protocol_version',
            'air_temperature', 'relative_humidity',
        ]
        data = data[subset]
        await con.run_sync(
            lambda sync_con: data.to_sql(
                name=SHT35DataRaw.__tablename__,
                con=sync_con,
                if_exists='append',
                chunksize=1024,
            ),
        )
        await con.commit()


@async_task(
    app=celery_app,
    name='download-biomet-data',
)
async def download_biomet_data(name: str) -> None:
    async with sessionmanager.connect() as con:
        # TODO: something is off with the type
        station = (
            await con.execute(select(Station).where(Station.name == name))
        ).one()
        start_date_atm_41 = (
            await con.execute(
                select(
                    func.max(ATM41DataRaw.measured_at).label('newest_data'),
                ).where(ATM41DataRaw.name == name),
            )
        ).scalar_one_or_none()
        if start_date_atm_41 is not None:
            # the API request is inclusive, so we need to move forward one tick
            start_date_atm_41 += timedelta(microseconds=1)
        data = api.get_readings(
            device_name=name,
            sort='measured_at',
            sort_direction='asc',
            start=start_date_atm_41,
            as_dataframe=True,
        )
        if data.empty:
            return

        data['name'] = name
        data = data.rename(columns=RENAMER)
        subset = [
            'air_temperature', 'relative_humidity', 'atmospheric_pressure',
            'vapor_pressure', 'wind_speed', 'wind_direction', 'u_wind', 'v_wind',
            'wind_speed_max', 'precipitation_sum', 'solar_radiation',
            'lightning_average_distance', 'lightning_strike_count',
            'sensor_temperature_internal', 'x_orientation_angle',
            'y_orientation_angle', 'name', 'battery_voltage',
            'protocol_version',
        ]
        data = data[subset]
        await con.run_sync(
            lambda sync_con: data.to_sql(
                name=ATM41DataRaw.__tablename__,
                con=sync_con,
                if_exists='append',
                chunksize=1024,
            ),
        )
        # now download the corresponding blackglobe data
        start_date_blg = (
            await con.execute(
                select(
                    func.max(BLGDataRaw.measured_at).label('newest_data'),
                ).where(BLGDataRaw.name == station.blg_name),
            )
        ).scalar_one_or_none()
        if start_date_blg is not None:
            # the API request is inclusive, so we need to move forward one tick
            start_date_blg += timedelta(microseconds=1)
        data = api.get_readings(
            device_name=station.blg_name,
            sort='measured_at',
            sort_direction='asc',
            start=start_date_blg,
            as_dataframe=True,
        )
        if data.empty:
            return

        data['name'] = station.blg_name
        data = data.rename(columns=RENAMER)
        subset = [
            'battery_voltage', 'protocol_version', 'black_globe_temperature',
            'thermistor_resistance', 'voltage_ratio', 'name',
        ]
        data = data[subset]
        await con.run_sync(
            lambda sync_con: data.to_sql(
                name=BLGDataRaw.__tablename__,
                con=sync_con,
                if_exists='append',
                chunksize=1024,
            ),
        )
        await con.commit()


@async_task(app=celery_app, name='download-data')
async def _sync_data_wrapper() -> None:
    async with sessionmanager.connect() as con:
        print('hi')
        stations = (await con.execute(select(Station))).all()
        for station in stations:
            if station.station_type == StationType.biomet:
                download_biomet_data.delay(station.name)
            else:
                download_temp_rh_data.delay(station.name)
