import os
from datetime import datetime
from datetime import timedelta
from typing import Any

import pandas as pd
from celery.schedules import crontab
from element import ElementApi
from pythermalcomfort.models import heat_index
from pythermalcomfort.models import utci
from pythermalcomfort.psychrometrics import t_dp
from pythermalcomfort.psychrometrics import t_mrt
from pythermalcomfort.psychrometrics import t_wb
from sqlalchemy import func
from sqlalchemy import select

from app.celery import async_task
from app.celery import celery_app
from app.database import sessionmanager
from app.models import ATM41DataRaw
from app.models import BiometData
from app.models import BLGDataRaw
from app.models import SHT35DataRaw
from app.models import Station
from app.models import StationType
from app.models import TempRHData


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

KLIMA_MICHEL = {
    'mbody': 75,  # body mass (kg)
    'age': 35,  # person's age (years)
    'height': 1.75,  # height (meters)
    'activity': 135,  # activity level (W)
    'sex': 1,  # 1=male 2=female
    'clo': 0.9,  # clothing amount (0-5)
}


# TODO: we can make those calculations smart, so more code is reused

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
                method='multi',
            ),
        )
        await con.commit()
        # now that we have data, calculate the derived parameters as a new task
        calculate_temp_rh.delay(name)


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
                method='multi',
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
                method='multi',
            ),
        )
        await con.commit()
        # now that we have data, calculate the derived biomet parameters as a new task
        calculate_biomet.delay(station.name)


@async_task(app=celery_app, name='download-data')
async def _sync_data_wrapper() -> None:
    async with sessionmanager.connect() as con:
        stations = (await con.execute(select(Station))).all()
        for station in stations:
            if station.station_type == StationType.biomet:
                download_biomet_data.delay(station.name)
            else:
                download_temp_rh_data.delay(station.name)


@async_task(app=celery_app, name='calculate-biomet')
async def calculate_biomet(name: str) -> None:
    async with sessionmanager.connect() as con:
        # 1. get information on the current station
        station = (
            await con.execute(select(Station).where(Station.name == name))
        ).one()
        # 2. get the newest biomet data, so we can start from there
        biomet_latest = (
            await con.execute(
                select(
                    func.max(BiometData.measured_at).label('newest_data'),
                ).where(BiometData.name == name),
            )
        ).scalar_one_or_none()
        # set it to a date early enough, so there was no data
        if biomet_latest is None:
            biomet_latest = datetime(2024, 1, 1)

        # 3. get the biomet data
        atm41_data = await con.run_sync(
            lambda sync_con: pd.read_sql(
                sql=select(ATM41DataRaw).where(
                    (ATM41DataRaw.name == name) &
                    (ATM41DataRaw.measured_at > biomet_latest),
                ).order_by(ATM41DataRaw.measured_at),
                con=sync_con,
            ),
        )
        # 4. get the biomet data
        blg_data = await con.run_sync(
            lambda sync_con: pd.read_sql(
                sql=select(
                    BLGDataRaw.measured_at.label('measured_at_blg'),
                    BLGDataRaw.black_globe_temperature,
                ).where(
                    (BLGDataRaw.name == station.blg_name) &
                    # we allow earlier blackglobe measurements to be able to tie
                    # it to the closes ATM41 measurement
                    (BLGDataRaw.measured_at > (biomet_latest - timedelta(minutes=5))),
                ).order_by(BLGDataRaw.measured_at),
                con=sync_con,
            ),
        )
        # 5. merge both with a tolerance of 5 minutes
        df_biomet = pd.merge_asof(
            left=atm41_data,
            right=blg_data,
            left_on='measured_at',
            right_on='measured_at_blg',
            direction='nearest',
            tolerance=timedelta(minutes=5),
        )
        # 6. remove the last rows if they don't have blackglobe data
        while (
            not df_biomet.empty and
            pd.isna(df_biomet.iloc[-1]['black_globe_temperature'])
        ):
            df_biomet = df_biomet.iloc[0:-1]

        # save the time difference between black globe and atm 41 for future reference
        df_biomet['blg_time_offset'] = (
            df_biomet['measured_at'] - df_biomet['measured_at_blg']
        ).dt.total_seconds()
        df_biomet = df_biomet.drop('measured_at_blg', axis=1)

        df_biomet = df_biomet.set_index('measured_at')

        df_biomet['mrt'] = t_mrt(
            tg=df_biomet['black_globe_temperature'],
            tdb=df_biomet['air_temperature'],
            v=df_biomet['wind_speed'],
            standard='ISO',
        )

        df_biomet['dew_point'] = t_dp(
            tdb=df_biomet['air_temperature'],
            rh=df_biomet['relative_humidity'],
        )

        df_biomet['wet_bulb_temperature'] = t_wb(
            tdb=df_biomet['air_temperature'],
            rh=df_biomet['relative_humidity'],
        )

        df_biomet['heat_index'] = heat_index(
            tdb=df_biomet['air_temperature'],
            rh=df_biomet['relative_humidity'],
        )

        utci_values = utci(
            tdb=df_biomet['air_temperature'],
            tr=df_biomet['mrt'],
            v=df_biomet['wind_speed'],
            rh=df_biomet['relative_humidity'],
            return_stress_category=True,
        )
        df_biomet['utci'] = utci_values['utci']
        df_biomet['utci_category'] = utci_values['stress_category']
        # TODO (LW): validate this
        # TODO: This does not work on a dataframe?
        # pet = pet_steady(
        #     tdb=df_biomet['air_temperature'],
        #     tr=df_biomet['mrt'],
        #     v=df_biomet['wind_speed'],
        #     rh=df_biomet['relative_humidity'],
        #     met=KLIMA_MICHEL['activity'] / 58.2,
        #     clo=KLIMA_MICHEL['clo'],
        #     p_atm=df_biomet['atmospheric_pressure'],
        #     # position of the individual
        #     # (1=sitting, 2=standing, 3=standing, forced convection)
        #     position=2,
        #     age=KLIMA_MICHEL['age'],
        #     sex=KLIMA_MICHEL['sex'],
        #     weight=KLIMA_MICHEL['mbody'],
        #     height=KLIMA_MICHEL['height'],
        #     wme=0,  # external work, [W/(m2)]
        # )
        await con.run_sync(
            lambda sync_con: df_biomet.to_sql(
                name=BiometData.__tablename__,
                con=sync_con,
                if_exists='append',
                chunksize=1024,
                method='multi',
            ),
        )
        await con.commit()


@async_task(app=celery_app, name='calculate-temp_rh')
async def calculate_temp_rh(name: str) -> None:
    async with sessionmanager.connect() as con:
        # 1. get the newest data, so we can start from there
        latest = (
            await con.execute(
                select(
                    func.max(TempRHData.measured_at).label('newest_data'),
                ).where(TempRHData.name == name),
            )
        ).scalar_one_or_none()
        # set it to a date early enough, so there was no data
        if latest is None:
            latest = datetime(2024, 1, 1)

        # 3. get the temp and rh data
        data = await con.run_sync(
            lambda sync_con: pd.read_sql(
                sql=select(SHT35DataRaw).where(
                    (SHT35DataRaw.name == name) &
                    (SHT35DataRaw.measured_at > latest),
                ).order_by(SHT35DataRaw.measured_at),
                con=sync_con,
            ),
        )
        data = data.set_index('measured_at')

        data['dew_point'] = t_dp(
            tdb=data['air_temperature'],
            rh=data['relative_humidity'],
        )

        data['wet_bulb_temperature'] = t_wb(
            tdb=data['air_temperature'],
            rh=data['relative_humidity'],
        )

        data['heat_index'] = heat_index(
            tdb=data['air_temperature'],
            rh=data['relative_humidity'],
        )
        await con.run_sync(
            lambda sync_con: data.to_sql(
                name=TempRHData.__tablename__,
                con=sync_con,
                if_exists='append',
                chunksize=1024,
                method='multi',
            ),
        )
        await con.commit()
