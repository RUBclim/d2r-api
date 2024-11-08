import os
from datetime import datetime
from datetime import timedelta
from typing import Any
from typing import overload
from typing import Union
from urllib.error import HTTPError

import numpy as np
import pandas as pd
from celery import chain
from celery import chord
from celery import group
from celery.schedules import crontab
from element import ElementApi
from numpy import floating
from numpy.typing import NDArray
from pythermalcomfort.models import heat_index
from pythermalcomfort.models import pet_steady
from pythermalcomfort.models import utci
from pythermalcomfort.psychrometrics import t_dp
from pythermalcomfort.psychrometrics import t_mrt
from pythermalcomfort.psychrometrics import t_wb
from pythermalcomfort.utilities import mapping
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.celery import async_task
from app.celery import celery_app
from app.database import sessionmanager
from app.models import _HeatStressCategories
from app.models import ATM41DataRaw
from app.models import BiometData
from app.models import BiometDataDaily
from app.models import BiometDataHourly
from app.models import BLGDataRaw
from app.models import LatestData
from app.models import PET_STRESS_CATEGORIES
from app.models import SHT35DataRaw
from app.models import Station
from app.models import StationType
from app.models import TempRHData
from app.models import TempRHDataDaily
from app.models import TempRHDataHourly

# TODO: the pythermalcomfort imports are incredibly slow - we might as well just
# vendor the functions we need or even use them in C/fortran as the original
# ones


@celery_app.on_after_configure.connect
def setup_periodic_tasks(
        sender: Any,
        **kwargs: dict[str, Any],
) -> None:  # pragma: no cover
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


def reduce_pressure(p: Union[float, 'pd.Series[float]'], alt: float) -> float:
    """Correct barometric pressure in **hPa** to sea level
    Wallace, J.M. and P.V. Hobbes. 197725 Atmospheric Science:
    An Introductory Survey. Academic Press
    """
    return p + 1013.25 * (1 - (1 - alt / 44307.69231)**5.25328)


# autopep8: off


@overload
def abshum_from_sat_vap_press(temp: float, relhum: float) -> float: ...


@overload
def abshum_from_sat_vap_press(
        temp: 'pd.Series[float]',
        relhum: 'pd.Series[float]',
) -> 'pd.Series[float]': ...
# autopep8: on


def abshum_from_sat_vap_press(
        temp: Union[float, 'pd.Series[float]'],
        relhum: Union[float, 'pd.Series[float]'],
) -> Union[float, 'pd.Series[float]', NDArray[floating[Any]]]:
    '''Compute absolute humidity from relative humidity and temperature.
    using an estimated saturation vapour pressure as calculated by
    :func:`estimate_sat_vap_pressure`. Formula was taken from:
    https://www.hatchability.com/Vaisala.pdf

    :param temp: temperature in degrees celsius
    :param relhum: relative humidity in %

    :return: absolute humidity in ``g m^-3``
    '''
    C = 2.16679
    temp_kelvin = temp + 273.15
    sat_vap_pressure = estimate_sat_vap_pressure(temp)
    vap_pressure = sat_vap_pressure * relhum
    abshum = C * vap_pressure / temp_kelvin
    return abshum

# autopep8: off


@overload
def estimate_sat_vap_pressure(temp: float) -> float: ...


@overload
def estimate_sat_vap_pressure(temp: 'pd.Series[float]') -> NDArray[floating[Any]]: ...
# autopep8: on


def estimate_sat_vap_pressure(
        temp: Union[float, 'pd.Series[float]'],
) -> float | NDArray[floating[Any]]:
    '''Estimate saturation vapour pressure (in Pa) at a given temperature
    using the August-Roche-Magnus formula.
    https://www.hatchability.com/Vaisala.pdf

    :param temp: temperature in degrees celsius

    :return: vapour pressure in Pa
    '''
    r = 6.1094 * np.exp(17.625 * temp / (temp + 243.04))
    return r


async def _download_data(
        name: str,
        target_table: type[SHT35DataRaw | ATM41DataRaw | BLGDataRaw],
        con: AsyncSession,
) -> tuple[Station, pd.DataFrame]:
    """Download data for a station

    :param name: The name of the device as the hexadecimal address e.g. ``DEC0054B0``.
    :param target_table: The target table to insert the data into. This has to
        correspond to the type of the station provided via ``name``.
    :param con: A async database session
    """
    station = (
        # one of which has to be present. The "name" should be unique across all
        # devices
        await con.execute(
            select(Station).where((Station.name == name) | (Station.blg_name == name)),
        )
    ).scalar_one()

    start_date = (
        await con.execute(
            select(
                func.max(target_table.measured_at).label('newest_data'),
            ).where(target_table.name == name),
        )
    ).scalar_one_or_none()
    if start_date is not None:
        # the API request is inclusive, so we need to move forward one tick
        start_date += timedelta(microseconds=1)
    else:
        start_date = station.setup_date

    data = api.get_readings(
        device_name=name,
        sort='measured_at',
        sort_direction='asc',
        start=start_date,
        as_dataframe=True,
    )
    # we cannot trust the api, we need to double check that we don't pass too much data
    # an cause an error when trying to insert into the database
    if not data.empty:
        data = data.loc[start_date:]  # type: ignore[misc]

    return station, data


@async_task(app=celery_app, name='refresh-all-views')
async def refresh_all_views(*args: Any, **kwargs: Any) -> None:
    """Refresh all views in the database. This is a task that is called after
    all data was inserted. We need to accept any arguments, as the chord task
    will pass the results of the individual tasks to this task. We don't care
    about the results, but need to accept them.
    """
    await LatestData.refresh()
    await BiometDataHourly.refresh()
    await TempRHDataHourly.refresh()
    await BiometDataDaily.refresh()
    await TempRHDataDaily.refresh()


@async_task(
    app=celery_app,
    name='download_temp_rh_data',
    autoretry_for=(HTTPError, TimeoutError),
    max_retries=3,
    default_retry_delay=20,
)
async def download_temp_rh_data(name: str) -> str | None:
    """Download data from the a temp-rh station via the Element-Api. This task also
    enqueues another task for calculating derived parameters for this station.

    :param name: The name of the device as the hexadecimal address e.g. ``DEC0054B0``.
    """
    async with sessionmanager.session() as sess:
        _, data = await _download_data(name=name, target_table=SHT35DataRaw, con=sess)
        if data.empty:
            return None

        data = data.copy()
        data['name'] = name
        data = data.rename(columns=RENAMER)
        data = data[[
            'name', 'battery_voltage', 'protocol_version',
            'air_temperature', 'relative_humidity',
        ]]
        con = await sess.connection()
        await con.run_sync(
            lambda con: data.to_sql(
                name=SHT35DataRaw.__tablename__,
                con=con,
                if_exists='append',
                chunksize=1024,
                method='multi',
            ),
        )
        await sess.commit()
        return name


@async_task(
    app=celery_app,
    name='download-biomet-data',
    autoretry_for=(HTTPError, TimeoutError),
    max_retries=3,
    default_retry_delay=20,
)
async def download_biomet_data(name: str) -> str | None:
    """Download data from the a biomet station via the Element-Api. This task also
    enqueues another task for calculating derived parameters for this station.

    :param name: The name of the device as the hexadecimal address e.g. ``DEC0054B0``.
    """
    async with sessionmanager.session() as sess:
        station, data = await _download_data(
            name=name,
            target_table=ATM41DataRaw,
            con=sess,
        )
        # if we have no data, simply skip this, but still try getting data for
        # from the blackglobe. Either station may be unavailable at some point.
        if not data.empty:
            # yes, it's stupid, but the only way to silence the warnings in pandas
            data = data.copy()
            data.loc[:, 'name'] = name
            data = data.rename(columns=RENAMER)
            data = data[[
                'air_temperature', 'relative_humidity', 'atmospheric_pressure',
                'vapor_pressure', 'wind_speed', 'wind_direction', 'u_wind', 'v_wind',
                'maximum_wind_speed', 'precipitation_sum', 'solar_radiation',
                'lightning_average_distance', 'lightning_strike_count',
                'sensor_temperature_internal', 'x_orientation_angle',
                'y_orientation_angle', 'name', 'battery_voltage',
                'protocol_version',
            ]]
            con = await sess.connection()
            await con.run_sync(
                lambda con: data.to_sql(
                    name=ATM41DataRaw.__tablename__,
                    con=con,
                    if_exists='append',
                    chunksize=1024,
                    method='multi',
                ),
            )
        # now download the corresponding blackglobe data
        assert station.blg_name is not None
        _, blg_data = await _download_data(
            name=station.blg_name,
            target_table=BLGDataRaw,
            con=sess,
        )
        if blg_data.empty:
            return None

        blg_data = blg_data.copy()
        blg_data['name'] = station.blg_name
        blg_data = blg_data.rename(columns=RENAMER)
        blg_data = blg_data[[
            'battery_voltage', 'protocol_version', 'black_globe_temperature',
            'thermistor_resistance', 'voltage_ratio', 'name',
        ]]
        con = await sess.connection()
        await con.run_sync(
            lambda sync_cont: blg_data.to_sql(
                name=BLGDataRaw.__tablename__,
                con=sync_cont,
                if_exists='append',
                chunksize=1024,
                method='multi',
            ),
        )
        await sess.commit()
        # return the station name for the next task to be picked up
        return station.name


@async_task(app=celery_app, name='download-data')
async def _sync_data_wrapper() -> None:
    """This enqueues all individual tasks for downloading data from biomet and temp-rh
    stations. The result of the task is no awaited and checked.
    """
    async with sessionmanager.session() as sess:
        stations = (await sess.execute(select(Station))).scalars().all()
        tasks = []
        for station in stations:
            if station.station_type == StationType.biomet:
                tasks.append(
                    chain(
                        download_biomet_data.s(station.name),
                        # we don't pass an arg here manually, as the task will return
                        # the name of the station, which will be passed on
                        calculate_biomet.s(),
                    ),
                )
            else:
                tasks.append(
                    chain(
                        download_temp_rh_data.s(station.name),
                        calculate_temp_rh.s(),
                    ),
                )
        # we now have all tasks prepared and can create a group and run them in parallel
        # the result ([None, ...]) will be passed to the refresh_all_views task, which
        # makes it wait for them. This way we can ensure that the views are only
        # refreshed once all data was inserted.
        task_group = group(tasks)
        task_chord = chord(task_group)
        task_chord(refresh_all_views.s())


@async_task(app=celery_app, name='calculate-biomet')
async def calculate_biomet(name: str | None) -> None:
    """Calculate derived parameters for a biomet station and insert the result into the
    respective database table.

    Currently the following parameters are calculated:

    - ``blg_time_offset`` the time offset between the blackglobe measurement and the
        atm41 measurement
    - ``atmospheric_pressure``
    - ``vapor_pressure``
    - ``atmospheric_pressure_reduced``
    - ``absolute_humidity``
    - ``mrt``
    - ``dew_point``
    - ``wet_bulb_temperature``
    - ``heat_index``
    - ``utci``
    - ``utci_category``
    - ``pet``
    - ``pet_category``
    """
    if name is None:
        return

    async with sessionmanager.session() as sess:
        # 1. get information on the current station
        station = (
            await sess.execute(select(Station).where(Station.name == name))
        ).scalar_one()
        # 2. get the newest biomet data, so we can start from there
        biomet_latest = (
            await sess.execute(
                select(
                    func.max(BiometData.measured_at).label('newest_data'),
                ).where(BiometData.name == name),
            )
        ).scalar_one_or_none()
        # set it to a date early enough, so there was no data
        if biomet_latest is None:
            biomet_latest = datetime(2024, 1, 1)

        # 3. get the biomet data
        con = await sess.connection()
        atm41_data = await con.run_sync(
            lambda con: pd.read_sql(
                sql=select(ATM41DataRaw).where(
                    (ATM41DataRaw.name == name) &
                    (ATM41DataRaw.measured_at > biomet_latest),
                ).order_by(ATM41DataRaw.measured_at),
                con=con,
            ),
        )
        # we cannot do anything if there is no biomet data
        if atm41_data.empty:
            return

        # 4. get the biomet data
        blg_data = await con.run_sync(
            lambda con: pd.read_sql(
                sql=select(
                    BLGDataRaw.measured_at.label('measured_at_blg'),
                    BLGDataRaw.black_globe_temperature,
                    BLGDataRaw.thermistor_resistance,
                    BLGDataRaw.voltage_ratio,
                    BLGDataRaw.battery_voltage.label('blg_battery_voltage'),
                ).where(
                    (BLGDataRaw.name == station.blg_name) &
                    # we allow earlier blackglobe measurements to be able to tie
                    # it to the closes ATM41 measurement
                    (BLGDataRaw.measured_at > (biomet_latest - timedelta(minutes=5))),
                ).order_by(BLGDataRaw.measured_at),
                con=con,
            ),
        )
        # if we have no BLG data we cannot merge both and need to wait until new
        # data comes up
        if blg_data.empty:
            return

        # 5. merge both with a tolerance of 5 minutes
        df_biomet = pd.merge_asof(
            left=atm41_data,
            right=blg_data,
            left_on='measured_at',
            right_on='measured_at_blg',
            direction='nearest',
            tolerance=timedelta(minutes=5),
        )
        # 6. remove the last rows if they don't have black globe data
        while (
            not df_biomet.empty and
            pd.isna(df_biomet.iloc[-1]['black_globe_temperature'])
        ):
            df_biomet = df_biomet.iloc[0:-1]

        # we may end up with an empty data after removing empty black globe data
        if df_biomet.empty:
            return

        # save the time difference between black globe and atm 41 for future reference
        df_biomet['blg_time_offset'] = (
            df_biomet['measured_at'] - df_biomet['measured_at_blg']
        ).dt.total_seconds()
        df_biomet = df_biomet.drop('measured_at_blg', axis=1)

        df_biomet = df_biomet.set_index('measured_at')

        # convert kPa to hPa
        df_biomet['atmospheric_pressure'] = df_biomet['atmospheric_pressure'] * 10
        df_biomet['vapor_pressure'] = df_biomet['vapor_pressure'] * 10
        df_biomet['atmospheric_pressure_reduced'] = reduce_pressure(
            p=df_biomet['atmospheric_pressure'],
            alt=station.altitude,
        )

        df_biomet['absolute_humidity'] = abshum_from_sat_vap_press(
            temp=df_biomet['air_temperature'],
            relhum=df_biomet['relative_humidity'],
        )

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
            limit_inputs=False,
        )
        df_biomet['utci'] = utci_values['utci']
        df_biomet['utci_category'] = utci_values['stress_category']
        # TODO (LW): validate this with the Klima Michel, we need to also
        # somehow filter this annoying warning, it floods our logs with nonsense
        # this only seems to work a per-row basis, hence the apply along axis=1
        df_biomet['pet'] = df_biomet[
            [
                'air_temperature', 'mrt', 'wind_speed',
                'relative_humidity', 'atmospheric_pressure',
            ]
        ].apply(
            lambda x: pet_steady(
                tdb=x['air_temperature'],
                tr=x['mrt'],
                v=x['wind_speed'],
                rh=x['relative_humidity'],
                met=KLIMA_MICHEL['activity'] / 58.2,
                clo=KLIMA_MICHEL['clo'],
                p_atm=x['atmospheric_pressure'],
                # position of the individual
                # (1=sitting, 2=standing, 3=standing, forced convection)
                position=2,
                age=KLIMA_MICHEL['age'],
                sex=KLIMA_MICHEL['sex'],
                weight=KLIMA_MICHEL['mbody'],
                height=KLIMA_MICHEL['height'],
                wme=0,  # external work, [W/(m2)]
            ),
            axis=1,
        )
        # TODO: do the categories even apply to PET?
        df_biomet['pet_category'] = mapping(df_biomet['pet'], PET_STRESS_CATEGORIES)

        con = await sess.connection()
        await con.run_sync(
            lambda con: df_biomet.to_sql(
                name=BiometData.__tablename__,
                con=con,
                if_exists='append',
                chunksize=1024,
                method='multi',
                dtype={
                    'utci_category': _HeatStressCategories,  # type: ignore[dict-item]
                    'pet_category': _HeatStressCategories,  # type: ignore[dict-item]
                },
            ),
        )
        await sess.commit()


@async_task(app=celery_app, name='calculate-temp_rh')
async def calculate_temp_rh(name: str | None) -> None:
    """Calculate derived parameters for a temp-rh station and insert the result into the
    respective database table.

    Currently the following parameters are calculated:

    - ``absolute_humidity``
    - ``dew_point``
    - ``wet_bulb_temperature``
    - ``heat_index``
    """
    if name is None:
        return

    async with sessionmanager.session() as sess:
        station = (
            await sess.execute(select(Station).where(Station.name == name))
        ).scalar_one()
        # 1. get the newest data, so we can start from there
        latest = (
            await sess.execute(
                select(
                    func.max(TempRHData.measured_at).label('newest_data'),
                ).where(TempRHData.name == name),
            )
        ).scalar_one_or_none()
        # set it to a date early enough, so there was no data
        if latest is None:
            latest = datetime(2024, 1, 1)

        # 3. get the temp and rh data
        con = await sess.connection()
        data = await con.run_sync(
            lambda con: pd.read_sql(
                sql=select(SHT35DataRaw).where(
                    (SHT35DataRaw.name == name) &
                    (SHT35DataRaw.measured_at > latest),
                ).order_by(SHT35DataRaw.measured_at),
                con=con,
            ),
        )
        if data.empty:
            return

        data = data.set_index('measured_at')
        # apply the calibration
        # also save the original values w/o calibration
        data['air_temperature_raw'] = data['air_temperature']
        data['relative_humidity_raw'] = data['relative_humidity']
        # now subtract the offset for the calibration
        data['air_temperature'] = data['air_temperature_raw'] - \
            float(station.temp_calib_offset)
        data['relative_humidity'] = data['relative_humidity_raw'] - \
            float(station.relhum_calib_offset)
        # if we reach a relhum > 100 after calibration, simply set it to 100
        data.loc[data['relative_humidity'] > 100, 'relative_humidity'] = 100
        # calculate derivates
        data['absolute_humidity'] = abshum_from_sat_vap_press(
            temp=data['air_temperature'],
            relhum=data['relative_humidity'],
        )
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
        con = await sess.connection()
        await con.run_sync(
            lambda con: data.to_sql(
                name=TempRHData.__tablename__,
                con=con,
                if_exists='append',
                chunksize=1024,
                method='multi',
            ),
        )
        await sess.commit()
