import os
from collections.abc import Sequence
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from typing import Any
from typing import NamedTuple
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
from pythermalcomfort.models import heat_index_rothfusz
from pythermalcomfort.models import pet_steady
from pythermalcomfort.models import utci
from pythermalcomfort.shared_functions import mapping
from pythermalcomfort.utilities import dew_point_tmp
from pythermalcomfort.utilities import mean_radiant_tmp
from pythermalcomfort.utilities import Postures
from pythermalcomfort.utilities import Sex
from pythermalcomfort.utilities import wet_bulb_tmp
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
from app.models import Sensor
from app.models import SensorDeployments
from app.models import SensorType
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
    'sex': Sex.male.value,
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


async def _download_sensor_data(
        sensor: Sensor,
        target_table: type[SHT35DataRaw | ATM41DataRaw | BLGDataRaw],
        con: AsyncSession,
) -> pd.DataFrame:
    """Download data for a sensor if it is deployed.

    :param sensor: The sensor object from the database
    :param target_table: The target table to insert the data into. This has to
        correspond to the type of the sensor provided via ``sensor``.
    :param con: A async database session
    """
    start_date = (
        await con.execute(
            select(
                func.max(target_table.measured_at).label('newest_data'),
            ).where(target_table.sensor_id == sensor.sensor_id),
        )
    ).scalar_one_or_none()
    if start_date is not None:
        # the API request is inclusive, so we need to move forward one tick
        start_date += timedelta(microseconds=1)
    else:
        # if we don't have any data from the sensor in the db yet, we need to check all
        # deployments and find the earliest one
        start_date = (
            await con.execute(
                select(SensorDeployments.setup_date).where(
                    SensorDeployments.sensor_id == sensor.sensor_id,
                ).order_by(SensorDeployments.setup_date).limit(1),
            )
        ).scalar_one_or_none()
        if start_date is None:
            # there never were any deployments of that sensor, hence we cannot get any
            # data for that sensor
            return pd.DataFrame()

    # when we download "sensor" data, we can always download up until now (just in case)
    # selecting the relevant temporal bits based on the deployments can happen when we
    # perform the calculations.
    data = api.get_readings(
        device_name=sensor.sensor_id,
        sort='measured_at',
        sort_direction='asc',
        start=start_date,
        as_dataframe=True,
        stream=True,
        # the server will end the stream after that timeout
        timeout=120_000,
    )
    # we cannot trust the api, we need to double check that we don't pass too much data
    # an cause an error when trying to insert into the database
    if not data.empty:
        data = data.loc[start_date:]  # type: ignore[misc]

    return data


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


@async_task(app=celery_app, name='download-data')
async def _sync_data_wrapper() -> None:
    """This enqueues all individual tasks for downloading data from biomet and temp-rh
    stations. The result of the task is no awaited and checked.
    """
    async with sessionmanager.session() as sess:
        stations = (await sess.execute(select(Station))).scalars().all()
        tasks = []
        for station in stations:
            match station.station_type:
                case StationType.biomet:
                    tasks.append(
                        chain(
                            download_station_data.s(station.station_id),
                            # we don't pass an arg here manually, as the task will
                            # return the name of the station, which will be passed on
                            calculate_biomet.s(),
                        ),
                    )
                case StationType.temprh:
                    tasks.append(
                        chain(
                            download_station_data.s(station.station_id),
                            calculate_temp_rh.s(),
                        ),
                    )
                case StationType.double:
                    tasks.append(
                        chain(
                            download_station_data.s(station.station_id),
                            group([calculate_biomet.s(), calculate_temp_rh.s()]),
                        ),
                    )
                case _:
                    raise NotImplementedError

        # we now have all tasks prepared and can create a group and run them in parallel
        # the result ([None, ...]) will be passed to the refresh_all_views task, which
        # makes it wait for them. This way we can ensure that the views are only
        # refreshed once all data was inserted.
        task_group = group(tasks)
        task_chord = chord(task_group)
        task_chord(refresh_all_views.s())


class DeploymentInfo(NamedTuple):
    latest: datetime
    station: Station
    deployments: Sequence[SensorDeployments]


async def get_station_deployments(
        station_id: str,
        target_table: type[BiometData | TempRHData],
        con: AsyncSession,
) -> DeploymentInfo:
    """Get the deployments for a station and the latest data for that station.

    :param station_id: The station id
    :param target_table: The target table to check for the latest data
    :param con: The database connection

    :return: A named tuple with the latest date with data, the station object and the
        deployments that are relevant.
    """

    station = (
        await con.execute(select(Station).where(Station.station_id == station_id))
    ).scalar_one()
    # 1. get the newest biomet data, so we can start from there
    latest = (
        await con.execute(
            select(
                func.max(target_table.measured_at).label('newest_data'),
            ).where(target_table.station_id == station_id),
        )
    ).scalar_one_or_none()
    # set it to a date early enough, so there was no data, this specific to this
    # network in Dortmund
    if latest is None:
        latest = datetime(2024, 1, 1)

    # 2. get the biomet data, we potentially need to combine multiple deployments
    deployments = (
        await con.execute(
            select(SensorDeployments).where(
                (SensorDeployments.station_id == station.station_id) &
                (SensorDeployments.setup_date < latest) &
                (
                    (SensorDeployments.teardown_date > latest) |
                    # we do not need any older deployment, but the current
                    (SensorDeployments.teardown_date.is_(None))
                ),
            ),
        )
    ).scalars().all()
    # we have no deployments via the query, maybe this is the first time we
    # calculate data for that station? Just get all of them!
    if not deployments:
        deployments = station.deployments
    return DeploymentInfo(latest=latest, station=station, deployments=deployments)


@async_task(app=celery_app, name='calculate-biomet')
async def calculate_biomet(station_id: str | None) -> None:
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
    # the previous task did not get any new data, hence we can skip the calculation
    if station_id is None:
        return

    async with sessionmanager.session() as sess:
        deployment_info = await get_station_deployments(
            station_id,
            target_table=BiometData,
            con=sess,
        )
        df_atm41_list = []
        df_blg_list = []
        con = await sess.connection()
        for deployment in deployment_info.deployments:
            if deployment.sensor.sensor_type == SensorType.atm41:
                df_tmp_atm41 = await con.run_sync(
                    lambda con: pd.read_sql(
                        sql=select(ATM41DataRaw).where(
                            (ATM41DataRaw.sensor_id == deployment.sensor_id) &
                            (ATM41DataRaw.measured_at > deployment_info.latest) &
                            (
                                ATM41DataRaw.measured_at <= (
                                    deployment.teardown_date
                                    if deployment.teardown_date
                                    else datetime.now(tz=timezone.utc) + timedelta(hours=1)  # noqa: E501
                                )
                            ),
                        ).order_by(ATM41DataRaw.measured_at),
                        con=con,
                        # we need explicit types, when nothing is set so calculations
                        # can use NaN
                        dtype={
                            'air_temperature': 'float64',
                            'relative_humidity': 'float64',
                            'atmospheric_pressure': 'float64',
                            'vapor_pressure': 'float64',
                            'wind_speed': 'float64',
                            'wind_direction': 'float64',
                            'u_wind': 'float64',
                            'v_wind': 'float64',
                            'maximum_wind_speed': 'float64',
                            'precipitation_sum': 'float64',
                            'solar_radiation': 'float64',
                            'lightning_average_distance': 'float64',
                            'lightning_strike_count': 'float64',
                            'sensor_temperature_internal': 'float64',
                            'x_orientation_angle': 'float64',
                            'y_orientation_angle': 'float64',
                            'battery_voltage': 'float64',
                            'protocol_version': 'Int64',
                        },
                    ),
                )
                if df_tmp_atm41.empty:
                    continue
                else:
                    df_atm41_list.append(df_tmp_atm41)

            elif deployment.sensor.sensor_type == SensorType.blg:
                df_tmp_blg = await con.run_sync(
                    lambda con: pd.read_sql(
                        sql=select(
                            BLGDataRaw.measured_at.label('measured_at_blg'),
                            BLGDataRaw.black_globe_temperature,
                            BLGDataRaw.thermistor_resistance,
                            BLGDataRaw.voltage_ratio,
                            BLGDataRaw.battery_voltage.label('blg_battery_voltage'),
                        ).where(
                            (BLGDataRaw.sensor_id == deployment.sensor_id) &
                            # we allow earlier black globe measurements to be able to
                            # tie it to the closest ATM41 measurement, however it must
                            # not be before the start of the current deployment
                            (BLGDataRaw.measured_at > deployment.setup_date) &
                            (BLGDataRaw.measured_at > (deployment_info.latest - timedelta(minutes=5))) &  # noqa: E501
                            (
                                BLGDataRaw.measured_at <= (
                                    deployment.teardown_date
                                    if deployment.teardown_date
                                    else datetime.now(tz=timezone.utc) + timedelta(hours=1)  # noqa: E501
                                )
                            ),
                        ).order_by(BLGDataRaw.measured_at),
                        con=con,
                        dtype={
                            'black_globe_temperature': 'float64',
                            'thermistor_resistance': 'float64',
                            'voltage_ratio': 'float64',
                            'blg_battery_voltage': 'float64',
                        },
                    ),
                )
                if df_tmp_blg.empty:
                    continue
                else:
                    df_blg_list.append(df_tmp_blg)

        # we can't do anything if one of the two has missing data
        if not df_atm41_list or not df_blg_list:
            return

        atm41_data = pd.concat(df_atm41_list).sort_values('measured_at')
        blg_data = pd.concat(df_blg_list).sort_values('measured_at_blg')

        # 3. merge both with a tolerance of 5 minutes
        df_biomet = pd.merge_asof(
            left=atm41_data,
            right=blg_data,
            left_on='measured_at',
            right_on='measured_at_blg',
            direction='nearest',
            tolerance=timedelta(minutes=5),
        )
        # 4. remove the last rows if they don't have black globe data
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
            alt=deployment_info.station.altitude,
        )

        df_biomet['absolute_humidity'] = abshum_from_sat_vap_press(
            temp=df_biomet['air_temperature'],
            relhum=df_biomet['relative_humidity'],
        )

        df_biomet['mrt'] = mean_radiant_tmp(
            tg=df_biomet['black_globe_temperature'],
            tdb=df_biomet['air_temperature'],
            v=df_biomet['wind_speed'],
            standard='ISO',
        )

        df_biomet['dew_point'] = dew_point_tmp(
            tdb=df_biomet['air_temperature'],
            rh=df_biomet['relative_humidity'],
        )

        df_biomet['wet_bulb_temperature'] = wet_bulb_tmp(
            tdb=df_biomet['air_temperature'],
            rh=df_biomet['relative_humidity'],
        )

        # we need to unpack the stupid object...
        df_biomet['heat_index'] = heat_index_rothfusz(
            tdb=df_biomet['air_temperature'],
            rh=df_biomet['relative_humidity'],
            round_output=False,
        ).hi

        utci_values = utci(
            tdb=df_biomet['air_temperature'],
            tr=df_biomet['mrt'],
            v=df_biomet['wind_speed'],
            rh=df_biomet['relative_humidity'],
            limit_inputs=False,
            round_output=False,
        )
        df_biomet['utci'] = utci_values['utci']
        df_biomet['utci_category'] = utci_values['stress_category']
        # TODO (LW): validate this with the Klima Michel
        # we cannot calculate pet with atmospheric pressures of 0 (sometimes sensors
        # send this value) we need to set them to a value that is not 0
        atmospheric_pressure_mask = df_biomet['atmospheric_pressure'] == 0
        df_biomet.loc[atmospheric_pressure_mask, 'atmospheric_pressure'] = 1013.25
        df_biomet['pet'] = pet_steady(
            tdb=df_biomet['air_temperature'],
            tr=df_biomet['mrt'],
            v=df_biomet['wind_speed'],
            rh=df_biomet['relative_humidity'],
            met=KLIMA_MICHEL['activity'] / 58.2,
            clo=KLIMA_MICHEL['clo'],
            p_atm=df_biomet['atmospheric_pressure'],
            position=Postures.standing.value,
            age=KLIMA_MICHEL['age'],
            sex=KLIMA_MICHEL['sex'],
            weight=KLIMA_MICHEL['mbody'],
            height=KLIMA_MICHEL['height'],
            wme=0,  # external work, [W/(m2)]
        ).pet
        df_biomet['pet_category'] = mapping(df_biomet['pet'], PET_STRESS_CATEGORIES)
        # reset the atmospheric pressure to 0 again
        df_biomet.loc[atmospheric_pressure_mask, 'atmospheric_pressure'] = 0
        df_biomet['station_id'] = station_id

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
async def calculate_temp_rh(station_id: str | None) -> None:
    """Calculate derived parameters for a temp-rh station and insert the result into the
    respective database table.

    Currently the following parameters are calculated:

    - ``absolute_humidity``
    - ``dew_point``
    - ``wet_bulb_temperature``
    - ``heat_index``
    """
    # the previous task did not get any new data, hence we can skip the calculation
    if station_id is None:
        return

    async with sessionmanager.session() as sess:
        deployment_info = await get_station_deployments(
            station_id,
            target_table=TempRHData,
            con=sess,
        )
        df_list = []
        con = await sess.connection()
        for deployment in deployment_info.deployments:
            # this is relevant, if this is a double station
            if deployment.sensor.sensor_type != SensorType.sht35:
                continue
            df_tmp = await con.run_sync(
                lambda con: pd.read_sql(
                    sql=select(SHT35DataRaw).where(
                        (SHT35DataRaw.sensor_id == deployment.sensor_id) &
                        (SHT35DataRaw.measured_at > deployment_info.latest) &
                        (
                            SHT35DataRaw.measured_at <= (
                                deployment.teardown_date
                                if deployment.teardown_date
                                else datetime.now(tz=timezone.utc) + timedelta(hours=1)
                            )
                        ),
                    ).order_by(SHT35DataRaw.measured_at),
                    con=con,
                    dtype={
                        'air_temperature': 'float64',
                        'relative_humidity': 'float64',
                        'battery_voltage': 'float64',
                        'protocol_version': 'Int64',
                    },
                ),
            )
            if df_tmp.empty:
                continue
            else:
                # apply the calibration
                # also save the original values w/o calibration
                df_tmp['air_temperature_raw'] = df_tmp['air_temperature']
                df_tmp['relative_humidity_raw'] = df_tmp['relative_humidity']
                # now subtract the offset for the calibration
                df_tmp['air_temperature'] = df_tmp['air_temperature_raw'] - \
                    float(deployment.sensor.temp_calib_offset)
                df_tmp['relative_humidity'] = df_tmp['relative_humidity_raw'] - \
                    float(deployment.sensor.relhum_calib_offset)
                # if we reach a relhum > 100 after calibration, simply set it to 100
                df_tmp.loc[df_tmp['relative_humidity'] > 100, 'relative_humidity'] = 100
                df_list.append(df_tmp)

        if not df_list:
            return

        data = pd.concat(df_list)

        data = data.set_index('measured_at')
        # calculate derivates
        data['absolute_humidity'] = abshum_from_sat_vap_press(
            temp=data['air_temperature'],
            relhum=data['relative_humidity'],
        )
        data['dew_point'] = dew_point_tmp(
            tdb=data['air_temperature'],
            rh=data['relative_humidity'],
        )

        data['wet_bulb_temperature'] = wet_bulb_tmp(
            tdb=data['air_temperature'],
            rh=data['relative_humidity'],
        )

        data['heat_index'] = heat_index_rothfusz(
            tdb=data['air_temperature'],
            rh=data['relative_humidity'],
        ).hi
        data['station_id'] = station_id
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


async def get_latest_data(station: Station, con: AsyncSession) -> datetime | None:
    """Get the latest date of data for a station.

    :param station: The station object
    :param con: The database connection

    :return: The latest date with data for the station
    """
    async def _max_date(model: type[TempRHData | BiometData]) -> datetime | None:
        return (
            await con.execute(
                select(func.max(model.measured_at)).where(
                    model.station_id == station.station_id,
                ),
            )
        ).scalar_one_or_none()

    match station.station_type:
        case StationType.temprh:
            return await _max_date(TempRHData)
        case StationType.biomet:
            return await _max_date(BiometData)
        case StationType.double:
            latest_biomet = await _max_date(BiometData)
            latest_temprh = await _max_date(TempRHData)
            if latest_biomet is None or latest_temprh is None:
                return None
            else:
                return min(latest_biomet, latest_temprh)
        case _:
            raise NotImplementedError


@async_task(
    app=celery_app,
    name='download_station_data',
    autoretry_for=(HTTPError, TimeoutError),
    max_retries=3,
    default_retry_delay=20,
)
async def download_station_data(station_id: str) -> str | None:
    new_data = None
    async with sessionmanager.session() as sess:
        # check what the latest data for that station is
        station = (
            await sess.execute(select(Station).where(Station.station_id == station_id))
        ).scalar_one()
        # 1. check what we have in the final data table for the current station
        latest_data = await get_latest_data(station=station, con=sess)
        # 2. get the deployments that intersect with the timespan until now
        if latest_data is not None:
            deployments = (
                await sess.execute(
                    select(SensorDeployments).where(
                        (SensorDeployments.station_id == station.station_id) &
                        (SensorDeployments.setup_date < latest_data) &
                        (
                            (SensorDeployments.teardown_date > latest_data) |
                            # we don't need older deployment, but the current
                            (SensorDeployments.teardown_date.is_(None))
                        ),
                    ),
                )
            ).scalars().all()
        else:
            # we never had any data for that station up until now, so we need all
            # deployments ever made to that station
            deployments = station.deployments
        # if there are no deployments ([]), we simply skip the entire iteration
        con = await sess.connection()
        for deployment in deployments:
            # check what kind of sensor we have
            target_table: type[SHT35DataRaw | ATM41DataRaw | BLGDataRaw]
            match deployment.sensor.sensor_type:
                case SensorType.sht35:
                    target_table = SHT35DataRaw
                    col_selection = [
                        'sensor_id', 'battery_voltage', 'protocol_version',
                        'air_temperature', 'relative_humidity',
                    ]
                case SensorType.atm41:
                    target_table = ATM41DataRaw
                    col_selection = [
                        'air_temperature', 'relative_humidity', 'atmospheric_pressure',
                        'vapor_pressure', 'wind_speed', 'wind_direction', 'u_wind',
                        'v_wind', 'maximum_wind_speed', 'precipitation_sum',
                        'solar_radiation', 'lightning_average_distance',
                        'lightning_strike_count', 'sensor_temperature_internal',
                        'x_orientation_angle', 'y_orientation_angle', 'sensor_id',
                        'battery_voltage', 'protocol_version',
                    ]
                case SensorType.blg:
                    target_table = BLGDataRaw
                    col_selection = [
                        'battery_voltage', 'protocol_version',
                        'black_globe_temperature', 'thermistor_resistance',
                        'voltage_ratio', 'sensor_id',
                    ]
                case _:
                    raise NotImplementedError

            # this function checks the times yet again on a per-sensor basis
            data = await _download_sensor_data(
                sensor=deployment.sensor,
                target_table=target_table,
                con=sess,
            )
            # we did not get any data, so we can skip the rest
            if data.empty:
                continue

            data = data.copy()
            data.loc[:, 'sensor_id'] = deployment.sensor_id
            data = data.rename(columns=RENAMER)
            data = data[col_selection]
            # sometimes sensors have duplicates because Element fucked up internally
            data = data.reset_index()
            data = data.drop_duplicates()
            await con.run_sync(
                lambda con: data.to_sql(
                    name=target_table.__tablename__,
                    con=con,
                    if_exists='append',
                    chunksize=1024,
                    method='multi',
                    index=False,
                ),
            )
            new_data = True
        await sess.commit()
        # return the station name for the next task to be picked up
        return station.station_id if new_data else None
