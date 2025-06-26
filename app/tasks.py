import os
from collections.abc import Sequence
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from typing import Any
from typing import Literal
from typing import NamedTuple
from typing import Union
from urllib.error import HTTPError

import numpy as np
import pandas as pd
from celery import Celery
from celery import chain
from celery import chord
from celery import group
from celery.result import AsyncResult
from celery.schedules import crontab
from element import ElementApi
from numpy.typing import NDArray
from sqlalchemy import and_
from sqlalchemy import Boolean
from sqlalchemy import func
from sqlalchemy import literal
from sqlalchemy import or_
from sqlalchemy import select
from sqlalchemy import union_all
from sqlalchemy.ext.asyncio import AsyncSession
from thermal_comfort import absolute_humidity
from thermal_comfort import dew_point
from thermal_comfort import heat_index_extended
from thermal_comfort import mean_radiant_temp
from thermal_comfort import pet_static
from thermal_comfort import specific_humidity
from thermal_comfort import utci_approx
from thermal_comfort import wet_bulb_temp

from app.celery import async_task
from app.celery import celery_app
from app.database import sessionmanager
from app.models import _HeatStressCategories
from app.models import ATM41DataRaw
from app.models import BiometData
from app.models import BiometDataDaily
from app.models import BiometDataHourly
from app.models import BLGDataRaw
from app.models import BuddyCheckQc
from app.models import HeatStressCategories
from app.models import LatestData
from app.models import MaterializedView
from app.models import PET_STRESS_CATEGORIES
from app.models import Sensor
from app.models import SensorDeployment
from app.models import SensorType
from app.models import SHT35DataRaw
from app.models import Station
from app.models import StationType
from app.models import TempRHData
from app.models import TempRHDataDaily
from app.models import TempRHDataHourly
from app.models import UTCI_STRESS_CATEGORIES
from app.qc import apply_buddy_check
from app.qc import apply_qc
from app.qc import BUDDY_CHECK_COLUMNS


# https://github.com/sbdchd/celery-types/issues/80
AsyncResult.__class_getitem__ = classmethod(  # type: ignore [attr-defined]
    lambda cls, *args, **kwargs: cls,
)


@celery_app.on_after_configure.connect
def setup_periodic_tasks(
        sender: Celery,
        **kwargs: dict[str, Any],
) -> None:  # pragma: no cover
    sender.add_periodic_task(
        crontab(minute='*/5'),
        _sync_data_wrapper.s(),
        name='download-data-periodic',
        expires=5*60,
    )
    sender.add_periodic_task(
        crontab(minute='2', hour='*/1'),
        check_for_new_sensors.s(),
        name='check-new-sensors-periodic',
        expires=5*60,
    )
    sender.add_periodic_task(
        crontab(minute='2', hour='1'),
        self_test_integrity.s(),
        name='self_test_integrity',
        expires=5*60,
    )
    sender.add_periodic_task(
        crontab(minute='2,22,42'),
        perform_spatial_buddy_check.s(),
        name='spatial-buddy-check-periodic',
        expires=10*60,
    )
    sender.add_periodic_task(
        # we download data every 5 minutes (0, 5, 10, 15, ...) so let's try
        # minute 3 to avoid collisions with the download task
        crontab(hour='1', minute='3'),
        # once a day refresh all views in case something was changed in older data
        refresh_all_views.s(),
        name='refresh-all-views-periodic',
        expires=60*60,
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

# in the original data telegram from the sensor there are no NULL values - instead
# they are simply represented as 0. All values are transmitted as an uint16. To get the
# correct values it needs to subtracted by 32768, which is the offset and then scaled by
# 10 or 100 depending on the value type. hence Null values end up being -3276.8 or
# -327.68, which is not a valid value.
# https://cdn.decentlab.com/download/datasheets/Decentlab-DL-ATM41-datasheet.pdf
# https://cdn.decentlab.com/download/datasheets/Decentlab-DL-SHT35-datasheet.pdf
NULL_VALUES: dict[str, dict[str, tuple[float, float]]] = {
    'temprh': {
        'air_temperature': (-45, 130),
        'relative_humidity': (0, float('nan')),
    },
    'biomet': {
        'solar_radiation': (-32768, 32767),
        'precipitation_sum': (-32.768, 32.767),
        'lightning_strike_count': (-32768, 32767),
        'lightning_average_distance': (-32768, 32767),
        'wind_speed': (-327.68, 327.67),
        'wind_direction': (-3276.8, 3276.7),
        'maximum_wind_speed': (-327.68, 327.67),
        'air_temperature': (-3276.8, 3276.7),
        'vapor_pressure': (-327.68, 327.67),
        'atmospheric_pressure': (-327.68, 327.67),
        # TODO: we had -3276.7 in the past?
        'relative_humidity': (-3276.8, 3276.7),
        'sensor_temperature_internal': (-3276.8, 3276.7),
        'x_orientation_angle': (-3276.8, 3276.7),
        'y_orientation_angle': (-3276.8, 3276.7),
        'u_wind': (-327.68, 327.67),
        'v_wind': (-327.68, 327.67),
        'battery_voltage': (-327.68, 327.67),
    },
}


def reduce_pressure(
        p: Union[float, 'pd.Series[float]'],
        alt: float,
) -> Union[float, 'pd.Series[float]']:
    """Correct barometric pressure in **hPa** to sea level
    Wallace, J.M. and P.V. Hobbes. 197725 Atmospheric Science:
    An Introductory Survey. Academic Press
    """
    return p + 1013.25 * (1 - (1 - alt / 44307.69231)**5.25328)


def category_mapping(
        value: Union[float, 'pd.Series[float]'],
        mapping: dict[float, HeatStressCategories],
        right: bool = True,
) -> NDArray[np.str_]:
    """Maps a value array to categories.

    Taken from: https://github.com/CenterForTheBuiltEnvironment/pythermalcomfort/blob/496f3799de287737f2ea53cc6a8c900052a29aaa/pythermalcomfort/utilities.py#L378-L397

    :param value: The numeric value to map
    :param mapping: A dictionary with the mapping of the values to categories
    :param right: Indicating whether the intervals include the right or the left
        bin edge.

    :returns: The category the value(s) fit(s) into
    """  # noqa: E501
    bins = np.array(list(mapping.keys()))
    words = np.append(np.array(list(mapping.values())), HeatStressCategories.unknown)
    return words[np.digitize(value, bins, right=right)]


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
                select(SensorDeployment.setup_date).where(
                    SensorDeployment.sensor_id == sensor.sensor_id,
                ).order_by(SensorDeployment.setup_date).limit(1),
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

TableNames = Literal[
    'latest_data',
    'biomet_data_hourly', 'biomet_data_daily',
    'temprh_data_hourly', 'temprh_data_daily',
]

# they are ordered by the duration they are expected to run to optimize the runtime
VIEW_MAPPING: dict[TableNames, type[MaterializedView]] = {
    'biomet_data_hourly': BiometDataHourly,
    'temprh_data_hourly': TempRHDataHourly,
    'temprh_data_daily': TempRHDataDaily,
    'biomet_data_daily': BiometDataDaily,
    'latest_data': LatestData,
}


@async_task(app=celery_app, name='refresh-view')
async def _refresh_view(
        view_name: TableNames,
        window_start: datetime | None,
        window_end: datetime | None,
) -> None:
    """Refresh a view as a celery task."""
    view = VIEW_MAPPING[view_name]
    await view.refresh(window_start=window_start, window_end=window_end)


@async_task(app=celery_app, name='refresh-all-views')
async def refresh_all_views(
        prev_res: Sequence[Any] = [],
        window_start: datetime | None = None,
        window_end: datetime | None = None,
        *args: Any,
        **kwargs: dict[str, Any],
) -> AsyncResult[Any]:
    """Refresh all views in the database. This is a task that is called after
    all data was inserted. We need to accept any arguments, as the chord task
    will pass the results of the individual tasks to this task. We don't care
    about the results, but need to accept them.

    :param prev_res: The results of the previous tasks, which we don't care about
        however, it is passed as the first positional argument by the chord task so
        we need to accept it.
    :param window_start: The start of the time window to refresh the views for
    :param window_end: The end of the time window to refresh the views for
    :param args: Additional positional arguments
    """
    refresh_group = group(
        _refresh_view.s(
            view_name=view_name,
            window_start=window_start,
            window_end=window_end,
        ) for view_name in VIEW_MAPPING
    )
    return refresh_group.apply_async()


@async_task(app=celery_app, name='download-data')
async def _sync_data_wrapper() -> None:
    """This enqueues all individual tasks for downloading data from biomet and temp-rh
    stations. The result of the task is no awaited and checked.
    """
    async with sessionmanager.session() as sess:
        stations = (
            await sess.execute(select(Station).order_by(Station.station_id))
        ).scalars().all()
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
        # TODO: this is not executed if any of the tasks fail
        # let's not refresh the entire database, but only the last day
        # we need to check the state of the view so in case the system was down for a
        # while we properly refresh to an up-to-date state.
        oldest_view_state = (
            await sess.execute(
                select(
                    func.least(
                        select(
                            func.max(BiometDataHourly.measured_at),
                        ).scalar_subquery(),
                        select(
                            func.max(BiometDataDaily.measured_at),
                        ).scalar_subquery(),
                        select(
                            func.max(TempRHDataHourly.measured_at),
                        ).scalar_subquery(),
                        select(
                            func.max(TempRHDataDaily.measured_at),
                        ).scalar_subquery(),
                    ),
                ),
            )
        ).scalar_one_or_none()
        if oldest_view_state is not None:
            # give an hour overlap to ensure that we don't miss any data
            # round down to the nearest full hour and then subtract one hour
            # from this. This avoids not using a full hour in the query even though it
            # would be present
            oldest_view_state = (
                pd.Timestamp(oldest_view_state).floor('1h') - timedelta(hours=1)
            ).to_pydatetime()

        task_chord(refresh_all_views.s(window_start=oldest_view_state))


class DeploymentInfo(NamedTuple):
    latest: datetime
    station: Station
    deployments: Sequence[SensorDeployment]


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
            select(SensorDeployment).where(
                # only for the station we currently look at
                SensorDeployment.station_id == station.station_id,
                or_(
                    and_(
                        # the setup must have been before the latest data, but then
                        # the teardown date must be NULL (an active station) or the
                        # teardown date must be after the latest data so we can download
                        # new data from this.
                        SensorDeployment.setup_date < latest,
                        or_(
                            SensorDeployment.teardown_date.is_(None),
                            SensorDeployment.teardown_date > latest,
                        ),
                    ),
                    # if that's not the case the setup must be after the latest data
                    # since we want to download up until right now
                    SensorDeployment.setup_date >= latest,
                ),
                # start with the oldest deployments first
            ).order_by(SensorDeployment.setup_date),
        )
    ).scalars().all()
    # we have no deployments via the query, maybe this is the first time we
    # calculate data for that station? Just get all of them!
    if not deployments:
        deployments = (await station.awaitable_attrs.deployments)
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
    - ``specific_humidity``
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
            if (await deployment.awaitable_attrs.sensor).sensor_type == SensorType.atm41:  # noqa: E501
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

            elif deployment.sensor.sensor_type == SensorType.blg:  # pragma: no branch
                df_tmp_blg = await con.run_sync(
                    lambda con: pd.read_sql(
                        sql=select(
                            BLGDataRaw.measured_at.label('measured_at_blg'),
                            BLGDataRaw.sensor_id.label('blg_sensor_id'),
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
        # now set the null values based on the uint16 representation of the values
        for col, null_value in NULL_VALUES['biomet'].items():
            if col in df_biomet.columns:
                df_biomet.loc[df_biomet[col].isin(null_value), col] = float('nan')

        # convert kPa to hPa
        df_biomet['atmospheric_pressure'] = df_biomet['atmospheric_pressure'] * 10
        df_biomet['vapor_pressure'] = df_biomet['vapor_pressure'] * 10
        # we need to add the mounting height above ground
        mounting_height = deployment_info.station.sensor_height_agl or 0.0
        df_biomet['atmospheric_pressure_reduced'] = reduce_pressure(
            p=df_biomet['atmospheric_pressure'],
            alt=deployment_info.station.altitude + float(mounting_height),
        )

        df_biomet['absolute_humidity'] = absolute_humidity(
            ta=df_biomet['air_temperature'],
            rh=df_biomet['relative_humidity'],
        )

        df_biomet['specific_humidity'] = specific_humidity(
            ta=df_biomet['air_temperature'],
            rh=df_biomet['relative_humidity'],
            p=df_biomet['atmospheric_pressure'],
        )

        df_biomet['mrt'] = mean_radiant_temp(
            ta=df_biomet['air_temperature'],
            tg=df_biomet['black_globe_temperature'],
            v=df_biomet['wind_speed'],
        )

        df_biomet['dew_point'] = dew_point(
            ta=df_biomet['air_temperature'],
            rh=df_biomet['relative_humidity'],
        )

        df_biomet['wet_bulb_temperature'] = wet_bulb_temp(
            ta=df_biomet['air_temperature'],
            rh=df_biomet['relative_humidity'],
        )

        df_biomet['heat_index'] = heat_index_extended(
            ta=df_biomet['air_temperature'],
            rh=df_biomet['relative_humidity'],
        )

        df_biomet['utci'] = utci_approx(
            ta=df_biomet['air_temperature'],
            tmrt=df_biomet['mrt'],
            v=df_biomet['wind_speed'],
            rh=df_biomet['relative_humidity'],
        )
        # retrieve the UTCI-stress category
        df_biomet['utci_category'] = category_mapping(
            df_biomet['utci'],
            UTCI_STRESS_CATEGORIES,
        )

        # we cannot calculate pet with atmospheric pressures of 0 (sometimes sensors
        # send this value) we need to set them to a value that is not 0
        atmospheric_pressure_mask = df_biomet['atmospheric_pressure'] == 0
        df_biomet.loc[atmospheric_pressure_mask, 'atmospheric_pressure'] = 1013.25
        df_biomet['pet'] = df_biomet[
            [
                'air_temperature', 'mrt', 'wind_speed',
                'relative_humidity', 'atmospheric_pressure',
            ]
        ].apply(
            lambda x: pet_static(
                ta=x['air_temperature'],
                tmrt=x['mrt'],
                v=x['wind_speed'],
                rh=x['relative_humidity'],
                p=x['atmospheric_pressure'],
            ),
            axis=1,
        )
        df_biomet['pet_category'] = category_mapping(
            df_biomet['pet'],
            PET_STRESS_CATEGORIES,
        )
        # reset the atmospheric pressure to 0 again
        df_biomet.loc[atmospheric_pressure_mask, 'atmospheric_pressure'] = 0
        df_biomet['station_id'] = station_id
        df_biomet = await apply_qc(data=df_biomet, station_id=station_id)
        con = await sess.connection()
        # the maximum number of parameters we can insert at once is 65535 so we need to
        # limit the chunksize accordingly
        await con.run_sync(
            lambda con: df_biomet.to_sql(
                name=BiometData.__tablename__,
                con=con,
                if_exists='append',
                chunksize=65535 // (
                    len(df_biomet.columns) +
                    len(df_biomet.index.names)
                ),
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
    - ``specific_humidity``
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
            if (await deployment.awaitable_attrs.sensor).sensor_type != SensorType.sht35:  # noqa: E501
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
        for col, null_value in NULL_VALUES['temprh'].items():
            if col in data.columns:
                data.loc[data[col].isin(null_value), col] = float('nan')

        # calculate derivates
        data['absolute_humidity'] = absolute_humidity(
            ta=data['air_temperature'],
            rh=data['relative_humidity'],
        )
        data['specific_humidity'] = specific_humidity(
            ta=data['air_temperature'],
            rh=data['relative_humidity'],
            # we use the default atmospheric pressure of 1013.25 hPa here
        )
        data['dew_point'] = dew_point(
            ta=data['air_temperature'],
            rh=data['relative_humidity'],
        )

        data['wet_bulb_temperature'] = wet_bulb_temp(
            ta=data['air_temperature'],
            rh=data['relative_humidity'],
        )

        data['heat_index'] = heat_index_extended(
            ta=data['air_temperature'],
            rh=data['relative_humidity'],
        )
        data['station_id'] = station_id
        data = await apply_qc(data=data, station_id=station_id)
        con = await sess.connection()
        await con.run_sync(
            lambda con: data.to_sql(
                name=TempRHData.__tablename__,
                con=con,
                if_exists='append',
                chunksize=65535 // (len(data.columns) + len(data.index.names)),
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
    # the element api has a rate limit of 20 requests per second as indicated by this
    # header in every response: x-ratelimit-limit 1200/60000 ms
    # let's be a bit more conservative and set it to 12 requests per second
    rate_limit='12/s',
)
async def download_station_data(station_id: str) -> str | None:
    if station_id:
        pass
    else:
        raise NotImplementedError('No station id provided')

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
                    select(SensorDeployment).where(
                        SensorDeployment.station_id == station.station_id,
                        or_(
                            and_(
                                SensorDeployment.setup_date < latest_data,
                                or_(
                                    SensorDeployment.teardown_date.is_(None),
                                    SensorDeployment.teardown_date > latest_data,
                                ),
                            ),
                            SensorDeployment.setup_date >= latest_data,
                        ),
                    ).order_by(SensorDeployment.setup_date),
                )
            ).scalars().all()
        else:
            # we never had any data for that station up until now, so we need all
            # deployments ever made to that station
            deployments = await station.awaitable_attrs.deployments
        # if there are no deployments ([]), we simply skip the entire iteration
        con = await sess.connection()
        for deployment in deployments:
            # check what kind of sensor we have
            target_table: type[SHT35DataRaw | ATM41DataRaw | BLGDataRaw]
            sensor = await deployment.awaitable_attrs.sensor
            match sensor.sensor_type:
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
                sensor=sensor,
                target_table=target_table,
                con=sess,
            )
            # we did not get any data, so we can skip the rest
            if data.empty:
                continue

            data = data.copy()
            data.loc[:, 'sensor_id'] = deployment.sensor_id
            data = data.rename(columns=RENAMER)
            # We will soon change how the download works, for now make sure that the
            # station id we assume matches the one we get from the API
            assert (data['station_id'] == station_id).all(), (
                f'API returned {data["station_id"].unique()} expected {station_id} '
                f'for sensor {sensor.sensor_id}'
            )
            # sometimes the API returns a very strange different set of columns
            # we can only ignore it...
            try:
                data = data[col_selection]
            except KeyError:
                print(
                    f'Could not process data for sensor {deployment.sensor_id}. '
                    f'Expected: {col_selection}, but got: {data.columns}',
                )
                return None

            # sometimes sensors have duplicates because Element fucked up internally
            data = data.reset_index()
            data = data.drop_duplicates()
            await con.run_sync(
                lambda con: data.to_sql(
                    name=target_table.__tablename__,
                    con=con,
                    if_exists='append',
                    chunksize=65535 // (len(data.columns) + len(data.index.names)),
                    method='multi',
                    index=False,
                ),
            )
            new_data = True
        await sess.commit()
        # return the station name for the next task to be picked up
        return station.station_id if new_data else None

DEVICE_TYPE_MAPPING = {
    'ATM41': SensorType.atm41,
    'SHT35': SensorType.sht35,
    'DL-BLG-001': SensorType.blg,
}


@async_task(
    app=celery_app,
    name='check-new-sensors',
    autoretry_for=(HTTPError, TimeoutError),
    max_retries=3,
    default_retry_delay=20,
)
async def check_for_new_sensors() -> None:
    # compile a list of sensors that are present in the Element system. They may be in
    # any of the folders assigned to the project
    project_folders = [
        f for f in api.get_folder_slugs() if f.startswith('stadt-dortmund-klimasensoren')  # noqa: E501
    ]
    # now get all hexadecimal device addresses in those folders
    _device_addrs: list[str] = []
    for folder in project_folders:
        _device_addrs.extend(api.get_device_addresses(folder))

    device_addrs = set(_device_addrs)
    # now get the sensors that we have in the database and compare both
    async with sessionmanager.session() as sess:
        sensors = set((await sess.execute(select(Sensor.sensor_id))).scalars().all())
        new_sensors = device_addrs - sensors
        if new_sensors:  # pragma: no branch
            for sensor_id in new_sensors:
                # get more detailed information about the sensor
                sensor_info = api.get_device(address=sensor_id)['body']
                # we have to omit the calibration information, since new sensors do not
                # have any calibration information
                sensor = Sensor(
                    sensor_id=sensor_id,
                    device_id=api.decentlab_id_from_address(sensor_id),
                    sensor_type=DEVICE_TYPE_MAPPING[
                        sensor_info['fields']['gerateinformation']['geratetyp']
                    ],
                )
                print(f'Adding new sensor: {sensor}')
                sess.add(sensor)
            await sess.commit()


@async_task(app=celery_app, name='self-test-integrity')
async def self_test_integrity() -> None:
    """Ideally this would be superflous and handle by the database definition, but
    this is hard to implement and complicates all tests. Hence we do it here.
    """
    # check that a sensor is not deployed simultaneously at two stations, so the
    # sensor_id must be unique
    async with sessionmanager.session() as sess:
        stm = (
            select(SensorDeployment.sensor_id)
            .group_by(SensorDeployment.sensor_id)
            .having(func.count() > 1)
        )
        # get all active deployments
        duplicates = (await sess.execute(stm)).scalars().all()
        if duplicates:
            raise ValueError(
                f'Found duplicate sensor deployments affecting theses sensor(s): '
                f'{", ".join(duplicates)}',
            )
        # TODO: check that contiguous deployments of the same sensor type do not overlap


@async_task(app=celery_app, name='buddy-checks', soft_time_limit=15 * 60)
async def perform_spatial_buddy_check() -> None:
    """Perform a spatial buddy check on the biomet and temp-rh data.

    We check the data with an offset of 8 minutes, so we can still catch data that
    is being uploaded right now.
    """
    # 1. check when the last buddy check was performed per station
    # now only select data that is newer than the last buddy check, make that
    # individually per station
    # check if the data is empty?
    # pass the data to the buddy check function
    # insert the result of the buddy check into the database
    async with sessionmanager.session() as sess:
        latest_buddy_checks = (
            select(
                BuddyCheckQc.station_id,
                func.max(BuddyCheckQc.measured_at).label('last_check'),
            )
            .group_by(BuddyCheckQc.station_id)
            .cte('last_buddy_checks')
        )
        biomet_query = (
            select(
                BiometData.measured_at,
                BiometData.station_id,
                Station.latitude,
                Station.longitude,
                Station.altitude,
                BiometData.air_temperature,
                BiometData.relative_humidity,
                BiometData.atmospheric_pressure,
            ).join(Station).join(latest_buddy_checks, isouter=True).where(
                (BiometData.measured_at > latest_buddy_checks.c.last_check) |
                (latest_buddy_checks.c.last_check.is_(None)),
            )
        ).cte('biomet_data_to_qc')
        temp_rh_query = (
            select(
                TempRHData.measured_at,
                TempRHData.station_id,
                Station.latitude,
                Station.longitude,
                Station.altitude,
                TempRHData.air_temperature,
                TempRHData.relative_humidity,
            ).join(Station).join(latest_buddy_checks, isouter=True).where(
                Station.station_type == StationType.temprh,
                (
                    (TempRHData.measured_at > latest_buddy_checks.c.last_check) |
                    (latest_buddy_checks.c.last_check.is_(None))
                ),
            )
        ).cte('temp_rh_data_to_qc')
        cut_off_date = (
            await sess.execute(
                select(
                    func.least(
                        select(func.max(BiometData.measured_at)).scalar_subquery(),
                        select(func.max(TempRHData.measured_at)).scalar_subquery(),
                    ),
                ),
            )
        ).scalar_one_or_none()
        if cut_off_date is None:
            # no data available, hence we can skip the entire buddy check
            return

        # we exclude the latest buddy check interval of 5 minutes. This improves the
        # availability of buddies since new data may still come in
        cut_off_date = pd.Timestamp(cut_off_date).floor(
            '5min',
        ) - timedelta(seconds=(2*60) + 30)
        data_query = union_all(
            select(
                biomet_query.c.measured_at,
                biomet_query.c.station_id,
                biomet_query.c.latitude,
                biomet_query.c.longitude,
                biomet_query.c.altitude,
                biomet_query.c.air_temperature,
                biomet_query.c.relative_humidity,
                biomet_query.c.atmospheric_pressure,
            ).where(biomet_query.c.measured_at <= cut_off_date),
            select(
                temp_rh_query.c.measured_at,
                temp_rh_query.c.station_id,
                temp_rh_query.c.latitude,
                temp_rh_query.c.longitude,
                temp_rh_query.c.altitude,
                temp_rh_query.c.air_temperature,
                temp_rh_query.c.relative_humidity,
                literal(None).label(BiometData.atmospheric_pressure.name),
            ).where(temp_rh_query.c.measured_at <= cut_off_date),
        )
        con = await sess.connection()
        db_data = await con.run_sync(
            lambda con: pd.read_sql(sql=data_query, con=con),
        )
        # no data, no qc
        if db_data.empty:
            return None

        qc_flags = await apply_buddy_check(db_data, config=BUDDY_CHECK_COLUMNS)

        await con.run_sync(
            lambda con: qc_flags.to_sql(
                name=BuddyCheckQc.__tablename__,
                con=con,
                method='multi',
                if_exists='append',
                chunksize=65535 // (len(qc_flags.columns) + len(qc_flags.index.names)),
                dtype={
                    'air_temperature_qc_isolated_check': Boolean,
                    'air_temperature_qc_buddy_check': Boolean,
                    'relative_humidity_qc_isolated_check': Boolean,
                    'relative_humidity_qc_buddy_check': Boolean,
                    'atmospheric_pressure_qc_isolated_check': Boolean,
                    'atmospheric_pressure_qc_buddy_check': Boolean,
                },
            ),
        )
        await sess.commit()
