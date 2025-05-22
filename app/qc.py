from collections.abc import Sequence
from datetime import timedelta
from functools import partial
from typing import Any

import pandas as pd
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import sessionmanager
from app.models import Station
from app.routers.v1 import TABLE_MAPPING


async def range_check(
        s: 'pd.Series[float]',
        *,
        lower_bound: float,
        upper_bound: float,
        **kwargs: dict[str, Any],
) -> 'pd.Series[bool]':
    """Check if the values in the series are within the specified range.

    :param s: The pandas Series to check.
    :param lower_bound: The lower bound of the range.
    :param upper_bound: The upper bound of the range.
    :return: A boolean Series indicating whether each value is within the range.
    """
    return ~((s >= lower_bound) & (s <= upper_bound))


async def persistence_check(
        s: 'pd.Series[float]',
        *,
        window: timedelta,
        excludes: Sequence[float] = [],
        station: Station,
        con: AsyncSession,
        **kwargs: dict[str, Any],
) -> 'pd.Series[bool]':
    """Check if the values in the series are persistent. For this we need to get
    more data from the database so that we can check if the values are the same for
    n minutes.

    :param s: The pandas Series to check.
    :return: A boolean Series indicating whether each value is persistent.
    """
    # get some additional data from the database so we can perform the check on
    # enough data to cover at least one window
    min_data_date = s.index.min()
    additional_data_start = min_data_date - window
    table = TABLE_MAPPING[station.station_type]['max']['table']
    query = (
        select(table).where(
            table.station_id == station.station_id,
            table.measured_at >= additional_data_start,
            table.measured_at < min_data_date,
        )
    )
    db_data = await con.run_sync(
        lambda con: pd.read_sql(
            sql=query,  # type: ignore[call-overload]
            con=con,
            index_col=['measured_at'],
        ),
    )
    # now find values that are the same
    if not db_data.empty:
        all_data = pd.concat([db_data[s.name], s]).sort_index()
    else:
        all_data = s.sort_index()
    # we shift the data by one so we get the minimum of consecutive values
    changes = all_data != all_data.shift()
    all_data = all_data.to_frame()
    all_data['changes'] = changes
    # now we create groups where persistent values are the same
    all_data['groups'] = changes.cumsum()
    # finally per group calculate the duration of the persistent values
    duration: 'pd.Series[timedelta]' = all_data.groupby(
        'groups',
    ).apply(_t_delta, include_groups=False)
    duration.name = 'duration'
    all_data = all_data.merge(duration, left_on='groups', right_index=True)
    all_data['flags'] = (
        all_data['duration'] >=
        window
    ) & ~all_data[s.name].isin(excludes)
    # TODO: there might be inconsistencies if we flag the first value of a persistent
    # series, or not. If the first value comes from the old data we fetched, we cannot
    # flag it, only the following values. For a series of persistent values this means
    # that the first one won't be flagged, but the rest will be.
    # we must only return the flags for the data we got passed in the first place
    return all_data['flags'].loc[s.index]


def _t_delta(x: 'pd.Series[float]') -> timedelta:
    """Calculate the time difference between the first and last value in a series."""
    max_date = x.index.max()
    min_date = x.index.min()
    return max_date - min_date


async def spike_dip_check(
        s: 'pd.Series[float]',
        *,
        delta: float,
        station: Station,
        con: AsyncSession,
        **kwargs: dict[str, Any],
) -> 'pd.Series[bool]':
    """check if there are spikes or dips in the data.

    :param s: The pandas Series to check.
    :param delta: The threshold for the spike/dip check per minute.
    :param station: The station to check.
    :param con: The database connection to use.

    :return: A boolean Series indicating whether each value is a spike or dip.
    """
    # we have to get the previous value from the database to check if the value is
    # a spike or dip
    table = TABLE_MAPPING[station.station_type]['max']['table']
    query = (
        select(table).where(
            table.station_id == station.station_id,
            table.measured_at < s.index.min(),
        ).order_by(table.measured_at.desc()).limit(1)
    )
    db_data = await con.run_sync(
        lambda con: pd.read_sql(
            sql=query,  # type: ignore[call-overload]
            con=con,
            index_col=['measured_at'],
        ),
    )
    # in case this is the very first time the qc runs
    if not db_data.empty:
        all_data = pd.concat([db_data[s.name], s]).sort_index()
    else:
        all_data = s.sort_index()
    all_data = all_data.to_frame()
    #  now find the value difference between the current and the previous value
    all_data['shifted'] = all_data.shift()
    df = all_data.reset_index()
    df['time_diff'] = abs(df['measured_at'] - df['measured_at'].shift())
    df['time_diff'] = df['time_diff'].dt.total_seconds() / 60
    all_data = df.set_index('measured_at')
    # normalize the difference by the time that passed between the two values
    all_data['value_diff'] = (
        abs(all_data[s.name] - all_data['shifted']) / all_data['time_diff']
    )
    return all_data['value_diff'] > delta


# The values are based on the QC-procedure used at RUB which was derived and adapted
# from the Guide to the Global Observing System by WMO
# https://library.wmo.int/viewer/35699/download?file=488-2017_en.pdf&type=pdf
# Plausible Value Range: The measured value has to be in this range
# Plausible Rate of Change: The measured value must not change more than this
# per minute.
# Minimum required Variability: The measured value must change after this time
# (excluding some values e.g. 0 for precipitation or 0 for solar radiation during night)
COLUMNS = {
    'air_temperature': [
        partial(range_check, lower_bound=-40, upper_bound=50),
        partial(persistence_check, window=timedelta(hours=3)),
        partial(spike_dip_check, delta=0.3),
    ],
    'relative_humidity': [
        partial(range_check, lower_bound=10, upper_bound=100),
        partial(spike_dip_check, delta=4),
        partial(persistence_check, window=timedelta(hours=5)),
    ],
    'atmospheric_pressure': [
        partial(range_check, lower_bound=860, upper_bound=1055),
        partial(persistence_check, window=timedelta(hours=6)),
        partial(spike_dip_check, delta=0.3),
    ],
    'wind_speed': [
        partial(range_check, lower_bound=0, upper_bound=30),
        partial(persistence_check, window=timedelta(hours=5)),
        partial(spike_dip_check, delta=20),
    ],
    'u_wind': [
        partial(range_check, lower_bound=-30, upper_bound=30),
        partial(persistence_check, window=timedelta(hours=5)),
        partial(spike_dip_check, delta=20),
    ],
    'v_wind': [
        partial(range_check, lower_bound=-30, upper_bound=30),
        partial(persistence_check, window=timedelta(hours=5)),
        partial(spike_dip_check, delta=20),
    ],
    'maximum_wind_speed': [
        partial(range_check, lower_bound=0, upper_bound=30),
        partial(persistence_check, window=timedelta(hours=1)),
    ],
    'wind_direction': [
        partial(range_check, lower_bound=0, upper_bound=360),
        partial(persistence_check, window=timedelta(hours=1), excludes=[0, 360]),
    ],
    'precipitation_sum': [
        partial(range_check, lower_bound=0, upper_bound=50),
        partial(persistence_check, window=timedelta(hours=2), excludes=[0]),
        partial(spike_dip_check, delta=20),
    ],
    'solar_radiation': [
        partial(range_check, lower_bound=0, upper_bound=1400),
        # TODO: this is dependent on the time of the day and somehow needs handling
        partial(persistence_check, window=timedelta(hours=3), excludes=[0, 1, 2]),
        partial(spike_dip_check, delta=800),
    ],
    'lightning_average_distance': [
        partial(range_check, lower_bound=0, upper_bound=40),
        partial(persistence_check, window=timedelta(hours=1), excludes=[0]),
    ],
    'lightning_strike_count': [
        partial(range_check, lower_bound=0, upper_bound=65535),
        partial(persistence_check, window=timedelta(hours=1), excludes=[0]),
    ],
    'x_orientation_angle': [
        partial(range_check, lower_bound=-3, upper_bound=3),
        partial(spike_dip_check, delta=1),
    ],
    'y_orientation_angle': [
        partial(range_check, lower_bound=-3, upper_bound=3),
        partial(spike_dip_check, delta=1),
    ],
    'black_globe_temperature': [
        partial(range_check, lower_bound=-40, upper_bound=90),
        partial(persistence_check, window=timedelta(hours=3)),
        partial(spike_dip_check, delta=40),
    ],
}


async def apply_qc(data: pd.DataFrame, station_id: str) -> pd.DataFrame:
    """Apply quality control to the data for a given station and time period.

    :param data: The data to apply quality control to.
    :param station: The station to apply quality control for.

    :return: The data with quality control applied.
    """
    data = data.sort_index()
    async with sessionmanager.session() as sess:
        station = (
            await sess.execute(select(Station).where(Station.station_id == station_id))
        ).scalar_one()
        con = await sess.connection()
        for column in data.columns:
            qc_functions = COLUMNS.get(column)
            if qc_functions:
                for qc_function in qc_functions:
                    res = await qc_function(
                        s=data[column],
                        station=station,
                        con=con,
                    )
                    data[f'{column}_qc_{qc_function.func.__name__}'] = res
    return data
