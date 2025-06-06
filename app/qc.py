from collections.abc import Callable
from collections.abc import Sequence
from datetime import timedelta
from functools import partial
from typing import Any
from typing import TypedDict

import numpy as np
import numpy.typing as npt
import pandas as pd
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncConnection
from titanlib import buddy_check
from titanlib import isolation_check
from titanlib import Points

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
        con: AsyncConnection,
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
        con: AsyncConnection,
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
    # there are jumps (a value suddenly jumps but remains at the level) and spikes
    # (a single values jumps for a single time step). for jumps only the first values
    # of the jumps is marked and for spikes the first spiked values and the flowed value
    # is marked since it's a dip after a spike.
    df['time_diff'] = abs(df['measured_at'] - df['measured_at'].shift())
    df['time_diff'] = df['time_diff'].dt.total_seconds() / 60
    all_data = df.set_index('measured_at')
    # normalize the difference by the time that passed between the two values
    all_data['value_diff'] = (
        abs(all_data[s.name] - all_data['shifted']) / all_data['time_diff']
    )
    all_data['flags'] = all_data['value_diff'] > delta
    return all_data['flags'].loc[s.index]


class BuddyCheckConfig(TypedDict):
    callable: Callable[..., npt.NDArray[np.integer]]
    radius: float
    num_min: int
    threshold: float
    max_elev_diff: float
    elev_gradient: float
    min_std: float
    num_iterations: int


async def apply_buddy_check(
        data: pd.DataFrame,
        config: dict[str, BuddyCheckConfig],
) -> pd.DataFrame:
    """Apply the buddy check to the data for the given time period.

    :param data: The data to apply the buddy check to. It must have a
    :return: A DataFrame with the buddy check results.
    """
    # create a new regular 5-minute index for the data
    data['measured_at_rounded'] = data['measured_at'].dt.round('5min')
    # sometimes the above creates duplicates when we have measurements that are
    # more often than every 5 minutes, so we need to drop them. We keep the last
    data = data.drop_duplicates(
        subset=['measured_at_rounded', 'station_id'],
        keep='last',
    ).set_index(['measured_at_rounded', 'station_id']).sort_index()
    dfs: list[pd.DataFrame] = []
    # step through the time steps
    for d in data.index.get_level_values(0).unique():
        df_current: pd.DataFrame = data.loc[d].copy()
        # prepare an initial Points object for the isolation check
        longitude = df_current['longitude'].to_numpy()
        latitude = df_current['latitude'].to_numpy()
        altitude = df_current['altitude'].to_numpy()
        points = Points(longitude, latitude, altitude)
        # step through the parameters we have a config for
        for param in config:
            param_config = config[param]
            # detect isolated stations
            isolation_flags = isolation_check(
                points,
                param_config['num_min'],
                param_config['radius'],
            )
            isolated_col = f'{param}_qc_isolated_check'
            df_current.loc[:, isolated_col] = isolation_flags.astype(bool)
            # select only stations that are not isolated
            db_data_non_isolated = df_current.loc[
                df_current[isolated_col] == False,  # noqa: E712
                param,
            ]
            non_iso_mask = ~df_current[isolated_col].to_numpy()
            # we need to recreate the points for only the non-isolated stations
            points_non_isolated = Points(
                longitude[non_iso_mask],
                latitude[non_iso_mask],
                altitude[non_iso_mask],
            )
            # get the correct parameter configuration and we can only qc stations
            # that are not isolated
            size = points_non_isolated.size()
            flags = buddy_check(
                points_non_isolated,
                db_data_non_isolated.to_numpy(),
                np.full(size, param_config['radius']),
                np.full(size, param_config['num_min']),
                param_config['threshold'],
                param_config['max_elev_diff'],
                param_config['elev_gradient'],
                param_config['min_std'],
                param_config['num_iterations'],
            )
            # store the flags in the DataFrame
            df_current.loc[
                db_data_non_isolated.index,
                f'{param}_qc_buddy_check',
            ] = flags.astype(bool)
            # we need to replace the NaN values with None for the database
            df_current[f'{param}_qc_buddy_check'] = df_current[
                f'{param}_qc_buddy_check'
            ].replace({np.nan: None})
            # TODO: if the value was nan, it is also flagged as True
        dfs.append(df_current)
    data = pd.concat(dfs)
    data = data.reset_index().set_index(['measured_at', 'station_id'])
    return data.filter(like='_check')


BUDDY_CHECK_COLUMNS: dict[str, BuddyCheckConfig] = {
    # TODO: all these values need calibration and adjustment
    'air_temperature': {
        'callable': buddy_check,
        'radius': 5500,
        'num_min': 3,
        'threshold': 2.7,
        'max_elev_diff': 100,
        'elev_gradient': -0.0065,
        'min_std': 2,
        'num_iterations': 5,
    },
    'relative_humidity': {
        'callable': buddy_check,
        'radius': 6000,
        'num_min': 3,
        'threshold': 7,
        'max_elev_diff': -1,  # do not check elevation difference
        'elev_gradient': 0,
        'min_std': 3,
        'num_iterations': 5,
    },
    'atmospheric_pressure': {
        'callable': buddy_check,
        'radius': 10000,
        'num_min': 3,
        'threshold': 3,
        'max_elev_diff': 100,
        'elev_gradient': 0.125,  # lapse rate at sea level
        'min_std': 1.5,
        'num_iterations': 5,
    },
    # TODO: at five minutes precipitation will likely give a lot of false positives and
    # the qc will be useless
}


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
                    res = await qc_function(s=data[column], station=station, con=con)
                    data[f'{column}_qc_{qc_function.func.__name__}'] = res
    return data
