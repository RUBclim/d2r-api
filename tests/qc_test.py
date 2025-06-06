from datetime import datetime
from datetime import timedelta
from datetime import timezone

import pandas as pd
import pytest
from pandas.testing import assert_series_equal
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import BiometData
from app.models import Station
from app.qc import apply_qc
from app.qc import persistence_check
from app.qc import range_check
from app.qc import spike_dip_check


@pytest.mark.parametrize(
    ('data', 'lower_bound', 'upper_bound', 'expected'),
    (
        (pd.Series([2, 4, 6]), 3, 5, pd.Series([True, False, True])),
        (pd.Series([2, float('nan'), 6]), 3, 5, pd.Series([True, True, True])),
    ),
)
@pytest.mark.anyio
async def test_range_check(
        data: 'pd.Series[float]',
        lower_bound: float,
        upper_bound: float,
        expected: 'pd.Series[float]',
) -> None:
    result = await range_check(data, lower_bound=lower_bound, upper_bound=upper_bound)
    pd.testing.assert_series_equal(result, expected)


@pytest.mark.anyio
async def test_persistence_check_no_data_in_db(
        db: AsyncSession,
        stations: list[Station],
) -> None:
    con = await db.connection()
    # create some example data that has persistence issues
    index = pd.date_range(start='2025-01-01', periods=12, freq='5min', tz='UTC')
    # randomize the index to have inconsistent timestamps
    randomized_index = index + pd.Series([
        timedelta(seconds=i)
        for i in [10, -100, 50, -20, 30, 0, 60, 80, 90, 40, -10, 0]
    ])
    new_data = pd.Series(
        [1.0, 1, 1, 1, 1, 2, 3, 3, 3, 3, 3, 5],
        index=randomized_index,
        name='air_temperature',
    )
    result = await persistence_check(
        s=new_data,
        window=timedelta(minutes=15),
        station=stations[0],
        con=con,
    )
    assert_series_equal(
        left=result,
        right=pd.Series(
            [True, True, True, True, True, False, True, True, True, True, True, False],
            index=randomized_index,
            name='flags',
        ),
    )


@pytest.mark.anyio
async def test_persistence_check_previous_data_in_db(
        db: AsyncSession,
        stations: list[Station],
) -> None:
    data = [
        BiometData(
            measured_at=datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc),
            station_id='DOB1',
            sensor_id='DEC1',
            air_temperature=1,
        ),
        BiometData(
            measured_at=datetime(2025, 1, 1, 0, 5, tzinfo=timezone.utc),
            station_id='DOB1',
            sensor_id='DEC1',
            air_temperature=1,
        ),
        BiometData(
            measured_at=datetime(2025, 1, 1, 0, 10, tzinfo=timezone.utc),
            station_id='DOB1',
            sensor_id='DEC1',
            air_temperature=1,
        ),
    ]
    db.add_all(data)
    await db.commit()

    con = await db.connection()
    # create some example data that has persistence issues
    index = pd.date_range(start='2025-01-01 00:15', periods=9, freq='5min', tz='UTC')
    # randomize the index to have inconsistent timestamps
    randomized_index = index + pd.Series([
        timedelta(seconds=i)
        for i in [20, 30, 0, 60, 80, 90, 40, 10, 0]
    ])
    new_data = pd.Series(
        [1.0, 1, 2, 3, 3, 3, 3, 3, 5],
        index=randomized_index,
        name='air_temperature',
    )
    result = await persistence_check(
        s=new_data,
        window=timedelta(minutes=15),
        station=stations[0],
        con=con,
    )
    # we only get the "new" data back, but flagged correctly!
    assert_series_equal(
        left=result,
        right=pd.Series(
            [True, True, False, True, True, True, True, True, False],
            index=randomized_index,
            name='flags',
        ),
    )


@pytest.mark.anyio
async def test_spike_dip_check_no_data_in_db(
        db: AsyncSession,
        stations: list[Station],
) -> None:
    con = await db.connection()
    # create some example data that has persistence issues
    index = pd.date_range(
        start='2025-01-01',
        periods=11,
        freq='5min',
        tz='UTC',
    )
    # randomize the index to have inconsistent timestamps
    randomized_index = index + pd.Series([
        timedelta(seconds=i)
        for i in [10, -100, 50, -20, 30, 0, 60, 80, 90, 40, -10]
    ])
    new_data = pd.Series(
        # 5 is the spike
        [1.0, 2, 3, 4, 3, 2, 1, 5, 4, 3, 2],
        index=randomized_index,
        name='air_temperature',
    )
    new_data.index.name = 'measured_at'
    result = await spike_dip_check(
        s=new_data,
        delta=0.5,
        station=stations[0],
        con=con,
    )
    expected = pd.Series(
        [False, False, False, False, False, False, False, True, False, False, False],
        index=randomized_index,
        name='flags',
    )
    expected.index.name = 'measured_at'
    assert_series_equal(left=result, right=expected)


@pytest.mark.anyio
async def test_spike_dip_check_previous_data_in_db(
        db: AsyncSession,
        stations: list[Station],
) -> None:
    data = [
        BiometData(
            measured_at=datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc),
            station_id='DOB1',
            sensor_id='DEC1',
            air_temperature=1,
        ),
        BiometData(
            measured_at=datetime(2025, 1, 1, 0, 5, tzinfo=timezone.utc),
            station_id='DOB1',
            sensor_id='DEC1',
            air_temperature=1,
        ),
        BiometData(
            measured_at=datetime(2025, 1, 1, 0, 10, tzinfo=timezone.utc),
            station_id='DOB1',
            sensor_id='DEC1',
            air_temperature=1,
        ),
    ]
    db.add_all(data)
    await db.commit()

    con = await db.connection()
    # create some example data that has persistence issues
    index = pd.date_range(start='2025-01-01 00:15', periods=9, freq='5min', tz='UTC')
    # randomize the index to have inconsistent timestamps
    randomized_index = index + pd.Series([
        timedelta(seconds=i)
        for i in [20, 30, 0, 60, 80, 90, 40, 10, 0]
    ])
    new_data = pd.Series(
        # first, middle, and last one are spikes
        [5.0, 4, 3, 2, 10, 2, 3, 4, 7],
        index=randomized_index,
        name='air_temperature',
    )
    new_data.index.name = 'measured_at'
    result = await spike_dip_check(
        s=new_data,
        delta=0.5,
        station=stations[0],
        con=con,
    )
    # we only get the "new" data back, but flagged correctly!
    expected = pd.Series(
        [True, False, False, False, True, True, False, False, True],
        index=randomized_index,
        name='flags',
    )
    expected.index.name = 'measured_at'
    assert_series_equal(left=result, right=expected)


@pytest.mark.anyio
async def test_apply_qc_range_check_air_temperature_fails(
        db: AsyncSession,
        stations: list[Station],
) -> None:
    data = pd.DataFrame(
        data={
            'air_temperature': [2.0, 50, 6.0],
            'relative_humidity': [40, 150, 15],
            'column_with_no_qc': [2.0, 50, 6.0],
        },
        index=pd.date_range(start='2025-01-01', periods=3, freq='5min', tz='UTC'),
    )
    data.index.name = 'measured_at'
    result = await apply_qc(
        data=data,
        station_id='DOB1',
    )
    # make sure we get new columns for the QC checks for each parameter
    assert set(result.columns) == {
        # initial columns with values
        'air_temperature',
        'column_with_no_qc',
        'relative_humidity',
        'relative_humidity_qc_persistence_check',
        'air_temperature_qc_spike_dip_check',
        'air_temperature_qc_persistence_check',
        'air_temperature_qc_range_check',
        'relative_humidity_qc_range_check',
        'relative_humidity_qc_spike_dip_check',
        # no qc columns for the column with no qc!
    }
