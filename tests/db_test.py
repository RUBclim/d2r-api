from datetime import date
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from decimal import Decimal

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import BiometData
from app.models import BiometDataDaily
from app.models import BiometDataHourly
from app.models import Station
from app.models import TempRHData
from app.models import TempRHDataDaily
from app.models import TempRHDataHourly


@pytest.mark.anyio
@pytest.mark.usefixtures('clean_db')
@pytest.mark.parametrize(
    ('data_table', 'view'),
    (
        (TempRHData, TempRHDataHourly),
        (BiometData, BiometDataHourly),
    ),
)
async def test_hourly_view_data_is_right_labelled(
        data_table: type[TempRHData | BiometData],
        view: type[TempRHDataHourly | BiometDataHourly],
        db: AsyncSession,
        stations: list[Station],
) -> None:
    station, = stations
    start_date = datetime(2024, 1, 1, 11, 55, tzinfo=timezone.utc)
    step = timedelta(minutes=5)
    for value in range(14):
        data = data_table(
            measured_at=start_date + (step * value),
            name=station.name,
            air_temperature=value,
        )
        db.add(data)
    await db.commit()
    await view.refresh()

    query = select(
        view.measured_at,
        view.air_temperature,
    ).order_by(view.measured_at)

    result = (await db.execute(query)).all()

    # we start with 11:55 hence this is part of the right-labeled 11-12:00 interval
    assert result[0] == (datetime(2024, 1, 1, 12, 0), Decimal('0'))
    # this starts at 12:00 and is part of the right-labeled 12-13:00 interval
    assert result[1] == (datetime(2024, 1, 1, 13, 0), Decimal('6.5'))
    # this is 13:00 and is part of the 13-14:00 interval
    assert result[2] == (datetime(2024, 1, 1, 14, 0), Decimal('13'))


@pytest.mark.anyio
@pytest.mark.usefixtures('clean_db')
@pytest.mark.parametrize(
    ('data_table', 'view'),
    (
        (TempRHData, TempRHDataDaily),
        (BiometData, BiometDataDaily),
    ),
)
async def test_daily_view_threshold_and_timezone(
        data_table: type[TempRHData | BiometData],
        view: type[TempRHDataDaily | BiometDataDaily],
        db: AsyncSession,
        stations: list[Station],
) -> None:
    station, = stations
    start_date = datetime(2024, 1, 1, 22, 0, tzinfo=timezone.utc)
    step = timedelta(minutes=5)
    # compile one day and two hours of values
    for value in range((12 * 24) + (12 * 2)):
        data = data_table(
            measured_at=start_date + (step * value),
            name=station.name,
            air_temperature=value,
        )
        db.add(data)
    await db.commit()
    await view.refresh()

    query = select(
        view.measured_at,
        view.air_temperature,
    ).order_by(view.measured_at)

    result = (await db.execute(query)).all()

    # we need to make sure that the daily mean is calculated at UTC+1
    assert result == [
        # threshold not reached
        (date(2024, 1, 1), None),
        # UTC+1 timezone is used
        (date(2024, 1, 2), Decimal('155.5')),
        # threshold not reached
        (date(2024, 1, 3), None),
    ]
