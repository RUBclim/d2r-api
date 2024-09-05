from datetime import datetime
from datetime import timezone

import freezegun
import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import BiometData
from app.models import HeatStressCategories
from app.models import LatestData
from app.models import Station


@pytest.mark.anyio
@pytest.mark.parametrize('stations', [2], indirect=True)
async def test_get_station_metadata(app: AsyncClient, stations: list[Station]) -> None:
    resp = await app.get('/v1/stations/metadata')
    assert resp.status_code == 200
    assert resp.json() == {
        'data': [
            {
                'altitude': 100.0,
                'district': 'Innenstadt',
                'latitude': 51.446,
                'lcz': '2',
                'long_name': 'test-station-1',
                'longitude': 7.2627,
                'name': 'DEC1',
                'station_type': 'biomet',
            },
            {
                'altitude': 100.0,
                'district': 'Innenstadt',
                'latitude': 51.446,
                'lcz': '2',
                'long_name': 'test-station-2',
                'longitude': 7.2627,
                'name': 'DEC2',
                'station_type': 'biomet',
            },
        ],
    }


@pytest.mark.anyio
async def test_get_station_metadata_no_stations(app: AsyncClient) -> None:
    resp = await app.get('/v1/stations/metadata')
    assert resp.status_code == 200
    assert resp.json() == {'data': []}


@pytest.mark.anyio
@pytest.mark.parametrize('biomet_data', [{'n_stations': 2, 'n_data': 3}], indirect=True)
@freezegun.freeze_time('2024-08-01 01:00')
async def test_get_station_latest_data(
        app: AsyncClient,
        biomet_data: list[BiometData],
) -> None:
    resp = await app.get('/v1/stations/latest_data', params={'param': 'utci'})
    assert resp.status_code == 200
    # something is fucked with refreshing the materialized view...
    assert resp.json() == {
        'data': [
            {
                'altitude': 100.0,
                'district': 'Innenstadt',
                'latitude': 51.446,
                'lcz': '2',
                'long_name': 'test-station-1',
                'longitude': 7.2627,
                'name': 'DEC1',
                'station_type': 'biomet',
                'measured_at': '2024-08-01T00:15:00Z',
                'utci': 35.5,
            },
            {
                'altitude': 100.0,
                'district': 'Innenstadt',
                'latitude': 51.446,
                'lcz': '2',
                'long_name': 'test-station-2',
                'longitude': 7.2627,
                'name': 'DEC2',
                'station_type': 'biomet',
                'measured_at': '2024-08-01T00:15:00Z',
                'utci': 35.5,
            },
        ],
    }


@pytest.mark.anyio
async def test_get_station_latest_data_no_data(app: AsyncClient) -> None:
    resp = await app.get('/v1/stations/latest_data', params={'param': 'utci'})
    assert resp.status_code == 200
    # something is fucked with refreshing the materialized view...
    assert resp.json() == {'data': []}


@pytest.mark.anyio
@pytest.mark.parametrize('stations', [2], indirect=True)
@freezegun.freeze_time('2024-08-01 02:00')
@pytest.mark.usefixtures('clean_db')
async def test_get_station_latest_data_data_of_one_station_too_old(
        app: AsyncClient,
        stations: list[Station],
        db: AsyncSession,
) -> None:
    # add data for the station
    up_to_date_data = BiometData(
        name=stations[0].name,
        measured_at=datetime(2024, 8, 1, 1, 30, tzinfo=timezone.utc),
        mrt=69.69,
    )
    db.add(up_to_date_data)
    too_old_data = BiometData(
        name=stations[1].name,
        measured_at=datetime(2024, 8, 1, 0, 0, tzinfo=timezone.utc),
        mrt=42.0,
    )
    db.add(too_old_data)
    await db.commit()
    # we need to refresh the views so we actually get the data
    await LatestData.refresh(db=db)
    await db.commit()

    resp = await app.get(
        '/v1/stations/latest_data',
        params={
            'param': 'mrt',
            'max_age': 'PT2H',  # 2 hours is the max age!
        },
    )
    assert resp.status_code == 200
    assert resp.json() == {
        'data': [{
            'altitude': 100.0,
            'district': 'Innenstadt',
            'latitude': 51.446,
            'lcz': '2',
            'long_name': 'test-station-1',
            'longitude': 7.2627,
            'name': 'DEC1',
            'station_type': 'biomet',
            'measured_at': '2024-08-01T01:30:00Z',
            'mrt': 69.69,
        }],
    }


@pytest.mark.anyio
@pytest.mark.parametrize('stations', [2], indirect=True)
@freezegun.freeze_time('2024-08-01 02:00')
@pytest.mark.usefixtures('clean_db')
async def test_get_station_latest_data_data_of_one_station_too_old_default(
        app: AsyncClient,
        stations: list[Station],
        db: AsyncSession,
) -> None:
    # add data for the station
    up_to_date_data = BiometData(
        name=stations[0].name,
        measured_at=datetime(2024, 8, 1, 1, 30, tzinfo=timezone.utc),
        mrt=69.69,
    )
    db.add(up_to_date_data)
    too_old_data = BiometData(
        name=stations[1].name,
        measured_at=datetime(2024, 8, 1, 0, 0, tzinfo=timezone.utc),
        mrt=42.0,
    )
    db.add(too_old_data)
    await db.commit()
    # we need to refresh the views so we actually get the data
    await LatestData.refresh(db=db)
    await db.commit()

    resp = await app.get('/v1/stations/latest_data', params={'param': 'mrt'})
    assert resp.status_code == 200
    assert resp.json() == {
        'data': [{
            'altitude': 100.0,
            'district': 'Innenstadt',
            'latitude': 51.446,
            'lcz': '2',
            'long_name': 'test-station-1',
            'longitude': 7.2627,
            'name': 'DEC1',
            'station_type': 'biomet',
            'measured_at': '2024-08-01T01:30:00Z',
            'mrt': 69.69,
        }],
    }


@pytest.mark.anyio
@pytest.mark.parametrize('stations', [2], indirect=True)
@freezegun.freeze_time('2024-08-01 01:35')
@pytest.mark.usefixtures('clean_db')
async def test_get_station_latest_data_multiple_params(
        app: AsyncClient,
        stations: list[Station],
        db: AsyncSession,
) -> None:
    # add data for the station
    up_to_date_data = BiometData(
        name=stations[0].name,
        measured_at=datetime(2024, 8, 1, 1, 30, tzinfo=timezone.utc),
        mrt=69.69,
        utci=45,
        utci_category=HeatStressCategories.extreme_heat_stress,
    )
    db.add(up_to_date_data)
    too_old_data = BiometData(
        name=stations[1].name,
        measured_at=datetime(2024, 8, 1, 0, 0, tzinfo=timezone.utc),
        mrt=42.0,
        utci=30,
        utci_category=HeatStressCategories.strong_heat_stress,
    )
    db.add(too_old_data)
    await db.commit()
    # we need to refresh the views so we actually get the data
    await LatestData.refresh(db=db)
    await db.commit()

    resp = await app.get(
        '/v1/stations/latest_data',
        params={'param': ['mrt', 'utci', 'utci_category']},
    )
    assert resp.status_code == 200
    assert resp.json() == {
        'data': [{
            'altitude': 100.0,
            'district': 'Innenstadt',
            'latitude': 51.446,
            'lcz': '2',
            'long_name': 'test-station-1',
            'longitude': 7.2627,
            'name': 'DEC1',
            'station_type': 'biomet',
            'measured_at': '2024-08-01T01:30:00Z',
            'mrt': 69.69,
            'utci': 45.0,
            'utci_category': 'extreme heat stress',
        }],
    }


@pytest.mark.anyio
@pytest.mark.parametrize('stations', [2], indirect=True)
@freezegun.freeze_time('2024-08-01 01:35')
@pytest.mark.usefixtures('clean_db')
async def test_get_districts_latest_data_angles_are_averaged(
        app: AsyncClient,
        stations: list[Station],
        db: AsyncSession,
) -> None:
    # add data for the station
    data_station_0 = BiometData(
        name=stations[0].name,
        measured_at=datetime(2024, 8, 1, 1, 30, tzinfo=timezone.utc),
        wind_direction=360,
        air_temperature=10,
    )
    db.add(data_station_0)
    data_station_1 = BiometData(
        name=stations[1].name,
        measured_at=datetime(2024, 8, 1, 1, 30, tzinfo=timezone.utc),
        wind_direction=10,
        air_temperature=20,
    )
    db.add(data_station_1)
    await db.commit()
    # we need to refresh the views so we actually get the data
    await LatestData.refresh(db=db)
    await db.commit()

    resp = await app.get(
        '/v1/districts/latest_data',
        params={'param': ['air_temperature', 'wind_direction']},
    )
    assert resp.status_code == 200
    assert resp.json() == {
        'data': [{
            'district': 'Innenstadt',
            'wind_direction': pytest.approx(5),
            'air_temperature': 15.0,
        }],
    }
