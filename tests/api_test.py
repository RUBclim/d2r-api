from datetime import datetime
from datetime import timezone

import freezegun
import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import BiometData
from app.models import BiometDataHourly
from app.models import HeatStressCategories
from app.models import LatestData
from app.models import Station
from app.models import StationType
from app.models import TempRHData
from app.models import TempRHDataHourly


@pytest.mark.anyio
async def test_get_healthcheck(app: AsyncClient) -> None:
    resp = await app.get('/v1/healthcheck')
    assert resp.status_code == 200
    assert resp.json() == {'message': "I'm healthy!"}


@pytest.mark.anyio
async def test_head_healthcheck(app: AsyncClient) -> None:
    resp = await app.head('/v1/healthcheck')
    assert resp.headers == {'content-length': '26', 'content-type': 'application/json'}


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
@pytest.mark.parametrize('stations', [3], indirect=True)
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
    # this station must not be included, since it does not provide all params
    null_data = BiometData(
        name=stations[2].name,
        measured_at=datetime(2024, 8, 1, 1, 30, tzinfo=timezone.utc),
        mrt=None,
        utci=30,
        utci_category=HeatStressCategories.strong_heat_stress,
    )
    db.add(null_data)
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
@pytest.mark.parametrize(
    'route',
    ('/v1/stations/latest_data', '/v1/districts/latest_data'),
)
@pytest.mark.parametrize(
    'param',
    (
        ['unknown', 'air_temperature'],
        'unknown',
        None,
    ),
)
async def test_get_station_latest_data_invalid_param_input(
        app: AsyncClient,
        param: list[str] | str | None,
        route: str,
) -> None:
    resp = await app.get(route, params={'param': param})
    assert resp.status_code == 422
    data = resp.json()
    assert 'Input should be' in data['detail'][0]['msg']


@pytest.mark.anyio
@pytest.mark.parametrize('max_age', ('PT1T', 'foo', '??'))
async def test_get_station_latest_data_invalid_time_provided(
        app: AsyncClient,
        max_age: str,
) -> None:
    resp = await app.get(
        '/v1/stations/latest_data',
        params={'param': 'air_temperature', 'max_age': max_age},
    )
    assert resp.status_code == 422
    data = resp.json()
    assert 'Input should be' in data['detail'][0]['msg']


@pytest.mark.anyio
async def test_get_station_latest_data_negative_time_provided(
        app: AsyncClient,
) -> None:
    resp = await app.get(
        '/v1/stations/latest_data',
        params={'param': 'air_temperature', 'max_age': '-PT1H'},
    )
    assert resp.status_code == 422
    assert resp.json() == {'detail': 'max_age must be positive'}


@pytest.mark.anyio
@pytest.mark.parametrize('stations', [3], indirect=True)
@freezegun.freeze_time('2024-08-01 01:35')
@pytest.mark.usefixtures('clean_db')
async def test_get_districts_latest_data_aggregates_are_correct(
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
        maximum_wind_speed=15,
        utci_category=HeatStressCategories.no_thermal_stress,
        precipitation_sum=3,
        lightning_strike_count=10,
    )
    db.add(data_station_0)
    data_station_1 = BiometData(
        name=stations[1].name,
        measured_at=datetime(2024, 8, 1, 1, 30, tzinfo=timezone.utc),
        wind_direction=10,
        air_temperature=20,
        maximum_wind_speed=10,
        utci_category=HeatStressCategories.no_thermal_stress,
        precipitation_sum=2,
        lightning_strike_count=20,
    )
    db.add(data_station_1)
    # the data is too old, hence omitted from the calculation
    data_station_2 = BiometData(
        name=stations[2].name,
        measured_at=datetime(2024, 8, 1, 0, 0, tzinfo=timezone.utc),
        wind_direction=180,
        air_temperature=40,
        maximum_wind_speed=20,
        utci_category=HeatStressCategories.extreme_heat_stress,
        precipitation_sum=1,
        lightning_strike_count=15,
    )
    db.add(data_station_2)
    station_with_missing_data = Station(
        name='missing_data',
        device_id=27,
        long_name='test-station-missing-data',
        latitude=51.447,
        longitude=7.268,
        altitude=100,
        station_type=StationType.temprh,
        leuchtennummer=120,
        district='Other District',
    )
    db.add(station_with_missing_data)
    data_missing = BiometData(
        name=station_with_missing_data.name,
        measured_at=datetime(2024, 8, 1, 0, 0, tzinfo=timezone.utc),
        wind_direction=None,
        air_temperature=40,
        maximum_wind_speed=None,
        utci_category=None,
        precipitation_sum=None,
        lightning_strike_count=None,
    )
    db.add(data_missing)
    await db.commit()
    # we need to refresh the views so we actually get the data
    await LatestData.refresh(db=db)
    await db.commit()

    resp = await app.get(
        '/v1/districts/latest_data',
        params={
            'param': [
                'air_temperature', 'wind_direction',
                'maximum_wind_speed', 'utci_category',
                'precipitation_sum', 'lightning_strike_count',
            ],
        },
    )
    assert resp.status_code == 200
    assert resp.json() == {
        'data': [{
            'district': 'Innenstadt',
            # care, approx messes with the output if a dict key is missing so
            # double check if a test is failing
            'wind_direction': pytest.approx(5),
            'air_temperature': 15.0,
            'maximum_wind_speed': 15,
            'utci_category': 'no thermal stress',
            'precipitation_sum': 2.5,
            'lightning_strike_count': 15,
        }],
    }


@pytest.mark.anyio
async def test_get_district_latest_data_negative_time_provided(
        app: AsyncClient,
) -> None:
    resp = await app.get(
        '/v1/districts/latest_data',
        params={'param': 'air_temperature', 'max_age': '-PT1H'},
    )
    assert resp.status_code == 422
    assert resp.json() == {'detail': 'max_age must be positive'}


@pytest.mark.anyio
@pytest.mark.parametrize('stations', [2], indirect=True)
@pytest.mark.usefixtures('clean_db')
async def test_get_trends_stations_biomet_and_temprh(
        app: AsyncClient,
        stations: list[Station],
        db: AsyncSession,
) -> None:
    # create a temprh station
    temp_rh_station = Station(
        name='DEC4',
        device_id=27,
        long_name='temprh-station',
        latitude=51.447,
        longitude=7.268,
        altitude=100,
        station_type=StationType.temprh,
        leuchtennummer=120,
        district='Other District',
    )
    db.add(temp_rh_station)
    await db.commit()
    # we need to create some data and this way we can also check that the materialized
    # view works as expected
    data = [
        # station 0
        # data before our requested range
        BiometData(
            name=stations[0].name,
            measured_at=datetime(2024, 8, 1, 8, 0),
            air_temperature=10,
        ),
        # data that will be aggregated by the materialized view
        BiometData(
            name=stations[0].name,
            measured_at=datetime(2024, 8, 1, 9, 10),
            air_temperature=10,
        ),
        BiometData(
            name=stations[0].name,
            measured_at=datetime(2024, 8, 1, 9, 20),
            air_temperature=11,
        ),
        BiometData(
            name=stations[0].name,
            measured_at=datetime(2024, 8, 1, 9, 30),
            air_temperature=15,
        ),
        # data after our requested range
        BiometData(
            name=stations[0].name,
            measured_at=datetime(2024, 8, 1, 10, 30),
            air_temperature=16,
        ),
        # station 1 (the same structure as above)
        BiometData(
            name=temp_rh_station.name,
            measured_at=datetime(2024, 8, 1, 8, 0),
            air_temperature=9,
        ),
        BiometData(
            name=temp_rh_station.name,
            measured_at=datetime(2024, 8, 1, 9, 10),
            air_temperature=9,
        ),
        BiometData(
            name=temp_rh_station.name,
            measured_at=datetime(2024, 8, 1, 9, 20),
            air_temperature=10,
        ),
        BiometData(
            name=temp_rh_station.name,
            measured_at=datetime(2024, 8, 1, 9, 30),
            air_temperature=14,
        ),
        BiometData(
            name=temp_rh_station.name,
            measured_at=datetime(2024, 8, 1, 10, 30),
            air_temperature=15,
        ),
        # a station that we don't request, but it theoretically would be supported!
        BiometData(
            name=stations[1].name,
            measured_at=datetime(2024, 8, 1, 9, 30),
            air_temperature=15,
        ),
    ]
    for d in data:
        db.add(d)

    await db.commit()
    await BiometDataHourly.refresh()

    resp = await app.get(
        '/v1/trends/air_temperature',
        params={
            'item_type': 'stations',
            'item_ids': ['DEC1', 'DEC4'],
            'start_date': datetime(2024, 8, 1, 0, 0),
            'end_date': datetime(2024, 8, 2, 23, 0),
            'hour': 10,
        },
    )
    assert resp.status_code == 200
    assert resp.json() == {
        'data': {
            'supported_ids': ['DEC1', 'DEC2', 'DEC4'],
            'trends': [
                {'DEC1': 12.0, 'measured_at': '2024-08-01T10:00:00Z'},
                {'DEC4': 11.0, 'measured_at': '2024-08-01T10:00:00Z'},
            ],
            'unit': '°C',
        },
    }


@pytest.mark.anyio
@pytest.mark.parametrize('stations', [2], indirect=True)
@pytest.mark.usefixtures('clean_db')
async def test_get_trends_stations_only_biomet(
        app: AsyncClient,
        stations: list[Station],
        db: AsyncSession,
) -> None:
    # we need to create some data and this way we can also check that the materialized
    # view works as expected
    data = [
        # station 0
        # data before our requested range
        BiometData(
            name=stations[0].name,
            measured_at=datetime(2024, 8, 1, 8, 0),
            air_temperature=10,
        ),
        # data that will be aggregated by the materialized view
        BiometData(
            name=stations[0].name,
            measured_at=datetime(2024, 8, 1, 9, 10),
            air_temperature=10,
        ),
        BiometData(
            name=stations[0].name,
            measured_at=datetime(2024, 8, 1, 9, 20),
            air_temperature=11,
        ),
        BiometData(
            name=stations[0].name,
            measured_at=datetime(2024, 8, 1, 9, 30),
            air_temperature=15,
        ),
        # data after our requested range
        BiometData(
            name=stations[0].name,
            measured_at=datetime(2024, 8, 1, 10, 30),
            air_temperature=16,
        ),
        # a station that we don't request, but it theoretically would be supported!
        BiometData(
            name=stations[1].name,
            measured_at=datetime(2024, 8, 1, 9, 30),
            air_temperature=15,
        ),
    ]
    for d in data:
        db.add(d)

    await db.commit()
    await BiometDataHourly.refresh()

    resp = await app.get(
        '/v1/trends/air_temperature',
        params={
            'item_type': 'stations',
            'item_ids': 'DEC1',
            'start_date': datetime(2024, 8, 1, 0, 0),
            'end_date': datetime(2024, 8, 2, 23, 0),
            'hour': 10,
        },
    )
    assert resp.status_code == 200
    assert resp.json() == {
        'data': {
            'supported_ids': ['DEC1', 'DEC2'],
            'trends': [{'DEC1': 12.0, 'measured_at': '2024-08-01T10:00:00Z'}],
            'unit': '°C',
        },
    }


@pytest.mark.anyio
@pytest.mark.parametrize('stations', [2], indirect=True)
@pytest.mark.usefixtures('clean_db')
async def test_get_trends_stations_only_biomet_counts_become_sums(
        app: AsyncClient,
        stations: list[Station],
        db: AsyncSession,
) -> None:
    # we need to create some data and this way we can also check that the materialized
    # view works as expected
    data = [
        # station 0
        # data before our requested range
        BiometData(
            name=stations[0].name,
            measured_at=datetime(2024, 8, 1, 8, 0),
            lightning_strike_count=10,
        ),
        # data that will be aggregated by the materialized view
        BiometData(
            name=stations[0].name,
            measured_at=datetime(2024, 8, 1, 9, 10),
            lightning_strike_count=10,
        ),
        BiometData(
            name=stations[0].name,
            measured_at=datetime(2024, 8, 1, 9, 20),
            lightning_strike_count=11,
        ),
        BiometData(
            name=stations[0].name,
            measured_at=datetime(2024, 8, 1, 9, 30),
            lightning_strike_count=15,
        ),
        # data after our requested range
        BiometData(
            name=stations[0].name,
            measured_at=datetime(2024, 8, 1, 10, 30),
            lightning_strike_count=16,
        ),
        # a station that we don't request, but it theoretically would be supported!
        BiometData(
            name=stations[1].name,
            measured_at=datetime(2024, 8, 1, 9, 30),
            lightning_strike_count=15,
        ),
    ]
    for d in data:
        db.add(d)

    await db.commit()
    await BiometDataHourly.refresh()

    resp = await app.get(
        '/v1/trends/lightning_strike_count',
        params={
            'item_type': 'stations',
            'item_ids': 'DEC1',
            'start_date': datetime(2024, 8, 1, 0, 0),
            'end_date': datetime(2024, 8, 2, 23, 0),
            'hour': 10,
        },
    )
    assert resp.status_code == 200
    assert resp.json() == {
        'data': {
            'supported_ids': ['DEC1', 'DEC2'],
            'trends': [{'DEC1': 36, 'measured_at': '2024-08-01T10:00:00Z'}],
            'unit': '-',
        },
    }


@pytest.mark.anyio
@pytest.mark.parametrize('stations', [1], indirect=True)
@pytest.mark.usefixtures('clean_db')
async def test_get_trends_stations_only_temprh(
        app: AsyncClient,
        stations: list[Station],
        db: AsyncSession,
) -> None:
    # create a temprh station
    temp_rh_station = Station(
        name='DEC4',
        device_id=27,
        long_name='temprh-station',
        latitude=51.447,
        longitude=7.268,
        altitude=100,
        station_type=StationType.temprh,
        leuchtennummer=120,
        district='Other District',
    )
    db.add(temp_rh_station)
    await db.commit()
    # we need to create some data and this way we can also check that the materialized
    # view works as expected
    data = [
        # station 0
        # data before our requested range
        BiometData(
            name=temp_rh_station.name,
            measured_at=datetime(2024, 8, 1, 8, 0),
            air_temperature=10,
        ),
        # data that will be aggregated by the materialized view
        BiometData(
            name=temp_rh_station.name,
            measured_at=datetime(2024, 8, 1, 9, 10),
            air_temperature=10,
        ),
        BiometData(
            name=temp_rh_station.name,
            measured_at=datetime(2024, 8, 1, 9, 20),
            air_temperature=11,
        ),
        BiometData(
            name=temp_rh_station.name,
            measured_at=datetime(2024, 8, 1, 9, 30),
            air_temperature=15,
        ),
        # data after our requested range
        BiometData(
            name=temp_rh_station.name,
            measured_at=datetime(2024, 8, 1, 10, 30),
            air_temperature=16,
        ),
        # a station that we don't request, but it theoretically would be supported!
        BiometData(
            name=stations[0].name,
            measured_at=datetime(2024, 8, 1, 9, 30),
            air_temperature=15,
        ),
    ]
    for d in data:
        db.add(d)

    await db.commit()
    await BiometDataHourly.refresh()

    resp = await app.get(
        '/v1/trends/air_temperature',
        params={
            'item_type': 'stations',
            'item_ids': 'DEC4',
            'start_date': datetime(2024, 8, 1, 0, 0),
            'end_date': datetime(2024, 8, 2, 23, 0),
            'hour': 10,
        },
    )
    assert resp.status_code == 200
    assert resp.json() == {
        'data': {
            'supported_ids': ['DEC1', 'DEC4'],
            'trends': [{'DEC4': 12.0, 'measured_at': '2024-08-01T10:00:00Z'}],
            'unit': '°C',
        },
    }


@pytest.mark.anyio
@pytest.mark.parametrize('stations', [1], indirect=True)
@pytest.mark.usefixtures('clean_db')
async def test_get_trends_stations_end_not_set(
        app: AsyncClient,
        stations: list[Station],
        db: AsyncSession,
) -> None:
    # we need to create some data and this way we can also check that the materialized
    # view works as expected
    data = [
        # station 0
        # data before our requested range
        BiometData(
            name=stations[0].name,
            measured_at=datetime(2024, 8, 1, 8, 0),
            air_temperature=10,
        ),
        # data that will be aggregated by the materialized view
        BiometData(
            name=stations[0].name,
            measured_at=datetime(2024, 8, 1, 9, 10),
            air_temperature=10,
        ),
        BiometData(
            name=stations[0].name,
            measured_at=datetime(2024, 8, 1, 9, 20),
            air_temperature=11,
        ),
        BiometData(
            name=stations[0].name,
            measured_at=datetime(2024, 8, 1, 9, 30),
            air_temperature=15,
        ),
        # data after our requested range
        BiometData(
            name=stations[0].name,
            measured_at=datetime(2024, 8, 1, 10, 30),
            air_temperature=16,
        ),
    ]
    for d in data:
        db.add(d)

    await db.commit()
    await BiometDataHourly.refresh()

    resp = await app.get(
        '/v1/trends/air_temperature',
        params={
            'item_type': 'stations',
            'item_ids': 'DEC1',
            'start_date': datetime(2024, 8, 1, 10, 0),
            'hour': 10,
        },
    )
    assert resp.status_code == 200
    assert resp.json() == {
        'data': {
            'supported_ids': ['DEC1'],
            'trends': [{'DEC1': 12.0, 'measured_at': '2024-08-01T10:00:00Z'}],
            'unit': '°C',
        },
    }


@pytest.mark.anyio
async def test_get_trends_stations_no_data_available(app: AsyncClient) -> None:
    resp = await app.get(
        '/v1/trends/air_temperature',
        params={
            'item_type': 'stations',
            'item_ids': 'DEC1',
            'start_date': datetime(2024, 8, 1, 10, 0),
            'hour': 10,
        },
    )
    assert resp.status_code == 200
    assert resp.json() == {
        'data': {
            'supported_ids': [],
            'trends': [],
            'unit': '°C',
        },
    }


@pytest.mark.anyio
@pytest.mark.parametrize('stations', [1], indirect=True)
@pytest.mark.usefixtures('clean_db')
async def test_get_trends_stations_does_not_provide_param(
        app: AsyncClient,
        db: AsyncSession,
        stations: list[Station],
) -> None:
    temp_rh_station = Station(
        name='DEC4',
        device_id=27,
        long_name='temprh-station',
        latitude=51.447,
        longitude=7.268,
        altitude=100,
        station_type=StationType.temprh,
        leuchtennummer=120,
        district='Other District',
    )
    db.add(temp_rh_station)
    biomet_station, = stations
    biomet_data = BiometData(
        name=biomet_station.name,
        measured_at=datetime(2024, 8, 1, 9, 30),
        mrt=70.5,
    )
    db.add(biomet_data)
    temp_rh_data = TempRHData(
        name=temp_rh_station.name,
        measured_at=datetime(2024, 8, 1, 10, 0),
        air_temperature=10.5,
        relative_humidity=65,
    )
    db.add(temp_rh_data)
    await db.commit()
    await TempRHDataHourly.refresh()
    await BiometDataHourly.refresh()

    resp = await app.get(
        '/v1/trends/mrt',
        params={
            'item_type': 'stations',
            'item_ids': 'DEC4',
            'start_date': datetime(2024, 8, 1, 10, 0),
            'hour': 10,
        },
    )
    assert resp.status_code == 200
    assert resp.json() == {
        'data': {
            'supported_ids': ['DEC1'],
            'trends': [],
            'unit': '°C',
        },
    }


@pytest.mark.anyio
@pytest.mark.parametrize('stations', [1], indirect=True)
@pytest.mark.parametrize(
    ('param', 'exp_unit'),
    (
        ('absolute_humidity', 'g/m³'),
        ('atmospheric_pressure', 'hPa'),
        ('atmospheric_pressure_reduced', 'hPa'),
        ('air_temperature', '°C'),
        ('dew_point', '°C'),
        ('heat_index', '°C'),
        ('lightning_average_distance', 'km'),
        ('lightning_strike_count', '-'),
        ('mrt', '°C'),
        ('pet', '°C'),
        ('pet_category', '-'),
        ('precipitation_sum', 'mm'),
        ('relative_humidity', '%'),
        ('solar_radiation', 'W/m²'),
        ('utci', '°C'),
        ('utci_category', '-'),
        ('vapor_pressure', 'hPa'),
        ('wet_bulb_temperature', '°C'),
        ('wind_direction', '°'),
        ('wind_speed', 'm/s'),
        ('maximum_wind_speed', 'm/s'),
    ),
)
@pytest.mark.usefixtures('clean_db')
async def test_get_trends_stations_units_correctly_extracted(
        app: AsyncClient,
        stations: list[Station],
        db: AsyncSession,
        param: str,
        exp_unit: str,
) -> None:
    # we need to create some data and this way we can also check that the materialized
    # view works as expected
    data = BiometData(
        name=stations[0].name,
        measured_at=datetime(2024, 8, 1, 8, 10),
        absolute_humidity=3,
        atmospheric_pressure=3,
        atmospheric_pressure_reduced=3,
        air_temperature=3,
        dew_point=3,
        heat_index=3,
        lightning_average_distance=3,
        lightning_strike_count=3,
        mrt=3,
        pet=3,
        pet_category=HeatStressCategories.extreme_cold_stress,
        precipitation_sum=3,
        relative_humidity=3,
        solar_radiation=3,
        utci=3,
        utci_category=HeatStressCategories.extreme_heat_stress,
        vapor_pressure=3,
        wet_bulb_temperature=3,
        wind_direction=3,
        wind_speed=3,
        maximum_wind_speed=3,
    )
    db.add(data)
    await db.commit()
    await BiometDataHourly.refresh()

    resp = await app.get(
        f'/v1/trends/{param}',
        params={
            'item_type': 'stations',
            'item_ids': 'DEC1',
            'start_date': datetime(2024, 8, 1, 8, 0),
            'hour': 9,
        },
    )
    assert resp.status_code == 200
    resp_data = resp.json()
    assert resp_data['data']['unit'] == exp_unit


@pytest.mark.anyio
@pytest.mark.usefixtures('clean_db')
async def test_get_trends_districts(
        app: AsyncClient,
        db: AsyncSession,
) -> None:
    # 1st create a few stations, we get two biomet stations from the fixture
    temp_rh_station_1 = Station(
        name='DEC1',
        device_id=27,
        long_name='temprh-station',
        latitude=51.447,
        longitude=7.268,
        altitude=100,
        station_type=StationType.temprh,
        leuchtennummer=120,
        district='District 1',
    )
    db.add(temp_rh_station_1)
    temp_rh_station_2 = Station(
        name='DEC2',
        device_id=27,
        long_name='temprh-station',
        latitude=51.447,
        longitude=7.268,
        altitude=100,
        station_type=StationType.temprh,
        leuchtennummer=120,
        district='District 2',
    )
    db.add(temp_rh_station_2)
    biomet_station_1 = Station(
        name='DEC3',
        device_id=27,
        long_name='biomet-station',
        latitude=51.447,
        longitude=7.268,
        altitude=100,
        station_type=StationType.temprh,
        leuchtennummer=120,
        district='District 1',
    )
    db.add(biomet_station_1)
    biomet_station_2 = Station(
        name='DEC4',
        device_id=27,
        long_name='biomet-station',
        latitude=51.447,
        longitude=7.268,
        altitude=100,
        station_type=StationType.temprh,
        leuchtennummer=120,
        district='District 2',
    )
    db.add(biomet_station_2)
    # now create some data for each station
    data = [
        # temp rh station 1
        TempRHData(
            name=temp_rh_station_1.name,
            measured_at=datetime(2024, 8, 1, 10, 10),
            air_temperature=10,
        ),
        TempRHData(
            name=temp_rh_station_1.name,
            measured_at=datetime(2024, 8, 1, 10, 20),
            air_temperature=12,
        ),
        TempRHData(
            name=temp_rh_station_1.name,
            measured_at=datetime(2024, 8, 2, 10, 30),
            air_temperature=15,
        ),
        # temp rh station 2
        TempRHData(
            name=temp_rh_station_2.name,
            measured_at=datetime(2024, 8, 1, 10, 10),
            air_temperature=12,
        ),
        TempRHData(
            name=temp_rh_station_2.name,
            measured_at=datetime(2024, 8, 1, 10, 20),
            air_temperature=14,
        ),
        TempRHData(
            name=temp_rh_station_2.name,
            measured_at=datetime(2024, 8, 2, 10, 30),
            air_temperature=18,
        ),
        # biomet station 1
        BiometData(
            name=biomet_station_1.name,
            measured_at=datetime(2024, 8, 1, 10, 10),
            air_temperature=15,
        ),
        BiometData(
            name=biomet_station_1.name,
            measured_at=datetime(2024, 8, 1, 10, 14),
            air_temperature=16,
        ),
        BiometData(
            name=biomet_station_1.name,
            measured_at=datetime(2024, 8, 2, 10, 19),
            air_temperature=17,
        ),
        # biomet station 2
        BiometData(
            name=biomet_station_2.name,
            measured_at=datetime(2024, 8, 1, 10, 10),
            air_temperature=15,
        ),
        BiometData(
            name=biomet_station_2.name,
            measured_at=datetime(2024, 8, 1, 10, 14),
            air_temperature=16,
        ),
        BiometData(
            name=biomet_station_2.name,
            measured_at=datetime(2024, 8, 2, 10, 19),
            air_temperature=17,
        ),
    ]
    for d in data:
        db.add(d)

    await db.commit()
    await BiometDataHourly.refresh()
    await TempRHDataHourly.refresh()

    resp = await app.get(
        '/v1/trends/air_temperature',
        params={
            'item_type': 'districts',
            'item_ids': ['District 2', 'District 1'],
            'start_date': datetime(2024, 8, 1, 1, 0),
            'end_date': datetime(2024, 8, 2, 13, 0),
            'hour': 11,
        },
    )
    assert resp.status_code == 200
    assert resp.json() == {
        'data': {
            'supported_ids': ['District 1', 'District 2'],
            'trends': [
                {'District 1': 13.25, 'measured_at': '2024-08-01T11:00:00Z'},
                {'District 1': 16.0, 'measured_at': '2024-08-02T11:00:00Z'},
                {'District 2': 14.25, 'measured_at': '2024-08-01T11:00:00Z'},
                {'District 2': 17.5, 'measured_at': '2024-08-02T11:00:00Z'},
            ],
            'unit': '°C',
        },
    }


@pytest.mark.anyio
@pytest.mark.usefixtures('clean_db')
@pytest.mark.parametrize(
    ('param', 'expected', 'unit'),
    (
        ('maximum_wind_speed', 14, 'm/s'),
        ('utci_category', 'extreme heat stress', '-'),
        ('wind_direction', 180, '°'),
        ('solar_radiation', 20, 'W/m²'),
    ),
)
@pytest.mark.parametrize('stations', [2], indirect=True)
async def test_get_trends_districts_aggregates_are_correct_no_temp_rh_data(
        app: AsyncClient,
        db: AsyncSession,
        stations: list[Station],
        param: str,
        expected: str | float,
        unit: str,
) -> None:
    data = [
        BiometData(
            name=stations[0].name,
            measured_at=datetime(2024, 8, 1, 10, 10),
            maximum_wind_speed=10,
            wind_direction=90,
            utci_category=HeatStressCategories.extreme_heat_stress,
            solar_radiation=5,
        ),
        BiometData(
            name=stations[0].name,
            measured_at=datetime(2024, 8, 1, 10, 14),
            maximum_wind_speed=5,
            wind_direction=270,
            utci_category=HeatStressCategories.extreme_heat_stress,
            solar_radiation=15,
        ),
        # biomet station 2
        BiometData(
            name=stations[1].name,
            measured_at=datetime(2024, 8, 1, 10, 10),
            maximum_wind_speed=12,
            wind_direction=90,
            utci_category=HeatStressCategories.extreme_heat_stress,
            solar_radiation=20,
        ),
        BiometData(
            name=stations[1].name,
            measured_at=datetime(2024, 8, 1, 10, 14),
            maximum_wind_speed=14,
            wind_direction=270,
            utci_category=HeatStressCategories.extreme_heat_stress,
            solar_radiation=40,
        ),
    ]
    for d in data:
        db.add(d)

    await db.commit()
    await BiometDataHourly.refresh()

    resp = await app.get(
        f'/v1/trends/{param}',
        params={
            'item_type': 'districts',
            'item_ids': ['Innenstadt'],
            'start_date': datetime(2024, 8, 1, 1, 0),
            'end_date': datetime(2024, 8, 2, 13, 0),
            'hour': 11,
        },
    )
    assert resp.status_code == 200
    assert resp.json() == {
        'data': {
            'supported_ids': ['Innenstadt'],
            'trends': [{'Innenstadt': expected, 'measured_at': '2024-08-01T11:00:00Z'}],
            'unit': unit,
        },
    }


@pytest.mark.anyio
@pytest.mark.usefixtures('clean_db')
@pytest.mark.parametrize('stations', [2], indirect=True)
@pytest.mark.parametrize(
    ('param', 'expected', 'unit'),
    (
        ('maximum_wind_speed', 14, 'm/s'),
        ('utci_category', 'extreme heat stress', '-'),
        ('wind_direction', 180, '°'),
        ('solar_radiation', 20, 'W/m²'),
        ('air_temperature', 15, '°C'),
    ),
)
async def test_get_trends_districts_aggregates_are_correct_biomet_and_temp_rh(
        app: AsyncClient,
        db: AsyncSession,
        stations: list[Station],
        param: str,
        expected: str | float,
        unit: str,
) -> None:
    # define Temp rh stations
    temp_rh_station_1 = Station(
        name='DEC4',
        device_id=27,
        long_name='test-station-4',
        latitude=51.447,
        longitude=7.268,
        altitude=100,
        station_type=StationType.temprh,
        leuchtennummer=120,
        district='Innenstadt',
    )
    db.add(temp_rh_station_1)
    temp_rh_station_2 = Station(
        name='DEC5',
        device_id=28,
        long_name='test-station-5',
        latitude=51.447,
        longitude=7.268,
        altitude=100,
        station_type=StationType.temprh,
        leuchtennummer=120,
        district='Innenstadt',
    )
    db.add(temp_rh_station_2)
    await db.commit()

    data = [
        BiometData(
            name=stations[0].name,
            measured_at=datetime(2024, 8, 1, 10, 10),
            maximum_wind_speed=10,
            wind_direction=90,
            utci_category=HeatStressCategories.extreme_heat_stress,
            solar_radiation=5,
        ),
        BiometData(
            name=stations[0].name,
            measured_at=datetime(2024, 8, 1, 10, 14),
            maximum_wind_speed=5,
            wind_direction=270,
            utci_category=HeatStressCategories.extreme_heat_stress,
            solar_radiation=15,
        ),
        # biomet station 2
        BiometData(
            name=stations[1].name,
            measured_at=datetime(2024, 8, 1, 10, 10),
            maximum_wind_speed=12,
            wind_direction=90,
            utci_category=HeatStressCategories.extreme_heat_stress,
            solar_radiation=20,
        ),
        BiometData(
            name=stations[1].name,
            measured_at=datetime(2024, 8, 1, 10, 14),
            maximum_wind_speed=14,
            wind_direction=270,
            utci_category=HeatStressCategories.extreme_heat_stress,
            solar_radiation=40,
        ),
        TempRHData(
            name=temp_rh_station_1.name,
            measured_at=datetime(2024, 8, 1, 10, 10),
            air_temperature=10,
        ),
        TempRHData(
            name=temp_rh_station_1.name,
            measured_at=datetime(2024, 8, 1, 10, 14),
            air_temperature=10,
        ),
        TempRHData(
            name=temp_rh_station_2.name,
            measured_at=datetime(2024, 8, 1, 10, 10),
            air_temperature=20,
        ),
        TempRHData(
            name=temp_rh_station_2.name,
            measured_at=datetime(2024, 8, 1, 10, 14),
            air_temperature=20,
        ),
    ]
    for d in data:
        db.add(d)

    await db.commit()
    await BiometDataHourly.refresh()
    await TempRHDataHourly.refresh()

    resp = await app.get(
        f'/v1/trends/{param}',
        params={
            'item_type': 'districts',
            'item_ids': ['Innenstadt'],
            'start_date': datetime(2024, 8, 1, 1, 0),
            'end_date': datetime(2024, 8, 2, 13, 0),
            'hour': 11,
        },
    )
    assert resp.status_code == 200
    assert resp.json() == {
        'data': {
            'supported_ids': ['Innenstadt'],
            'trends': [{'Innenstadt': expected, 'measured_at': '2024-08-01T11:00:00Z'}],
            'unit': unit,
        },
    }


@pytest.mark.anyio
async def test_get_data_start_greater_end_date(app: AsyncClient) -> None:
    resp = await app.get(
        '/v1/data/DEC1234',
        params={
            'start_date': datetime(2024, 8, 1, 14, 0),
            'end_date': datetime(2024, 8, 1, 13, 0),
            'param': 'air_temperature',
        },
    )
    assert resp.status_code == 422
    assert resp.json() == {'detail': 'start_date must not be > end_date'}


@pytest.mark.anyio
async def test_get_data_period_too_long(app: AsyncClient) -> None:
    resp = await app.get(
        '/v1/data/DEC1234',
        params={
            'start_date': datetime(2024, 8, 1, 14, 0),
            'end_date': datetime(2024, 9, 1, 13, 0),
            'param': 'air_temperature',
        },
    )
    assert resp.status_code == 422
    assert resp.json() == {'detail': 'a maximum of 30 days is allowed per request'}


@pytest.mark.anyio
async def test_get_data_station_not_found(app: AsyncClient) -> None:
    resp = await app.get(
        '/v1/data/DEC1234',
        params={
            'start_date': datetime(2024, 8, 1, 14, 0),
            'end_date': datetime(2024, 8, 1, 15, 0),
            'param': 'air_temperature',
        },
    )
    assert resp.status_code == 404
    assert resp.json() == {'detail': 'Station not found'}


@pytest.mark.anyio
@pytest.mark.parametrize('stations', [1], indirect=True)
@pytest.mark.usefixtures('clean_db')
async def test_get_biomet_data_multiple_params(
        app: AsyncClient,
        stations: list[Station],
        db: AsyncSession,
) -> None:
    # generate some data for the station
    data = [
        BiometData(
            name=stations[0].name,
            measured_at=datetime(2024, 8, 1, 10, minute),
            air_temperature=minute/2,
            mrt=minute*2,
        ) for minute in range(0, 40, 10)
    ]
    for d in data:
        db.add(d)

    await db.commit()

    resp = await app.get(
        '/v1/data/DEC1',
        params={
            'start_date': datetime(2024, 8, 1, 10, 0),
            'end_date': datetime(2024, 8, 1, 11, 0),
            'param': ['air_temperature', 'mrt'],
        },
    )
    assert resp.status_code == 200
    assert resp.json() == {
        'data': [
            {
                'air_temperature': 0.0,
                'measured_at': '2024-08-01T10:00:00Z',
                'mrt': 0.0,
            },
            {
                'air_temperature': 5.0,
                'measured_at': '2024-08-01T10:10:00Z',
                'mrt': 20.0,
            },
            {
                'air_temperature': 10.0,
                'measured_at': '2024-08-01T10:20:00Z',
                'mrt': 40.0,
            },
            {
                'air_temperature': 15.0,
                'measured_at': '2024-08-01T10:30:00Z',
                'mrt': 60.0,
            },
        ],
    }


@pytest.mark.anyio
@pytest.mark.usefixtures('clean_db')
async def test_get_temp_rh_data_multiple_params(
        app: AsyncClient,
        db: AsyncSession,
) -> None:
    # generate some data for the station
    station = Station(
        name='DEC1',
        device_id=27,
        long_name='test-station-1',
        latitude=51.447,
        longitude=7.268,
        altitude=100,
        station_type=StationType.temprh,
        leuchtennummer=120,
        district='Innenstadt',
    )
    db.add(station)
    await db.commit()
    data = [
        TempRHData(
            name=station.name,
            measured_at=datetime(2024, 8, 1, 10, minute),
            air_temperature=minute/2,
            relative_humidity=minute*2,
        ) for minute in range(0, 40, 10)
    ]
    for d in data:
        db.add(d)

    await db.commit()

    resp = await app.get(
        '/v1/data/DEC1',
        params={
            'start_date': datetime(2024, 8, 1, 10, 0),
            'end_date': datetime(2024, 8, 1, 11, 0),
            'param': ['air_temperature', 'relative_humidity'],
        },
    )
    assert resp.status_code == 200
    assert resp.json() == {
        'data': [
            {
                'air_temperature': 0.0,
                'measured_at': '2024-08-01T10:00:00Z',
                'relative_humidity': 0.0,
            },
            {
                'air_temperature': 5.0,
                'measured_at': '2024-08-01T10:10:00Z',
                'relative_humidity': 20.0,
            },
            {
                'air_temperature': 10.0,
                'measured_at': '2024-08-01T10:20:00Z',
                'relative_humidity': 40.0,
            },
            {
                'air_temperature': 15.0,
                'measured_at': '2024-08-01T10:30:00Z',
                'relative_humidity': 60.0,
            },
        ],
    }
