import re
from datetime import datetime
from datetime import timedelta
from datetime import timezone

import freezegun
import pytest
from httpx import AsyncClient
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import BiometData
from app.models import BiometDataDaily
from app.models import BiometDataHourly
from app.models import HeatStressCategories
from app.models import LatestData
from app.models import Sensor
from app.models import SensorDeployment
from app.models import SensorType
from app.models import Station
from app.models import StationType
from app.models import TempRHData
from app.models import TempRHDataDaily
from app.models import TempRHDataHourly
from app.routers.v1 import compute_colormap_range
from app.schemas import ParamSettings

VERSION_PATTERN = re.compile(
    r'^\d+\.\d+(\.\d+)?(?:\.dev\d+\+g[0-9a-f]+(?:\.d[0-9]{8})?)?$',
)


@pytest.mark.parametrize(
    'endpoint',
    (
        '/v1/stations/metadata',
        '/v1/stations/latest_data?param=air_temperature',
        '/v1/districts/latest_data?param=air_temperature',
        '/v1/trends/air_temperature?spatial_level=stations&item_ids=DOB1&start_date=2024-08-01&hour=3',  # noqa: E501
        'v1/data/DOB1?start_date=2024-08-01&end_date=2024-08-02&param=air_temperature',
        'v1/network-snapshot?param=air_temperature&scale=hourly&date=2024-08-02',
    ),
)
@pytest.mark.parametrize('stations', [1], indirect=True)
@freezegun.freeze_time('2024-08-01 01:00')
@pytest.mark.anyio
async def test_every_response_contains_timestamp_and_version(
        app: AsyncClient,
        endpoint: str,
        stations: list[Station],
) -> None:
    resp = await app.get(endpoint)
    assert resp.status_code == 200
    data = resp.json()
    assert data['timestamp'] == 1722474000000
    assert VERSION_PATTERN.match(data['version'])


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
async def test_get_station_metadata(
        app: AsyncClient,
        stations: list[Station],
        db: AsyncSession,
) -> None:
    deployments = [
        SensorDeployment(
            sensor_id='DEC1',
            station_id='DOB1',
            setup_date=datetime(2024, 5, 1, tzinfo=timezone.utc),
        ),
        SensorDeployment(
            sensor_id='DEC2',
            station_id='DOB2',
            setup_date=datetime(2024, 5, 1, tzinfo=timezone.utc),
        ),
    ]
    db.add_all(deployments)
    await db.commit()
    resp = await app.get('/v1/stations/metadata')
    assert resp.status_code == 200
    assert resp.json()['data'] == [
        {
            'altitude': 100.0,
            'city': 'Dortmund',
            'country': 'Germany',
            'district': 'Innenstadt',
            'latitude': 51.446,
            'lcz': '2',
            'long_name': 'test-station-1',
            'longitude': 7.2627,
            'number': None,
            'plz': 12345,
            'sensor_distance_from_mounting_structure': None,
            'sensor_height_agl': None,
            'station_id': 'DOB1',
            'station_type': 'biomet',
            'street': 'test-street',
            'svf': None,
            'urban_atlas_class_name': None,
            'urban_atlas_class_nr': None,
        },
        {
            'altitude': 100.0,
            'city': 'Dortmund',
            'country': 'Germany',
            'district': 'Innenstadt',
            'latitude': 51.446,
            'lcz': '2',
            'long_name': 'test-station-2',
            'longitude': 7.2627,
            'number': None,
            'plz': 12345,
            'sensor_distance_from_mounting_structure': None,
            'sensor_height_agl': None,
            'station_id': 'DOB2',
            'station_type': 'biomet',
            'street': 'test-street',
            'svf': None,
            'urban_atlas_class_name': None,
            'urban_atlas_class_nr': None,
        },
    ]


@pytest.mark.anyio
@pytest.mark.parametrize('stations', [2], indirect=True)
async def test_get_station_metadata_include_inactive(
        app: AsyncClient,
        stations: list[Station],
) -> None:
    resp = await app.get('/v1/stations/metadata', params={'include_inactive': True})
    assert resp.status_code == 200
    assert resp.json()['data'] == [
        {
            'altitude': 100.0,
            'city': 'Dortmund',
            'country': 'Germany',
            'district': 'Innenstadt',
            'latitude': 51.446,
            'lcz': '2',
            'long_name': 'test-station-1',
            'longitude': 7.2627,
            'number': None,
            'plz': 12345,
            'sensor_distance_from_mounting_structure': None,
            'sensor_height_agl': None,
            'station_id': 'DOB1',
            'station_type': 'biomet',
            'street': 'test-street',
            'svf': None,
            'urban_atlas_class_name': None,
            'urban_atlas_class_nr': None,
        },
        {
            'altitude': 100.0,
            'city': 'Dortmund',
            'country': 'Germany',
            'district': 'Innenstadt',
            'latitude': 51.446,
            'lcz': '2',
            'long_name': 'test-station-2',
            'longitude': 7.2627,
            'number': None,
            'plz': 12345,
            'sensor_distance_from_mounting_structure': None,
            'sensor_height_agl': None,
            'station_id': 'DOB2',
            'station_type': 'biomet',
            'street': 'test-street',
            'svf': None,
            'urban_atlas_class_name': None,
            'urban_atlas_class_nr': None,
        },
    ]


@pytest.mark.anyio
async def test_get_station_metadata_no_stations(app: AsyncClient) -> None:
    resp = await app.get('/v1/stations/metadata')
    assert resp.status_code == 200
    assert resp.json()['data'] == []


@pytest.mark.anyio
@pytest.mark.parametrize('stations', [2], indirect=True)
async def test_get_station_metadata_no_deployments_at_station(
        app: AsyncClient,
        stations: list[Station],
) -> None:
    resp = await app.get('/v1/stations/metadata')
    assert resp.status_code == 200
    assert resp.json()['data'] == []


@pytest.mark.anyio
@pytest.mark.parametrize('stations', [2], indirect=True)
async def test_get_station_metadata_subset_of_cols(
        app: AsyncClient,
        stations: list[Station],
) -> None:
    resp = await app.get(
        '/v1/stations/metadata',
        params={'include_inactive': True, 'param': ['altitude', 'city']},
    )
    assert resp.status_code == 200
    assert resp.json()['data'] == [
        {
            'station_id': 'DOB1',
            'altitude': 100.0,
            'city': 'Dortmund',
        },
        {
            'station_id': 'DOB2',
            'altitude': 100.0,
            'city': 'Dortmund',
        },
    ]


@pytest.mark.anyio
@pytest.mark.parametrize('biomet_data', [{'n_stations': 2, 'n_data': 3}], indirect=True)
@freezegun.freeze_time('2024-08-01 01:00')
async def test_get_station_latest_data(
        app: AsyncClient,
        biomet_data: list[BiometData],
) -> None:
    resp = await app.get('/v1/stations/latest_data', params={'param': 'utci'})
    assert resp.status_code == 200
    assert resp.json()['data'] == [
        {
            'altitude': 100.0,
            'district': 'Innenstadt',
            'latitude': 51.446,
            'lcz': '2',
            'long_name': 'test-station-1',
            'longitude': 7.2627,
            'station_id': 'DOB1',
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
            'station_id': 'DOB2',
            'station_type': 'biomet',
            'measured_at': '2024-08-01T00:15:00Z',
            'utci': 35.5,
        },
    ]


@pytest.mark.anyio
async def test_get_station_latest_data_no_data(app: AsyncClient) -> None:
    resp = await app.get('/v1/stations/latest_data', params={'param': 'utci'})
    assert resp.status_code == 200
    assert resp.json()['data'] == []


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
        station_id=stations[0].station_id,
        sensor_id='DEC1',
        blg_sensor_id='DEC2',
        measured_at=datetime(2024, 8, 1, 1, 30, tzinfo=timezone.utc),
        mrt=69.69,
    )
    db.add(up_to_date_data)
    too_old_data = BiometData(
        station_id=stations[1].station_id,
        sensor_id='DEC1',
        blg_sensor_id='DEC2',
        measured_at=datetime(2024, 8, 1, 0, 0, tzinfo=timezone.utc),
        mrt=42.0,
    )
    db.add(too_old_data)
    await db.commit()
    # we need to refresh the views so we actually get the data
    await LatestData.refresh()
    await db.commit()

    resp = await app.get(
        '/v1/stations/latest_data',
        params={
            'param': 'mrt',
            'max_age': 'PT2H',  # 2 hours is the max age!
        },
    )
    assert resp.status_code == 200
    assert resp.json()['data'] == [{
        'altitude': 100.0,
        'district': 'Innenstadt',
        'latitude': 51.446,
        'lcz': '2',
        'long_name': 'test-station-1',
        'longitude': 7.2627,
        'station_id': 'DOB1',
        'station_type': 'biomet',
        'measured_at': '2024-08-01T01:30:00Z',
        'mrt': 69.69,
    }]


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
        station_id=stations[0].station_id,
        sensor_id='DEC1',
        blg_sensor_id='DEC2',
        measured_at=datetime(2024, 8, 1, 1, 30, tzinfo=timezone.utc),
        mrt=69.69,
    )
    db.add(up_to_date_data)
    too_old_data = BiometData(
        station_id=stations[1].station_id,
        sensor_id='DEC1',
        blg_sensor_id='DEC2',
        measured_at=datetime(2024, 8, 1, 0, 0, tzinfo=timezone.utc),
        mrt=42.0,
    )
    db.add(too_old_data)
    await db.commit()
    # we need to refresh the views so we actually get the data
    await LatestData.refresh()
    await db.commit()

    resp = await app.get('/v1/stations/latest_data', params={'param': 'mrt'})
    assert resp.status_code == 200
    assert resp.json()['data'] == [{
        'altitude': 100.0,
        'district': 'Innenstadt',
        'latitude': 51.446,
        'lcz': '2',
        'long_name': 'test-station-1',
        'longitude': 7.2627,
        'station_id': 'DOB1',
        'station_type': 'biomet',
        'measured_at': '2024-08-01T01:30:00Z',
        'mrt': 69.69,
    }]


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
        station_id=stations[0].station_id,
        sensor_id='DEC1',
        blg_sensor_id='DEC2',
        measured_at=datetime(2024, 8, 1, 1, 30, tzinfo=timezone.utc),
        mrt=69.69,
        utci=45,
        utci_category=HeatStressCategories.extreme_heat_stress,
    )
    db.add(up_to_date_data)
    too_old_data = BiometData(
        station_id=stations[1].station_id,
        sensor_id='DEC1',
        blg_sensor_id='DEC2',
        measured_at=datetime(2024, 8, 1, 0, 0, tzinfo=timezone.utc),
        mrt=42.0,
        utci=30,
        utci_category=HeatStressCategories.strong_heat_stress,
    )
    db.add(too_old_data)
    # this station must not be included, since it does not provide all params
    null_data = BiometData(
        station_id=stations[2].station_id,
        sensor_id='DEC1',
        blg_sensor_id='DEC2',
        measured_at=datetime(2024, 8, 1, 1, 30, tzinfo=timezone.utc),
        mrt=None,
        utci=30,
        utci_category=HeatStressCategories.strong_heat_stress,
    )
    db.add(null_data)
    await db.commit()
    # we need to refresh the views so we actually get the data
    await LatestData.refresh()
    await db.commit()

    resp = await app.get(
        '/v1/stations/latest_data',
        params={'param': ['mrt', 'utci', 'utci_category']},
    )
    assert resp.status_code == 200
    assert resp.json()['data'] == [{
        'altitude': 100.0,
        'district': 'Innenstadt',
        'latitude': 51.446,
        'lcz': '2',
        'long_name': 'test-station-1',
        'longitude': 7.2627,
        'station_id': 'DOB1',
        'station_type': 'biomet',
        'measured_at': '2024-08-01T01:30:00Z',
        'mrt': 69.69,
        'utci': 45.0,
        'utci_category': 'extreme heat stress',
    }]


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
        station_id=stations[0].station_id,
        sensor_id='DEC1',
        blg_sensor_id='DEC2',
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
        station_id=stations[1].station_id,
        sensor_id='DEC1',
        blg_sensor_id='DEC2',
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
        station_id=stations[2].station_id,
        sensor_id='DEC1',
        blg_sensor_id='DEC2',
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
        station_id='missing_data',
        long_name='test-station-missing-data',
        latitude=51.447,
        longitude=7.268,
        altitude=100,
        station_type=StationType.temprh,
        leuchtennummer=120,
        district='Other District',
        city='Dortmund',
        country='Germany',
        street='test-street',
        plz=12345,
    )
    db.add(station_with_missing_data)
    data_missing = BiometData(
        station_id=station_with_missing_data.station_id,
        sensor_id='DEC1',
        blg_sensor_id='DEC2',
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
    await LatestData.refresh()
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
    assert resp.json()['data'] == [{
        'district': 'Innenstadt',
        # care, approx messes with the output if a dict key is missing so
        # double check if a test is failing
        'wind_direction': pytest.approx(5),
        'air_temperature': 15.0,
        'maximum_wind_speed': 15,
        'utci_category': 'no thermal stress',
        'precipitation_sum': 2.5,
        'lightning_strike_count': 15,
    }]


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
        station_id='DOB4',
        long_name='temprh-station',
        latitude=51.447,
        longitude=7.268,
        altitude=100,
        station_type=StationType.temprh,
        leuchtennummer=120,
        district='Other District',
        city='Dortmund',
        country='Germany',
        street='test-street',
        plz=12345,
    )
    db.add(temp_rh_station)
    await db.commit()
    # we need to create some data and this way we can also check that the materialized
    # view works as expected
    data = [
        # station 0
        # data before our requested range
        BiometData(
            station_id=stations[0].station_id,
            sensor_id='DEC1',
            blg_sensor_id='DEC2',
            measured_at=datetime(2024, 8, 1, 8, 0, tzinfo=timezone.utc),
            air_temperature=10,
        ),
        # data that will be aggregated by the materialized view
        BiometData(
            station_id=stations[0].station_id,
            sensor_id='DEC1',
            blg_sensor_id='DEC2',
            measured_at=datetime(2024, 8, 1, 9, 10, tzinfo=timezone.utc),
            air_temperature=10,
        ),
        BiometData(
            station_id=stations[0].station_id,
            sensor_id='DEC1',
            blg_sensor_id='DEC2',
            measured_at=datetime(2024, 8, 1, 9, 20, tzinfo=timezone.utc),
            air_temperature=11,
        ),
        BiometData(
            station_id=stations[0].station_id,
            sensor_id='DEC1',
            blg_sensor_id='DEC2',
            measured_at=datetime(2024, 8, 1, 9, 30, tzinfo=timezone.utc),
            air_temperature=15,
        ),
        # data after our requested range
        BiometData(
            station_id=stations[0].station_id,
            sensor_id='DEC1',
            blg_sensor_id='DEC2',
            measured_at=datetime(2024, 8, 1, 10, 30, tzinfo=timezone.utc),
            air_temperature=16,
        ),
        # station 1 (the same structure as above)
        BiometData(
            station_id=temp_rh_station.station_id,
            measured_at=datetime(2024, 8, 1, 8, 0, tzinfo=timezone.utc),
            sensor_id='DEC1',
            blg_sensor_id='DEC2',
            air_temperature=9,
        ),
        BiometData(
            station_id=temp_rh_station.station_id,
            sensor_id='DEC1',
            blg_sensor_id='DEC2',
            measured_at=datetime(2024, 8, 1, 9, 10, tzinfo=timezone.utc),
            air_temperature=9,
        ),
        BiometData(
            station_id=temp_rh_station.station_id,
            sensor_id='DEC1',
            blg_sensor_id='DEC2',
            measured_at=datetime(2024, 8, 1, 9, 20, tzinfo=timezone.utc),
            air_temperature=10,
        ),
        BiometData(
            station_id=temp_rh_station.station_id,
            sensor_id='DEC1',
            blg_sensor_id='DEC2',
            measured_at=datetime(2024, 8, 1, 9, 30, tzinfo=timezone.utc),
            air_temperature=14,
        ),
        BiometData(
            station_id=temp_rh_station.station_id,
            sensor_id='DEC1',
            blg_sensor_id='DEC2',
            measured_at=datetime(2024, 8, 1, 10, 30, tzinfo=timezone.utc),
            air_temperature=15,
        ),
        # a station that we don't request, but it theoretically would be supported!
        BiometData(
            station_id=stations[1].station_id,
            sensor_id='DEC1',
            blg_sensor_id='DEC2',
            measured_at=datetime(2024, 8, 1, 9, 30, tzinfo=timezone.utc),
            air_temperature=15,
        ),
    ]
    db.add_all(data)

    await db.commit()
    await BiometDataHourly.refresh()

    resp = await app.get(
        '/v1/trends/air_temperature',
        params={
            'spatial_level': 'stations',
            'item_ids': ['DOB1', 'DOB4'],
            'start_date': datetime(2024, 8, 1, 0, 0).isoformat(),
            'end_date': datetime(2024, 8, 2, 23, 0).isoformat(),
            'hour': 10,
        },
    )
    assert resp.status_code == 200
    assert resp.json()['data'] == {
        'supported_ids': ['DOB1', 'DOB2', 'DOB4'],
        'trends': [
            {'DOB1': 12.0, 'measured_at': '2024-08-01T10:00:00Z'},
            {'DOB4': 11.0, 'measured_at': '2024-08-01T10:00:00Z'},
        ],
        'unit': '°C',
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
            station_id=stations[0].station_id,
            sensor_id='DEC1',
            blg_sensor_id='DEC2',
            measured_at=datetime(2024, 8, 1, 8, 0, tzinfo=timezone.utc),
            air_temperature=10,
        ),
        # data that will be aggregated by the materialized view
        BiometData(
            station_id=stations[0].station_id,
            sensor_id='DEC1',
            blg_sensor_id='DEC2',
            measured_at=datetime(2024, 8, 1, 9, 10, tzinfo=timezone.utc),
            air_temperature=10,
        ),
        BiometData(
            station_id=stations[0].station_id,
            sensor_id='DEC1',
            blg_sensor_id='DEC2',
            measured_at=datetime(2024, 8, 1, 9, 20, tzinfo=timezone.utc),
            air_temperature=11,
        ),
        BiometData(
            station_id=stations[0].station_id,
            sensor_id='DEC1',
            blg_sensor_id='DEC2',
            measured_at=datetime(2024, 8, 1, 9, 30, tzinfo=timezone.utc),
            air_temperature=15,
        ),
        # data after our requested range
        BiometData(
            station_id=stations[0].station_id,
            sensor_id='DEC1',
            blg_sensor_id='DEC2',
            measured_at=datetime(2024, 8, 1, 10, 30, tzinfo=timezone.utc),
            air_temperature=16,
        ),
        # a station that we don't request, but it theoretically would be supported!
        BiometData(
            station_id=stations[1].station_id,
            sensor_id='DEC1',
            blg_sensor_id='DEC2',
            measured_at=datetime(2024, 8, 1, 9, 30, tzinfo=timezone.utc),
            air_temperature=15,
        ),
    ]
    db.add_all(data)

    await db.commit()
    await BiometDataHourly.refresh()

    resp = await app.get(
        '/v1/trends/air_temperature',
        params={
            'spatial_level': 'stations',
            'item_ids': 'DOB1',
            'start_date': datetime(2024, 8, 1, 0, 0).isoformat(),
            'end_date': datetime(2024, 8, 2, 23, 0).isoformat(),
            'hour': 10,
        },
    )
    assert resp.status_code == 200
    assert resp.json()['data'] == {
        'supported_ids': ['DOB1', 'DOB2'],
        'trends': [{'DOB1': 12.0, 'measured_at': '2024-08-01T10:00:00Z'}],
        'unit': '°C',
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
            station_id=stations[0].station_id,
            sensor_id='DEC1',
            blg_sensor_id='DEC2',
            measured_at=datetime(2024, 8, 1, 8, 0, tzinfo=timezone.utc),
            lightning_strike_count=10,
        ),
        # data that will be aggregated by the materialized view
        BiometData(
            station_id=stations[0].station_id,
            sensor_id='DEC1',
            blg_sensor_id='DEC2',
            measured_at=datetime(2024, 8, 1, 9, 10, tzinfo=timezone.utc),
            lightning_strike_count=10,
        ),
        BiometData(
            station_id=stations[0].station_id,
            sensor_id='DEC1',
            blg_sensor_id='DEC2',
            measured_at=datetime(2024, 8, 1, 9, 20, tzinfo=timezone.utc),
            lightning_strike_count=11,
        ),
        BiometData(
            station_id=stations[0].station_id,
            sensor_id='DEC1',
            blg_sensor_id='DEC2',
            measured_at=datetime(2024, 8, 1, 9, 30, tzinfo=timezone.utc),
            lightning_strike_count=15,
        ),
        # data after our requested range
        BiometData(
            station_id=stations[0].station_id,
            sensor_id='DEC1',
            blg_sensor_id='DEC2',
            measured_at=datetime(2024, 8, 1, 10, 30, tzinfo=timezone.utc),
            lightning_strike_count=16,
        ),
        # a station that we don't request, but it theoretically would be supported!
        BiometData(
            station_id=stations[1].station_id,
            sensor_id='DEC1',
            blg_sensor_id='DEC2',
            measured_at=datetime(2024, 8, 1, 9, 30, tzinfo=timezone.utc),
            lightning_strike_count=15,
        ),
    ]
    db.add_all(data)

    await db.commit()
    await BiometDataHourly.refresh()

    resp = await app.get(
        '/v1/trends/lightning_strike_count',
        params={
            'spatial_level': 'stations',
            'item_ids': 'DOB1',
            'start_date': datetime(2024, 8, 1, 0, 0).isoformat(),
            'end_date': datetime(2024, 8, 2, 23, 0).isoformat(),
            'hour': 10,
        },
    )
    assert resp.status_code == 200
    assert resp.json()['data'] == {
        'supported_ids': ['DOB1', 'DOB2'],
        'trends': [{'DOB1': 36, 'measured_at': '2024-08-01T10:00:00Z'}],
        'unit': '-',
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
        station_id='DOB4',
        long_name='temprh-station',
        latitude=51.447,
        longitude=7.268,
        altitude=100,
        station_type=StationType.temprh,
        leuchtennummer=120,
        district='Other District',
        city='Dortmund',
        country='Germany',
        street='test-street',
        plz=12345,
    )
    db.add(temp_rh_station)
    await db.commit()
    # we need to create some data and this way we can also check that the materialized
    # view works as expected
    data = [
        # station 0
        # data before our requested range
        BiometData(
            station_id=temp_rh_station.station_id,
            sensor_id='DEC1',
            blg_sensor_id='DEC2',
            measured_at=datetime(2024, 8, 1, 8, 0, tzinfo=timezone.utc),
            air_temperature=10,
        ),
        # data that will be aggregated by the materialized view
        BiometData(
            station_id=temp_rh_station.station_id,
            sensor_id='DEC1',
            blg_sensor_id='DEC2',
            measured_at=datetime(2024, 8, 1, 9, 10, tzinfo=timezone.utc),
            air_temperature=10,
        ),
        BiometData(
            station_id=temp_rh_station.station_id,
            sensor_id='DEC1',
            blg_sensor_id='DEC2',
            measured_at=datetime(2024, 8, 1, 9, 20, tzinfo=timezone.utc),
            air_temperature=11,
        ),
        BiometData(
            station_id=temp_rh_station.station_id,
            sensor_id='DEC1',
            blg_sensor_id='DEC2',
            measured_at=datetime(2024, 8, 1, 9, 30, tzinfo=timezone.utc),
            air_temperature=15,
        ),
        # data after our requested range
        BiometData(
            station_id=temp_rh_station.station_id,
            sensor_id='DEC1',
            blg_sensor_id='DEC2',
            measured_at=datetime(2024, 8, 1, 10, 30, tzinfo=timezone.utc),
            air_temperature=16,
        ),
        # a station that we don't request, but it theoretically would be supported!
        BiometData(
            station_id=stations[0].station_id,
            sensor_id='DEC1',
            blg_sensor_id='DEC2',
            measured_at=datetime(2024, 8, 1, 9, 30, tzinfo=timezone.utc),
            air_temperature=15,
        ),
    ]
    db.add_all(data)

    await db.commit()
    await BiometDataHourly.refresh()

    resp = await app.get(
        '/v1/trends/air_temperature',
        params={
            'spatial_level': 'stations',
            'item_ids': 'DOB4',
            'start_date': datetime(2024, 8, 1, 0, 0).isoformat(),
            'end_date': datetime(2024, 8, 2, 23, 0).isoformat(),
            'hour': 10,
        },
    )
    assert resp.status_code == 200
    assert resp.json()['data'] == {
        'supported_ids': ['DOB1', 'DOB4'],
        'trends': [{'DOB4': 12.0, 'measured_at': '2024-08-01T10:00:00Z'}],
        'unit': '°C',
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
            station_id=stations[0].station_id,
            sensor_id='DEC1',
            blg_sensor_id='DEC2',
            measured_at=datetime(2024, 8, 1, 8, 0, tzinfo=timezone.utc),
            air_temperature=10,
        ),
        # data that will be aggregated by the materialized view
        BiometData(
            station_id=stations[0].station_id,
            sensor_id='DEC1',
            blg_sensor_id='DEC2',
            measured_at=datetime(2024, 8, 1, 9, 10, tzinfo=timezone.utc),
            air_temperature=10,
        ),
        BiometData(
            station_id=stations[0].station_id,
            sensor_id='DEC1',
            blg_sensor_id='DEC2',
            measured_at=datetime(2024, 8, 1, 9, 20, tzinfo=timezone.utc),
            air_temperature=11,
        ),
        BiometData(
            station_id=stations[0].station_id,
            sensor_id='DEC1',
            blg_sensor_id='DEC2',
            measured_at=datetime(2024, 8, 1, 9, 30, tzinfo=timezone.utc),
            air_temperature=15,
        ),
        # data after our requested range
        BiometData(
            station_id=stations[0].station_id,
            sensor_id='DEC1',
            blg_sensor_id='DEC2',
            measured_at=datetime(2024, 8, 1, 10, 30, tzinfo=timezone.utc),
            air_temperature=16,
        ),
    ]
    db.add_all(data)

    await db.commit()
    await BiometDataHourly.refresh()

    resp = await app.get(
        '/v1/trends/air_temperature',
        params={
            'spatial_level': 'stations',
            'item_ids': 'DOB1',
            'start_date': datetime(2024, 8, 1, 10, 0).isoformat(),
            'hour': 10,
        },
    )
    assert resp.status_code == 200
    assert resp.json()['data'] == {
        'supported_ids': ['DOB1'],
        'trends': [{'DOB1': 12.0, 'measured_at': '2024-08-01T10:00:00Z'}],
        'unit': '°C',
    }


@pytest.mark.anyio
async def test_get_trends_stations_no_data_available(app: AsyncClient) -> None:
    resp = await app.get(
        '/v1/trends/air_temperature',
        params={
            'spatial_level': 'stations',
            'item_ids': 'DOB1',
            'start_date': datetime(2024, 8, 1, 10, 0).isoformat(),
            'hour': 10,
        },
    )
    assert resp.status_code == 200
    assert resp.json()['data'] == {
        'supported_ids': [],
        'trends': [],
        'unit': '°C',
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
        station_id='DOB4',
        long_name='temprh-station',
        latitude=51.447,
        longitude=7.268,
        altitude=100,
        station_type=StationType.temprh,
        leuchtennummer=120,
        district='Other District',
        city='Dortmund',
        country='Germany',
        street='test-street',
        plz=12345,
    )
    db.add(temp_rh_station)
    biomet_station, = stations
    biomet_data = BiometData(
        station_id=biomet_station.station_id,
        sensor_id='DEC1',
        blg_sensor_id='DEC2',
        measured_at=datetime(2024, 8, 1, 9, 30),
        mrt=70.5,
    )
    db.add(biomet_data)
    temp_rh_data = TempRHData(
        station_id=temp_rh_station.station_id,
        sensor_id='DEC1',
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
            'spatial_level': 'stations',
            'item_ids': 'DOB4',
            'start_date': datetime(2024, 8, 1, 10, 0).isoformat(),
            'hour': 10,
        },
    )
    assert resp.status_code == 200
    assert resp.json()['data'] == {
        'supported_ids': ['DOB1'],
        'trends': [],
        'unit': '°C',
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
        station_id=stations[0].station_id,
        sensor_id='DEC1',
        blg_sensor_id='DEC2',
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
            'spatial_level': 'stations',
            'item_ids': 'DOB1',
            'start_date': datetime(2024, 8, 1, 8, 0).isoformat(),
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
    sensors = [
        Sensor(
            sensor_id='DEC3',
            device_id=11111,
            sensor_type=SensorType.sht35,
        ),
        Sensor(
            sensor_id='DEC1',
            device_id=11111,
            sensor_type=SensorType.atm41,
        ),
        Sensor(
            sensor_id='DEC2',
            device_id=22222,
            sensor_type=SensorType.blg,
        ),
    ]
    db.add_all(sensors)
    # 1st create a few stations, we get two biomet stations from the fixture
    temp_rh_station_1 = Station(
        station_id='DOB1',
        long_name='temprh-station',
        latitude=51.447,
        longitude=7.268,
        altitude=100,
        station_type=StationType.temprh,
        leuchtennummer=120,
        district='District 1',
        city='Dortmund',
        country='Germany',
        street='test-street',
        plz=12345,
    )
    db.add(temp_rh_station_1)
    temp_rh_station_2 = Station(
        station_id='DOB2',
        long_name='temprh-station',
        latitude=51.447,
        longitude=7.268,
        altitude=100,
        station_type=StationType.temprh,
        leuchtennummer=120,
        district='District 2',
        city='Dortmund',
        country='Germany',
        street='test-street',
        plz=12345,
    )
    db.add(temp_rh_station_2)
    biomet_station_1 = Station(
        station_id='DOB3',
        long_name='biomet-station',
        latitude=51.447,
        longitude=7.268,
        altitude=100,
        station_type=StationType.temprh,
        leuchtennummer=120,
        district='District 1',
        city='Dortmund',
        country='Germany',
        street='test-street',
        plz=12345,
    )
    db.add(biomet_station_1)
    biomet_station_2 = Station(
        station_id='DOB4',
        long_name='biomet-station',
        latitude=51.447,
        longitude=7.268,
        altitude=100,
        station_type=StationType.temprh,
        leuchtennummer=120,
        district='District 2',
        city='Dortmund',
        country='Germany',
        street='test-street',
        plz=12345,
    )
    db.add(biomet_station_2)
    # now create some data for each station
    data = [
        # temp rh station 1
        TempRHData(
            station_id=temp_rh_station_1.station_id,
            sensor_id='DEC3',
            measured_at=datetime(2024, 8, 1, 10, 10, tzinfo=timezone.utc),
            air_temperature=10,
        ),
        TempRHData(
            station_id=temp_rh_station_1.station_id,
            sensor_id='DEC3',
            measured_at=datetime(2024, 8, 1, 10, 20, tzinfo=timezone.utc),
            air_temperature=12,
        ),
        TempRHData(
            station_id=temp_rh_station_1.station_id,
            sensor_id='DEC3',
            measured_at=datetime(2024, 8, 2, 10, 30, tzinfo=timezone.utc),
            air_temperature=15,
        ),
        # temp rh station 2
        TempRHData(
            station_id=temp_rh_station_2.station_id,
            sensor_id='DEC3',
            measured_at=datetime(2024, 8, 1, 10, 10, tzinfo=timezone.utc),
            air_temperature=12,
        ),
        TempRHData(
            station_id=temp_rh_station_2.station_id,
            sensor_id='DEC3',
            measured_at=datetime(2024, 8, 1, 10, 20, tzinfo=timezone.utc),
            air_temperature=14,
        ),
        TempRHData(
            station_id=temp_rh_station_2.station_id,
            sensor_id='DEC3',
            measured_at=datetime(2024, 8, 2, 10, 30, tzinfo=timezone.utc),
            air_temperature=18,
        ),
        # biomet station 1
        BiometData(
            station_id=biomet_station_1.station_id,
            sensor_id='DEC1',
            blg_sensor_id='DEC2',
            measured_at=datetime(2024, 8, 1, 10, 10, tzinfo=timezone.utc),
            air_temperature=15,
        ),
        BiometData(
            station_id=biomet_station_1.station_id,
            sensor_id='DEC1',
            blg_sensor_id='DEC2',
            measured_at=datetime(2024, 8, 1, 10, 14, tzinfo=timezone.utc),
            air_temperature=16,
        ),
        BiometData(
            station_id=biomet_station_1.station_id,
            sensor_id='DEC1',
            blg_sensor_id='DEC2',
            measured_at=datetime(2024, 8, 2, 10, 19, tzinfo=timezone.utc),
            air_temperature=17,
        ),
        # biomet station 2
        BiometData(
            station_id=biomet_station_2.station_id,
            sensor_id='DEC1',
            blg_sensor_id='DEC2',
            measured_at=datetime(2024, 8, 1, 10, 10, tzinfo=timezone.utc),
            air_temperature=15,
        ),
        BiometData(
            station_id=biomet_station_2.station_id,
            sensor_id='DEC1',
            blg_sensor_id='DEC2',
            measured_at=datetime(2024, 8, 1, 10, 14, tzinfo=timezone.utc),
            air_temperature=16,
        ),
        BiometData(
            station_id=biomet_station_2.station_id,
            sensor_id='DEC1',
            blg_sensor_id='DEC2',
            measured_at=datetime(2024, 8, 2, 10, 19, tzinfo=timezone.utc),
            air_temperature=17,
        ),
    ]
    db.add_all(data)

    await db.commit()
    await BiometDataHourly.refresh()
    await TempRHDataHourly.refresh()

    resp = await app.get(
        '/v1/trends/air_temperature',
        params={
            'spatial_level': 'districts',
            'item_ids': ['District 2', 'District 1'],
            'start_date': datetime(2024, 8, 1, 1, 0).isoformat(),
            'end_date': datetime(2024, 8, 2, 13, 0).isoformat(),
            'hour': 11,
        },
    )
    assert resp.status_code == 200
    assert resp.json()['data'] == {
        'supported_ids': ['District 1', 'District 2'],
        'trends': [
            {'District 1': 13.25, 'measured_at': '2024-08-01T11:00:00Z'},
            {'District 1': 16.0, 'measured_at': '2024-08-02T11:00:00Z'},
            {'District 2': 14.25, 'measured_at': '2024-08-01T11:00:00Z'},
            {'District 2': 17.5, 'measured_at': '2024-08-02T11:00:00Z'},
        ],
        'unit': '°C',
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
            station_id=stations[0].station_id,
            sensor_id='DEC1',
            blg_sensor_id='DEC2',
            measured_at=datetime(2024, 8, 1, 10, 10, tzinfo=timezone.utc),
            maximum_wind_speed=10,
            wind_direction=90,
            utci_category=HeatStressCategories.extreme_heat_stress,
            solar_radiation=5,
        ),
        BiometData(
            station_id=stations[0].station_id,
            sensor_id='DEC1',
            blg_sensor_id='DEC2',
            measured_at=datetime(2024, 8, 1, 10, 14, tzinfo=timezone.utc),
            maximum_wind_speed=5,
            wind_direction=270,
            utci_category=HeatStressCategories.extreme_heat_stress,
            solar_radiation=15,
        ),
        # biomet station 2
        BiometData(
            station_id=stations[1].station_id,
            sensor_id='DEC1',
            blg_sensor_id='DEC2',
            measured_at=datetime(2024, 8, 1, 10, 10, tzinfo=timezone.utc),
            maximum_wind_speed=12,
            wind_direction=90,
            utci_category=HeatStressCategories.extreme_heat_stress,
            solar_radiation=20,
        ),
        BiometData(
            station_id=stations[1].station_id,
            sensor_id='DEC1',
            blg_sensor_id='DEC2',
            measured_at=datetime(2024, 8, 1, 10, 14, tzinfo=timezone.utc),
            maximum_wind_speed=14,
            wind_direction=270,
            utci_category=HeatStressCategories.extreme_heat_stress,
            solar_radiation=40,
        ),
    ]
    db.add_all(data)

    await db.commit()
    await BiometDataHourly.refresh()

    resp = await app.get(
        f'/v1/trends/{param}',
        params={
            'spatial_level': 'districts',
            'item_ids': ['Innenstadt'],
            'start_date': datetime(2024, 8, 1, 1, 0).isoformat(),
            'end_date': datetime(2024, 8, 2, 13, 0).isoformat(),
            'hour': 11,
        },
    )
    assert resp.status_code == 200
    assert resp.json()['data'] == {
        'supported_ids': ['Innenstadt'],
        'trends': [{'Innenstadt': expected, 'measured_at': '2024-08-01T11:00:00Z'}],
        'unit': unit,
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
        station_id='DOT4',
        long_name='test-station-4',
        latitude=51.447,
        longitude=7.268,
        altitude=100,
        station_type=StationType.temprh,
        leuchtennummer=120,
        district='Innenstadt',
        city='Dortmund',
        country='Germany',
        street='test-street',
        plz=12345,
    )
    db.add(temp_rh_station_1)
    temp_rh_station_2 = Station(
        station_id='DOT5',
        long_name='test-station-5',
        latitude=51.447,
        longitude=7.268,
        altitude=100,
        station_type=StationType.temprh,
        leuchtennummer=120,
        district='Innenstadt',
        city='Dortmund',
        country='Germany',
        street='test-street',
        plz=12345,
    )
    db.add(temp_rh_station_2)
    await db.commit()

    data = [
        BiometData(
            station_id=stations[0].station_id,
            sensor_id='DEC1',
            blg_sensor_id='DEC2',
            measured_at=datetime(2024, 8, 1, 10, 10, tzinfo=timezone.utc),
            maximum_wind_speed=10,
            wind_direction=90,
            utci_category=HeatStressCategories.extreme_heat_stress,
            solar_radiation=5,
        ),
        BiometData(
            station_id=stations[0].station_id,
            sensor_id='DEC1',
            blg_sensor_id='DEC2',
            measured_at=datetime(2024, 8, 1, 10, 14, tzinfo=timezone.utc),
            maximum_wind_speed=5,
            wind_direction=270,
            utci_category=HeatStressCategories.extreme_heat_stress,
            solar_radiation=15,
        ),
        # biomet station 2
        BiometData(
            station_id=stations[1].station_id,
            sensor_id='DEC1',
            blg_sensor_id='DEC2',
            measured_at=datetime(2024, 8, 1, 10, 10, tzinfo=timezone.utc),
            maximum_wind_speed=12,
            wind_direction=90,
            utci_category=HeatStressCategories.extreme_heat_stress,
            solar_radiation=20,
        ),
        BiometData(
            station_id=stations[1].station_id,
            sensor_id='DEC1',
            blg_sensor_id='DEC2',
            measured_at=datetime(2024, 8, 1, 10, 14, tzinfo=timezone.utc),
            maximum_wind_speed=14,
            wind_direction=270,
            utci_category=HeatStressCategories.extreme_heat_stress,
            solar_radiation=40,
        ),
        TempRHData(
            station_id=temp_rh_station_1.station_id,
            sensor_id='DEC1',
            measured_at=datetime(2024, 8, 1, 10, 10, tzinfo=timezone.utc),
            air_temperature=10,
        ),
        TempRHData(
            station_id=temp_rh_station_1.station_id,
            sensor_id='DEC1',
            measured_at=datetime(2024, 8, 1, 10, 14, tzinfo=timezone.utc),
            air_temperature=10,
        ),
        TempRHData(
            station_id=temp_rh_station_2.station_id,
            sensor_id='DEC1',
            measured_at=datetime(2024, 8, 1, 10, 10, tzinfo=timezone.utc),
            air_temperature=20,
        ),
        TempRHData(
            station_id=temp_rh_station_2.station_id,
            sensor_id='DEC1',
            measured_at=datetime(2024, 8, 1, 10, 14, tzinfo=timezone.utc),
            air_temperature=20,
        ),
    ]
    db.add_all(data)

    await db.commit()
    await BiometDataHourly.refresh()
    await TempRHDataHourly.refresh()

    resp = await app.get(
        f'/v1/trends/{param}',
        params={
            'spatial_level': 'districts',
            'item_ids': ['Innenstadt'],
            'start_date': datetime(2024, 8, 1, 1, 0).isoformat(),
            'end_date': datetime(2024, 8, 2, 13, 0).isoformat(),
            'hour': 11,
        },
    )
    assert resp.status_code == 200
    assert resp.json()['data'] == {
        'supported_ids': ['Innenstadt'],
        'trends': [{'Innenstadt': expected, 'measured_at': '2024-08-01T11:00:00Z'}],
        'unit': unit,
    }


@pytest.mark.anyio
async def test_get_data_start_greater_end_date(app: AsyncClient) -> None:
    resp = await app.get(
        '/v1/data/DOB1234',
        params={
            'start_date': datetime(2024, 8, 1, 14, 0).isoformat(),
            'end_date': datetime(2024, 8, 1, 13, 0).isoformat(),
            'param': 'air_temperature',
        },
    )
    assert resp.status_code == 422
    assert resp.json() == {'detail': 'start_date must not be greater than end_date'}


@pytest.mark.anyio
@pytest.mark.parametrize(
    ('scale', 'end_date', 'days'),
    (
        pytest.param('max', datetime(2024, 9, 1, 15, 0), 31, id='scale: max'),
        pytest.param('hourly', datetime(2025, 9, 1, 14, 0), 365, id='scale: hourly'),
        pytest.param('daily', datetime(2035, 7, 30, 15, 0), 3650, id='scale: daily'),
    ),
)
async def test_get_data_period_too_long(
        app: AsyncClient,
        scale: str,
        end_date: datetime,
        days: int,
) -> None:
    resp = await app.get(
        '/v1/data/DOB1234',
        params={
            'start_date': datetime(2024, 8, 1, 14, 0).isoformat(),
            'end_date': end_date.isoformat(),
            'param': 'air_temperature',
            'scale': scale,
        },
    )
    assert resp.status_code == 422
    assert resp.json() == {'detail': f'a maximum of {days} days is allowed per request'}


@pytest.mark.anyio
async def test_get_data_station_not_found(app: AsyncClient) -> None:
    resp = await app.get(
        '/v1/data/DOB1234',
        params={
            'start_date': datetime(2024, 8, 1, 14, 0).isoformat(),
            'end_date': datetime(2024, 8, 1, 15, 0).isoformat(),
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
            station_id=stations[0].station_id,
            sensor_id='DEC1',
            blg_sensor_id='DEC2',
            measured_at=datetime(2024, 8, 1, 10, minute, tzinfo=timezone.utc),
            air_temperature=minute/2,
            mrt=minute*2,
        ) for minute in range(0, 40, 10)
    ]
    db.add_all(data)

    await db.commit()

    resp = await app.get(
        '/v1/data/DOB1',
        params={
            'start_date': datetime(2024, 8, 1, 10, 0).isoformat(),
            'end_date': datetime(2024, 8, 1, 11, 0).isoformat(),
            'param': ['air_temperature', 'mrt'],
        },
    )
    assert resp.status_code == 200
    assert resp.json()['data'] == [
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
    ]


@pytest.mark.anyio
@pytest.mark.usefixtures('clean_db')
async def test_get_temp_rh_data_multiple_params(
        app: AsyncClient,
        db: AsyncSession,
) -> None:
    # generate some data for the station
    station = Station(
        station_id='DOT1',
        long_name='test-station-1',
        latitude=51.447,
        longitude=7.268,
        altitude=100,
        station_type=StationType.temprh,
        leuchtennummer=120,
        district='Innenstadt',
        city='Dortmund',
        country='Germany',
        street='test-street',
        plz=12345,
    )
    db.add(station)
    sht_sensor = Sensor(
        sensor_id='DEC3',
        device_id=33333,
        sensor_type=SensorType.sht35,
    )
    db.add(sht_sensor)
    await db.commit()
    data = [
        TempRHData(
            station_id=station.station_id,
            sensor_id='DEC3',
            measured_at=datetime(2024, 8, 1, 10, minute, tzinfo=timezone.utc),
            air_temperature=minute/2,
            relative_humidity=minute*2,
        ) for minute in range(0, 40, 10)
    ]
    db.add_all(data)

    await db.commit()

    resp = await app.get(
        '/v1/data/DOT1',
        params={
            'start_date': datetime(2024, 8, 1, 10, 0).isoformat(),
            'end_date': datetime(2024, 8, 1, 11, 0).isoformat(),
            'param': ['air_temperature', 'relative_humidity'],
        },
    )
    assert resp.status_code == 200
    assert resp.json()['data'] == [
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
    ]


@pytest.mark.anyio
@pytest.mark.usefixtures('clean_db')
async def test_get_data_from_double_station_multiple_params(
        app: AsyncClient,
        db: AsyncSession,
) -> None:
    # generate some data for the station
    station = Station(
        station_id='DOD1',
        long_name='double-station-1',
        latitude=51.447,
        longitude=7.268,
        altitude=100,
        station_type=StationType.double,
        leuchtennummer=120,
        district='Innenstadt',
        city='Dortmund',
        country='Germany',
        street='test-street',
        plz=12345,
    )
    db.add(station)
    sensors = [
        Sensor(
            sensor_id='DEC1',
            device_id=11111,
            sensor_type=SensorType.atm41,
        ),
        Sensor(
            sensor_id='DEC2',
            device_id=222222,
            sensor_type=SensorType.blg,
        ),
        Sensor(
            sensor_id='DEC3',
            device_id=33333,
            sensor_type=SensorType.sht35,
        ),
    ]
    db.add_all(sensors)
    # deploy the sensors??
    await db.commit()
    biomet_data = [
        BiometData(
            station_id=station.station_id,
            sensor_id='DEC1',
            blg_sensor_id='DEC2',
            measured_at=datetime(2024, 8, 1, 10, minute, tzinfo=timezone.utc),
            air_temperature=minute/2,
            relative_humidity=minute*2,
        ) for minute in range(0, 40, 10)
    ]
    db.add_all(biomet_data)

    await db.commit()

    resp = await app.get(
        '/v1/data/DOD1',
        params={
            'start_date': datetime(2024, 8, 1, 10, 0).isoformat(),
            'end_date': datetime(2024, 8, 1, 11, 0).isoformat(),
            'param': ['air_temperature', 'relative_humidity'],
        },
    )
    assert resp.status_code == 200
    assert resp.json()['data'] == [
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
    ]


@pytest.mark.anyio
@pytest.mark.usefixtures('clean_db')
@pytest.mark.parametrize('stations', [1], indirect=True)
async def test_get_data_biomet_hourly_null_values_are_filled(
        app: AsyncClient,
        db: AsyncSession,
        stations: list[Station],
) -> None:
    # create data for two stations
    data = [
        BiometData(
            station_id=stations[0].station_id,
            sensor_id='DEC1',
            blg_sensor_id='DEC2',
            measured_at=datetime(2024, 8, 1, 10, 10, tzinfo=timezone.utc),
            maximum_wind_speed=12.0,
            relative_humidity=50.5,
        ),
        BiometData(
            station_id=stations[0].station_id,
            sensor_id='DEC1',
            blg_sensor_id='DEC2',
            measured_at=datetime(2024, 8, 1, 12, 10, tzinfo=timezone.utc),
            maximum_wind_speed=6.0,
            relative_humidity=60.5,
        ),
    ]
    db.add_all(data)

    await db.commit()
    await BiometDataHourly.refresh()

    resp = await app.get(
        '/v1/data/DOB1',
        params={
            'start_date': datetime(2024, 8, 1, 8).isoformat(),
            'end_date': datetime(2024, 8, 1, 14).isoformat(),
            'param': ['maximum_wind_speed', 'relative_humidity'],
            'scale': 'hourly',
        },
    )
    assert resp.status_code == 200
    assert resp.json()['data'] == [
        {
            'measured_at': '2024-08-01T11:00:00Z',
            'maximum_wind_speed': 12,
            'relative_humidity': 50.5,
        },
        {
            'measured_at': '2024-08-01T12:00:00Z',
            'maximum_wind_speed': None,
            'relative_humidity': None,
        },
        {
            'measured_at': '2024-08-01T13:00:00Z',
            'maximum_wind_speed': 6,
            'relative_humidity': 60.5,
        },
    ]


@pytest.mark.anyio
@pytest.mark.usefixtures('clean_db')
@pytest.mark.parametrize('stations', [1], indirect=True)
async def test_get_data_biomet_hourly_no_gap_filling(
        app: AsyncClient,
        db: AsyncSession,
        stations: list[Station],
) -> None:
    # create data for two stations
    data = [
        BiometData(
            station_id=stations[0].station_id,
            sensor_id='DEC1',
            blg_sensor_id='DEC2',
            measured_at=datetime(2024, 8, 1, 10, 10, tzinfo=timezone.utc),
            maximum_wind_speed=12.0,
            relative_humidity=50.5,
        ),
        BiometData(
            station_id=stations[0].station_id,
            sensor_id='DEC1',
            blg_sensor_id='DEC2',
            measured_at=datetime(2024, 8, 1, 12, 10, tzinfo=timezone.utc),
            maximum_wind_speed=6.0,
            relative_humidity=60.5,
        ),
    ]
    db.add_all(data)

    await db.commit()
    await BiometDataHourly.refresh()

    resp = await app.get(
        '/v1/data/DOB1',
        params={
            'start_date': datetime(2024, 8, 1, 8).isoformat(),
            'end_date': datetime(2024, 8, 1, 14).isoformat(),
            'param': ['maximum_wind_speed', 'relative_humidity'],
            'scale': 'hourly',
            'fill_gaps': False,
        },
    )
    assert resp.status_code == 200
    assert resp.json()['data'] == [
        {
            'measured_at': '2024-08-01T11:00:00Z',
            'maximum_wind_speed': 12,
            'relative_humidity': 50.5,
        },
        {
            'measured_at': '2024-08-01T13:00:00Z',
            'maximum_wind_speed': 6,
            'relative_humidity': 60.5,
        },
    ]


@pytest.mark.anyio
@pytest.mark.usefixtures('clean_db')
async def test_get_data_temprh_hourly_null_values_are_filled(
        app: AsyncClient,
        db: AsyncSession,
) -> None:
    station = Station(
        station_id='DOB1',
        long_name='test-station-1',
        latitude=51.447,
        longitude=7.268,
        altitude=100,
        station_type=StationType.temprh,
        leuchtennummer=120,
        district='Innenstadt',
        city='Dortmund',
        country='Germany',
        street='test-street',
        plz=12345,
    )
    db.add(station)
    sht_sensor = Sensor(
        sensor_id='DEC3',
        device_id=33333,
        sensor_type=SensorType.sht35,
    )
    db.add(sht_sensor)
    await db.commit()
    # create data for two stations
    data = [
        TempRHData(
            station_id=station.station_id,
            sensor_id='DEC3',
            measured_at=datetime(2024, 8, 1, 10, 10, tzinfo=timezone.utc),
            air_temperature=12.0,
            relative_humidity=50.5,
        ),
        TempRHData(
            station_id=station.station_id,
            sensor_id='DEC3',
            measured_at=datetime(2024, 8, 1, 12, 10, tzinfo=timezone.utc),
            air_temperature=6.0,
            relative_humidity=60.5,
        ),
    ]
    db.add_all(data)

    await db.commit()
    await TempRHDataHourly.refresh()

    resp = await app.get(
        '/v1/data/DOB1',
        params={
            'start_date': datetime(2024, 8, 1, 8).isoformat(),
            'end_date': datetime(2024, 8, 1, 14).isoformat(),
            'param': ['air_temperature', 'relative_humidity'],
            'scale': 'hourly',
        },
    )
    assert resp.status_code == 200
    assert resp.json()['data'] == [
        {
            'measured_at': '2024-08-01T11:00:00Z',
            'air_temperature': 12,
            'relative_humidity': 50.5,
        },
        {
            'measured_at': '2024-08-01T12:00:00Z',
            'air_temperature': None,
            'relative_humidity': None,
        },
        {
            'measured_at': '2024-08-01T13:00:00Z',
            'air_temperature': 6,
            'relative_humidity': 60.5,
        },
    ]


@pytest.mark.anyio
@pytest.mark.usefixtures('clean_db')
@pytest.mark.parametrize('stations', [1], indirect=True)
async def test_get_data_biomet_daily_null_values_are_filled(
        app: AsyncClient,
        db: AsyncSession,
        stations: list[Station],
) -> None:
    # create data for two stations
    # we need to create a lot of data so it is filled
    # to exceed the threshold, we need to insert enough values
    data = []
    for minutes in range(0, 23*60, 5):
        step = timedelta(minutes=minutes)
        tmp_data = [
            BiometData(
                measured_at=datetime(2024, 8, 1, 0, tzinfo=timezone.utc) + step,
                sensor_id='DEC1',
                blg_sensor_id='DEC2',
                station_id=stations[0].station_id,
                maximum_wind_speed=12,
                relative_humidity=50.5,
            ),
            # two days are missing inbetween
            BiometData(
                measured_at=datetime(2024, 8, 3, 0, tzinfo=timezone.utc) + step,
                station_id=stations[0].station_id,
                sensor_id='DEC1',
                blg_sensor_id='DEC2',
                maximum_wind_speed=6,
                relative_humidity=60.5,
            ),
        ]
        data.extend(tmp_data)

    db.add_all(data)

    await db.commit()
    await BiometDataDaily.refresh()

    resp = await app.get(
        '/v1/data/DOB1',
        params={
            'start_date': datetime(2024, 8, 1).isoformat(),
            'end_date': datetime(2024, 8, 4).isoformat(),
            'param': ['maximum_wind_speed', 'relative_humidity'],
            'scale': 'daily',
        },
    )
    assert resp.status_code == 200
    assert resp.json()['data'] == [
        {
            'measured_at': '2024-08-01T00:00:00Z',
            'maximum_wind_speed': 12,
            'relative_humidity': 50.5,
        },
        {
            'measured_at': '2024-08-02T00:00:00Z',
            'maximum_wind_speed': None,
            'relative_humidity': None,
        },
        {
            'measured_at': '2024-08-03T00:00:00Z',
            'maximum_wind_speed': 6,
            'relative_humidity': 60.5,
        },
    ]


@pytest.mark.anyio
@pytest.mark.usefixtures('clean_db')
async def test_get_data_temprh_daily_null_values_are_filled(
        app: AsyncClient,
        db: AsyncSession,
) -> None:
    station = Station(
        station_id='DOB1',
        long_name='test-station-1',
        latitude=51.447,
        longitude=7.268,
        altitude=100,
        station_type=StationType.temprh,
        leuchtennummer=120,
        district='Innenstadt',
        city='Dortmund',
        country='Germany',
        street='test-street',
        plz=12345,
    )
    db.add(station)
    sht_sensor = Sensor(
        sensor_id='DEC3',
        device_id=33333,
        sensor_type=SensorType.sht35,
    )
    db.add(sht_sensor)
    await db.commit()

    # to exceed the threshold, we need to insert enough values
    data = []
    for minutes in range(0, 23*60, 5):
        step = timedelta(minutes=minutes)
        tmp_data = [
            TempRHData(
                measured_at=datetime(2024, 8, 1, 0, tzinfo=timezone.utc) + step,
                station_id=station.station_id,
                sensor_id='DEC3',
                air_temperature=12,
                relative_humidity=50.5,
            ),
            # two days are missing inbetween
            TempRHData(
                measured_at=datetime(2024, 8, 3, 0, tzinfo=timezone.utc) + step,
                sensor_id='DEC3',
                station_id=station.station_id,
                air_temperature=6,
                relative_humidity=60.5,
            ),
        ]
        data.extend(tmp_data)

    db.add_all(data)

    await db.commit()
    await TempRHDataDaily.refresh()

    resp = await app.get(
        '/v1/data/DOB1',
        params={
            'start_date': datetime(2024, 8, 1).isoformat(),
            'end_date': datetime(2024, 8, 4).isoformat(),
            'param': ['air_temperature', 'relative_humidity'],
            'scale': 'daily',
        },
    )
    assert resp.status_code == 200
    assert resp.json()['data'] == [
        {
            'measured_at': '2024-08-01T00:00:00Z',
            'air_temperature': 12,
            'relative_humidity': 50.5,
        },
        {
            'measured_at': '2024-08-02T00:00:00Z',
            'air_temperature': None,
            'relative_humidity': None,
        },
        {
            'measured_at': '2024-08-03T00:00:00Z',
            'air_temperature': 6,
            'relative_humidity': 60.5,
        },
    ]


@pytest.mark.anyio
@pytest.mark.usefixtures('clean_db')
async def test_get_data_biomet_daily_no_gap_filling(
        app: AsyncClient,
        db: AsyncSession,
) -> None:
    station = Station(
        station_id='DOB1',
        long_name='test-station-1',
        latitude=51.447,
        longitude=7.268,
        altitude=100,
        station_type=StationType.temprh,
        leuchtennummer=120,
        district='Innenstadt',
        city='Dortmund',
        country='Germany',
        street='test-street',
        plz=12345,
    )
    db.add(station)
    sht_sensor = Sensor(
        sensor_id='DEC3',
        device_id=33333,
        sensor_type=SensorType.sht35,
    )
    db.add(sht_sensor)
    await db.commit()

    # to exceed the threshold, we need to insert enough values
    data = []
    for minutes in range(0, 23*60, 5):
        step = timedelta(minutes=minutes)
        tmp_data = [
            TempRHData(
                measured_at=datetime(2024, 8, 1, 0, tzinfo=timezone.utc) + step,
                station_id=station.station_id,
                sensor_id='DEC3',
                air_temperature=12,
                relative_humidity=50.5,
            ),
            # two days are missing inbetween
            TempRHData(
                measured_at=datetime(2024, 8, 3, 0, tzinfo=timezone.utc) + step,
                station_id=station.station_id,
                sensor_id='DEC3',
                air_temperature=6,
                relative_humidity=60.5,
            ),
        ]
        data.extend(tmp_data)

    db.add_all(data)

    await db.commit()
    await TempRHDataDaily.refresh()

    resp = await app.get(
        '/v1/data/DOB1',
        params={
            'start_date': datetime(2024, 8, 1).isoformat(),
            'end_date': datetime(2024, 8, 4).isoformat(),
            'param': ['air_temperature', 'relative_humidity'],
            'scale': 'daily',
            'fill_gaps': False,
        },
    )
    assert resp.status_code == 200
    assert resp.json()['data'] == [
        {
            'measured_at': '2024-08-01T00:00:00Z',
            'air_temperature': 12,
            'relative_humidity': 50.5,
        },
        {
            'measured_at': '2024-08-03T00:00:00Z',
            'air_temperature': 6,
            'relative_humidity': 60.5,
        },
    ]


@pytest.mark.anyio
@pytest.mark.usefixtures('clean_db')
async def test_get_temp_rh_data_daily_multiple_params(
        app: AsyncClient,
        db: AsyncSession,
) -> None:
    # generate some data for the station
    station = Station(
        station_id='DOT1',
        long_name='test-station-1',
        latitude=51.447,
        longitude=7.268,
        altitude=100,
        station_type=StationType.temprh,
        leuchtennummer=120,
        district='Innenstadt',
        city='Dortmund',
        country='Germany',
        street='test-street',
        plz=12345,
    )
    db.add(station)
    sht_sensor = Sensor(
        sensor_id='DEC3',
        device_id=33333,
        sensor_type=SensorType.sht35,
    )
    db.add(sht_sensor)
    await db.commit()
    data = [
        TempRHData(
            station_id=station.station_id,
            measured_at=datetime(2024, 8, 1, 10, minute, tzinfo=timezone.utc),
            sensor_id='DEC3',
            air_temperature=minute/2,
            relative_humidity=minute*2,
        ) for minute in range(0, 40, 10)
    ]
    db.add_all(data)

    await db.commit()
    await TempRHDataDaily.refresh()
    await db.commit()

    resp = await app.get(
        '/v1/data/DOT1',
        params={
            'start_date': datetime(2024, 8, 1, 0, 0).isoformat(),
            'end_date': datetime(2024, 8, 1, 0, 0).isoformat(),
            'param': ['air_temperature', 'relative_humidity'],
            'scale': 'daily',
        },
    )
    assert resp.status_code == 200
    assert resp.json()['data'] == [
        {
            'air_temperature': None,
            'measured_at': '2024-08-01T00:00:00Z',
            'relative_humidity': None,
        },
    ]


@pytest.mark.anyio
@pytest.mark.usefixtures('clean_db')
async def test_get_temp_rh_data_scale_hourly_multiple_params(
        app: AsyncClient,
        db: AsyncSession,
) -> None:
    # generate some data for the station
    station = Station(
        station_id='DOT1',
        long_name='test-station-1',
        latitude=51.447,
        longitude=7.268,
        altitude=100,
        station_type=StationType.temprh,
        leuchtennummer=120,
        district='Innenstadt',
        city='Dortmund',
        country='Germany',
        street='test-street',
        plz=12345,
    )
    db.add(station)
    sht_sensor = Sensor(
        sensor_id='DEC3',
        device_id=33333,
        sensor_type=SensorType.sht35,
    )
    db.add(sht_sensor)
    await db.commit()
    data = [
        TempRHData(
            station_id=station.station_id,
            sensor_id='DEC3',
            measured_at=datetime(2024, 8, 1, 10, minute, tzinfo=timezone.utc),
            air_temperature=minute/2,
            relative_humidity=minute*2,
        ) for minute in range(0, 40, 10)
    ]
    db.add_all(data)

    await db.commit()
    await TempRHDataHourly.refresh()
    resp = await app.get(
        '/v1/data/DOT1',
        params={
            'start_date': datetime(2024, 8, 1, 10, 0).isoformat(),
            'end_date': datetime(2024, 8, 1, 11, 0).isoformat(),
            'param': ['air_temperature', 'relative_humidity'],
            'scale': 'hourly',
        },
    )
    assert resp.status_code == 200
    assert resp.json()['data'] == [
        {
            'air_temperature': 7.5,
            'measured_at': '2024-08-01T11:00:00Z',
            'relative_humidity': 30.0,
        },
    ]


@pytest.mark.anyio
@pytest.mark.usefixtures('clean_db')
async def test_get_temp_rh_data_scale_max_param_not_found(
        app: AsyncClient,
        db: AsyncSession,
) -> None:
    # generate some data for the station
    station = Station(
        station_id='DOT1',
        long_name='test-station-1',
        latitude=51.447,
        longitude=7.268,
        altitude=100,
        station_type=StationType.temprh,
        leuchtennummer=120,
        district='Innenstadt',
        city='Dortmund',
        country='Germany',
        street='test-street',
        plz=12345,
    )
    db.add(station)
    await db.commit()
    resp = await app.get(
        '/v1/data/DOT1',
        params={
            'start_date': datetime(2024, 8, 1, 10, 0).isoformat(),
            'end_date': datetime(2024, 8, 1, 11, 0).isoformat(),
            # this is a valid param, however not available with scale max
            'param': ['air_temperature', 'relative_humidity_max'],
            'scale': 'max',
        },
    )
    assert resp.status_code == 422
    assert resp.json() == {
        'detail': [
            {
                'ctx': {
                    'expected': (
                        'absolute_humidity, air_temperature, dew_point, heat_index, '
                        'relative_humidity, specific_humidity, wet_bulb_temperature'
                    ),
                },
                'input': 'relative_humidity_max',
                'loc': ['query', 'param', 1],
                'msg': (
                    'This station is of type "temprh", hence the input should be: '
                    'absolute_humidity, air_temperature, dew_point, heat_index, '
                    'relative_humidity, specific_humidity, wet_bulb_temperature'
                ),
                'type': 'enum',
            },
        ],
    }


@pytest.mark.anyio
async def test_robots_txt(app: AsyncClient) -> None:
    resp = await app.get('/robots.txt')
    assert resp.text == '''\
User-agent: *
Disallow: /
'''


@pytest.mark.anyio
async def test_index_redirects_to_docs(app: AsyncClient) -> None:
    resp = await app.get('/')
    assert resp.status_code == 301
    assert resp.headers['location'] == '/docs'


@pytest.mark.anyio
@pytest.mark.usefixtures('clean_db')
@pytest.mark.parametrize('stations', [2], indirect=True)
async def test_get_network_values_hourly(
        app: AsyncClient,
        db: AsyncSession,
        stations: list[Station],
) -> None:
    # create some temp_rh stations
    temp_rh_stations: list[Station] = []
    for i in range(2):
        station = Station(
            station_id=f'DOT-temprh-{i}',
            long_name=f'DOT-temprh-{i}',
            latitude=51.447,
            longitude=7.268,
            altitude=100,
            station_type=StationType.temprh,
            leuchtennummer=120,
            district='Other District',
            city='Dortmund',
            country='Germany',
            street='test-street',
            plz=12345,
        )
        db.add(station)
        temp_rh_stations.append(station)
    await db.commit()

    start_date = datetime(2024, 1, 1, 11, 55, tzinfo=timezone.utc)
    step = timedelta(minutes=5)
    for biomet_station, temp_rh_station in zip(stations, temp_rh_stations, strict=True):
        for value in range(14):
            # insert some values for biomet
            biomet_data = BiometData(
                measured_at=start_date + (step * value),
                station_id=biomet_station.station_id,
                sensor_id='DEC1',
                blg_sensor_id='DEC2',
                air_temperature=value,
                wind_speed=value / 2,
            )
            db.add(biomet_data)
            # insert some values for temprh
            temp_rh_data = TempRHData(
                measured_at=start_date + (step * value),
                sensor_id='DEC1',
                station_id=temp_rh_station.station_id,
                air_temperature=value,
            )
            db.add(temp_rh_data)

    await db.commit()
    await TempRHDataHourly.refresh()
    await BiometDataHourly.refresh()
    resp = await app.get(
        '/v1/network-snapshot',
        params={
            'param': ['air_temperature', 'wind_speed'],
            'scale': 'hourly',
            'date': datetime(2024, 1, 1, 13).isoformat(),
            'suggest_viz': True,
        },
    )
    assert resp.status_code == 200
    json_resp = resp.json()
    assert json_resp['data'] == [
        {
            'air_temperature': 6.5,
            'measured_at': '2024-01-01T13:00:00Z',
            'station_id': 'DOB1',
            'station_type': 'biomet',
            'wind_speed': 3.25,
        },
        {
            'air_temperature': 6.5,
            'measured_at': '2024-01-01T13:00:00Z',
            'station_id': 'DOB2',
            'station_type': 'biomet',
            'wind_speed': 3.25,
        },
        {
            'air_temperature': 6.5,
            'measured_at': '2024-01-01T13:00:00Z',
            'station_id': 'DOT-temprh-0',
            'station_type': 'temprh',
            # temprh supports no windspeed
            'wind_speed': None,
        },
        {
            'air_temperature': 6.5,
            'measured_at': '2024-01-01T13:00:00Z',
            'station_id': 'DOT-temprh-1',
            'station_type': 'temprh',
            'wind_speed': None,
        },
    ]
    # check visualization is suggested correctly
    assert json_resp['visualization'] == {
        'air_temperature': {'cmax': 9.5, 'cmin': 3.5, 'vmin': 6.5, 'vmax': 6.5},
        'wind_speed': {'cmax': 3.85, 'cmin': 2.65, 'vmin': 3.25, 'vmax': 3.25},
    }


@pytest.mark.anyio
@pytest.mark.usefixtures('clean_db')
@pytest.mark.parametrize('stations', [2], indirect=True)
async def test_get_network_values_hourly_missing_values_are_null(
        app: AsyncClient,
        db: AsyncSession,
        stations: list[Station],
) -> None:
    # create some temp_rh stations
    temp_rh_stations: list[Station] = []
    for i in range(2):
        station = Station(
            station_id=f'DOT-temprh-{i}',
            long_name=f'DOT-temprh-{i}',
            latitude=51.447,
            longitude=7.268,
            altitude=100,
            station_type=StationType.temprh,
            leuchtennummer=120,
            district='Other District',
            city='Dortmund',
            country='Germany',
            street='test-street',
            plz=12345,
        )
        db.add(station)
        temp_rh_stations.append(station)
    await db.commit()

    start_date = datetime(2024, 1, 1, 11, 30, tzinfo=timezone.utc)
    step = timedelta(hours=1)
    for biomet_station, temp_rh_station in zip(stations, temp_rh_stations, strict=True):
        # skip every other value
        for value in range(1, 5, 2):
            # insert some values for biomet
            biomet_data = BiometData(
                measured_at=start_date + (step * value),
                sensor_id='DEC1',
                blg_sensor_id='DEC2',
                station_id=biomet_station.station_id,
                air_temperature=value,
                wind_speed=value / 2,
            )
            db.add(biomet_data)
            # insert some values for temprh
            # make this offset compared to the biomet data!
            temp_rh_data = TempRHData(
                measured_at=start_date + (step * (value - 1)),
                sensor_id='DEC1',
                station_id=temp_rh_station.station_id,
                air_temperature=value,
            )
            db.add(temp_rh_data)

    await db.commit()
    await TempRHDataHourly.refresh()
    await BiometDataHourly.refresh()
    # here the temprh stations should show null values, but the biomet
    # stations should have actual values
    resp = await app.get(
        '/v1/network-snapshot',
        params={
            'param': ['air_temperature', 'wind_speed'],
            'scale': 'hourly',
            'date': datetime(2024, 1, 1, 13).isoformat(),
        },
    )
    assert resp.status_code == 200
    assert resp.json()['data'] == [
        {
            'air_temperature': 1.0,
            'measured_at': '2024-01-01T13:00:00Z',
            'station_id': 'DOB1',
            'station_type': 'biomet',
            'wind_speed': 0.5,
        },
        {
            'air_temperature': 1.0,
            'measured_at': '2024-01-01T13:00:00Z',
            'station_id': 'DOB2',
            'station_type': 'biomet',
            'wind_speed': 0.5,
        },
        {
            'air_temperature': None,
            'measured_at': '2024-01-01T13:00:00Z',
            'station_id': 'DOT-temprh-0',
            'station_type': 'temprh',
            # temprh supports no windspeed
            'wind_speed': None,
        },
        {
            'air_temperature': None,
            'measured_at': '2024-01-01T13:00:00Z',
            'station_id': 'DOT-temprh-1',
            'station_type': 'temprh',
            'wind_speed': None,
        },
    ]
    # here the biomet stations should show null values, but the temprh
    # stations should have actual values
    resp = await app.get(
        '/v1/network-snapshot',
        params={
            'param': ['air_temperature', 'wind_speed'],
            'scale': 'hourly',
            'date': datetime(2024, 1, 1, 14).isoformat(),
        },
    )
    assert resp.status_code == 200
    assert resp.json()['data'] == [
        {
            'air_temperature': None,
            'measured_at': '2024-01-01T14:00:00Z',
            'station_id': 'DOB1',
            'station_type': 'biomet',
            'wind_speed': None,
        },
        {
            'air_temperature': None,
            'measured_at': '2024-01-01T14:00:00Z',
            'station_id': 'DOB2',
            'station_type': 'biomet',
            'wind_speed': None,
        },
        {
            'air_temperature': 3.0,
            'measured_at': '2024-01-01T14:00:00Z',
            'station_id': 'DOT-temprh-0',
            'station_type': 'temprh',
            # temprh supports no windspeed
            'wind_speed': None,
        },
        {
            'air_temperature': 3.0,
            'measured_at': '2024-01-01T14:00:00Z',
            'station_id': 'DOT-temprh-1',
            'station_type': 'temprh',
            'wind_speed': None,
        },
    ]


@pytest.mark.anyio
@pytest.mark.usefixtures('clean_db')
@pytest.mark.parametrize('stations', [2], indirect=True)
async def test_get_network_values_daily(
        app: AsyncClient,
        db: AsyncSession,
        stations: list[Station],
) -> None:
    # create some temp_rh stations
    temp_rh_stations = []
    for i in range(2):
        station = Station(
            station_id=f'DOT-temprh-{i}',
            long_name=f'DOT-temprh-{i}',
            latitude=51.447,
            longitude=7.268,
            altitude=100,
            station_type=StationType.temprh,
            leuchtennummer=120,
            district='Other District',
            city='Dortmund',
            country='Germany',
            street='test-street',
            plz=12345,
        )
        db.add(station)
        temp_rh_stations.append(station)
    await db.commit()

    start_date = datetime(2024, 1, 1, 22, 0, tzinfo=timezone.utc)
    step = timedelta(minutes=5)
    for biomet_station, temp_rh_station in zip(stations, temp_rh_stations, strict=True):
        # compile one day and two hours of values
        for value in range((12 * 24) + (12 * 2)):
            # insert some values for biomet
            biomet_data = BiometData(
                measured_at=start_date + (step * value),
                sensor_id='DEC1',
                blg_sensor_id='DEC2',
                station_id=biomet_station.station_id,
                air_temperature=value,
                wind_speed=value / 2,
            )
            db.add(biomet_data)
            # insert some values for temprh
            temp_rh_data = TempRHData(
                measured_at=start_date + (step * value),
                sensor_id='DEC1',
                station_id=temp_rh_station.station_id,
                air_temperature=value,
            )
            db.add(temp_rh_data)

    await db.commit()
    await TempRHDataDaily.refresh()
    await BiometDataDaily.refresh()

    resp = await app.get(
        '/v1/network-snapshot',
        params={
            'param': ['air_temperature', 'wind_speed'],
            'scale': 'daily',
            'date': datetime(2024, 1, 2, 0, 0).isoformat(),
        },
    )
    assert resp.status_code == 200
    # no visualization is requested, hence this must not be set
    assert resp.json()['visualization'] is None
    assert resp.json()['data'] == [
        {
            'air_temperature': 155.5,
            'measured_at': '2024-01-02T00:00:00',
            'station_id': 'DOB1',
            'station_type': 'biomet',
            'wind_speed': 77.75,
        },
        {
            'air_temperature': 155.5,
            'measured_at': '2024-01-02T00:00:00',
            'station_id': 'DOB2',
            'station_type': 'biomet',
            'wind_speed': 77.75,
        },
        {
            'air_temperature': 155.5,
            'measured_at': '2024-01-02T00:00:00',
            'station_id': 'DOT-temprh-0',
            'station_type': 'temprh',
            'wind_speed': None,
        },
        {
            'air_temperature': 155.5,
            'measured_at': '2024-01-02T00:00:00',
            'station_id': 'DOT-temprh-1',
            'station_type': 'temprh',
            'wind_speed': None,
        },
    ]


@pytest.mark.anyio
@pytest.mark.usefixtures('clean_db')
@pytest.mark.parametrize('stations', [1], indirect=True)
async def test_get_network_values_daily_missing_values_are_null(
        app: AsyncClient,
        db: AsyncSession,
        stations: list[Station],
) -> None:
    temp_rh_station = Station(
        station_id='DOT-temprh-1',
        long_name='DOT-temprh-1',
        latitude=51.447,
        longitude=7.268,
        altitude=100,
        station_type=StationType.temprh,
        leuchtennummer=120,
        district='Other District',
        city='Dortmund',
        country='Germany',
        street='test-street',
        plz=12345,
    )
    db.add(temp_rh_station)
    await db.commit()

    # to exceed the threshold, we need to insert enough values
    data = []
    for minutes in range(0, 23*60, 5):
        step = timedelta(minutes=minutes)
        tmp_data = [
            BiometData(
                measured_at=datetime(2024, 1, 1, 0, tzinfo=timezone.utc) + step,
                sensor_id='DEC1',
                blg_sensor_id='DEC2',
                station_id=stations[0].station_id,
                air_temperature=10.5,
                wind_speed=3.5,
            ),
            # 1 day missing inbetween
            BiometData(
                measured_at=datetime(2024, 1, 3, 0, tzinfo=timezone.utc) + step,
                sensor_id='DEC1',
                blg_sensor_id='DEC2',
                station_id=stations[0].station_id,
                air_temperature=12.0,
                wind_speed=1.5,
            ),
            TempRHData(
                measured_at=datetime(2024, 1, 1, 0, tzinfo=timezone.utc) + step,
                sensor_id='DEC1',
                station_id=temp_rh_station.station_id,
                air_temperature=11.4,
            ),
            # two days missing inbetween
            TempRHData(
                measured_at=datetime(2024, 1, 4, 0, tzinfo=timezone.utc) + step,
                station_id=temp_rh_station.station_id,
                sensor_id='DEC1',
                air_temperature=9.5,
            ),
        ]
        data.extend(tmp_data)

    db.add_all(data)

    await db.commit()
    await TempRHDataDaily.refresh()
    await BiometDataDaily.refresh()

    # here both stations should have data!
    resp = await app.get(
        '/v1/network-snapshot',
        params={
            'param': ['air_temperature', 'wind_speed'],
            'scale': 'daily',
            'date': datetime(2024, 1, 1, 0, 0).isoformat(),
        },
    )
    assert resp.status_code == 200
    assert resp.json()['data'] == [
        {
            'measured_at': '2024-01-01T00:00:00',
            'air_temperature': 10.5,
            'station_id': 'DOB1',
            'station_type': 'biomet',
            'wind_speed': 3.5,
        },
        {
            'measured_at': '2024-01-01T00:00:00',
            'air_temperature': 11.4,
            'station_id': 'DOT-temprh-1',
            'station_type': 'temprh',
            'wind_speed': None,
        },
    ]
    # here only the biomet should have data, but the other is filled in with NULL
    resp = await app.get(
        '/v1/network-snapshot',
        params={
            'param': ['air_temperature', 'wind_speed'],
            'scale': 'daily',
            'date': datetime(2024, 1, 3, 0, 0).isoformat(),
        },
    )
    assert resp.status_code == 200
    assert resp.json()['data'] == [
        {
            'measured_at': '2024-01-03T00:00:00',
            'air_temperature': 12.0,
            'station_id': 'DOB1',
            'station_type': 'biomet',
            'wind_speed': 1.5,
        },
        {
            'measured_at': '2024-01-03T00:00:00',
            'air_temperature': None,
            'station_id': 'DOT-temprh-1',
            'station_type': 'temprh',
            'wind_speed': None,
        },
    ]
    # here only the temprh should have data since it was after the last measurement
    resp = await app.get(
        '/v1/network-snapshot',
        params={
            'param': ['air_temperature', 'wind_speed'],
            'scale': 'daily',
            'date': datetime(2024, 1, 4, 0, 0).isoformat(),
        },
    )
    assert resp.status_code == 200
    assert resp.json()['data'] == [
        {
            'measured_at': '2024-01-04T00:00:00',
            'air_temperature': 9.5,
            'station_id': 'DOT-temprh-1',
            'station_type': 'temprh',
            'wind_speed': None,
        },
    ]


@pytest.mark.anyio
@pytest.mark.usefixtures('clean_db')
@pytest.mark.parametrize('stations', [2], indirect=True)
async def test_get_network_values_daily_temprh_supports_no_param(
        app: AsyncClient,
        db: AsyncSession,
        stations: list[Station],
) -> None:
    # create some temp_rh stations
    temp_rh_stations: list[Station] = []
    for i in range(2):
        station = Station(
            station_id=f'DOT-temprh-{i}',
            long_name=f'DOT-temprh-{i}',
            latitude=51.447,
            longitude=7.268,
            altitude=100,
            station_type=StationType.temprh,
            leuchtennummer=120,
            district='Other District',
            city='Dortmund',
            country='Germany',
            street='test-street',
            plz=12345,
        )
        db.add(station)
        temp_rh_stations.append(station)
    await db.commit()

    start_date = datetime(2024, 1, 1, 22, 0, tzinfo=timezone.utc)
    step = timedelta(minutes=5)
    for biomet_station, temp_rh_station in zip(stations, temp_rh_stations, strict=True):
        # compile one day and two hours of values
        for value in range((12 * 24) + (12 * 2)):
            # insert some values for biomet
            biomet_data = BiometData(
                measured_at=start_date + (step * value),
                sensor_id='DEC1',
                blg_sensor_id='DEC2',
                station_id=biomet_station.station_id,
                utci=value * 2,
                wind_speed=value / 2,
            )
            db.add(biomet_data)
            # insert some values for temprh (event though we don't request them)
            temp_rh_data = TempRHData(
                measured_at=start_date + (step * value),
                sensor_id='DEC1',
                station_id=temp_rh_station.station_id,
            )
            db.add(temp_rh_data)

    await db.commit()
    await TempRHDataDaily.refresh()
    await BiometDataDaily.refresh()

    resp = await app.get(
        '/v1/network-snapshot',
        params={
            'param': ['utci', 'wind_speed'],
            'scale': 'daily',
            'date': datetime(2024, 1, 2, 0, 0).isoformat(),
        },
    )
    assert resp.status_code == 200
    assert resp.json()['data'] == [
        {
            'utci': 311,
            'measured_at': '2024-01-02T00:00:00',
            'station_id': 'DOB1',
            'station_type': 'biomet',
            'wind_speed': 77.75,
        },
        {
            'utci': 311,
            'measured_at': '2024-01-02T00:00:00',
            'station_id': 'DOB2',
            'station_type': 'biomet',
            'wind_speed': 77.75,
        },
        # the other two stations are omitted
    ]


@pytest.mark.anyio
@pytest.mark.usefixtures('clean_db')
@pytest.mark.parametrize('stations', [2], indirect=True)
async def test_get_network_values_hourly_colormap_custom_handles(
        app: AsyncClient,
        db: AsyncSession,
        stations: list[Station],
) -> None:
    start_date = datetime(2024, 1, 1, 11, 55, tzinfo=timezone.utc)
    step = timedelta(minutes=5)
    for biomet_station in stations:
        for value in range(14):
            # insert some values for biomet
            biomet_data = BiometData(
                measured_at=start_date + (step * value),
                station_id=biomet_station.station_id,
                sensor_id='DEC1',
                blg_sensor_id='DEC2',
                pet_category=HeatStressCategories.no_thermal_stress,
                utci_category=HeatStressCategories.no_thermal_stress,
                wind_direction=180,
            )
            db.add(biomet_data)

    await db.commit()
    await BiometDataHourly.refresh()
    resp = await app.get(
        '/v1/network-snapshot',
        params={
            'param': ['pet_category', 'utci_category', 'wind_direction'],
            'scale': 'hourly',
            'date': datetime(2024, 1, 1, 13).isoformat(),
            'suggest_viz': True,
        },
    )
    assert resp.status_code == 200
    # check visualization is suggested correctly
    assert resp.json()['visualization'] == {
        'pet_category': None,
        'utci_category': None,
        'wind_direction': {'cmax': 360, 'cmin': 0, 'vmin': 180, 'vmax': 180},
    }


@pytest.mark.anyio
async def test_download_station_data_station_not_found(app: AsyncClient) -> None:
    resp = await app.get('/v1/download/DOTNOX')
    assert resp.status_code == 404
    data = resp.json()
    assert data == {'detail': 'station not found'}


@pytest.mark.anyio
@pytest.mark.parametrize(
    ('station_id', 'station_type'),
    (
        ('DOT', StationType.temprh),
        ('DOB', StationType.biomet),
        ('DOD', StationType.double),
    ),
)
@pytest.mark.parametrize('scale', ('max', 'hourly', 'daily'))
@pytest.mark.usefixtures('clean_db')
async def test_download_station_data_different_type_and_scale(
        station_id: str,
        station_type: StationType,
        scale: str,
        db: AsyncSession,
        app: AsyncClient,
) -> None:
    station = Station(
        station_id=station_id,
        long_name='test-station-1',
        latitude=51.447,
        longitude=7.268,
        altitude=100,
        station_type=station_type,
        leuchtennummer=120,
        district='Innenstadt',
        city='Dortmund',
        country='Germany',
        street='test-street',
        plz=12345,
    )
    db.add(station)
    await db.commit()
    resp = await app.get(f'/v1/download/{station_id}', params={'scale': scale})
    assert resp.status_code == 200
    csv_file = []
    async for line in resp.aiter_lines():
        csv_file.append(line.strip())

    # only the header
    assert len(csv_file) == 1


@pytest.mark.anyio
@pytest.mark.parametrize(
    'biomet_data', [{'n_stations': 1, 'n_data': 10}],
    indirect=True,
)
async def test_download_station_no_data_for_station(
        app: AsyncClient,
        biomet_data: list[BiometData],
) -> None:
    resp = await app.get(
        '/v1/download/DOB1',
        params={
            'fill_gaps': True,
            'start_date': datetime(2024, 8, 3).isoformat(),
        },
    )
    assert resp.status_code == 200
    csv_file = []
    async for line in resp.aiter_lines():
        csv_file.append(line.strip())

    assert len(csv_file) == 1
    # header correct
    assert csv_file[0] == 'station_id,measured_at,absolute_humidity,specific_humidity,atmospheric_pressure,atmospheric_pressure_reduced,air_temperature,dew_point,heat_index,lightning_average_distance,lightning_strike_count,mrt,pet,pet_category,precipitation_sum,relative_humidity,solar_radiation,utci,utci_category,vapor_pressure,wet_bulb_temperature,wind_direction,wind_speed,maximum_wind_speed'  # noqa: E501


@pytest.mark.anyio
@pytest.mark.parametrize(
    'biomet_data', [{'n_stations': 1, 'n_data': 10}],
    indirect=True,
)
async def test_download_station_data_no_dates_set(
        app: AsyncClient,
        biomet_data: list[BiometData],
) -> None:
    resp = await app.get('/v1/download/DOB1', params={'fill_gaps': True})
    assert resp.status_code == 200
    csv_file = []
    async for line in resp.aiter_lines():
        csv_file.append(line.strip())

    # 11 data points + 1 header
    assert len(csv_file) == 12
    # header correct
    assert csv_file[0] == 'station_id,measured_at,absolute_humidity,specific_humidity,atmospheric_pressure,atmospheric_pressure_reduced,air_temperature,dew_point,heat_index,lightning_average_distance,lightning_strike_count,mrt,pet,pet_category,precipitation_sum,relative_humidity,solar_radiation,utci,utci_category,vapor_pressure,wet_bulb_temperature,wind_direction,wind_speed,maximum_wind_speed'  # noqa: E501
    assert csv_file[1] == 'DOB1,2024-08-01 00:00:00+00:00,,,,,,,,,,,,,,,,35.5,,,,,,'
    assert csv_file[-1] == 'DOB1,2024-08-01 00:50:00+00:00,,,,,,,,,,,,,,,,35.5,,,,,,'


@pytest.mark.anyio
@pytest.mark.parametrize(
    'biomet_data', [{'n_stations': 1, 'n_data': 10}],
    indirect=True,
)
async def test_download_station_data_only_start_date_set(
        app: AsyncClient,
        biomet_data: list[BiometData],
) -> None:
    resp = await app.get(
        '/v1/download/DOB1',
        params={
            'fill_gaps': True,
            'start_date': datetime(2024, 8, 1, 0, 30).isoformat(),
        },
    )
    assert resp.status_code == 200
    csv_file = []
    async for line in resp.aiter_lines():
        csv_file.append(line.strip())

    # 11 data points + 1 header
    assert len(csv_file) == 6
    assert csv_file[1] == 'DOB1,2024-08-01 00:30:00+00:00,,,,,,,,,,,,,,,,35.5,,,,,,'
    assert csv_file[-1] == 'DOB1,2024-08-01 00:50:00+00:00,,,,,,,,,,,,,,,,35.5,,,,,,'


@pytest.mark.anyio
@pytest.mark.parametrize(
    'biomet_data', [{'n_stations': 1, 'n_data': 10}],
    indirect=True,
)
async def test_download_station_data_only_end_date_set(
        app: AsyncClient,
        biomet_data: list[BiometData],
) -> None:
    resp = await app.get(
        '/v1/download/DOB1',
        params={
            'fill_gaps': True,
            'end_date': datetime(2024, 8, 1, 0, 30).isoformat(),
        },
    )
    assert resp.status_code == 200
    csv_file = []
    async for line in resp.aiter_lines():
        csv_file.append(line.strip())

    # 11 data points + 1 header
    assert len(csv_file) == 8
    assert csv_file[1] == 'DOB1,2024-08-01 00:00:00+00:00,,,,,,,,,,,,,,,,35.5,,,,,,'
    assert csv_file[-1] == 'DOB1,2024-08-01 00:30:00+00:00,,,,,,,,,,,,,,,,35.5,,,,,,'


@pytest.mark.anyio
@pytest.mark.parametrize(
    'biomet_data', [{'n_stations': 1, 'n_data': 10}],
    indirect=True,
)
async def test_download_station_data_both_dates_set(
        app: AsyncClient,
        biomet_data: list[BiometData],
) -> None:
    resp = await app.get(
        '/v1/download/DOB1',
        params={
            'fill_gaps': True,
            'start_date': datetime(2024, 8, 1, 0, 10).isoformat(),
            'end_date': datetime(2024, 8, 1, 0, 30).isoformat(),
        },
    )
    assert resp.status_code == 200
    csv_file = []
    async for line in resp.aiter_lines():
        csv_file.append(line.strip())

    # 11 data points + 1 header
    assert len(csv_file) == 6
    assert csv_file[1] == 'DOB1,2024-08-01 00:10:00+00:00,,,,,,,,,,,,,,,,35.5,,,,,,'
    assert csv_file[-1] == 'DOB1,2024-08-01 00:30:00+00:00,,,,,,,,,,,,,,,,35.5,,,,,,'


@pytest.mark.anyio
@pytest.mark.parametrize(
    'biomet_data', [{'n_stations': 1, 'n_data': 12}],
    indirect=True,
)
async def test_download_station_data_hourly(
        app: AsyncClient,
        biomet_data: list[BiometData],
) -> None:
    resp = await app.get('/v1/download/DOB1', params={'scale': 'hourly'})
    assert resp.status_code == 200
    csv_file = []
    async for line in resp.aiter_lines():
        csv_file.append(line.strip())

    # 1 data point + 1 header
    assert len(csv_file) == 3
    # header correct
    assert csv_file[0] == 'station_id,measured_at,absolute_humidity,absolute_humidity_max,absolute_humidity_min,specific_humidity,specific_humidity_max,specific_humidity_min,atmospheric_pressure,atmospheric_pressure_max,atmospheric_pressure_min,atmospheric_pressure_reduced,atmospheric_pressure_reduced_max,atmospheric_pressure_reduced_min,air_temperature,air_temperature_max,air_temperature_min,dew_point,dew_point_max,dew_point_min,heat_index,heat_index_max,heat_index_min,lightning_average_distance,lightning_average_distance_max,lightning_average_distance_min,lightning_strike_count,mrt,mrt_max,mrt_min,pet,pet_max,pet_min,pet_category,precipitation_sum,relative_humidity,relative_humidity_max,relative_humidity_min,solar_radiation,solar_radiation_max,solar_radiation_min,utci,utci_max,utci_min,utci_category,vapor_pressure,vapor_pressure_max,vapor_pressure_min,wet_bulb_temperature,wet_bulb_temperature_max,wet_bulb_temperature_min,wind_direction,wind_speed,wind_speed_max,wind_speed_min,maximum_wind_speed'  # noqa: E501
    assert csv_file[1] == 'DOB1,2024-08-01 01:00:00+00:00,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,35.5000000000000000,35.5,35.5,,,,,,,,,,,,'  # noqa: E501
    assert csv_file[-1] == 'DOB1,2024-08-01 02:00:00+00:00,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,35.5000000000000000,35.5,35.5,,,,,,,,,,,,'  # noqa: E501


@pytest.mark.anyio
@pytest.mark.parametrize(
    'biomet_data', [{'n_stations': 1, 'n_data': 12*21}],
    indirect=True,
)
async def test_download_station_data_daily(
        app: AsyncClient,
        biomet_data: list[BiometData],
) -> None:
    resp = await app.get('/v1/download/DOB1', params={'scale': 'daily'})
    assert resp.status_code == 200
    csv_file = []
    async for line in resp.aiter_lines():
        csv_file.append(line.strip())

    # 1 data point + 1 header
    assert len(csv_file) == 2
    # header correct
    assert csv_file[0] == 'station_id,measured_at,absolute_humidity,absolute_humidity_max,absolute_humidity_min,specific_humidity,specific_humidity_max,specific_humidity_min,atmospheric_pressure,atmospheric_pressure_max,atmospheric_pressure_min,atmospheric_pressure_reduced,atmospheric_pressure_reduced_max,atmospheric_pressure_reduced_min,air_temperature,air_temperature_max,air_temperature_min,dew_point,dew_point_max,dew_point_min,heat_index,heat_index_max,heat_index_min,lightning_average_distance,lightning_average_distance_max,lightning_average_distance_min,lightning_strike_count,mrt,mrt_max,mrt_min,pet,pet_max,pet_min,pet_category,precipitation_sum,relative_humidity,relative_humidity_max,relative_humidity_min,solar_radiation,solar_radiation_max,solar_radiation_min,utci,utci_max,utci_min,utci_category,vapor_pressure,vapor_pressure_max,vapor_pressure_min,wet_bulb_temperature,wet_bulb_temperature_max,wet_bulb_temperature_min,wind_direction,wind_speed,wind_speed_max,wind_speed_min,maximum_wind_speed'  # noqa: E501
    assert csv_file[1] == 'DOB1,2024-08-01,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,35.5000000000000000,35.5,35.5,,,,,,,,,,,,'  # noqa: E501


@pytest.mark.anyio
@pytest.mark.parametrize(
    'biomet_data', [{'n_stations': 1, 'n_data': 12*22*3}],
    indirect=True,
)
async def test_download_station_data_hourly_gaps_filled(
        app: AsyncClient,
        biomet_data: list[BiometData],
        db: AsyncSession,
) -> None:
    # intentionally create a gap
    await db.execute(
        delete(BiometData).where(
            BiometData.measured_at.between(
                datetime(2024, 8, 2, 1),
                datetime(2024, 8, 3, 1),
            ),
        ),
    )
    await db.commit()
    await BiometDataDaily.refresh()
    resp = await app.get(
        '/v1/download/DOB1',
        params={'scale': 'daily', 'fill_gaps': True},
    )
    assert resp.status_code == 200
    csv_file = []
    async for line in resp.aiter_lines():
        csv_file.append(line.strip())

    assert len(csv_file) == 4
    # header correct
    assert csv_file[0] == 'station_id,measured_at,absolute_humidity,absolute_humidity_max,absolute_humidity_min,specific_humidity,specific_humidity_max,specific_humidity_min,atmospheric_pressure,atmospheric_pressure_max,atmospheric_pressure_min,atmospheric_pressure_reduced,atmospheric_pressure_reduced_max,atmospheric_pressure_reduced_min,air_temperature,air_temperature_max,air_temperature_min,dew_point,dew_point_max,dew_point_min,heat_index,heat_index_max,heat_index_min,lightning_average_distance,lightning_average_distance_max,lightning_average_distance_min,lightning_strike_count,mrt,mrt_max,mrt_min,pet,pet_max,pet_min,pet_category,precipitation_sum,relative_humidity,relative_humidity_max,relative_humidity_min,solar_radiation,solar_radiation_max,solar_radiation_min,utci,utci_max,utci_min,utci_category,vapor_pressure,vapor_pressure_max,vapor_pressure_min,wet_bulb_temperature,wet_bulb_temperature_max,wet_bulb_temperature_min,wind_direction,wind_speed,wind_speed_max,wind_speed_min,maximum_wind_speed'  # noqa: E501
    assert csv_file[1] == 'DOB1,2024-08-01,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,35.5000000000000000,35.5,35.5,,,,,,,,,,,,'  # noqa: E501
    assert csv_file[2] == 'DOB1,2024-08-02,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,'  # noqa: E501
    assert csv_file[3] == 'DOB1,2024-08-03,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,35.5000000000000000,35.5,35.5,,,,,,,,,,,,'  # noqa: E501


@pytest.mark.parametrize(
    ('data_min', 'data_max', 'param_setting'),
    (
        (None, None, None),
        (None, 10, None),
        (10, None, None),
    ),
)
def test_compute_cmap_invalid_input(
        data_min: float | None,
        data_max: float | None,
        param_setting: None,
) -> None:
    vmin, vmax = compute_colormap_range(
        data_min=data_min,
        data_max=data_max,
        param_setting=param_setting,
    )
    assert vmin is None
    assert vmax is None


@pytest.mark.parametrize(
    ('data_min', 'data_max', 'param_setting', 'expected'),
    (
        pytest.param(10, 10, None, (10, 10), id='unknown fallback'),
        pytest.param(
            11, 12,
            ParamSettings(percentile_5=5, percentile_95=15, fraction=0.5),
            (9, 14),
            id='data range too small',
        ),
        pytest.param(
            10, 10,
            ParamSettings(percentile_5=5, percentile_95=15, fraction=0.5),
            (7.5, 12.5),
            id='min and may equal',
        ),
        pytest.param(
            4, 5,
            ParamSettings(percentile_5=5, percentile_95=15, valid_min=4, fraction=0.5),
            (4, 7),
            id='extension exceed minimum valid range',
        ),
        pytest.param(
            10, 17,
            ParamSettings(
                percentile_5=5,
                percentile_95=15,
                valid_min=4,
                valid_max=16,
                fraction=0.5,
            ),
            (10, 16),
            id='extension exceed maxmimum valid range',
        ),
        pytest.param(
            8, 14,
            ParamSettings(
                percentile_5=5,
                percentile_95=15,
                valid_min=4,
                valid_max=16,
                fraction=0.1,
            ),
            (8, 14),
            id='data range already big enough',
        ),
    ),
)
def test_compute_cmap(
        data_min: float,
        data_max: float,
        param_setting: ParamSettings,
        expected: tuple[float, float],
) -> None:
    restult = compute_colormap_range(
        data_min=data_min,
        data_max=data_max,
        param_setting=param_setting,
    )
    assert restult == expected
