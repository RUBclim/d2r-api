from datetime import date
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from decimal import Decimal

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import ATM41DataRaw
from app.models import BiometData
from app.models import BiometDataDaily
from app.models import BiometDataHourly
from app.models import BLGDataRaw
from app.models import HeatStressCategories
from app.models import LatestData
from app.models import Sensor
from app.models import SensorDeployment
from app.models import SensorType
from app.models import SHT35DataRaw
from app.models import Station
from app.models import StationType
from app.models import TempRHData
from app.models import TempRHDataDaily
from app.models import TempRHDataHourly


@pytest.mark.anyio
@pytest.mark.usefixtures('clean_db')
@pytest.mark.parametrize(
    ('data_table', 'view', 'sensor_type'),
    (
        (TempRHData, TempRHDataHourly, SensorType.sht35),
        (BiometData, BiometDataHourly, SensorType.atm41),
    ),
)
async def test_hourly_view_data_is_right_labelled(
        data_table: type[TempRHData | BiometData],
        view: type[TempRHDataHourly | BiometDataHourly],
        sensor_type: SensorType,
        db: AsyncSession,
        stations: list[Station],
) -> None:
    station, = stations
    start_date = datetime(2024, 1, 1, 11, 55, tzinfo=timezone.utc)
    step = timedelta(minutes=5)
    sensors = [
        Sensor(
            sensor_id='ABC',
            device_id=12345,
            sensor_type=sensor_type,
        ),
        Sensor(
            sensor_id='DEF',
            device_id=54321,
            sensor_type=SensorType.blg,
        ),
    ]
    for sensor in sensors:
        db.add(sensor)

    await db.commit()
    for value in range(14):
        if sensor_type == SensorType.sht35:
            data = data_table(
                measured_at=start_date + (step * value),
                station_id=station.station_id,
                air_temperature=value,
                sensor_id='ABC',
            )
        else:
            data = data_table(
                measured_at=start_date + (step * value),
                station_id=station.station_id,
                air_temperature=value,
                sensor_id='ABC',
                blg_sensor_id='DEF',
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
    assert result[0] == (datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc), Decimal('0'))
    # this starts at 12:00 and is part of the right-labeled 12-13:00 interval
    assert result[1] == (
        datetime(2024, 1, 1, 13, 0, tzinfo=timezone.utc),
        Decimal('6.5'),
    )
    # this is 13:00 and is part of the 13-14:00 interval
    assert result[2] == (
        datetime(2024, 1, 1, 14, 0, tzinfo=timezone.utc),
        Decimal('13'),
    )


@pytest.mark.anyio
@pytest.mark.usefixtures('clean_db')
async def test_hourly_view_data_lightning_distance_only_when_strikes(
        db: AsyncSession,
        stations: list[Station],
) -> None:
    station, = stations
    start_date = datetime(2024, 1, 1, 11, 55, tzinfo=timezone.utc)
    step = timedelta(minutes=5)
    sensor = Sensor(
        sensor_id='ABC',
        device_id=12345,
        sensor_type=SensorType.atm41,
    )
    db.add(sensor)

    await db.commit()
    for value, strike_dist in zip(range(4), [0, 0, 1, 2]):
        data = BiometData(
            measured_at=start_date + (step * value),
            station_id=station.station_id,
            lightning_average_distance=strike_dist,
            sensor_id='ABC',
        )
        db.add(data)
    await db.commit()
    await BiometDataHourly.refresh()

    query = select(
        BiometDataHourly.measured_at,
        BiometDataHourly.lightning_average_distance,
    ).order_by(BiometDataHourly.measured_at)

    result = (await db.execute(query)).all()
    assert result == [
        (datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc), None),
        (datetime(2024, 1, 1, 13, 0, tzinfo=timezone.utc), 1.5),
    ]


@pytest.mark.anyio
@pytest.mark.usefixtures('clean_db')
async def test_daily_view_data_lightning_distance_only_when_strikes(
        db: AsyncSession,
        stations: list[Station],
) -> None:
    station, = stations
    start_date = datetime(2024, 1, 1, 11, 55, tzinfo=timezone.utc)
    step = timedelta(minutes=5)
    sensor = Sensor(
        sensor_id='ABC',
        device_id=12345,
        sensor_type=SensorType.atm41,
    )
    db.add(sensor)

    strike_distances = [0, 0, 1, 2] * 100
    await db.commit()
    for value, strike_dist in enumerate(strike_distances):
        data = BiometData(
            measured_at=start_date + (step * value),
            station_id=station.station_id,
            lightning_average_distance=strike_dist,
            sensor_id='ABC',
        )
        db.add(data)
    await db.commit()
    await BiometDataDaily.refresh()

    query = select(
        BiometDataDaily.measured_at,
        BiometDataDaily.lightning_average_distance,
    ).order_by(BiometDataDaily.measured_at)

    result = (await db.execute(query)).all()
    assert result == [
        (date(2024, 1, 1), None),
        (date(2024, 1, 2), Decimal('1.5')),
    ]


@pytest.mark.anyio
@pytest.mark.usefixtures('clean_db')
@pytest.mark.parametrize(
    ('data_table', 'view', 'sensor_type'),
    (
        (TempRHData, TempRHDataDaily, SensorType.sht35),
        (BiometData, BiometDataDaily, SensorType.atm41),
    ),
)
@pytest.mark.parametrize(
    'month',
    (
        pytest.param(1, id='winter DST'),
        pytest.param(6, id='summer DSG'),
    ),
)
async def test_daily_view_threshold_and_timezone(
        data_table: type[TempRHData | BiometData],
        view: type[TempRHDataDaily | BiometDataDaily],
        sensor_type: SensorType,
        db: AsyncSession,
        stations: list[Station],
        month: int,
) -> None:
    station, = stations
    start_date = datetime(2024, month, 1, 22, 0, tzinfo=timezone.utc)
    step = timedelta(minutes=5)
    sensors = [
        Sensor(
            sensor_id='ABC',
            device_id=12345,
            sensor_type=sensor_type,
        ),
        Sensor(
            sensor_id='DEF',
            device_id=54321,
            sensor_type=SensorType.blg,
        ),
    ]
    for sensor in sensors:
        db.add(sensor)
    await db.commit()
    # compile one day and two hours of values
    for value in range((12 * 24) + (12 * 2)):
        if sensor_type == SensorType.sht35:
            data = data_table(
                measured_at=start_date + (step * value),
                station_id=station.station_id,
                air_temperature=value,
                sensor_id='ABC',
            )
        else:
            data = data_table(
                measured_at=start_date + (step * value),
                station_id=station.station_id,
                air_temperature=value,
                sensor_id='ABC',
                blg_sensor_id='DEF',
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
        (date(2024, month, 1), None),
        # UTC+1 timezone is used
        (date(2024, month, 2), Decimal('155.5')),
        # threshold not reached
        (date(2024, month, 3), None),
    ]


@pytest.mark.anyio
@pytest.mark.usefixtures('clean_db')
async def test_full_address_from_station() -> None:
    station = Station(
        station_id='test-station',
        long_name='test-station-1',
        latitude=51.4460,
        longitude=7.2627,
        altitude=100,
        station_type=StationType.temprh,
        leuchtennummer=100,
        lcz='2',
        street='Teststraße',
        number='1a',
        plz='12345',
        district='Innenstadt',
        city='Dortmund',
        country='Germany',
    )
    assert station.full_address == 'Teststraße 1a, 12345 Dortmund Innenstadt, Germany'


@pytest.mark.anyio
@pytest.mark.usefixtures('clean_db')
async def test_full_address_from_station_district_missing() -> None:
    station = Station(
        station_id='test-station',
        long_name='test-station-1',
        latitude=51.4460,
        longitude=7.2627,
        altitude=100,
        station_type=StationType.temprh,
        leuchtennummer=100,
        lcz='2',
        street='Teststraße',
        number='1a',
        plz='12345',
        district='Scharnhorst',
        city='Dortmund',
        country='Germany',
    )
    assert station.full_address == 'Teststraße 1a, 12345 Dortmund Scharnhorst, Germany'


@pytest.mark.anyio
@pytest.mark.usefixtures('clean_db')
async def test_full_address_from_station_number_missing() -> None:
    station = Station(
        station_id='test-station',
        long_name='test-station-1',
        latitude=51.4460,
        longitude=7.2627,
        altitude=100,
        station_type=StationType.temprh,
        leuchtennummer=100,
        lcz='2',
        street='Teststraße',
        number=None,
        plz='12345',
        district='Innenstadt',
        city='Dortmund',
        country='Germany',
    )
    assert station.full_address == 'Teststraße, 12345 Dortmund Innenstadt, Germany'


@pytest.fixture
async def make_test_data(db: AsyncSession, clean_db: None) -> None:
    # build up some metadata structure
    # first we need some stations
    stations = [
        Station(
            station_id='DOT1',
            long_name='station-2',
            latitude=51.447,
            longitude=7.268,
            altitude=100,
            station_type=StationType.temprh,
            leuchtennummer=120,
            district='district',
            city='Dortmund',
            country='Germany',
            street='test-street',
            plz=12345,
        ),
        Station(
            station_id='DOB1',
            long_name='station-1',
            latitude=51.447,
            longitude=7.268,
            altitude=100,
            station_type=StationType.biomet,
            leuchtennummer=120,
            district='district',
            city='Dortmund',
            country='Germany',
            street='test-street',
            plz=12345,
        ),
    ]
    for station in stations:
        db.add(station)
    await db.commit()
    # now we need some sensors
    sensors = [
        Sensor(
            sensor_id='DEC1',
            device_id=11111,
            sensor_type=SensorType.sht35,
        ),
        Sensor(
            sensor_id='DEC2',
            device_id=22222,
            sensor_type=SensorType.atm41,
        ),
        Sensor(
            sensor_id='DEC3',
            device_id=33333,
            sensor_type=SensorType.blg,
        ),
        Sensor(
            sensor_id='DEC4',
            device_id=44444,
            sensor_type=SensorType.sht35,
        ),
        Sensor(
            sensor_id='DEC5',
            device_id=55555,
            sensor_type=SensorType.atm41,
        ),
    ]
    for sensor in sensors:
        db.add(sensor)
    await db.commit()
    # now we need some sensor deployments
    deployments = [
        # temprh
        SensorDeployment(
            deployment_id=1,
            sensor_id='DEC1',
            station_id='DOB1',
            setup_date=datetime(2023, 1, 1, tzinfo=timezone.utc),
            teardown_date=datetime(2023, 1, 10, tzinfo=timezone.utc),
        ),
        SensorDeployment(
            deployment_id=2,
            sensor_id='DEC4',
            station_id='DOT1',
            setup_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
            teardown_date=datetime(2024, 1, 10, tzinfo=timezone.utc),
        ),
        SensorDeployment(
            deployment_id=3,
            sensor_id='DEC1',
            station_id='DOT1',
            setup_date=datetime(2024, 1, 10, tzinfo=timezone.utc),
        ),
        # biomet
        SensorDeployment(
            deployment_id=4,
            sensor_id='DEC2',
            station_id='DOB1',
            setup_date=datetime(2024, 5, 1, tzinfo=timezone.utc),
            teardown_date=datetime(2024, 5, 10, tzinfo=timezone.utc),
        ),
        SensorDeployment(
            deployment_id=5,
            sensor_id='DEC5',
            station_id='DOB1',
            setup_date=datetime(2024, 5, 10, tzinfo=timezone.utc),
        ),
        SensorDeployment(
            deployment_id=6,
            sensor_id='DEC3',
            station_id='DOB1',
            setup_date=datetime(2024, 5, 1, tzinfo=timezone.utc),
        ),
    ]
    for deployment in deployments:
        db.add(deployment)
    await db.commit()
    data = [
        SHT35DataRaw(
            sensor_id=sensors[0].sensor_id,
            measured_at=datetime(2024, 1, 1, 0, tzinfo=timezone.utc),
        ),
        ATM41DataRaw(
            sensor_id=sensors[1].sensor_id,
            measured_at=datetime(2024, 5, 1, 0, tzinfo=timezone.utc),
        ),
        BLGDataRaw(
            sensor_id=sensors[2].sensor_id,
            measured_at=datetime(2024, 5, 1, 0, tzinfo=timezone.utc),
        ),
        BiometData(
            station_id=stations[1].station_id,
            sensor_id=sensors[1].sensor_id,
            blg_sensor_id=sensors[2].sensor_id,
            measured_at=datetime(2024, 5, 1, 0, tzinfo=timezone.utc),
        ),
        TempRHData(
            station_id=stations[0].station_id,
            sensor_id=sensors[0].sensor_id,
            measured_at=datetime(2024, 1, 1, 0, tzinfo=timezone.utc),
        ),
    ]
    for d in data:
        db.add(d)
    await db.commit()


@pytest.mark.anyio
@pytest.mark.usefixtures('make_test_data')
async def test_temprh_station_relationships(db: AsyncSession) -> None:
    # check the temprh station
    # sensors
    temp_station = (
        await db.execute(select(Station).where(Station.station_id == 'DOT1'))
    ).scalar()
    assert temp_station is not None
    active_sensors = await temp_station.awaitable_attrs.active_sensors
    active_sensor_id = [i.sensor_id for i in active_sensors]
    former_sensors = await temp_station.awaitable_attrs.former_sensors
    former_sensor_id = [i.sensor_id for i in former_sensors]
    assert active_sensor_id == ['DEC1']
    assert former_sensor_id == ['DEC4']
    # deployments
    active_deployments = await temp_station.awaitable_attrs.active_deployments
    active_deployment_id = [i.sensor_id for i in active_deployments]
    former_deployments = await temp_station.awaitable_attrs.former_deployments
    former_deployment_id = [i.sensor_id for i in former_deployments]
    assert active_deployment_id == ['DEC1']
    assert former_deployment_id == ['DEC4']
    temp_deployments = await temp_station.awaitable_attrs.deployments
    temp_deployments_id = [d.sensor_id for d in temp_deployments]
    assert temp_deployments_id == ['DEC4', 'DEC1']
    temp_station_active_sensors = await temp_station.awaitable_attrs.active_sensors
    assert [i.sensor_id for i in temp_station_active_sensors] == ['DEC1']
    temp_station_former_sensors = await temp_station.awaitable_attrs.former_sensors
    assert [i.sensor_id for i in temp_station_former_sensors] == ['DEC4']


@pytest.mark.anyio
@pytest.mark.usefixtures('make_test_data')
async def test_biomet_station_relationships(db: AsyncSession) -> None:
    # now check the biomet station
    biomet_station = (
        await db.execute(select(Station).where(Station.station_id == 'DOB1'))
    ).scalar()
    assert biomet_station is not None
    active_sensors = await biomet_station.awaitable_attrs.active_sensors
    active_sensor_id = [i.sensor_id for i in active_sensors]
    former_sensors = await biomet_station.awaitable_attrs.former_sensors
    former_sensor_id = [i.sensor_id for i in former_sensors]
    assert active_sensor_id == ['DEC3', 'DEC5']
    assert former_sensor_id == ['DEC1', 'DEC2']
    # deployments
    biomet_deployments = await biomet_station.awaitable_attrs.deployments
    biomet_deployments_id = [d.sensor_id for d in biomet_deployments]
    assert biomet_deployments_id == ['DEC1', 'DEC2', 'DEC3', 'DEC5']


@pytest.mark.anyio
@pytest.mark.usefixtures('make_test_data')
async def test_deployments_backreference(db: AsyncSession) -> None:
    temp_station = (
        await db.execute(select(Station).where(Station.station_id == 'DOT1'))
    ).scalar()
    assert temp_station is not None
    # each sensor deployment should have some back reference to the station
    temp_station_active_deployments = (
        await temp_station.awaitable_attrs.active_deployments
    )
    active_sensor = await temp_station_active_deployments[0].awaitable_attrs.station
    assert active_sensor.station_id == 'DOT1'
    temp_station_former_deployments = (
        await temp_station.awaitable_attrs.former_deployments
    )
    sensor = await temp_station_former_deployments[0].awaitable_attrs.sensor
    assert sensor.sensor_id == 'DEC4'

    # sensor relationships
    temp_station_active_sensors = await temp_station.awaitable_attrs.active_sensors
    assert temp_station_active_sensors[0].sensor_id == 'DEC1'


@pytest.mark.anyio
@pytest.mark.usefixtures('make_test_data')
async def test_data_table_relations(db: AsyncSession) -> None:
    # Now test the data table relations
    sht_data = (await db.execute(select(SHT35DataRaw))).scalar()
    assert sht_data is not None
    assert (await sht_data.awaitable_attrs.sensor).sensor_id == 'DEC1'
    atm_data = (await db.execute(select(ATM41DataRaw))).scalar()
    assert atm_data is not None
    assert (await atm_data.awaitable_attrs.sensor).sensor_id == 'DEC2'
    blg_data = (await db.execute(select(BLGDataRaw))).scalar()
    assert blg_data is not None
    assert (await blg_data.awaitable_attrs.sensor).sensor_id == 'DEC3'
    biomet_data = (await db.execute(select(BiometData))).scalar()
    assert biomet_data is not None
    assert (await biomet_data.awaitable_attrs.station).station_id == 'DOB1'
    assert (await biomet_data.awaitable_attrs.sensor).sensor_id == 'DEC2'
    blg_sensor = await biomet_data.awaitable_attrs.blg_sensor
    assert blg_sensor is not None
    assert blg_sensor.sensor_id == 'DEC3'
    biomet_data_deployments = await biomet_data.awaitable_attrs.deployments
    assert len(biomet_data_deployments) == 2
    # make sure the deployments have both station types
    assert [i.sensor.sensor_type for i in biomet_data_deployments] == [
        SensorType.atm41,
        SensorType.blg,
    ]
    # an old deployment, but values are still associated with it!
    assert [i.deployment_id for i in biomet_data.deployments] == [4, 6]

    temprh_data = (await db.execute(select(TempRHData))).scalar()
    assert temprh_data is not None
    temprh_data_station = await temprh_data.awaitable_attrs.station
    assert temprh_data_station.station_id == 'DOT1'
    # get the sensor id
    temprh_data_sensor = await temprh_data.awaitable_attrs.sensor
    assert temprh_data_sensor.sensor_id == 'DEC1'
    # this deployment has already ended, but the value is associated with the deployment
    temprh_data_deployment = (await temprh_data.awaitable_attrs.deployment)
    assert temprh_data_deployment.deployment_id == 2


@pytest.mark.anyio
@pytest.mark.usefixtures('make_test_data')
async def test_view_relations_latest_data(db: AsyncSession) -> None:
    # now test the materialized views
    await LatestData.refresh()
    latest_data = (
        await db.execute(select(LatestData).order_by(LatestData.station_id))
    ).scalars().all()
    assert len(latest_data) == 2
    assert [i.station_id for i in latest_data] == ['DOB1', 'DOT1']
    assert [
        (await i.awaitable_attrs.station).station_id for i in latest_data
    ] == ['DOB1', 'DOT1']


@pytest.mark.anyio
@pytest.mark.usefixtures('make_test_data')
async def test_view_relations_biomet_data_hourly(db: AsyncSession) -> None:
    await BiometDataHourly.refresh()
    hourly_data_biomet = (await db.execute(select(BiometDataHourly))).scalars().all()
    assert len(hourly_data_biomet) == 1
    assert hourly_data_biomet[0].station_id == 'DOB1'
    # access via the station
    hourly_data_biomet_station = await hourly_data_biomet[0].awaitable_attrs.station
    assert hourly_data_biomet_station.station_id == 'DOB1'


@pytest.mark.anyio
@pytest.mark.usefixtures('make_test_data')
async def test_view_relations_temprh_data_hourly(db: AsyncSession) -> None:
    await TempRHDataHourly.refresh()
    hourly_data_temprh = (await db.execute(select(TempRHDataHourly))).scalars().all()
    assert len(hourly_data_temprh) == 1
    assert hourly_data_temprh[0].station_id == 'DOT1'
    hourly_data_temprh_station = await hourly_data_temprh[0].awaitable_attrs.station
    assert hourly_data_temprh_station.station_id == 'DOT1'


@pytest.mark.anyio
@pytest.mark.usefixtures('make_test_data')
async def test_view_relations_biomet_data_daily(db: AsyncSession) -> None:
    await BiometDataDaily.refresh()
    daily_data_biomet = (await db.execute(select(BiometDataDaily))).scalars().all()
    assert len(daily_data_biomet) == 1
    assert daily_data_biomet[0].station_id == 'DOB1'
    daily_data_biomet_station = await daily_data_biomet[0].awaitable_attrs.station
    assert daily_data_biomet_station.station_id == 'DOB1'


@pytest.mark.anyio
@pytest.mark.usefixtures('make_test_data')
async def test_view_relations_temprh_data_daily(db: AsyncSession) -> None:
    await TempRHDataDaily.refresh()
    daily_data_temprh = (await db.execute(select(TempRHDataDaily))).scalars().all()
    assert len(daily_data_temprh) == 1
    assert daily_data_temprh[0].station_id == 'DOT1'
    daily_data_temprh_station = await daily_data_temprh[0].awaitable_attrs.station
    assert daily_data_temprh_station.station_id == 'DOT1'


@pytest.mark.anyio
async def test_db_reprs() -> None:
    # this should just make sure that the reprs don't raise an exception
    station = Station(
        station_id='test-station',
        long_name='test-station-1',
        latitude=51.4460,
        longitude=7.2627,
        altitude=100,
        station_type=StationType.temprh,
        leuchtennummer=100,
        lcz='2',
        street='Teststraße',
        number='1a',
        plz='12345',
        district='Innenstadt',
        city='Dortmund',
        country='Germany',
    )
    repr(station)
    sensor = Sensor(
        sensor_id='DEC1',
        device_id=11111,
        sensor_type=SensorType.sht35,
    )
    repr(sensor)
    deployment = SensorDeployment(
        sensor_id='DEC1',
        station_id='DOT1',
        setup_date=datetime(2023, 1, 1, tzinfo=timezone.utc),
        teardown_date=datetime(2023, 1, 10, tzinfo=timezone.utc),
    )
    repr(deployment)
    sht35_data = SHT35DataRaw(
        sensor_id='DEC1',
        measured_at=datetime(2024, 1, 1, 0, tzinfo=timezone.utc),
    )
    repr(sht35_data)
    atm41_data = ATM41DataRaw(
        sensor_id='DEC1',
        measured_at=datetime(2024, 1, 1, 0, tzinfo=timezone.utc),
    )
    repr(atm41_data)
    blg_data = BLGDataRaw(
        sensor_id='DEC1',
        measured_at=datetime(2024, 1, 1, 0, tzinfo=timezone.utc),
    )
    repr(blg_data)
    biomet_data = BiometData(
        station_id='DOB1',
        sensor_id='DEC1',
        blg_sensor_id='DEC2',
        measured_at=datetime(2024, 1, 1, 0, tzinfo=timezone.utc),
    )
    repr(biomet_data)
    # views
    latest_data = LatestData(
        station_id='DOT1',
        measured_at=datetime(2024, 1, 1, 0, tzinfo=timezone.utc),
    )
    repr(latest_data)
    biomet_hourly = BiometDataHourly(
        station_id='DOB1',
        measured_at=datetime(2024, 1, 1, 0, tzinfo=timezone.utc),
    )
    repr(biomet_hourly)
    biomet_daily = BiometDataDaily(
        station_id='DOB1',
        measured_at=date(2024, 1, 1),
    )
    repr(biomet_daily)
    temprh_hourly = TempRHDataHourly(
        station_id='DOT1',
        measured_at=datetime(2024, 1, 1, 0, tzinfo=timezone.utc),
    )
    repr(temprh_hourly)
    temprh_daily = TempRHDataDaily(
        station_id='DOT1',
        measured_at=date(2024, 1, 1),
    )
    repr(temprh_daily)


@pytest.mark.anyio
@pytest.mark.usefixtures('clean_db', 'stations')
@pytest.mark.parametrize('table', (BiometData, TempRHData))
async def test_generated_qc_flag_is_true_when_null(
        table: type[BiometData | TempRHData],
        db: AsyncSession,
) -> None:
    data = table(
        measured_at=datetime(2024, 1, 1, 0, tzinfo=timezone.utc),
        station_id='DOB1',
        sensor_id='DEC1',
    )
    db.add(data)
    await db.commit()
    assert data.qc_flagged is True


@pytest.mark.anyio
@pytest.mark.usefixtures('clean_db', 'stations')
async def test_generated_qc_flag_is_false_when_all_passed(db: AsyncSession) -> None:
    biomet_data = BiometData(
        measured_at=datetime(2024, 1, 1, 0, tzinfo=timezone.utc),
        station_id='DOB1',
        sensor_id='DEC1',
        air_temperature_qc_range_check=False,
        air_temperature_qc_persistence_check=False,
        air_temperature_qc_spike_dip_check=False,
        relative_humidity_qc_range_check=False,
        relative_humidity_qc_persistence_check=False,
        relative_humidity_qc_spike_dip_check=False,
        atmospheric_pressure_qc_range_check=False,
        atmospheric_pressure_qc_persistence_check=False,
        atmospheric_pressure_qc_spike_dip_check=False,
        wind_speed_qc_range_check=False,
        wind_speed_qc_persistence_check=False,
        wind_speed_qc_spike_dip_check=False,
        wind_direction_qc_range_check=False,
        wind_direction_qc_persistence_check=False,
        u_wind_qc_range_check=False,
        u_wind_qc_persistence_check=False,
        u_wind_qc_spike_dip_check=False,
        v_wind_qc_range_check=False,
        v_wind_qc_persistence_check=False,
        v_wind_qc_spike_dip_check=False,
        maximum_wind_speed_qc_range_check=False,
        maximum_wind_speed_qc_persistence_check=False,
        precipitation_sum_qc_range_check=False,
        precipitation_sum_qc_persistence_check=False,
        precipitation_sum_qc_spike_dip_check=False,
        solar_radiation_qc_range_check=False,
        solar_radiation_qc_persistence_check=False,
        solar_radiation_qc_spike_dip_check=False,
        lightning_average_distance_qc_range_check=False,
        lightning_average_distance_qc_persistence_check=False,
        lightning_strike_count_qc_range_check=False,
        lightning_strike_count_qc_persistence_check=False,
        x_orientation_angle_qc_range_check=False,
        x_orientation_angle_qc_spike_dip_check=False,
        y_orientation_angle_qc_range_check=False,
        y_orientation_angle_qc_spike_dip_check=False,
        black_globe_temperature_qc_range_check=False,
        black_globe_temperature_qc_persistence_check=False,
        black_globe_temperature_qc_spike_dip_check=False,
    )
    db.add(biomet_data)
    await db.commit()
    assert biomet_data.qc_flagged is False


@pytest.mark.anyio
@pytest.mark.usefixtures('clean_db', 'stations')
async def test_generated_qc_flag_is_true_when_single_test_failed(
        db: AsyncSession,
) -> None:
    biomet_data = BiometData(
        measured_at=datetime(2024, 1, 1, 0, tzinfo=timezone.utc),
        station_id='DOB1',
        sensor_id='DEC1',
        air_temperature_qc_range_check=False,
        air_temperature_qc_persistence_check=False,
        air_temperature_qc_spike_dip_check=False,
        relative_humidity_qc_range_check=False,
        relative_humidity_qc_persistence_check=False,
        relative_humidity_qc_spike_dip_check=False,
        atmospheric_pressure_qc_range_check=False,
        atmospheric_pressure_qc_persistence_check=False,
        atmospheric_pressure_qc_spike_dip_check=False,
        wind_speed_qc_range_check=False,
        wind_speed_qc_persistence_check=False,
        wind_speed_qc_spike_dip_check=False,
        wind_direction_qc_range_check=False,
        wind_direction_qc_persistence_check=False,
        u_wind_qc_range_check=False,
        u_wind_qc_persistence_check=False,
        u_wind_qc_spike_dip_check=False,
        v_wind_qc_range_check=False,
        v_wind_qc_persistence_check=False,
        v_wind_qc_spike_dip_check=False,
        maximum_wind_speed_qc_range_check=False,
        maximum_wind_speed_qc_persistence_check=False,
        precipitation_sum_qc_range_check=False,
        precipitation_sum_qc_persistence_check=False,
        precipitation_sum_qc_spike_dip_check=False,
        solar_radiation_qc_range_check=False,
        solar_radiation_qc_persistence_check=False,
        solar_radiation_qc_spike_dip_check=False,
        lightning_average_distance_qc_range_check=False,
        lightning_average_distance_qc_persistence_check=False,
        lightning_strike_count_qc_range_check=False,
        lightning_strike_count_qc_persistence_check=False,
        x_orientation_angle_qc_range_check=False,
        x_orientation_angle_qc_spike_dip_check=False,
        y_orientation_angle_qc_range_check=False,
        y_orientation_angle_qc_spike_dip_check=False,
        black_globe_temperature_qc_range_check=False,
        black_globe_temperature_qc_persistence_check=False,
        black_globe_temperature_qc_spike_dip_check=True,
    )
    db.add(biomet_data)
    await db.commit()
    assert biomet_data.qc_flagged is True


@pytest.mark.anyio
@pytest.mark.usefixtures('clean_db', 'stations')
async def test_temp_rh_data_hourly_view_order_is_correct(db: AsyncSession) -> None:
    d = TempRHData(
        measured_at=datetime(2024, 1, 1, 0, tzinfo=timezone.utc),
        station_id='DOB1',
        sensor_id='DEC1',
        air_temperature_raw=0,
        air_temperature=1,
        relative_humidity_raw=2,
        relative_humidity=3,
        dew_point=4,
        absolute_humidity=5,
        specific_humidity=6,
        heat_index=7,
        wet_bulb_temperature=8,
        battery_voltage=9,
        protocol_version=10,
    )
    db.add(d)
    await db.commit()
    await TempRHDataHourly.refresh()
    q = select(TempRHDataHourly)
    result = (await db.execute(q)).scalar_one()

    # this way it's easier to find where it differs
    assert result.measured_at == datetime(2024, 1, 1, 1, 0, tzinfo=timezone.utc)
    assert result.absolute_humidity == Decimal('5.0')
    assert result.absolute_humidity_min == Decimal('5.0')
    assert result.absolute_humidity_max == Decimal('5.0')
    assert result.air_temperature == Decimal('1.0')
    assert result.air_temperature_min == Decimal('1.0')
    assert result.air_temperature_max == Decimal('1.0')
    assert result.air_temperature_raw == Decimal('0.0')
    assert result.air_temperature_raw_min == Decimal('0.0')
    assert result.air_temperature_raw_max == Decimal('0.0')
    assert result.battery_voltage == Decimal('9.0')
    assert result.battery_voltage_min == Decimal('9.0')
    assert result.battery_voltage_max == Decimal('9.0')
    assert result.dew_point == Decimal('4.0')
    assert result.dew_point_min == Decimal('4.0')
    assert result.dew_point_max == Decimal('4.0')
    assert result.heat_index == Decimal('7.0')
    assert result.heat_index_min == Decimal('7.0')
    assert result.heat_index_max == Decimal('7.0')
    assert result.protocol_version == 10
    assert result.relative_humidity == Decimal('3.0')
    assert result.relative_humidity_min == Decimal('3.0')
    assert result.relative_humidity_max == Decimal('3.0')
    assert result.relative_humidity_raw == Decimal('2.0')
    assert result.relative_humidity_raw_min == Decimal('2.0')
    assert result.relative_humidity_raw_max == Decimal('2.0')
    assert result.specific_humidity == Decimal('6.0')
    assert result.specific_humidity_min == Decimal('6.0')
    assert result.specific_humidity_max == Decimal('6.0')
    assert result.wet_bulb_temperature == Decimal('8.0')
    assert result.wet_bulb_temperature_min == Decimal('8.0')
    assert result.wet_bulb_temperature_max == Decimal('8.0')


@pytest.mark.anyio
@pytest.mark.usefixtures('clean_db', 'stations')
async def test_temp_rh_data_daily_view_order_is_correct(db: AsyncSession) -> None:
    for i in range(250):
        diff = timedelta(minutes=5 * i)
        d = TempRHData(
            measured_at=datetime(2024, 1, 1, 0, tzinfo=timezone.utc) + diff,
            station_id='DOB1',
            sensor_id='DEC1',
            air_temperature_raw=0,
            air_temperature=1,
            relative_humidity_raw=2,
            relative_humidity=3,
            dew_point=4,
            absolute_humidity=5,
            specific_humidity=6,
            heat_index=7,
            wet_bulb_temperature=8,
            battery_voltage=9,
            protocol_version=10,
        )
        db.add(d)
    await db.commit()
    await TempRHDataDaily.refresh()
    q = select(TempRHDataDaily)
    result = (await db.execute(q)).scalar_one()

    # this way it's easier to find where it differs
    assert result.measured_at == date(2024, 1, 1)
    assert result.absolute_humidity == Decimal('5.0')
    assert result.absolute_humidity_min == Decimal('5.0')
    assert result.absolute_humidity_max == Decimal('5.0')
    assert result.air_temperature == Decimal('1.0')
    assert result.air_temperature_min == Decimal('1.0')
    assert result.air_temperature_max == Decimal('1.0')
    assert result.air_temperature_raw == Decimal('0.0')
    assert result.air_temperature_raw_min == Decimal('0.0')
    assert result.air_temperature_raw_max == Decimal('0.0')
    assert result.battery_voltage == Decimal('9.0')
    assert result.battery_voltage_min == Decimal('9.0')
    assert result.battery_voltage_max == Decimal('9.0')
    assert result.dew_point == Decimal('4.0')
    assert result.dew_point_min == Decimal('4.0')
    assert result.dew_point_max == Decimal('4.0')
    assert result.heat_index == Decimal('7.0')
    assert result.heat_index_min == Decimal('7.0')
    assert result.heat_index_max == Decimal('7.0')
    assert result.protocol_version == 10
    assert result.relative_humidity == Decimal('3.0')
    assert result.relative_humidity_min == Decimal('3.0')
    assert result.relative_humidity_max == Decimal('3.0')
    assert result.relative_humidity_raw == Decimal('2.0')
    assert result.relative_humidity_raw_min == Decimal('2.0')
    assert result.relative_humidity_raw_max == Decimal('2.0')
    assert result.specific_humidity == Decimal('6.0')
    assert result.specific_humidity_min == Decimal('6.0')
    assert result.specific_humidity_max == Decimal('6.0')
    assert result.wet_bulb_temperature == Decimal('8.0')
    assert result.wet_bulb_temperature_min == Decimal('8.0')
    assert result.wet_bulb_temperature_max == Decimal('8.0')


@pytest.mark.anyio
@pytest.mark.usefixtures('clean_db', 'stations')
async def test_biomet_data_hourly_view_order_is_correct(db: AsyncSession) -> None:
    d = BiometData(
        measured_at=datetime(2024, 1, 1, 0, tzinfo=timezone.utc),
        station_id='DOB1',
        sensor_id='DEC1',
        air_temperature=1,
        relative_humidity=3,
        dew_point=4,
        absolute_humidity=5,
        specific_humidity=6,
        heat_index=7,
        wet_bulb_temperature=8,
        battery_voltage=9,
        protocol_version=10,
        atmospheric_pressure=11,
        vapor_pressure=12,
        wind_speed=13,
        wind_direction=14,
        u_wind=15,
        v_wind=16,
        maximum_wind_speed=17,
        precipitation_sum=18,
        solar_radiation=19,
        lightning_average_distance=20,
        lightning_strike_count=21,
        x_orientation_angle=22,
        y_orientation_angle=23,
        black_globe_temperature=24,
        thermistor_resistance=25,
        voltage_ratio=26,
        mrt=27,
        utci=28,
        utci_category=HeatStressCategories.extreme_heat_stress,
        pet=29,
        pet_category=HeatStressCategories.extreme_heat_stress,
        atmospheric_pressure_reduced=30,
        blg_battery_voltage=31,
    )
    db.add(d)
    await db.commit()
    await BiometDataHourly.refresh()
    q = select(BiometDataHourly)
    result = (await db.execute(q)).scalar_one()

    # this way it's easier to find where it differs
    assert result.measured_at == datetime(2024, 1, 1, 1, 0, tzinfo=timezone.utc)
    assert result.absolute_humidity == Decimal('5.0')
    assert result.absolute_humidity_min == Decimal('5.0')
    assert result.absolute_humidity_max == Decimal('5.0')
    assert result.air_temperature == Decimal('1.0')
    assert result.air_temperature_min == Decimal('1.0')
    assert result.air_temperature_max == Decimal('1.0')
    assert result.battery_voltage == Decimal('9.0')
    assert result.battery_voltage_min == Decimal('9.0')
    assert result.battery_voltage_max == Decimal('9.0')
    assert result.dew_point == Decimal('4.0')
    assert result.dew_point_min == Decimal('4.0')
    assert result.dew_point_max == Decimal('4.0')
    assert result.heat_index == Decimal('7.0')
    assert result.heat_index_min == Decimal('7.0')
    assert result.heat_index_max == Decimal('7.0')
    assert result.protocol_version == 10
    assert result.relative_humidity == Decimal('3.0')
    assert result.relative_humidity_min == Decimal('3.0')
    assert result.relative_humidity_max == Decimal('3.0')
    assert result.specific_humidity == Decimal('6.0')
    assert result.specific_humidity_min == Decimal('6.0')
    assert result.specific_humidity_max == Decimal('6.0')
    assert result.wet_bulb_temperature == Decimal('8.0')
    assert result.wet_bulb_temperature_min == Decimal('8.0')
    assert result.wet_bulb_temperature_max == Decimal('8.0')
    assert result.atmospheric_pressure == Decimal('11.0')
    assert result.atmospheric_pressure_min == Decimal('11.0')
    assert result.atmospheric_pressure_max == Decimal('11.0')
    assert result.vapor_pressure == Decimal('12.0')
    assert result.vapor_pressure_min == Decimal('12.0')
    assert result.vapor_pressure_max == Decimal('12.0')
    assert result.wind_speed == Decimal('13.0')
    assert result.wind_speed_min == Decimal('13.0')
    assert result.wind_speed_max == Decimal('13.0')
    assert result.wind_direction == Decimal('14.0')
    assert result.u_wind == Decimal('15.0')
    assert result.u_wind_min == Decimal('15.0')
    assert result.u_wind_max == Decimal('15.0')
    assert result.v_wind == Decimal('16.0')
    assert result.v_wind_min == Decimal('16.0')
    assert result.v_wind_max == Decimal('16.0')
    assert result.maximum_wind_speed == Decimal('17.0')
    assert result.precipitation_sum == Decimal('18.0')
    assert result.solar_radiation == Decimal('19.0')
    assert result.solar_radiation_min == Decimal('19.0')
    assert result.solar_radiation_max == Decimal('19.0')
    assert result.lightning_average_distance == Decimal('20.0')
    assert result.lightning_average_distance_min == Decimal('20.0')
    assert result.lightning_average_distance_max == Decimal('20.0')
    assert result.lightning_strike_count == Decimal('21.0')
    assert result.x_orientation_angle == Decimal('22.0')
    assert result.x_orientation_angle_min == Decimal('22.0')
    assert result.x_orientation_angle_max == Decimal('22.0')
    assert result.y_orientation_angle == Decimal('23.0')
    assert result.y_orientation_angle_min == Decimal('23.0')
    assert result.y_orientation_angle_max == Decimal('23.0')
    assert result.black_globe_temperature == Decimal('24.0')
    assert result.black_globe_temperature_min == Decimal('24.0')
    assert result.black_globe_temperature_max == Decimal('24.0')
    assert result.thermistor_resistance == Decimal('25.0')
    assert result.thermistor_resistance_min == Decimal('25.0')
    assert result.thermistor_resistance_max == Decimal('25.0')
    assert result.voltage_ratio == Decimal('26.0')
    assert result.voltage_ratio_min == Decimal('26.0')
    assert result.voltage_ratio_max == Decimal('26.0')
    assert result.mrt == Decimal('27.0')
    assert result.mrt_min == Decimal('27.0')
    assert result.mrt_max == Decimal('27.0')
    assert result.utci == Decimal('28.0')
    assert result.utci_min == Decimal('28.0')
    assert result.utci_max == Decimal('28.0')
    assert result.utci_category == HeatStressCategories.extreme_heat_stress
    assert result.pet == Decimal('29.0')
    assert result.pet_min == Decimal('29.0')
    assert result.pet_max == Decimal('29.0')
    assert result.pet_category == HeatStressCategories.extreme_heat_stress
    assert result.atmospheric_pressure_reduced == Decimal('30.0')
    assert result.atmospheric_pressure_reduced_min == Decimal('30.0')
    assert result.atmospheric_pressure_reduced_max == Decimal('30.0')
    assert result.blg_battery_voltage == Decimal('31.0')
    assert result.blg_battery_voltage_min == Decimal('31.0')
    assert result.blg_battery_voltage_max == Decimal('31.0')


@pytest.mark.anyio
@pytest.mark.usefixtures('clean_db', 'stations')
async def test_biomet_data_daily_view_order_is_correct(db: AsyncSession) -> None:
    for i in range(250):
        diff = timedelta(minutes=5 * i)
        d = BiometData(
            measured_at=datetime(2024, 1, 1, 0, tzinfo=timezone.utc) + diff,
            station_id='DOB1',
            sensor_id='DEC1',
            air_temperature=32,
            relative_humidity=3,
            dew_point=4,
            absolute_humidity=5,
            specific_humidity=6,
            heat_index=7,
            wet_bulb_temperature=8,
            battery_voltage=9,
            protocol_version=10,
            atmospheric_pressure=11,
            vapor_pressure=12,
            wind_speed=13,
            wind_direction=14,
            u_wind=15,
            v_wind=16,
            maximum_wind_speed=17,
            precipitation_sum=0,
            solar_radiation=19,
            lightning_average_distance=20,
            lightning_strike_count=1,
            x_orientation_angle=22,
            y_orientation_angle=23,
            black_globe_temperature=24,
            thermistor_resistance=25,
            voltage_ratio=26,
            mrt=27,
            utci=28,
            utci_category=HeatStressCategories.extreme_heat_stress,
            pet=29,
            pet_category=HeatStressCategories.extreme_heat_stress,
            atmospheric_pressure_reduced=30,
            blg_battery_voltage=31,
        )
        db.add(d)
    await db.commit()
    await BiometDataDaily.refresh()
    q = select(BiometDataDaily)
    result = (await db.execute(q)).scalar_one()

    # this way it's easier to find where it differs
    assert result.measured_at == date(2024, 1, 1)
    assert result.absolute_humidity == Decimal('5.0')
    assert result.absolute_humidity_min == Decimal('5.0')
    assert result.absolute_humidity_max == Decimal('5.0')
    assert result.air_temperature == Decimal('32.0')
    assert result.air_temperature_min == Decimal('32.0')
    assert result.air_temperature_max == Decimal('32.0')
    assert result.battery_voltage == Decimal('9.0')
    assert result.battery_voltage_min == Decimal('9.0')
    assert result.battery_voltage_max == Decimal('9.0')
    assert result.dew_point == Decimal('4.0')
    assert result.dew_point_min == Decimal('4.0')
    assert result.dew_point_max == Decimal('4.0')
    assert result.heat_index == Decimal('7.0')
    assert result.heat_index_min == Decimal('7.0')
    assert result.heat_index_max == Decimal('7.0')
    assert result.protocol_version == 10
    assert result.relative_humidity == Decimal('3.0')
    assert result.relative_humidity_min == Decimal('3.0')
    assert result.relative_humidity_max == Decimal('3.0')
    assert result.specific_humidity == Decimal('6.0')
    assert result.specific_humidity_min == Decimal('6.0')
    assert result.specific_humidity_max == Decimal('6.0')
    assert result.wet_bulb_temperature == Decimal('8.0')
    assert result.wet_bulb_temperature_min == Decimal('8.0')
    assert result.wet_bulb_temperature_max == Decimal('8.0')
    assert result.atmospheric_pressure == Decimal('11.0')
    assert result.atmospheric_pressure_min == Decimal('11.0')
    assert result.atmospheric_pressure_max == Decimal('11.0')
    assert result.vapor_pressure == Decimal('12.0')
    assert result.vapor_pressure_min == Decimal('12.0')
    assert result.vapor_pressure_max == Decimal('12.0')
    assert result.wind_speed == Decimal('13.0')
    assert result.wind_speed_min == Decimal('13.0')
    assert result.wind_speed_max == Decimal('13.0')
    assert result.wind_direction == Decimal('14.0')
    assert result.u_wind == Decimal('15.0')
    assert result.u_wind_min == Decimal('15.0')
    assert result.u_wind_max == Decimal('15.0')
    assert result.v_wind == Decimal('16.0')
    assert result.v_wind_min == Decimal('16.0')
    assert result.v_wind_max == Decimal('16.0')
    assert result.maximum_wind_speed == Decimal('17.0')
    assert result.precipitation_sum == Decimal('0.0')
    assert result.solar_radiation == Decimal('19.0')
    assert result.solar_radiation_min == Decimal('19.0')
    assert result.solar_radiation_max == Decimal('19.0')
    assert result.lightning_average_distance == Decimal('20.0')
    assert result.lightning_average_distance_min == Decimal('20.0')
    assert result.lightning_average_distance_max == Decimal('20.0')
    assert result.lightning_strike_count == Decimal('250.0')
    assert result.x_orientation_angle == Decimal('22.0')
    assert result.x_orientation_angle_min == Decimal('22.0')
    assert result.x_orientation_angle_max == Decimal('22.0')
    assert result.y_orientation_angle == Decimal('23.0')
    assert result.y_orientation_angle_min == Decimal('23.0')
    assert result.y_orientation_angle_max == Decimal('23.0')
    assert result.black_globe_temperature == Decimal('24.0')
    assert result.black_globe_temperature_min == Decimal('24.0')
    assert result.black_globe_temperature_max == Decimal('24.0')
    assert result.thermistor_resistance == Decimal('25.0')
    assert result.thermistor_resistance_min == Decimal('25.0')
    assert result.thermistor_resistance_max == Decimal('25.0')
    assert result.voltage_ratio == Decimal('26.0')
    assert result.voltage_ratio_min == Decimal('26.0')
    assert result.voltage_ratio_max == Decimal('26.0')
    assert result.mrt == Decimal('27.0')
    assert result.mrt_min == Decimal('27.0')
    assert result.mrt_max == Decimal('27.0')
    assert result.utci == Decimal('28.0')
    assert result.utci_min == Decimal('28.0')
    assert result.utci_max == Decimal('28.0')
    assert result.utci_category == HeatStressCategories.extreme_heat_stress
    assert result.pet == Decimal('29.0')
    assert result.pet_min == Decimal('29.0')
    assert result.pet_max == Decimal('29.0')
    assert result.pet_category == HeatStressCategories.extreme_heat_stress
    assert result.atmospheric_pressure_reduced == Decimal('30.0')
    assert result.atmospheric_pressure_reduced_min == Decimal('30.0')
    assert result.atmospheric_pressure_reduced_max == Decimal('30.0')
    assert result.blg_battery_voltage == Decimal('31.0')
    assert result.blg_battery_voltage_min == Decimal('31.0')
    assert result.blg_battery_voltage_max == Decimal('31.0')
