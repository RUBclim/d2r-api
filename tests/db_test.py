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
            sensor_id=sensors[0].sensor_id,
            station_id=stations[1].station_id,
            setup_date=datetime(2023, 1, 1, tzinfo=timezone.utc),
            teardown_date=datetime(2023, 1, 10, tzinfo=timezone.utc),
        ),
        SensorDeployment(
            deployment_id=2,
            sensor_id=sensors[3].sensor_id,
            station_id=stations[0].station_id,
            setup_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
            teardown_date=datetime(2024, 1, 10, tzinfo=timezone.utc),
        ),
        SensorDeployment(
            deployment_id=3,
            sensor_id=sensors[0].sensor_id,
            station_id=stations[0].station_id,
            setup_date=datetime(2024, 1, 10, tzinfo=timezone.utc),
        ),
        # biomet
        SensorDeployment(
            deployment_id=4,
            sensor_id=sensors[1].sensor_id,
            station_id=stations[1].station_id,
            setup_date=datetime(2024, 5, 1, tzinfo=timezone.utc),
            teardown_date=datetime(2024, 5, 10, tzinfo=timezone.utc),
        ),
        SensorDeployment(
            deployment_id=5,
            sensor_id=sensors[4].sensor_id,
            station_id=stations[1].station_id,
            setup_date=datetime(2024, 5, 10, tzinfo=timezone.utc),
        ),
        SensorDeployment(
            deployment_id=6,
            sensor_id=sensors[2].sensor_id,
            station_id=stations[1].station_id,
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
    active_sensor_id = [i.sensor_id for i in temp_station.active_sensors]
    former_sensor_id = [i.sensor_id for i in temp_station.former_sensors]
    assert active_sensor_id == ['DEC1']
    assert former_sensor_id == ['DEC4']
    # deployments
    active_deployment = [i.sensor_id for i in temp_station.active_deployments]
    former_deployment = [i.sensor_id for i in temp_station.former_deployments]
    assert active_deployment == ['DEC1']
    assert former_deployment == ['DEC4']
    temp_deployments = [d.sensor_id for d in temp_station.deployments]
    assert temp_deployments == ['DEC4', 'DEC1']
    assert [i.sensor_id for i in temp_station.active_sensors] == ['DEC1']
    assert [i.sensor_id for i in temp_station.former_sensors] == ['DEC4']


@pytest.mark.anyio
@pytest.mark.usefixtures('make_test_data')
async def test_biomet_station_relationships(db: AsyncSession) -> None:
    # now check the biomet station
    biomet_station = (
        await db.execute(select(Station).where(Station.station_id == 'DOB1'))
    ).scalar()
    assert biomet_station is not None
    active_sensor_id = [i.sensor_id for i in biomet_station.active_sensors]
    former_sensor_id = [i.sensor_id for i in biomet_station.former_sensors]
    assert active_sensor_id == ['DEC3', 'DEC5']
    assert former_sensor_id == ['DEC1', 'DEC2']
    # deployments
    biomet_deployments = [d.sensor_id for d in biomet_station.deployments]
    assert biomet_deployments == ['DEC1', 'DEC2', 'DEC3', 'DEC5']


@pytest.mark.anyio
@pytest.mark.usefixtures('make_test_data')
async def test_deployments_backreference(db: AsyncSession) -> None:
    temp_station = (
        await db.execute(select(Station).where(Station.station_id == 'DOT1'))
    ).scalar()
    assert temp_station is not None
    # each sensor deployment should have some back reference to the station
    temp_station.active_deployments[0].station.station_id == 'DOT1'
    temp_station.former_deployments[0].sensor.sensor_id == 'DEC1'

    # sensor relationships
    temp_station.active_sensors[0].deployments[0].sensor_id == 'DEC1'


@pytest.mark.anyio
@pytest.mark.usefixtures('make_test_data')
async def test_data_table_relations(db: AsyncSession) -> None:
    # Now test the data table relations
    sht_data = (await db.execute(select(SHT35DataRaw))).scalar()
    assert sht_data is not None
    assert sht_data.sensor.sensor_id == 'DEC1'
    atm_data = (await db.execute(select(ATM41DataRaw))).scalar()
    assert atm_data is not None
    assert atm_data.sensor.sensor_id == 'DEC2'
    blg_data = (await db.execute(select(BLGDataRaw))).scalar()
    assert blg_data is not None
    assert blg_data.sensor.sensor_id == 'DEC3'
    biomet_data = (await db.execute(select(BiometData))).scalar()
    assert biomet_data is not None
    assert biomet_data.station.station_id == 'DOB1'
    assert biomet_data.sensor.sensor_id == 'DEC2'
    assert biomet_data.blg_sensor is not None
    assert biomet_data.blg_sensor.sensor_id == 'DEC3'
    # mhm, that does not work as expected?
    assert len(biomet_data.deployments) == 2
    # make sure the deployments have both station types
    assert [i.sensor.sensor_type for i in biomet_data.deployments] == [
        SensorType.atm41,
        SensorType.blg,
    ]
    # an old deployment, but values are still associated with it!
    assert [i.deployment_id for i in biomet_data.deployments] == [4, 6]

    temprh_data = (await db.execute(select(TempRHData))).scalar()
    assert temprh_data is not None
    assert temprh_data.station.station_id == 'DOT1'
    assert temprh_data.sensor.sensor_id == 'DEC1'
    # this deployment has already ended, but the value is associated with the deployment
    assert temprh_data.deployment.deployment_id == 2


@pytest.mark.anyio
@pytest.mark.usefixtures('make_test_data')
async def test_view_relations(db: AsyncSession) -> None:
    # now test the materialized views
    await LatestData.refresh()
    latest_data = (
        await db.execute(select(LatestData).order_by(LatestData.station_id))
    ).scalars().all()
    assert len(latest_data) == 2
    assert [i.station_id for i in latest_data] == ['DOB1', 'DOT1']
    assert [i.station.station_id for i in latest_data] == ['DOB1', 'DOT1']

    await BiometDataHourly.refresh()
    hourly_data_biomet = (await db.execute(select(BiometDataHourly))).scalars().all()
    assert len(hourly_data_biomet) == 1
    assert hourly_data_biomet[0].station_id == 'DOB1'
    assert hourly_data_biomet[0].station.station_id == 'DOB1'

    await TempRHDataHourly.refresh()
    hourly_data_temprh = (await db.execute(select(TempRHDataHourly))).scalars().all()
    assert len(hourly_data_temprh) == 1
    assert hourly_data_temprh[0].station_id == 'DOT1'
    assert hourly_data_temprh[0].station.station_id == 'DOT1'

    await BiometDataDaily.refresh()
    daily_data_biomet = (await db.execute(select(BiometDataDaily))).scalars().all()
    assert len(daily_data_biomet) == 1
    assert daily_data_biomet[0].station_id == 'DOB1'
    assert daily_data_biomet[0].station.station_id == 'DOB1'

    await TempRHDataDaily.refresh()
    daily_data_temprh = (await db.execute(select(TempRHDataDaily))).scalars().all()
    assert len(daily_data_temprh) == 1
    assert daily_data_temprh[0].station_id == 'DOT1'
    assert daily_data_temprh[0].station.station_id == 'DOT1'


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
