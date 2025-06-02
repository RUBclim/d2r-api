import asyncio
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Coroutine
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from decimal import Decimal
from unittest import mock
from unittest.mock import call

import pandas as pd
import pytest
from element import ElementApi
from pandas.testing import assert_frame_equal
from sqlalchemy import delete
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

import app.tasks
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
from app.tasks import _download_sensor_data
from app.tasks import _sync_data_wrapper
from app.tasks import calculate_biomet
from app.tasks import calculate_temp_rh
from app.tasks import check_for_new_sensors
from app.tasks import download_station_data
from app.tasks import refresh_all_views
from app.tasks import self_test_integrity


@pytest.fixture
async def deployed_temprh_station(db: AsyncSession, clean_db: None) -> None:
    station = Station(
        station_id='DOTWFH',
        long_name='Westfalenhalle',
        latitude=51.49605,
        longitude=7.45847,
        altitude=0.0,
        station_type=StationType.temprh,
        leuchtennummer=0,
        district='44139',
        city='Dortmund',
        country='Germany',
        street='test-street',
        plz=12345,
    )
    db.add(station)
    sensor = Sensor(
        sensor_id='DEC1',
        device_id=11111,
        sensor_type=SensorType.sht35,
    )
    db.add(sensor)
    deployment = SensorDeployment(
        sensor_id='DEC1',
        station_id='DOTWFH',
        setup_date=datetime(2024, 9, 9, 0, 10, tzinfo=timezone.utc),
    )
    db.add(deployment)
    await db.commit()


@pytest.fixture
async def deployed_biomet_station(db: AsyncSession, clean_db: None) -> None:
    station = Station(
        station_id='DOBFRP',
        long_name='Friedensplatz',
        latitude=51.49605,
        longitude=7.45847,
        altitude=150,
        station_type=StationType.biomet,
        leuchtennummer=0,
        district='44139',
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
            device_id=22222,
            sensor_type=SensorType.blg,
        ),
    ]
    for s in sensors:
        db.add(s)

    deployments = [
        SensorDeployment(
            sensor_id='DEC1',
            station_id='DOBFRP',
            setup_date=datetime(2024, 9, 9, 0, 10, tzinfo=timezone.utc),
        ),
        SensorDeployment(
            sensor_id='DEC2',
            station_id='DOBFRP',
            setup_date=datetime(2024, 9, 9, 0, 15, tzinfo=timezone.utc),
        ),
    ]
    for d in deployments:
        db.add(d)

    await db.commit()


@pytest.mark.anyio
@pytest.mark.usefixtures('clean_db')
async def test_download_sensor_data_undeployed_sensor(db: AsyncSession) -> None:
    sensor = Sensor(
        sensor_id='DEC1',
        device_id=11111,
        sensor_type=SensorType.sht35,
    )
    db.add(sensor)
    await db.commit()
    r = await _download_sensor_data(sensor=sensor, target_table=SHT35DataRaw, con=db)
    assert r.empty is True
    assert_frame_equal(r, pd.DataFrame())


@pytest.mark.filterwarnings('ignore::RuntimeWarning')
@pytest.mark.anyio
@pytest.mark.usefixtures('deployed_temprh_station')
async def test_download_temp_rh_data_no_new_data(db: AsyncSession) -> None:
    station_data = SHT35DataRaw(
        sensor_id='DEC1',
        measured_at=datetime(2024, 9, 9, 0, 30, tzinfo=timezone.utc),
    )
    db.add(station_data)
    await db.commit()
    with mock.patch.object(
        ElementApi,
        'get_readings',
        return_value=pd.DataFrame(),
    ) as readings:
        await download_station_data('DOTWFH')
        readings.assert_called_once()
        assert readings.call_args == call(
            device_name='DEC1',
            sort='measured_at',
            sort_direction='asc',
            start=datetime(2024, 9, 9, 0, 30, 0, 1,  tzinfo=timezone.utc),
            as_dataframe=True,
            stream=True,
            timeout=120000,
        )
    # check if we have data in the database
    data_in_db = (
        await db.execute(
            select(SHT35DataRaw.measured_at).order_by(SHT35DataRaw.measured_at),
        )
    ).scalar_one()
    assert data_in_db == datetime(2024, 9, 9, 0, 30, tzinfo=timezone.utc)


@pytest.mark.filterwarnings('ignore::RuntimeWarning')
@pytest.mark.anyio
@pytest.mark.usefixtures('deployed_temprh_station')
async def test_download_temp_rh_data_no_data_in_db(db: AsyncSession) -> None:
    mock_data = pd.read_csv(
        'testing/DEC0054A4_data.csv',
        index_col='measured_at',
        parse_dates=['measured_at'],
    )

    with (
        mock.patch.object(ElementApi, 'get_readings', return_value=mock_data) as rd,
    ):
        await download_station_data('DOTWFH')
        rd.assert_called_once()
        assert rd.call_args == call(
            device_name='DEC1',
            sort='measured_at',
            sort_direction='asc',
            # this should take the exact date of the deployment
            start=datetime(2024, 9, 9, 0, 10, tzinfo=timezone.utc),
            as_dataframe=True,
            stream=True,
            timeout=120000,
        )

    # check if we have data in the database
    data_in_db = (
        await db.execute(
            select(SHT35DataRaw.measured_at).order_by(SHT35DataRaw.measured_at),
        )
    ).scalars().all()
    assert data_in_db == [
        datetime(2024, 9, 9, 0, 35, 14, 303520, tzinfo=timezone.utc),
        datetime(2024, 9, 9, 0, 40, 16, 11771, tzinfo=timezone.utc),
        datetime(2024, 9, 9, 0, 45, 19, 442665, tzinfo=timezone.utc),
    ]


@pytest.mark.filterwarnings('ignore::RuntimeWarning')
@pytest.mark.anyio
@pytest.mark.usefixtures('deployed_temprh_station')
async def test_download_temp_rh(db: AsyncSession) -> None:
    # add some data to the database, so we can check when to start
    station_data = SHT35DataRaw(
        sensor_id='DEC1',
        measured_at=datetime(2024, 9, 9, 0, 30, tzinfo=timezone.utc),
    )
    db.add(station_data)
    await db.commit()
    mock_data = pd.read_csv(
        'testing/DEC0054A4_data.csv',
        index_col='measured_at',
        parse_dates=['measured_at'],
    )
    with (
        mock.patch.object(ElementApi, 'get_readings', return_value=mock_data) as rd,
    ):
        await download_station_data('DOTWFH')
        rd.assert_called_once()
        assert rd.call_args == call(
            device_name='DEC1',
            sort='measured_at',
            sort_direction='asc',
            start=datetime(2024, 9, 9, 0, 30, 0, 1, tzinfo=timezone.utc),
            as_dataframe=True,
            stream=True,
            timeout=120000,
        )
    # check if we have data in the database
    data_in_db = (
        await db.execute(
            select(SHT35DataRaw.measured_at).order_by(SHT35DataRaw.measured_at),
        )
    ).scalars().all()
    assert data_in_db == [
        datetime(2024, 9, 9, 0, 30, tzinfo=timezone.utc),
        datetime(2024, 9, 9, 0, 35, 14, 303520, tzinfo=timezone.utc),
        datetime(2024, 9, 9, 0, 40, 16, 11771, tzinfo=timezone.utc),
        datetime(2024, 9, 9, 0, 45, 19, 442665, tzinfo=timezone.utc),
    ]


@pytest.mark.filterwarnings('ignore::RuntimeWarning')
@pytest.mark.anyio
@pytest.mark.usefixtures('deployed_biomet_station')
async def test_download_biomet_data_blg_and_atm_no_new_data(db: AsyncSession) -> None:
    station_data = ATM41DataRaw(
        sensor_id='DEC1',
        measured_at=datetime(2024, 9, 9, 0, 30, tzinfo=timezone.utc),
    )
    db.add(station_data)
    await db.commit()

    with mock.patch.object(
        ElementApi,
        'get_readings',
        return_value=pd.DataFrame(),
    ) as readings:
        await download_station_data('DOBFRP')
        assert readings.call_count == 2
        assert readings.call_args_list[0] == call(
            device_name='DEC1',
            sort='measured_at',
            sort_direction='asc',
            # determined by the latest data
            start=datetime(2024, 9, 9, 0, 30, 0, 1, tzinfo=timezone.utc),
            as_dataframe=True,
            stream=True,
            timeout=120000,
        )
        assert readings.call_args_list[1] == call(
            device_name='DEC2',
            sort='measured_at',
            sort_direction='asc',
            # there was no blackglobe data in the db, hence determined by setup_date
            start=datetime(2024, 9, 9, 0, 15, tzinfo=timezone.utc),
            as_dataframe=True,
            stream=True,
            timeout=120000,
        )

    # check if we have data in the database
    biomet_data_in_db = (
        await db.execute(
            select(ATM41DataRaw.measured_at).order_by(ATM41DataRaw.measured_at),
        )
    ).scalar_one()
    # there was and still is no blg data in the db
    blg_data_in_db = (await db.execute(select(BLGDataRaw.measured_at))).scalars().all()
    assert len(blg_data_in_db) == 0
    assert biomet_data_in_db == datetime(2024, 9, 9, 0, 30, tzinfo=timezone.utc)


@pytest.mark.filterwarnings('ignore::RuntimeWarning')
@pytest.mark.anyio
@pytest.mark.usefixtures('deployed_biomet_station')
async def test_download_biomet_data_blg_and_atm_no_data_in_db(db: AsyncSession) -> None:
    mock_data_biomet = pd.read_csv(
        'testing/DEC00546D_data.csv',
        index_col='measured_at',
        parse_dates=['measured_at'],
    )
    mock_data_blg = pd.read_csv(
        'testing/DEC005491_data.csv',
        index_col='measured_at',
        parse_dates=['measured_at'],
    )

    with (
        mock.patch.object(
            ElementApi,
            'get_readings',
            side_effect=[mock_data_biomet, mock_data_blg],
        ) as rd,
    ):
        await download_station_data('DOBFRP')
        assert rd.call_count == 2
        assert rd.call_args_list[0] == call(
            device_name='DEC1',
            sort='measured_at',
            sort_direction='asc',
            # this comes via the setup date
            start=datetime(2024, 9, 9, 0, 10, tzinfo=timezone.utc),
            as_dataframe=True,
            stream=True,
            timeout=120000,
        )
        assert rd.call_args_list[1] == call(
            device_name='DEC2',
            sort='measured_at',
            sort_direction='asc',
            start=datetime(2024, 9, 9, 0, 15, tzinfo=timezone.utc),
            as_dataframe=True,
            stream=True,
            timeout=120000,
        )

    # check if we have data in the database
    biomet_data_in_db = (
        await db.execute(
            select(ATM41DataRaw.measured_at).order_by(ATM41DataRaw.measured_at),
        )
    ).scalars().all()
    assert biomet_data_in_db == [
        datetime(2024, 9, 9, 0, 38, 53, 112530, tzinfo=timezone.utc),
        datetime(2024, 9, 9, 0, 43, 47, 831474, tzinfo=timezone.utc),
        datetime(2024, 9, 9, 0, 48, 49, 117131, tzinfo=timezone.utc),
        datetime(2024, 9, 9, 0, 53, 54, 713730, tzinfo=timezone.utc),
    ]
    # there was and still is no blg data in the db
    blg_data_in_db = (await db.execute(select(BLGDataRaw.measured_at))).scalars().all()
    assert blg_data_in_db == [
        datetime(2024, 9, 9, 0, 36, 1, 236129, tzinfo=timezone.utc),
        datetime(2024, 9, 9, 0, 41, 5, 574698, tzinfo=timezone.utc),
        datetime(2024, 9, 9, 0, 46, 4, 942636, tzinfo=timezone.utc),
        datetime(2024, 9, 9, 0, 51, 2, 980163, tzinfo=timezone.utc),
        datetime(2024, 9, 9, 0, 56, 2, 416326, tzinfo=timezone.utc),
    ]


@pytest.mark.anyio
@pytest.mark.usefixtures('clean_db')
@pytest.mark.filterwarnings('ignore::RuntimeWarning')
async def test_download_station_data_bug_too_few_deployments(
        db: AsyncSession,
) -> None:
    """this was a bug found in production where not all needed deployments were
    extracted due to a wrong AND/OR condition in the sql-query.
    """
    stations = [
        Station(
            station_id='DOBHAP',
            long_name='Hansaplatz',
            latitude=51.5131622588889,
            longitude=7.464264400429746,
            altitude=87.38,
            station_type=StationType.biomet,
            leuchtennummer=0,
            district='Mitte',
            city='Dortmund',
            country='Germany',
            street='Hansaplatz',
            plz=44137,
        ),
        Station(
            station_id='DOTABC',
            long_name='Abc',
            latitude=51.5131622588889,
            longitude=7.464264400429746,
            altitude=87.38,
            station_type=StationType.temprh,
            leuchtennummer=0,
            district='Mitte',
            city='Dortmund',
            country='Germany',
            street='Abc',
            plz=44137,
        ),
    ]
    for station in stations:
        db.add(station)
    sensors = [
        Sensor(
            sensor_id='DEC005475',
            device_id=21621,
            sensor_type=SensorType.atm41,
        ),
        Sensor(
            sensor_id='DEC005305',
            device_id=21253,
            sensor_type=SensorType.atm41,
        ),
        Sensor(
            sensor_id='DEC00548B',
            device_id=21643,
            sensor_type=SensorType.blg,
        ),
        Sensor(
            sensor_id='DEC1234AB',
            device_id=123456,
            sensor_type=SensorType.sht35,
        ),
    ]
    for s in sensors:
        db.add(s)
    deployments = [
        SensorDeployment(
            deployment_id=4,
            sensor_id='DEC005475',
            station_id='DOBHAP',
            setup_date=datetime(2024, 8, 5, 23, 59, tzinfo=timezone.utc),
            teardown_date=datetime(2025, 1, 31, 11, 20, tzinfo=timezone.utc),
        ),
        SensorDeployment(
            deployment_id=89,
            sensor_id='DEC00548B',
            station_id='DOBHAP',
            setup_date=datetime(2024, 8, 5, 0, 0, tzinfo=timezone.utc),
        ),
        SensorDeployment(
            deployment_id=88,
            sensor_id='DEC005305',
            station_id='DOBHAP',
            setup_date=datetime(2025, 1, 31, 0, 0, tzinfo=timezone.utc),
        ),
        # add an unrelated deployment
        SensorDeployment(
            deployment_id=1,
            sensor_id='DEC1234AB',
            station_id='DOTABC',
            setup_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
        ),
    ]
    for dp in deployments:
        db.add(dp)

    data = [
        BLGDataRaw(
            measured_at=datetime(2025, 3, 20, 18, 17, 14, 975152, tzinfo=timezone.utc),
            sensor_id='DEC00548B',
        ),
        ATM41DataRaw(
            measured_at=datetime(2025, 1, 23, 12, 44, 30, 315041, tzinfo=timezone.utc),
            sensor_id='DEC005475',
        ),
        BiometData(
            station_id='DOBHAP',
            sensor_id='DEC005475',
            blg_sensor_id='DEC00548B',
            measured_at=datetime(2025, 1, 8, 14, 23, 8, 763975, tzinfo=timezone.utc),
        ),
    ]
    for d in data:
        db.add(d)

    await db.commit()

    # 1. blg
    mock_data_blg = pd.read_csv(
        'testing/DEC00548B_blg_data.csv',
        index_col='measured_at',
        parse_dates=['measured_at'],
    )
    mock_data_atm41 = pd.read_csv(
        'testing/DEC005305_atm41.csv',
        index_col='measured_at',
        parse_dates=['measured_at'],
    )
    with (
        mock.patch.object(
            ElementApi,
            'get_readings',
            side_effect=[
                mock_data_blg, pd.DataFrame(), mock_data_atm41,
                pd.DataFrame(),
            ],
        ) as rd,
    ):
        await download_station_data('DOBHAP')

        assert rd.call_count == 3
        # first the blackglobe
        assert rd.call_args_list[0] == call(
            device_name='DEC00548B',
            sort='measured_at',
            sort_direction='asc',
            start=datetime(2025, 3, 20, 18, 17, 14, 975153, tzinfo=timezone.utc),
            as_dataframe=True,
            stream=True,
            timeout=120000,
        )
        # next the torn down atm41 sensor
        assert rd.call_args_list[1] == call(
            device_name='DEC005475',
            sort='measured_at',
            sort_direction='asc',
            start=datetime(2025, 1, 23, 12, 44, 30, 315042, tzinfo=timezone.utc),
            as_dataframe=True,
            stream=True,
            timeout=120000,
        )
        # lastly the new atm41 sensor should be called?
        assert rd.call_args_list[2] == call(
            device_name='DEC005305',
            sort='measured_at',
            sort_direction='asc',
            # determined by the measurement the setup date of the sensor
            start=datetime(2025, 1, 31, 0, 0, tzinfo=timezone.utc),
            as_dataframe=True,
            stream=True,
            timeout=120000,
        )
    # check if we have data in the database
    biomet_data_in_db = (
        await db.execute(
            select(ATM41DataRaw.measured_at).order_by(ATM41DataRaw.measured_at),
        )
    ).scalars().all()

    assert biomet_data_in_db == [
        datetime(2025, 1, 23, 12, 44, 30, 315041, tzinfo=timezone.utc),
        datetime(2025, 1, 31, 0, 21, 35, 359165, tzinfo=timezone.utc),
        datetime(2025, 3, 20, 19, 36, 54, 692829, tzinfo=timezone.utc),
    ]
    # we also got new blackglobe data
    blg_data_in_db = (
        await db.execute(
            select(BLGDataRaw.measured_at).order_by(BLGDataRaw.measured_at),
        )
    ).scalars().all()
    assert blg_data_in_db == [
        datetime(2025, 3, 20, 18, 17, 14, 975152, tzinfo=timezone.utc),
        datetime(2025, 3, 20, 18, 22, 15, 757524, tzinfo=timezone.utc),
        datetime(2025, 3, 20, 19, 37, 12, 236561, tzinfo=timezone.utc),
    ]
    # let's calculate the data
    await calculate_biomet('DOBHAP')
    # check if we have data in the database
    final_biomet_data_in_db = (
        await db.execute(
            select(BiometData.measured_at).order_by(BiometData.measured_at),
        )
    ).scalars().all()
    assert final_biomet_data_in_db == [
        datetime(2025, 1, 8, 14, 23, 8, 763975, tzinfo=timezone.utc),
        datetime(2025, 1, 23, 12, 44, 30, 315041, tzinfo=timezone.utc),
        datetime(2025, 1, 31, 0, 21, 35, 359165, tzinfo=timezone.utc),
        datetime(2025, 3, 20, 19, 36, 54, 692829, tzinfo=timezone.utc),
    ]


@pytest.mark.filterwarnings('ignore::RuntimeWarning')
@pytest.mark.anyio
@pytest.mark.usefixtures('deployed_biomet_station')
async def test_download_biomet_data(db: AsyncSession) -> None:
    # add some data to the database
    atm_41_data = ATM41DataRaw(
        sensor_id='DEC1',
        measured_at=datetime(2024, 9, 9, 0, 40, tzinfo=timezone.utc),
    )
    db.add(atm_41_data)
    blg_data = BLGDataRaw(
        sensor_id='DEC2',
        measured_at=datetime(2024, 9, 9, 0, 38, tzinfo=timezone.utc),
    )
    db.add(blg_data)
    await db.commit()

    mock_data_biomet = pd.read_csv(
        'testing/DEC00546D_data.csv',
        index_col='measured_at',
        parse_dates=['measured_at'],
    )
    mock_data_blg = pd.read_csv(
        'testing/DEC005491_data.csv',
        index_col='measured_at',
        parse_dates=['measured_at'],
    )

    with (
        mock.patch.object(
            ElementApi,
            'get_readings',
            side_effect=[mock_data_biomet, mock_data_blg],
        ) as rd,
    ):
        await download_station_data('DOBFRP')
        assert rd.call_count == 2
        assert rd.call_args_list[0] == call(
            device_name='DEC1',
            sort='measured_at',
            sort_direction='asc',
            start=datetime(2024, 9, 9, 0, 40, 0, 1, tzinfo=timezone.utc),
            as_dataframe=True,
            stream=True,
            timeout=120000,
        )
        assert rd.call_args_list[1] == call(
            device_name='DEC2',
            sort='measured_at',
            sort_direction='asc',
            start=datetime(2024, 9, 9, 0, 38, 0, 1, tzinfo=timezone.utc),
            as_dataframe=True,
            stream=True,
            timeout=120000,
        )

    # check if we have data in the database
    biomet_data_in_db = (
        await db.execute(
            select(ATM41DataRaw.measured_at).order_by(ATM41DataRaw.measured_at),
        )
    ).scalars().all()
    assert biomet_data_in_db == [
        (datetime(2024, 9, 9, 0, 40, tzinfo=timezone.utc)),
        (datetime(2024, 9, 9, 0, 43, 47, 831474, tzinfo=timezone.utc)),
        (datetime(2024, 9, 9, 0, 48, 49, 117131, tzinfo=timezone.utc)),
        (datetime(2024, 9, 9, 0, 53, 54, 713730, tzinfo=timezone.utc)),
    ]
    # there was and still is no blg data in the db
    blg_data_in_db = (await db.execute(select(BLGDataRaw.measured_at))).scalars().all()
    assert blg_data_in_db == [
        (datetime(2024, 9, 9, 0, 38, tzinfo=timezone.utc)),
        (datetime(2024, 9, 9, 0, 41, 5, 574698, tzinfo=timezone.utc)),
        (datetime(2024, 9, 9, 0, 46, 4, 942636, tzinfo=timezone.utc)),
        (datetime(2024, 9, 9, 0, 51, 2, 980163, tzinfo=timezone.utc)),
        (datetime(2024, 9, 9, 0, 56, 2, 416326, tzinfo=timezone.utc)),
    ]


@pytest.mark.anyio
@pytest.mark.usefixtures('deployed_temprh_station')
async def test_calculate_temp_rh_no_data_in_db(db: AsyncSession) -> None:
    await calculate_temp_rh('DOTWFH')
    # check nothing was inserted
    data = (await db.execute(select(TempRHData))).all()
    assert len(data) == 0


@pytest.mark.anyio
@pytest.mark.usefixtures('clean_db')
async def test_calculate_values_for_double_stations(db: AsyncSession) -> None:
    station = Station(
        station_id='DODSTH',
        long_name='Some Station',
        latitude=51.49605,
        longitude=7.45847,
        altitude=0.0,
        station_type=StationType.double,
        leuchtennummer=0,
        district='44139',
        city='Dortmund',
        country='Germany',
        street='test-street',
        plz=12345,
    )
    db.add(station)
    await db.commit()
    await calculate_temp_rh('DODSTH')
    await calculate_biomet('DODSTH')
    # check nothing was inserted
    temprh_data = (await db.execute(select(TempRHData))).all()
    assert len(temprh_data) == 0
    biomet_data = (await db.execute(select(BiometData))).all()
    assert len(biomet_data) == 0


@pytest.mark.anyio
@pytest.mark.usefixtures('clean_db')
@pytest.mark.filterwarnings('ignore::RuntimeWarning')
async def test_calculate_temp_rh_no_data_in_final_table_but_data_available(
        db: AsyncSession,
) -> None:
    station = Station(
        station_id='DOTWFH',
        long_name='Westfalenhalle',
        latitude=51.49605,
        longitude=7.45847,
        altitude=0.0,
        station_type=StationType.temprh,
        leuchtennummer=0,
        district='44139',
        city='Dortmund',
        country='Germany',
        street='test-street',
        plz=12345,
    )
    db.add(station)
    # 2nd station (which should not be touched)
    station_2 = Station(
        station_id='DOTWFP',
        long_name='Westfalenhalle',
        latitude=51.49605,
        longitude=7.45847,
        altitude=0.0,
        station_type=StationType.temprh,
        leuchtennummer=0,
        district='44139',
        city='Dortmund',
        country='Germany',
        street='test-street',
        plz=12345,
    )
    db.add(station_2)
    # add sensors to the stations
    sensors = [
        Sensor(
            sensor_id='DEC0054A4',
            device_id=11111,
            sensor_type=SensorType.sht35,
            temp_calib_offset=1,
            relhum_calib_offset=-2,
        ),
        Sensor(
            sensor_id='DEC0054A5',
            device_id=22222,
            sensor_type=SensorType.sht35,
        ),
    ]
    for s in sensors:
        db.add(s)
    deployments = [
        SensorDeployment(
            sensor_id='DEC0054A4',
            station_id='DOTWFH',
            setup_date=datetime(2024, 9, 9, 0, 10, tzinfo=timezone.utc),
        ),
        SensorDeployment(
            sensor_id='DEC0054A5',
            station_id='DOTWFP',
            setup_date=datetime(2024, 9, 9, 0, 10, tzinfo=timezone.utc),
        ),
    ]
    for deployment in deployments:
        db.add(deployment)
    # add some data where the calibration pushes the relative humidity > 100
    station_data = SHT35DataRaw(
        sensor_id='DEC0054A4',
        measured_at=datetime(2024, 9, 10, 5, 25, tzinfo=timezone.utc),
        relative_humidity=99.5,
    )
    db.add(station_data)

    # some data that should not be touched, since it's from the other station, that has
    # no calibration associated with it
    data_2 = SHT35DataRaw(
        sensor_id='DEC0054A5',
        measured_at=datetime(2024, 9, 10, 5, 20, tzinfo=timezone.utc),
        air_temperature=10.5,
        relative_humidity=65.5,
    )
    db.add(data_2)
    await db.commit()
    # add some data
    con = await db.connection()
    temp_rh_data = pd.read_csv(
        'testing/SHT35_DEC0054A4_raw_data.csv',
        index_col='measured_at',
        parse_dates=['measured_at'],
    )
    await con.run_sync(
        lambda con: temp_rh_data.to_sql(
            name=SHT35DataRaw.__tablename__,
            con=con,
            if_exists='append',
        ),
    )
    await db.commit()
    await calculate_temp_rh('DOTWFH')
    # check something was inserted
    data = (
        await db.execute(
            select(TempRHData).order_by(TempRHData.measured_at),
        )
    ).scalars().all()
    # 4 rows from csv + 2 rows created manually, where we only use one of them
    # since we only calculate for DOTWFH
    assert len(data) == 5
    d = data[0]
    assert d.measured_at == datetime(
        2024, 9, 10, 5, 5, 15, 858960, tzinfo=timezone.utc,
    )
    assert d.sensor_id == 'DEC0054A4'
    # test all calculations
    assert float(d.air_temperature_raw) == pytest.approx(12.47, abs=1e-2)
    assert float(d.relative_humidity_raw) == pytest.approx(85.62, abs=1e-2)
    # make sure the calibration offset is applied
    assert float(d.air_temperature) == pytest.approx(11.47, abs=1e-2)
    assert float(d.relative_humidity) == pytest.approx(87.62, abs=1e-2)

    # other simply passthrough values
    assert float(d.battery_voltage) == 3.056
    assert float(d.protocol_version) == 2

    # some applied calculations must be correct
    assert float(d.dew_point) == pytest.approx(9.48, abs=1e-2)
    assert float(d.absolute_humidity) == pytest.approx(9.01, abs=1e-2)
    assert float(d.specific_humidity) == pytest.approx(7.3, abs=1e-2)
    assert float(d.heat_index) == pytest.approx(10.96, abs=1e-2)
    assert float(d.wet_bulb_temperature) == pytest.approx(10.1, abs=1e-1)

    # the calibration would have pushed it > 100, make sure we manually set it back
    assert data[-1].relative_humidity == 100


@pytest.mark.anyio
@pytest.mark.usefixtures('clean_db')
@pytest.mark.filterwarnings('ignore::RuntimeWarning')
async def test_calculate_temp_rh_previous_data_exists_in_final_table(
        db: AsyncSession,
) -> None:
    station = Station(
        station_id='DOTWFH',
        long_name='Westfalenhalle',
        latitude=51.49605,
        longitude=7.45847,
        altitude=0.0,
        station_type=StationType.temprh,
        leuchtennummer=0,
        district='44139',
        city='Dortmund',
        country='Germany',
        street='test-street',
        plz=12345,
    )
    db.add(station)
    # 2nd station (which should not be touched)
    station_2 = Station(
        station_id='DOTWFP',
        long_name='Westfalenhalle',
        latitude=51.49605,
        longitude=7.45847,
        altitude=0.0,
        station_type=StationType.temprh,
        leuchtennummer=0,
        district='44139',
        city='Dortmund',
        country='Germany',
        street='test-street',
        plz=12345,
    )
    db.add(station_2)
    # add sensors to the stations
    sensors = [
        Sensor(
            sensor_id='DEC0054A4',
            device_id=11111,
            sensor_type=SensorType.sht35,
            temp_calib_offset=1,
            relhum_calib_offset=-2,
        ),
        Sensor(
            sensor_id='DEC0054A5',
            device_id=22222,
            sensor_type=SensorType.sht35,
        ),
    ]
    for s in sensors:
        db.add(s)
    deployments = [
        SensorDeployment(
            sensor_id='DEC0054A4',
            station_id='DOTWFH',
            setup_date=datetime(2024, 9, 9, 0, 10, tzinfo=timezone.utc),
        ),
        SensorDeployment(
            sensor_id='DEC0054A5',
            station_id='DOTWFP',
            setup_date=datetime(2024, 9, 9, 0, 10, tzinfo=timezone.utc),
        ),
    ]
    for d in deployments:
        db.add(d)

    data = TempRHData(
        station_id='DOTWFH',
        sensor_id='DEC0054A4',
        measured_at=datetime(2024, 9, 10, 5, 17, tzinfo=timezone.utc),
    )
    db.add(data)
    data_2 = TempRHData(
        station_id='DOTWFP',
        sensor_id='DEC0054A5',
        measured_at=datetime(2024, 9, 10, 5, 30, tzinfo=timezone.utc),
    )
    db.add(data_2)
    await db.commit()
    # add some data
    con = await db.connection()
    temp_rh_data = pd.read_csv(
        'testing/SHT35_DEC0054A4_raw_data.csv',
        index_col='measured_at',
        parse_dates=['measured_at'],
    )
    await con.run_sync(
        lambda con: temp_rh_data.to_sql(
            name=SHT35DataRaw.__tablename__,
            con=con,
            if_exists='append',
        ),
    )
    await db.commit()
    await calculate_temp_rh('DOTWFH')
    # check something was inserted
    ret_data = (
        await db.execute(
            select(TempRHData).order_by(TempRHData.measured_at, TempRHData.sensor_id),
        )
    ).scalars().all()
    assert len(ret_data) == 3
    # this was already there
    assert ret_data[0].measured_at == datetime(2024, 9, 10, 5, 17, tzinfo=timezone.utc)
    assert ret_data[0].sensor_id == 'DEC0054A4'
    # this was inserted
    assert ret_data[1].measured_at == datetime(
        2024, 9, 10, 5, 20, 17, 549835, tzinfo=timezone.utc,
    )
    assert ret_data[1].sensor_id == 'DEC0054A4'
    # last station?
    assert ret_data[2].sensor_id == 'DEC0054A5'


@pytest.mark.anyio
@pytest.mark.usefixtures('deployed_biomet_station')
async def test_calculate_biomet_no_data_available(db: AsyncSession) -> None:
    await calculate_biomet('DOBFRP')
    # check nothing was inserted into the database
    data = (await db.execute(select(BiometData))).all()
    assert len(data) == 0


@pytest.mark.anyio
@pytest.mark.filterwarnings('ignore::RuntimeWarning')
@pytest.mark.usefixtures('deployed_biomet_station')
async def test_calculate_biomet_only_null_values_available(db: AsyncSession) -> None:
    atm_data = ATM41DataRaw(
        sensor_id='DEC1',
        measured_at=datetime(2024, 9, 10, 5, 15, tzinfo=timezone.utc),
        air_temperature=None,
        battery_voltage=3.178,
        protocol_version=2,
    )
    blg_data = BLGDataRaw(
        sensor_id='DEC2',
        measured_at=datetime(2024, 9, 10, 5, 15, tzinfo=timezone.utc),
        black_globe_temperature=30.5,
    )
    db.add(atm_data)
    db.add(blg_data)
    await db.commit()
    # check that we can perform our calculation even though we're missing most data
    await calculate_biomet('DOBFRP')
    # check a single row was inserted into the database
    data = (await db.execute(select(BiometData))).all()
    assert len(data) == 1


@pytest.mark.filterwarnings('ignore::RuntimeWarning')
@pytest.mark.anyio
@pytest.mark.usefixtures('deployed_biomet_station')
async def test_calculate_biomet_only_atm41_data_available(db: AsyncSession) -> None:
    con = await db.connection()
    temp_rh_data = pd.read_csv(
        'testing/ATM41_DEC00546D_raw_data.csv',
        index_col='measured_at',
        parse_dates=['measured_at'],
    )
    await con.run_sync(
        lambda con: temp_rh_data.to_sql(
            name=ATM41DataRaw.__tablename__,
            con=con,
            if_exists='append',
        ),
    )
    await db.commit()
    await calculate_biomet('DOBFRP')
    # check nothing was inserted into the database, because there was no blg data
    data = (await db.execute(select(BiometData))).all()
    assert len(data) == 0


@pytest.mark.anyio
@pytest.mark.usefixtures('deployed_biomet_station')
async def test_calculate_biomet_both_data_present(db: AsyncSession) -> None:
    # add some data
    biomet_existing_data = BiometData(
        station_id='DOBFRP',
        sensor_id='DEC1',
        measured_at=datetime(2024, 9, 10, 5, 15, tzinfo=timezone.utc),
    )
    db.add(biomet_existing_data)
    await db.commit()

    con = await db.connection()
    biomet_data = pd.read_csv(
        'testing/ATM41_DEC00546D_raw_data.csv',
        index_col='measured_at',
        parse_dates=['measured_at'],
    )
    blg_data = pd.read_csv(
        'testing/BLG_DEC005491_raw_data.csv',
        index_col='measured_at',
        parse_dates=['measured_at'],
    )
    await con.run_sync(
        lambda con: biomet_data.to_sql(
            name=ATM41DataRaw.__tablename__,
            con=con,
            if_exists='append',
        ),
    )
    await con.run_sync(
        lambda con: blg_data.to_sql(
            name=BLGDataRaw.__tablename__,
            con=con,
            if_exists='append',
        ),
    )
    await db.commit()

    await calculate_biomet('DOBFRP')
    # check nothing was inserted into the database, because there was no blg data
    data = (
        await db.execute(select(BiometData).order_by(BiometData.measured_at))
    ).scalars().all()
    assert len(data) == 2
    # the value we set as the latest
    assert data[0].measured_at == datetime(2024, 9, 10, 5, 15, tzinfo=timezone.utc)
    # check all calculations for the other value
    d = data[1]
    assert d.measured_at == datetime(2024, 9, 10, 5, 18, 52, 21037, tzinfo=timezone.utc)
    assert d.station_id == 'DOBFRP'
    assert d.sensor_id == 'DEC1'
    # check offset is correct
    # atm41 date: 2024-09-10 05:18:52.021037
    # blg date: 2024-09-10 05:20:59.092966+00
    assert float(d.blg_time_offset) == -127.071929
    # check the calculations
    assert float(d.black_globe_temperature) == pytest.approx(12.57, abs=10e-2)
    assert float(d.air_temperature) == 12.5
    assert float(d.wind_speed) == 1.66
    assert float(d.mrt) == pytest.approx(12.8, abs=10e-2)

    assert float(d.relative_humidity) == 87.1
    assert float(d.utci) == pytest.approx(12.00, abs=10e-2)
    assert d.utci_category == HeatStressCategories.no_thermal_stress

    assert float(d.pet) == pytest.approx(8.33, abs=10e-2)
    assert d.pet_category == HeatStressCategories.moderate_cold_stress

    assert float(d.atmospheric_pressure) == 996.9
    assert float(d.atmospheric_pressure_reduced) == pytest.approx(1014.79, abs=1e-2)
    assert float(d.vapor_pressure) == 12.6

    assert float(d.wind_direction) == 207.8
    assert float(d.u_wind) == -0.78
    assert float(d.v_wind) == -1.47
    assert float(d.maximum_wind_speed) == 3.13
    assert float(d.precipitation_sum) == 0
    assert float(d.solar_radiation) == 5
    assert float(d.lightning_average_distance) == 0
    assert float(d.lightning_strike_count) == 0
    assert float(d.sensor_temperature_internal) == 12.1
    assert float(d.x_orientation_angle) == -0.3
    assert float(d.y_orientation_angle) == -0.6
    assert float(d.black_globe_temperature) == pytest.approx(12.57, abs=1e-2)
    assert float(d.thermistor_resistance) == pytest.approx(182448.92, abs=1e-2)
    assert float(d.voltage_ratio) == pytest.approx(0.00447529554367065)
    assert float(d.battery_voltage) == 2.866
    assert float(d.blg_battery_voltage) == 3.039
    assert float(d.protocol_version) == 2
    assert float(d.dew_point) == pytest.approx(10.41, abs=1e-2)
    assert float(d.absolute_humidity) == pytest.approx(9.55, abs=1e-2)
    assert float(d.specific_humidity) == pytest.approx(7.89, abs=1e-2)
    assert float(d.heat_index) == pytest.approx(12.07, abs=1e-2)
    assert float(d.wet_bulb_temperature) == pytest.approx(11.1, abs=1e-1)


@pytest.mark.anyio
@pytest.mark.usefixtures('deployed_biomet_station')
@pytest.mark.filterwarnings('ignore::RuntimeWarning')
async def test_calculate_biomet_blg_exceeds_join_tolerance(db: AsyncSession) -> None:
    atm41_data = ATM41DataRaw(
        sensor_id='DEC1',
        measured_at=datetime(2024, 9, 10, 0, tzinfo=timezone.utc),
    )
    db.add(atm41_data)
    blg_data = BLGDataRaw(
        sensor_id='DEC2',
        measured_at=datetime(2024, 9, 10, 0, 5, 1, tzinfo=timezone.utc),
    )
    db.add(blg_data)
    await db.commit()

    await calculate_biomet('DOBFRP')
    data = (await db.execute(select(BiometData))).all()
    assert len(data) == 0


@pytest.mark.filterwarnings('ignore::RuntimeWarning')
@pytest.mark.anyio
@pytest.mark.usefixtures('clean_db')
async def test_wrapper_for_update(db: AsyncSession) -> None:
    temp_rh_station = Station(
        station_id='DOTWFH',
        long_name='Westfalenhalle',
        latitude=51.49605,
        longitude=7.45847,
        altitude=0.0,
        station_type=StationType.temprh,
        leuchtennummer=0,
        district='44139',
        city='Dortmund',
        country='Germany',
        street='test-street',
        plz=12345,
    )
    db.add(temp_rh_station)
    biomet_station = Station(
        station_id='DOTWFP',
        long_name='Westfalenhalle',
        latitude=51.49626168307185,
        longitude=7.458186573064577,
        altitude=150,
        station_type=StationType.biomet,
        leuchtennummer=0,
        district='44139',
        city='Dortmund',
        country='Germany',
        street='test-street',
        plz=12345,
    )
    db.add(biomet_station)
    double_station = Station(
        station_id='DOTHPL',
        long_name='Hansaplatz',
        latitude=51.5,
        longitude=7.5,
        altitude=0,
        station_type=StationType.double,
        leuchtennummer=0,
        district='44139',
        city='Dortmund',
        country='Germany',
        street='test-street',
        plz=12345,
    )
    db.add(double_station)
    await db.commit()

    with (
        mock.patch.object(app.tasks, 'download_station_data') as dl_data,
        mock.patch.object(app.tasks, 'refresh_all_views'),
        mock.patch.object(app.tasks, 'chord') as chord,
        mock.patch.object(app.tasks, 'chain'),
        mock.patch.object(app.tasks, 'calculate_temp_rh') as calc_temp_rh,
        mock.patch.object(app.tasks, 'calculate_biomet') as calc_biomet,
    ):
        # the underlying functions are never awaited, which causes a resource warning we
        # ignore here since it's not part of the test. AFAIK there is no way to avoid
        # this.
        await _sync_data_wrapper()
        assert dl_data.s.call_count == 3
        assert dl_data.s.call_args_list[0] == call('DOTHPL')
        assert dl_data.s.call_args_list[1] == call('DOTWFH')
        assert dl_data.s.call_args_list[2] == call('DOTWFP')
        # the calculation functions
        # once for the temprh station, once for the double station
        assert calc_temp_rh.s.call_count == 2
        assert calc_biomet.s.call_count == 2
        chord.assert_called_once()


@pytest.mark.parametrize('c', (calculate_biomet, calculate_temp_rh))
@pytest.mark.anyio
async def test_calculation_task_none_passed(
        c: Callable[[None | str], Awaitable[None | str]],
) -> None:
    res = await c(None)
    assert res is None


@pytest.mark.usefixtures('clean_db')
@pytest.mark.anyio
async def test_self_test_integrity_fails_on_duped_deployment(db: AsyncSession) -> None:
    # create a structure to test
    stations = [
        Station(
            station_id='DOT1',
            long_name='temprh-station-1',
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
        ),
        Station(
            station_id='DOT2',
            long_name='temprh-station-2',
            latitude=51.547,
            longitude=7.368,
            altitude=100,
            station_type=StationType.temprh,
            leuchtennummer=121,
            district='Other District',
            city='Dortmund',
            country='Germany',
            street='other-test-street',
            plz=12345,
        ),
    ]
    for s in stations:
        db.add(s)

    sensor = Sensor(
        sensor_id='DEC1',
        device_id=11111,
        sensor_type=SensorType.sht35,
    )
    db.add(sensor)
    # now deploy the sensor at two statiosn
    deployments = [
        SensorDeployment(
            deployment_id=1,
            sensor_id='DEC1',
            station_id='DOT1',
            setup_date=datetime(2023, 1, 1, tzinfo=timezone.utc),
        ),
        SensorDeployment(
            deployment_id=2,
            sensor_id='DEC1',
            station_id='DOT2',
            setup_date=datetime(2023, 1, 1, tzinfo=timezone.utc),
        ),
    ]
    for d in deployments:
        db.add(d)
    await db.commit()

    with pytest.raises(ValueError) as exc_info:
        await self_test_integrity()
    assert exc_info.value.args[0] == (
        'Found duplicate sensor deployments affecting theses sensor(s): DEC1'
    )


@pytest.mark.usefixtures('clean_db')
@pytest.mark.anyio
async def test_self_test_integrity_ok(db: AsyncSession) -> None:
    # create a structure to test
    stations = [
        Station(
            station_id='DOT1',
            long_name='temprh-station-1',
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
        ),
        Station(
            station_id='DOT2',
            long_name='temprh-station-2',
            latitude=51.547,
            longitude=7.368,
            altitude=100,
            station_type=StationType.temprh,
            leuchtennummer=121,
            district='Other District',
            city='Dortmund',
            country='Germany',
            street='other-test-street',
            plz=12345,
        ),
    ]
    for s in stations:
        db.add(s)

    sensors = [
        Sensor(
            sensor_id='DEC1',
            device_id=11111,
            sensor_type=SensorType.sht35,
        ),
        Sensor(
            sensor_id='DEC2',
            device_id=22222,
            sensor_type=SensorType.sht35,
        ),
    ]
    for sensor in sensors:
        db.add(sensor)

    # now deploy the sensor at two statiosn
    deployments = [
        SensorDeployment(
            deployment_id=1,
            sensor_id='DEC1',
            station_id='DOT1',
            setup_date=datetime(2023, 1, 1, tzinfo=timezone.utc),
        ),
        SensorDeployment(
            deployment_id=2,
            sensor_id='DEC2',
            station_id='DOT2',
            setup_date=datetime(2023, 1, 1, tzinfo=timezone.utc),
        ),
    ]
    for d in deployments:
        db.add(d)
    await db.commit()

    # no error etc.
    assert await self_test_integrity() is None


@pytest.mark.usefixtures('clean_db')
@pytest.mark.anyio
async def test_refresh_all_views(db: AsyncSession) -> None:
    # add some data
    stations = [
        Station(
            station_id='DOTWFH',
            long_name='Westfalenhalle',
            latitude=51.49605,
            longitude=7.45847,
            altitude=0.0,
            station_type=StationType.temprh,
            leuchtennummer=0,
            district='44139',
            city='Dortmund',
            country='Germany',
            street='test-street',
            plz=12345,
        ),
        Station(
            station_id='DOBFRP',
            long_name='Friedensplatz',
            latitude=51.39605,
            longitude=7.25847,
            altitude=0.0,
            station_type=StationType.biomet,
            leuchtennummer=0,
            district='44139',
            city='Dortmund',
            country='Germany',
            street='test-street',
            plz=12345,
        ),
    ]
    for s in stations:
        db.add(s)

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
    ]
    for sensor in sensors:
        db.add(sensor)

    start = datetime(2025, 1, 1, 0, 5)
    for i in range(287):
        db.add(
            BiometData(
                measured_at=start + (i * timedelta(minutes=5)),
                station_id='DOBFRP',
                sensor_id='DEC2',
                blg_sensor_id='DEC3',
                air_temperature=10.5,
            ),
        )
        db.add(
            TempRHData(
                measured_at=start + (i * timedelta(minutes=5)),
                station_id='DOTWFH',
                sensor_id='DEC1',
                air_temperature=12.5,
            ),
        )
    await db.commit()
    # check the data is there...
    assert len((await db.execute(select(BiometData))).all()) == 287
    assert len((await db.execute(select(TempRHData))).all()) == 287
    # ...but the views are empty
    assert len((await db.execute(select(LatestData))).all()) == 0
    assert len((await db.execute(select(BiometDataHourly))).all()) == 0
    assert len((await db.execute(select(BiometDataDaily))).all()) == 0
    assert len((await db.execute(select(TempRHDataHourly))).all()) == 0
    assert len((await db.execute(select(TempRHDataDaily))).all()) == 0

    class FakeGroup:
        """mock a celery group"""

        def __init__(
                self,
                callables: list[Coroutine[str, None, Awaitable[None]]],
        ) -> None:
            self.callables = callables

        async def apply_async(self) -> list[Awaitable[None]]:
            # We return a list of coroutines that awaits all .s() calls
            # as if Celery executed them
            return await asyncio.gather(*self.callables)

    with mock.patch.object(app.tasks, 'group', side_effect=FakeGroup):
        # we are a bit in async hell with nested coroutines
        coro = await refresh_all_views()
        await coro

        # however, after refreshing them they should contain data
        assert len((await db.execute(select(LatestData))).all()) == 2
        assert len((await db.execute(select(BiometDataHourly))).all()) == 24
        # we calculate daily at UTC+1 which shifts the date by one hour
        assert len((await db.execute(select(BiometDataDaily))).all()) == 2
        assert len((await db.execute(select(TempRHDataHourly))).all()) == 24
        assert len((await db.execute(select(TempRHDataDaily))).all()) == 2
        # now delete the data and refresh again
        assert (await db.execute(delete(BiometData)))
        assert (await db.execute(delete(TempRHData)))
        await db.commit()
        coro = await refresh_all_views()
        await coro

    # the views are empty again
    # ...but the views are empty
    assert len((await db.execute(select(LatestData))).all()) == 0
    assert len((await db.execute(select(BiometDataHourly))).all()) == 0
    assert len((await db.execute(select(BiometDataDaily))).all()) == 0
    assert len((await db.execute(select(TempRHDataHourly))).all()) == 0
    assert len((await db.execute(select(TempRHDataDaily))).all()) == 0


@pytest.mark.usefixtures('clean_db')
@pytest.mark.anyio
async def test_check_for_new_sensors(db: AsyncSession) -> None:
    # create some sensors in the database
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
    ]
    for s in sensors:
        db.add(s)

    await db.commit()
    with (
        mock.patch.object(
            ElementApi,
            'get_folder_slugs',
            return_value=[
                'stadt-dortmund-klimasensoren-aktiv',
                'stadt-dortmund-klimasensoren-lager',
                'Erlebnisroute',
            ],
        ),
        mock.patch.object(
            ElementApi,
            'get_device_addresses',
            # intentioally make them overlap
            side_effect=[['DEC1', 'DEC2', 'DEC3'], ['DEC3', 'DEC4']],
        ) as get_device_address,
        mock.patch.object(
            ElementApi,
            'get_device',
            return_value={
                'body': {
                    'fields': {
                        'gerateinformation': {
                            'geratetyp': 'DL-BLG-001',
                        },
                    },
                },
            },
        ) as get_device,
        mock.patch.object(
            ElementApi,
            'decentlab_id_from_address',
            return_value=44444,
        ) as dl_id_from_addr,
    ):
        await check_for_new_sensors()
        assert get_device_address.call_count == 2
        # check that the correct folders were selected
        assert get_device_address.call_args_list[0] == call(
            'stadt-dortmund-klimasensoren-aktiv',
        )
        assert get_device_address.call_args_list[1] == call(
            'stadt-dortmund-klimasensoren-lager',
        )

        # check we only check a single device
        assert get_device.call_count == 1
        assert get_device.call_args_list[0] == call(address='DEC4')
        assert dl_id_from_addr.call_count == 1
        assert dl_id_from_addr.call_args_list[0] == call('DEC4')
        # now check that it was added to the database
        new_sensor = (
            await db.execute(select(Sensor).where(Sensor.sensor_id == 'DEC4'))
        ).scalars().one()
        assert new_sensor.sensor_id == 'DEC4'
        assert new_sensor.device_id == 44444
        assert new_sensor.sensor_type == SensorType.blg


@pytest.mark.usefixtures('clean_db')
@pytest.mark.usefixtures('deployed_biomet_station')
@pytest.mark.anyio
async def test_calculate_biomet_missing_values_are_detected(db: AsyncSession) -> None:
    atm_data = ATM41DataRaw(
        sensor_id='DEC1',
        measured_at=datetime(2024, 9, 9, 0, 30, tzinfo=timezone.utc),
        air_temperature=-3276.8,
        relative_humidity=-3276.8,
        atmospheric_pressure=-327.68,
        vapor_pressure=-327.68,
        wind_speed=-327.68,
        wind_direction=-3276.8,
        u_wind=-327.68,
        v_wind=-327.68,
        maximum_wind_speed=-327.68,
        precipitation_sum=-32.768,
        solar_radiation=-3276.8,
        lightning_average_distance=-32768,
        lightning_strike_count=-32768,
        sensor_temperature_internal=-3276.8,
        x_orientation_angle=-3276.8,
        y_orientation_angle=-3276.8,
        battery_voltage=3.178,
    )
    blg_data = BLGDataRaw(
        sensor_id='DEC2',
        measured_at=datetime(2024, 9, 9, 0, 30, tzinfo=timezone.utc),
        black_globe_temperature=10,
    )
    db.add(atm_data)
    db.add(blg_data)
    await db.commit()
    await calculate_biomet('DOBFRP')
    biomet_data_in_db = (
        await db.execute(
            select(BiometData).order_by(BiometData.measured_at),
        )
    ).scalars().one()
    assert biomet_data_in_db.air_temperature is None
    assert biomet_data_in_db.relative_humidity is None
    assert biomet_data_in_db.atmospheric_pressure is None
    assert biomet_data_in_db.vapor_pressure is None
    assert biomet_data_in_db.wind_speed is None
    assert biomet_data_in_db.wind_direction is None
    assert biomet_data_in_db.u_wind is None
    assert biomet_data_in_db.v_wind is None
    assert biomet_data_in_db.maximum_wind_speed is None
    assert biomet_data_in_db.precipitation_sum is None
    assert biomet_data_in_db.solar_radiation is None
    assert biomet_data_in_db.lightning_average_distance is None
    assert biomet_data_in_db.lightning_strike_count is None
    assert biomet_data_in_db.sensor_temperature_internal is None
    assert biomet_data_in_db.x_orientation_angle is None
    assert biomet_data_in_db.y_orientation_angle is None
    # make sure the other calculations passed
    assert biomet_data_in_db.black_globe_temperature == 10
    assert biomet_data_in_db.battery_voltage == Decimal('3.178')
    assert biomet_data_in_db.utci_category == HeatStressCategories.unknown
    assert biomet_data_in_db.utci_category == HeatStressCategories.unknown


@pytest.mark.usefixtures('clean_db')
@pytest.mark.usefixtures('deployed_temprh_station')
@pytest.mark.anyio
async def test_calculate_temprh_missing_values_are_detected(db: AsyncSession) -> None:
    station_data = SHT35DataRaw(
        sensor_id='DEC1',
        measured_at=datetime(2024, 9, 9, 0, 30, tzinfo=timezone.utc),
        air_temperature=-45,
        relative_humidity=0,
        battery_voltage=3.178,
    )
    db.add(station_data)

    await db.commit()
    await calculate_temp_rh('DOTWFH')
    temprh_data_in_db = (
        await db.execute(
            select(TempRHData).order_by(TempRHData.measured_at),
        )
    ).scalars().one()
    assert temprh_data_in_db.air_temperature is None
    assert temprh_data_in_db.relative_humidity is None
    assert temprh_data_in_db.battery_voltage == Decimal('3.178')
