from collections.abc import Awaitable
from collections.abc import Callable
from datetime import datetime
from datetime import timezone
from unittest import mock
from unittest.mock import call

import pandas as pd
import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

import app.tasks
from app.models import ATM41DataRaw
from app.models import BiometData
from app.models import BLGDataRaw
from app.models import HeatStressCategories
from app.models import SHT35DataRaw
from app.models import Station
from app.models import StationType
from app.models import TempRHData
from app.tasks import _sync_data_wrapper
from app.tasks import calculate_biomet
from app.tasks import calculate_temp_rh
from app.tasks import download_biomet_data
from app.tasks import download_temp_rh_data
from app.tasks import ElementApi


@pytest.mark.filterwarnings('ignore::RuntimeWarning')
@pytest.mark.anyio
@pytest.mark.usefixtures('clean_db')
async def test_download_temp_rh_data_no_new_data(db: AsyncSession) -> None:
    station = Station(
        name='DEC0054A4',
        device_id=21668,
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
    station_data = SHT35DataRaw(
        name='DEC0054A4',
        measured_at=datetime(2024, 9, 9, 0, 30, tzinfo=timezone.utc),
    )
    db.add(station_data)
    await db.commit()

    with mock.patch.object(
        ElementApi,
        'get_readings',
        return_value=pd.DataFrame(),
    ) as readings:
        await download_temp_rh_data('DEC0054A4')
        readings.assert_called_once()
        assert readings.call_args == call(
            device_name='DEC0054A4',
            sort='measured_at',
            sort_direction='asc',
            start=datetime(2024, 9, 9, 0, 30, 0, 1, tzinfo=timezone.utc),
            as_dataframe=True,
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
@pytest.mark.usefixtures('clean_db')
async def test_download_temp_rh_data_no_data_in_db(db: AsyncSession) -> None:
    station = Station(
        name='DEC0054A4',
        device_id=21668,
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
    await db.commit()

    mock_data = pd.read_csv(
        'testing/DEC0054A4_data.csv',
        index_col='measured_at',
        parse_dates=['measured_at'],
    )

    with (
        mock.patch.object(ElementApi, 'get_readings', return_value=mock_data) as rd,
    ):
        await download_temp_rh_data('DEC0054A4')
        rd.assert_called_once()
        assert rd.call_args == call(
            device_name='DEC0054A4',
            sort='measured_at',
            sort_direction='asc',
            start=None,
            as_dataframe=True,
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
@pytest.mark.usefixtures('clean_db')
async def test_download_temp_rh(db: AsyncSession) -> None:
    station = Station(
        name='DEC0054A4',
        device_id=21668,
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
    # add some data to the database, so we can check when to start
    station_data = SHT35DataRaw(
        name='DEC0054A4',
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
        await download_temp_rh_data('DEC0054A4')
        rd.assert_called_once()
        assert rd.call_args == call(
            device_name='DEC0054A4',
            sort='measured_at',
            sort_direction='asc',
            start=datetime(2024, 9, 9, 0, 30, 0, 1, tzinfo=timezone.utc),
            as_dataframe=True,
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
@pytest.mark.usefixtures('clean_db')
async def test_download_biomet_data_blg_and_atm_no_new_data(db: AsyncSession) -> None:
    station = Station(
        name='DEC00546D',
        device_id=21613,
        long_name='Westfalenhalle',
        latitude=51.49626168307185,
        longitude=7.458186573064577,
        altitude=0.0,
        station_type=StationType.biomet,
        leuchtennummer=0,
        district='44139',
        blg_name='DEC005491',
        blg_device_id=21649,
        city='Dortmund',
        country='Germany',
        street='test-street',
        plz=12345,
    )
    db.add(station)
    station_data = ATM41DataRaw(
        name='DEC00546D',
        measured_at=datetime(2024, 9, 9, 0, 30, tzinfo=timezone.utc),
    )
    db.add(station_data)
    await db.commit()

    with mock.patch.object(
        ElementApi,
        'get_readings',
        return_value=pd.DataFrame(),
    ) as readings:
        await download_biomet_data('DEC00546D')
        assert readings.call_count == 2
        assert readings.call_args_list[0] == call(
            device_name='DEC00546D',
            sort='measured_at',
            sort_direction='asc',
            start=datetime(2024, 9, 9, 0, 30, 0, 1, tzinfo=timezone.utc),
            as_dataframe=True,
        )
        assert readings.call_args_list[1] == call(
            device_name='DEC005491',
            sort='measured_at',
            sort_direction='asc',
            # there was no blackglobe data in the db
            start=None,
            as_dataframe=True,
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
@pytest.mark.usefixtures('clean_db')
async def test_download_biomet_data_blg_and_atm_no_data_in_db(db: AsyncSession) -> None:
    station = Station(
        name='DEC00546D',
        device_id=21613,
        long_name='Westfalenhalle',
        latitude=51.49626168307185,
        longitude=7.458186573064577,
        altitude=0.0,
        station_type=StationType.biomet,
        leuchtennummer=0,
        district='44139',
        blg_name='DEC005491',
        blg_device_id=21649,
        city='Dortmund',
        country='Germany',
        street='test-street',
        plz=12345,
    )
    db.add(station)
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
        await download_biomet_data('DEC00546D')
        assert rd.call_count == 2
        assert rd.call_args_list[0] == call(
            device_name='DEC00546D',
            sort='measured_at',
            sort_direction='asc',
            start=None,
            as_dataframe=True,
        )
        assert rd.call_args_list[1] == call(
            device_name='DEC005491',
            sort='measured_at',
            sort_direction='asc',
            start=None,
            as_dataframe=True,
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


@pytest.mark.filterwarnings('ignore::RuntimeWarning')
@pytest.mark.anyio
@pytest.mark.usefixtures('clean_db')
async def test_download_biomet_data(db: AsyncSession) -> None:
    station = Station(
        name='DEC00546D',
        device_id=21613,
        long_name='Westfalenhalle',
        latitude=51.49626168307185,
        longitude=7.458186573064577,
        altitude=0.0,
        station_type=StationType.biomet,
        leuchtennummer=0,
        district='44139',
        blg_name='DEC005491',
        blg_device_id=21649,
        city='Dortmund',
        country='Germany',
        street='test-street',
        plz=12345,
    )
    db.add(station)
    # add some data to the database
    atm_41_data = ATM41DataRaw(
        name='DEC00546D',
        measured_at=datetime(2024, 9, 9, 0, 40, tzinfo=timezone.utc),
    )
    db.add(atm_41_data)
    blg_data = BLGDataRaw(
        name='DEC005491',
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
        await download_biomet_data('DEC00546D')
        assert rd.call_count == 2
        assert rd.call_args_list[0] == call(
            device_name='DEC00546D',
            sort='measured_at',
            sort_direction='asc',
            start=datetime(2024, 9, 9, 0, 40, 0, 1, tzinfo=timezone.utc),
            as_dataframe=True,
        )
        assert rd.call_args_list[1] == call(
            device_name='DEC005491',
            sort='measured_at',
            sort_direction='asc',
            start=datetime(2024, 9, 9, 0, 38, 0, 1, tzinfo=timezone.utc),
            as_dataframe=True,
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
@pytest.mark.usefixtures('clean_db')
async def test_calculate_temp_rh_no_data_in_db(db: AsyncSession) -> None:
    station = Station(
        name='DEC0054A4',
        device_id=21668,
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
    await db.commit()
    await calculate_temp_rh('DEC0054A4')
    # check nothing was inserted
    data = (await db.execute(select(TempRHData))).all()
    assert len(data) == 0


@pytest.mark.anyio
@pytest.mark.usefixtures('clean_db')
@pytest.mark.filterwarnings('ignore::RuntimeWarning')
async def test_calculate_temp_rh_no_data_in_final_table_but_data_available(
        db: AsyncSession,
) -> None:
    station = Station(
        name='DEC0054A4',
        device_id=21668,
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
        # offset 1 means the station is 1 K too warm compared to the reference hence
        # this has to be subtracted
        temp_calib_offset=1,
        relhum_calib_offset=-2,
    )
    db.add(station)
    # add some data where the calibration pushes the relative humidity > 100
    station_data = SHT35DataRaw(
        name='DEC0054A4',
        measured_at=datetime(2024, 9, 10, 5, 25, tzinfo=timezone.utc),
        relative_humidity=99.5,
    )
    db.add(station_data)
    # 2nd station (which should not be touched)
    station_2 = Station(
        name='DEC0054A5',
        device_id=21669,
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
    # some data that should not be touched
    data_2 = SHT35DataRaw(
        name='DEC0054A5',
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
    await calculate_temp_rh('DEC0054A4')
    # check something was inserted
    data = (
        await db.execute(
            select(TempRHData).order_by(TempRHData.measured_at),
        )
    ).scalars().all()
    assert len(data) == 5
    d = data[0]
    assert d.measured_at == datetime(
        2024, 9, 10, 5, 5, 15, 858960, tzinfo=timezone.utc,
    )
    assert d.name == 'DEC0054A4'
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
    assert float(d.dew_point) == pytest.approx(9.5, abs=1e-2)
    assert float(d.absolute_humidity) == pytest.approx(9.01, abs=1e-2)
    assert float(d.heat_index) == pytest.approx(25.8, abs=1e-2)
    assert float(d.wet_bulb_temperature) == pytest.approx(10.1, abs=1e-2)

    # the calibration would have pushed it > 100, make sure we manually set it back
    assert data[-1].relative_humidity == 100

# merge both, one side completely missing


@pytest.mark.anyio
@pytest.mark.usefixtures('clean_db')
@pytest.mark.filterwarnings('ignore::RuntimeWarning')
async def test_calculate_temp_rh_previous_data_exists_in_final_table(
        db: AsyncSession,
) -> None:
    station = Station(
        name='DEC0054A4',
        device_id=21668,
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
        temp_calib_offset=1,
        relhum_calib_offset=2,
    )
    db.add(station)
    # other station with much newer data
    station_2 = Station(
        name='DEC0054A5',
        device_id=21669,
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
    data = TempRHData(
        name=station.name,
        measured_at=datetime(2024, 9, 10, 5, 17, tzinfo=timezone.utc),
    )
    db.add(data)
    data_2 = TempRHData(
        name='DEC0054A5',
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
    await calculate_temp_rh('DEC0054A4')
    # check something was inserted
    ret_data = (
        await db.execute(
            select(TempRHData).order_by(TempRHData.measured_at, TempRHData.name),
        )
    ).scalars().all()
    assert len(ret_data) == 3
    # this was already there
    assert ret_data[0].measured_at == datetime(2024, 9, 10, 5, 17, tzinfo=timezone.utc)
    assert ret_data[0].name == 'DEC0054A4'
    # this was inserted
    assert ret_data[1].measured_at == datetime(
        2024, 9, 10, 5, 20, 17, 549835, tzinfo=timezone.utc,
    )
    assert ret_data[1].name == 'DEC0054A4'
    # last station?
    assert ret_data[2].name == 'DEC0054A5'

# merge both, one side completely missing


@pytest.mark.anyio
@pytest.mark.usefixtures('clean_db')
async def test_calculate_biomet_no_data_available(db: AsyncSession) -> None:
    station = Station(
        name='DEC00546D',
        device_id=21613,
        long_name='Westfalenhalle',
        latitude=51.49626168307185,
        longitude=7.458186573064577,
        altitude=0.0,
        station_type=StationType.biomet,
        leuchtennummer=0,
        district='44139',
        blg_name='DEC005491',
        blg_device_id=21649,
        city='Dortmund',
        country='Germany',
        street='test-street',
        plz=12345,
    )
    db.add(station)
    await db.commit()
    await calculate_biomet('DEC00546D')
    # check nothing was inserted into the database
    data = (await db.execute(select(BiometData))).all()
    assert len(data) == 0


@pytest.mark.anyio
@pytest.mark.usefixtures('clean_db')
async def test_calculate_biomet_only_null_values_available(db: AsyncSession) -> None:
    station = Station(
        name='DEC00546D',
        device_id=21613,
        long_name='Westfalenhalle',
        latitude=51.49626168307185,
        longitude=7.458186573064577,
        altitude=0.0,
        station_type=StationType.biomet,
        leuchtennummer=0,
        district='44139',
        blg_name='DEC005491',
        blg_device_id=21649,
        city='Dortmund',
        country='Germany',
        street='test-street',
        plz=12345,
    )
    db.add(station)
    await db.commit()
    atm_data = ATM41DataRaw(
        name='DEC00546D',
        measured_at=datetime(2024, 9, 10, 5, 15, tzinfo=timezone.utc),
        air_temperature=None,
        battery_voltage=3.178,
        protocol_version=2,
    )
    blg_data = BLGDataRaw(
        name='DEC005491',
        measured_at=datetime(2024, 9, 10, 5, 15, tzinfo=timezone.utc),
        black_globe_temperature=30.5,
    )
    db.add(atm_data)
    db.add(blg_data)
    await db.commit()
    # check that we can perform our calculation even though we're missing most data
    await calculate_biomet('DEC00546D')
    # check a single row was inserted into the database
    data = (await db.execute(select(BiometData))).all()
    assert len(data) == 1


@pytest.mark.filterwarnings('ignore::RuntimeWarning')
@pytest.mark.anyio
@pytest.mark.usefixtures('clean_db')
async def test_calculate_biomet_only_atm41_data_available(db: AsyncSession) -> None:
    station = Station(
        name='DEC00546D',
        device_id=21613,
        long_name='Westfalenhalle',
        latitude=51.49626168307185,
        longitude=7.458186573064577,
        altitude=0.0,
        station_type=StationType.biomet,
        leuchtennummer=0,
        district='44139',
        blg_name='DEC005491',
        blg_device_id=21649,
        city='Dortmund',
        country='Germany',
        street='test-street',
        plz=12345,
    )
    db.add(station)
    await db.commit()
    # add some data
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
    await calculate_biomet('DEC00546D')
    # check nothing was inserted into the database, because there was no blg data
    data = (await db.execute(select(BiometData))).all()
    assert len(data) == 0


@pytest.mark.anyio
@pytest.mark.usefixtures('clean_db')
async def test_calculate_biomet_both_data_present(db: AsyncSession) -> None:
    station = Station(
        name='DEC00546D',
        device_id=21613,
        long_name='Westfalenhalle',
        latitude=51.49626168307185,
        longitude=7.458186573064577,
        altitude=150,
        station_type=StationType.biomet,
        leuchtennummer=0,
        district='44139',
        blg_name='DEC005491',
        blg_device_id=21649,
        city='Dortmund',
        country='Germany',
        street='test-street',
        plz=12345,
    )
    db.add(station)
    # add some data
    biomet_existing_data = BiometData(
        name=station.name,
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

    await calculate_biomet('DEC00546D')
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
    assert d.name == 'DEC00546D'
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

    assert float(d.pet) == 9.9
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
    assert float(d.dew_point) == 10.4
    assert float(d.absolute_humidity) == pytest.approx(9.55, abs=1e-2)
    assert float(d.heat_index) == pytest.approx(24.1, abs=1e-1)
    assert float(d.wet_bulb_temperature) == 11.1


@pytest.mark.anyio
@pytest.mark.usefixtures('clean_db')
@pytest.mark.filterwarnings('ignore::RuntimeWarning')
async def test_calculate_biomet_blg_exceeds_join_tolerance(db: AsyncSession) -> None:
    station = Station(
        name='DEC00546D',
        device_id=21613,
        long_name='Westfalenhalle',
        latitude=51.49626168307185,
        longitude=7.458186573064577,
        altitude=150,
        station_type=StationType.biomet,
        leuchtennummer=0,
        district='44139',
        blg_name='DEC005491',
        blg_device_id=21649,
        city='Dortmund',
        country='Germany',
        street='test-street',
        plz=12345,
    )
    db.add(station)

    atm41_data = ATM41DataRaw(
        name='DEC00546D',
        measured_at=datetime(2024, 9, 10, 0, tzinfo=timezone.utc),
    )
    db.add(atm41_data)
    blg_data = BLGDataRaw(
        name='DEC005491',
        measured_at=datetime(2024, 9, 10, 0, 5, 1, tzinfo=timezone.utc),
    )
    db.add(blg_data)
    await db.commit()

    await calculate_biomet('DEC00546D')
    data = (await db.execute(select(BiometData))).all()
    assert len(data) == 0


@pytest.mark.filterwarnings('ignore::RuntimeWarning')
@pytest.mark.anyio
@pytest.mark.usefixtures('clean_db')
async def test_wrapper_for_update(db: AsyncSession) -> None:
    temp_rh_station = Station(
        name='DEC0054A4',
        device_id=21668,
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
        name='DEC00546D',
        device_id=21613,
        long_name='Westfalenhalle',
        latitude=51.49626168307185,
        longitude=7.458186573064577,
        altitude=150,
        station_type=StationType.biomet,
        leuchtennummer=0,
        district='44139',
        blg_name='DEC005491',
        blg_device_id=21649,
        city='Dortmund',
        country='Germany',
        street='test-street',
        plz=12345,
    )
    db.add(biomet_station)

    await db.commit()

    with (
        mock.patch.object(app.tasks, 'download_temp_rh_data') as dl_trh,
        mock.patch.object(app.tasks, 'download_biomet_data') as dl_bio,
        mock.patch.object(app.tasks, 'refresh_all_views'),
        mock.patch.object(app.tasks, 'chord') as chord,
        mock.patch.object(app.tasks, 'chain'),
        mock.patch.object(app.tasks, 'calculate_temp_rh'),
        mock.patch.object(app.tasks, 'calculate_biomet'),
    ):
        # the underlying functions are never awaited, which causes a resource warning we
        # ignore here since it's not part of the test. AFAIK there is no way to avoid
        # this.
        await _sync_data_wrapper()
        dl_trh.s.assert_called_once_with('DEC0054A4')
        dl_bio.s.assert_called_once_with('DEC00546D')
        chord.assert_called_once()


@pytest.mark.parametrize('c', (calculate_biomet, calculate_temp_rh))
@pytest.mark.anyio
async def test_calculation_task_none_passed(
        c: Callable[[None | str], Awaitable[None | str]],
) -> None:
    res = await c(None)
    assert res is None


# TODO: check that views are refreshed!
