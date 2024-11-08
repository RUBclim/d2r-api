from collections.abc import AsyncGenerator
from datetime import datetime
from datetime import timedelta
from datetime import timezone

import pytest
from _pytest.fixtures import SubRequest
from asgi_lifespan import LifespanManager
from httpx import ASGITransport
from httpx import AsyncClient
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import sessionmanager
from app.main import create_app
from app.models import ATM41DataRaw
from app.models import BiometData
from app.models import BiometDataDaily
from app.models import BiometDataHourly
from app.models import BLGDataRaw
from app.models import LatestData
from app.models import SHT35DataRaw
from app.models import Station
from app.models import StationType
from app.models import TempRHData
from app.models import TempRHDataDaily
from app.models import TempRHDataHourly


@pytest.fixture(scope='session', autouse=True)
async def create_dbs():
    """this is needed so if we start with a test not involving the app fixture. The db
    will be there.
    """
    app = create_app()
    async with LifespanManager(app) as manager:
        async with AsyncClient(
            transport=ASGITransport(app=manager.app),
            base_url='http://test',
        ):
            yield


@pytest.fixture
async def clean_db(db: AsyncSession) -> AsyncGenerator[None]:
    yield
    await db.execute(delete(BiometData))
    await db.execute(delete(TempRHData))
    await db.execute(delete(SHT35DataRaw))
    await db.execute(delete(ATM41DataRaw))
    await db.execute(delete(BLGDataRaw))
    await db.execute(delete(Station))
    await db.commit()
    await LatestData.refresh()
    await BiometDataHourly.refresh()
    await BiometDataDaily.refresh()
    await TempRHDataHourly.refresh()
    await TempRHDataDaily.refresh()
    await db.commit()


@pytest.fixture
async def app() -> AsyncGenerator[AsyncClient]:
    """This has to always be the first fixture that is called"""
    app = create_app()
    async with LifespanManager(app) as manager:
        async with AsyncClient(
            transport=ASGITransport(app=manager.app),
            base_url='http://test',
        ) as client:
            yield client


@pytest.fixture(scope='session')
def anyio_backend():
    return 'asyncio'


@pytest.fixture(scope='session')
async def db() -> AsyncGenerator[AsyncSession]:
    async with sessionmanager.session() as sess:
        yield sess


def _create_stations(n: int) -> list[Station]:
    stations: list[Station] = []
    for i in range(1, n + 1):
        test_name = f'DEC{i}'
        station = Station(
            name=test_name,
            device_id=1,
            long_name=f'test-station-{i}',
            latitude=51.4460,
            longitude=7.2627,
            altitude=100,
            station_type=StationType.biomet,
            blg_name=f'DEC{i}{i}',
            blg_device_id=int(f'{i}{i}'),
            leuchtennummer=100,
            district='Innenstadt',
            lcz='2',
            city='Dortmund',
            country='Germany',
            street='test-street',
            plz=12345,
        )
        stations.append(station)

    return stations


@pytest.fixture
async def stations(
        db: AsyncSession,
        request: SubRequest,
        clean_db: None,
) -> AsyncGenerator[list[Station]]:
    n = request.param if hasattr(request, 'param') else 1
    stations = _create_stations(n)
    for station in stations:
        db.add(station)
    await db.commit()
    yield stations


@pytest.fixture
async def biomet_data(
        db: AsyncSession,
        request: SubRequest,
        clean_db: None,
) -> AsyncGenerator[list[BiometData]]:
    n_stations = request.param['n_stations']
    n_data = request.param['n_data']
    stations = _create_stations(n=n_stations)
    start_date = datetime(2024, 8, 1, 0, tzinfo=timezone.utc)
    biomet_data_list = []
    for station in stations:
        db.add(station)
        for i in range(n_data + 1):
            biomet_data = BiometData(
                name=station.name,
                measured_at=start_date + timedelta(minutes=5*i),
                utci=35.5,
                # TODO: add more values and dynamically change them
            )
            db.add(biomet_data)
            biomet_data_list.append(biomet_data)

    await db.commit()
    await LatestData.refresh()
    await BiometDataHourly.refresh()
    await db.commit()
    yield biomet_data_list
