from collections.abc import AsyncGenerator

import pytest
from _pytest.fixtures import SubRequest
from asgi_lifespan import LifespanManager
from httpx import ASGITransport
from httpx import AsyncClient
from sqlalchemy import delete
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import Base
from app.database import sessionmanager
from app.main import create_app
from app.models import latest_data_view
from app.models import Station
from app.models import StationType


@pytest.fixture(scope='session', autouse=True)
async def create_tables(db: AsyncSession) -> None:
    con = await db.connection()
    await con.run_sync(Base.metadata.create_all)
    await con.execute(text(latest_data_view))


@pytest.fixture
async def app() -> AsyncGenerator[AsyncClient]:
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


@pytest.fixture
async def stations(db: AsyncSession, request: SubRequest) -> AsyncGenerator[None]:
    names = []
    for i in range(1, request.param + 1):
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
        )
        db.add(station)
        names.append(test_name)

    await db.commit()
    yield
    await db.execute(delete(Station).where(Station.name.in_(names)))
    await db.commit()
