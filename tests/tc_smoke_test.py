import asyncio
import os
from collections.abc import Callable
from collections.abc import Generator
from typing import Any
from typing import TypeVar

import pytest
from flask import Flask
from flask.testing import FlaskClient
from terracotta import update_settings
from terracotta.drivers import TerracottaDriver
from terracotta.server import create_app
from werkzeug.test import TestResponse

from app.tc_ingester import ingest_raster


T = TypeVar('T')


async def _call(f: Callable[[], T]) -> T:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(executor=None, func=f)


class AsyncTestClient(FlaskClient):
    """ A facade for the flask test client so we can use it async tests but still run
    sync flask. It was taken from here: https://stackoverflow.com/a/75674848/17798119
    """

    async def get(self, *args: Any, **kwargs: Any) -> TestResponse:
        return await _call(lambda: super(AsyncTestClient, self).get(*args, **kwargs))


@pytest.fixture
def setup_rasters(raster_driver: TerracottaDriver) -> None:
    # insert the various rasters
    RASTERS = (
        'DO_MRT_2025_113_12_v0.7.2_cog.tif',
        'DO_PET-class_2025_113_12_v0.7.2_cog.tif',
        'DO_TA_2025_113_12_v0.7.2_cog.tif',
        'DO_PET_2025_113_12_v0.7.2_cog.tif',
        'DO_RH_2025_113_12_v0.7.2_cog.tif',
        'DO_UTCI_2025_113_12_v0.7.2_cog.tif',
        'DO_UTCI-class_2025_113_12_v0.7.2_cog.tif',
    )
    for r in RASTERS:
        ingest_raster(path=f'testing/rasters/{r}', override_path='testing/rasters')


@pytest.fixture
def app() -> Generator[Flask]:
    driver_path = (
        f"{os.environ['POSTGRES_USER']}:{os.environ['POSTGRES_PASSWORD']}@"
        f"{os.environ['TC_DATABASE_HOST']}:{os.environ['PGPORT']}/"
        f"{os.environ['TC_DATABASE_NAME']}"
    )
    app = create_app()
    app.config['TESTING'] = True
    update_settings(DRIVER_PATH=driver_path)
    yield app


@pytest.fixture
def client(app: Flask) -> Generator[FlaskClient]:
    return AsyncTestClient(app, TestResponse, True)


@pytest.mark.usefixtures('setup_rasters')
@pytest.mark.anyio
async def test_get_datasets(client: AsyncTestClient) -> None:
    response = await client.get('/datasets')
    assert response.status_code == 200
    assert response.json == {
        'datasets': [
            {'doy': '113', 'hour': '12', 'param': 'MRT', 'year': '2025'},
            {'doy': '113', 'hour': '12', 'param': 'PET', 'year': '2025'},
            {'doy': '113', 'hour': '12', 'param': 'PET_CLASS', 'year': '2025'},
            {'doy': '113', 'hour': '12', 'param': 'RH', 'year': '2025'},
            {'doy': '113', 'hour': '12', 'param': 'TA', 'year': '2025'},
            {'doy': '113', 'hour': '12', 'param': 'UTCI', 'year': '2025'},
            {'doy': '113', 'hour': '12', 'param': 'UTCI_CLASS', 'year': '2025'},
        ],
        'limit': 100,
        'page': 0,
    }


@pytest.mark.usefixtures('setup_rasters')
@pytest.mark.anyio
async def test_get_keys(client: AsyncTestClient) -> None:
    response = await client.get('/keys')
    assert response.status_code == 200
    assert response.json == {
        'keys': [
            {'description': 'the parameter e.g. UTCI', 'key': 'param'},
            {'description': 'the year of the data', 'key': 'year'},
            {'description': 'the day of the year', 'key': 'doy'},
            {'description': 'the hour of the day', 'key': 'hour'},
        ],
    }


@pytest.mark.usefixtures('setup_rasters')
@pytest.mark.anyio
async def test_get_metadata(client: AsyncTestClient) -> None:
    response = await client.get('/metadata/MRT/2025/113/12')
    assert response.status_code == 200
    assert response.json is not None
    assert response.json['range'] == [22.004995346069336, 39.25629806518555]


@pytest.mark.usefixtures('setup_rasters')
@pytest.mark.anyio
async def test_get_singleband_preview(client: AsyncTestClient) -> None:
    response = await client.get('/singleband/MRT/2025/113/12/preview.png')
    assert response.status_code == 200
    with open('testing/rasters/expected_overview.png', 'rb') as f:
        expected = f.read()
    assert response.data == expected


@pytest.mark.usefixtures('setup_rasters')
@pytest.mark.anyio
async def test_get_singleband_tile(client: AsyncTestClient) -> None:
    response = await client.get(
        '/singleband/MRT/2025/113/12/17/68254/43582.png?colormap=turbo',
    )
    assert response.status_code == 200
    with open('testing/rasters/expected_tile.png', 'rb') as f:
        expected = f.read()

    assert response.data == expected
