import os
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import cast

import sentry_sdk
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.staticfiles import StaticFiles
from sentry_sdk.integrations.fastapi import FastApiIntegration
from sentry_sdk.integrations.starlette import StarletteIntegration
from sqlalchemy import Table
from sqlalchemy import text

from app import ALLOW_ORIGIN_REGEX
from app.database import angle_avg_funcs
from app.database import Base
from app.database import sessionmanager
from app.models import LatestData
from app.routers import general
from app.routers import v1
from app.schemas import get_current_version


sentry_sdk.init(
    dsn=os.environ.get('SENTRY_DSN'),
    integrations=[StarletteIntegration(), FastApiIntegration()],
    traces_sample_rate=float(os.environ.get('SENTRY_SAMPLE_RATE', 0.0)),
)


def create_app() -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
        async with sessionmanager.connect() as con:
            # we need to exclude tables that actually represent views.
            # We trick sqlalchemy into thinking this was a table, but of course
            # we must prevent it trying to create it.
            views: set[type[LatestData]] = {LatestData}
            view_names = {n.__tablename__ for n in views}
            tables_to_create = [
                v for k, v in Base.metadata.tables.items() if k not in view_names
            ]
            await con.run_sync(Base.metadata.create_all, tables=tables_to_create)
            await con.execute(text(angle_avg_funcs))

        # create the views which cannot be created as part of a transaction
        async with sessionmanager.connect(as_transaction=False) as con:
            for v in views:
                await con.execute(text(v.creation_sql))
                # create indexes for views
                view_table_obj = cast(Table, v.__table__)
                for idx in view_table_obj.indexes:
                    await con.run_sync(idx.create, checkfirst=True)
        yield
        await sessionmanager.close()

    app = FastAPI(
        title='D2R-API',
        description=(
            'API for getting data from the (bio-) meteorological network deployed '
            'in the [Data2Resilience (D2R) Project](https://data2resilience.de/)\n\n'
            '**Please cite the data and network:**\n\n\n'
            'Hüser, C., Wolf, L., Gottschalk, N., Kittner, J., Kraas, B., '
            'Mittelstädt, C., Reinhart, V., Sismanidis, P., Wawrzyniak, N., & '
            'Bechtel, B. (2026). Data2Resilience - A Biometeorological Weather Station '
            'Network in Dortmund (1.0.0). Zenodo. '
            '[https://doi.org/10.5281/zenodo.18221203](https://doi.org/10.5281/zenodo.18221203).'  # noqa: E501
        ),
        license_info={
            'name': 'CC BY 4.0',
            'url': 'https://creativecommons.org/licenses/by/4.0/',
        },
        contact={
            'name': 'Bochum Urban Climate Lab',
            'url': 'https://climate.rub.de',
            'email': 'climate@rub.de',
        },
        version=get_current_version(),
        openapi_tags=[
            {
                'name': 'stations',
                'description': 'operations on a per-station or all-stations level',
            },
        ],
        lifespan=lifespan,
    )
    # we want this as a router, so we can do easy url-versioning
    app.include_router(router=v1.router)
    app.include_router(router=general.router)
    # compress (gzip) all responses larger than 1.5 kb
    app.add_middleware(GZipMiddleware, minimum_size=1500)
    # Allow cross-origin requests for development purposes from localhost and
    # allow data2resilience.de and its subdomains
    app.add_middleware(
        CORSMiddleware,
        # this should match all sorts of things needed for development and production
        # https://localhost:5000
        # http://localhost:80
        # http://localhost:443
        # http://localhost
        # https://dashboard.data2resilience.de
        # https://dashboard-foo.data2resilience.de
        # https://dashboard_foo-bar-123.data2resilience.de
        # https://dashboard.data2resilience.app
        # https://dashboard.data2resilience.app:8080
        # https://data-2-resilience-fooo-vogelinos-projects.vercel.app
        # https://data-2-resilience-fooo-vogelinos-projects.vercel.app
        # https://data-2-resilience.vercel.app
        allow_origin_regex=ALLOW_ORIGIN_REGEX,
    )
    # in production, static files should be served by the webserver (e.g. nginx)
    app.mount('/static', StaticFiles(directory='app/static'), name='static')
    return app


app = create_app()
