import os
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import cast

import sentry_sdk
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from psycopg.errors import DuplicateTable
from sentry_sdk.integrations.fastapi import FastApiIntegration
from sentry_sdk.integrations.starlette import StarletteIntegration
from sqlalchemy import Table
from sqlalchemy import text
from sqlalchemy.exc import ProgrammingError

from app import ALLOW_ORIGIN_REGEX
from app.database import angle_avg_funcs
from app.database import Base
from app.database import sessionmanager
from app.models import BiometDataDaily
from app.models import BiometDataHourly
from app.models import LatestData
from app.models import TempRHDataDaily
from app.models import TempRHDataHourly
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
            views: set[
                type[
                    LatestData | BiometDataHourly | BiometDataDaily | TempRHDataHourly |
                    TempRHDataDaily
                ]
            ] = {
                LatestData, BiometDataHourly, BiometDataDaily, TempRHDataHourly,
                TempRHDataDaily,
            }
            view_names = {n.__tablename__ for n in views}
            tables_to_create = [
                v for k, v in Base.metadata.tables.items() if k not in view_names
            ]
            await con.run_sync(Base.metadata.create_all, tables=tables_to_create)
            await con.execute(text(angle_avg_funcs))

        # create the views which cannot be created as part of a transaction
        async with sessionmanager.connect(as_transaction=False) as con:
            for v in views:
                await con.execute(v.creation_sql)
                # create indexes for views
                view_table_obj = cast(Table, v.__table__)
                for idx in view_table_obj.indexes:
                    try:
                        await con.run_sync(idx.create, checkfirst=True)
                    # timescale materialized views already have some indexes created by
                    # default (i.e. on the temporal dimension)
                    except ProgrammingError as e:
                        if isinstance(e.orig, DuplicateTable):
                            pass
                        else:  # pragma: no cover
                            raise
        yield
        await sessionmanager.close()

    app = FastAPI(
        title='D2R-API',
        description=(
            'API for getting data from the (bio-) meteorological network deployed '
            'in the [Data2Resilience (D2R) Project](https://data2resilience.de/)'
        ),
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
    return app


app = create_app()
