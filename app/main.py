import os
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import sentry_sdk
from fastapi import FastAPI
from sentry_sdk.integrations.fastapi import FastApiIntegration
from sentry_sdk.integrations.starlette import StarletteIntegration
from sqlalchemy import text

from app.database import angle_avg_funcs
from app.database import Base
from app.database import sessionmanager
from app.models import BiometDataHourly
from app.models import LatestData
from app.models import TempRHDataHourly
from app.routers import main


sentry_sdk.init(
    dsn=os.environ.get('SENTRY_DSN'),
    integrations=[StarletteIntegration(), FastApiIntegration()],
    traces_sample_rate=os.environ.get('SENTRY_SAMPLE_RATE', 0),
)


def create_app() -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
        async with sessionmanager.connect() as con:
            # we need to exclude tables that actually represent a views
            # we trick sqlalchemy into thinking this was a table, but of course
            # we must prevent it trying to create it.
            excluded = {
                LatestData.__tablename__,
                BiometDataHourly.__tablename__,
                TempRHDataHourly.__tablename__,
            }
            tables_to_creates = [
                v for k, v in Base.metadata.tables.items() if k not in excluded
            ]
            await con.run_sync(Base.metadata.create_all, tables=tables_to_creates)
            await con.execute(text(angle_avg_funcs))

        # create the views which cannot be created as part of a transaction
        async with sessionmanager.connect(as_transaction=False) as con:
            await con.execute(BiometDataHourly.creation_sql)
            await con.execute(LatestData.creation_sql)
            await con.execute(TempRHDataHourly.creation_sql)
        yield
        if sessionmanager._engine is not None:
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
        version='0.0.0',
        openapi_tags=[
            {
                'name': 'stations',
                'description': 'operations on a per-station or all stations level',
            },
            {
                'name': 'districts',
                'description': 'operations on a per-district or all districts level',
            },
        ],
        lifespan=lifespan,
    )
    # we want this as a router, so we can do easy url-versioning
    app.include_router(router=main.router, prefix='/v1')
    return app


app = create_app()
