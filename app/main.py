import os
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import sentry_sdk
from fastapi import FastAPI
from sentry_sdk.integrations.fastapi import FastApiIntegration
from sentry_sdk.integrations.starlette import StarletteIntegration
from sqlalchemy import text

from app.database import Base
from app.database import sessionmanager
from app.models import latest_data_view
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
            await con.run_sync(Base.metadata.create_all)
            await con.execute(text(latest_data_view))
        yield
        if sessionmanager._engine is not None:
            await sessionmanager.close()

    app = FastAPI(lifespan=lifespan)
    # we want this as a router, so we can do easy url-versioning
    app.include_router(router=main.router, prefix='/v1')
    return app


app = create_app()
