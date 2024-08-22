import os

import sentry_sdk
from fastapi import FastAPI
from sentry_sdk.integrations.fastapi import FastApiIntegration
from sentry_sdk.integrations.starlette import StarletteIntegration


sentry_sdk.init(
    dsn=os.environ.get('SENTRY_DSN'),
    integrations=[StarletteIntegration(), FastApiIntegration()],
    traces_sample_rate=os.environ.get('SENTRY_SAMPLE_RATE', 0),
)


app = FastAPI()


@app.get('/')
async def root() -> dict[str, str]:
    from app.tasks import do_work
    do_work.delay()
    return {'message': 'Hello World'}
