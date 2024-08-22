import os
from typing import Any

import sentry_sdk
from celery import Celery
from celery import signals
from sentry_sdk.integrations.celery import CeleryIntegration

celery = Celery(
    'd2r-api',
    broker=os.environ['CELERY_BROKER_URL'],
    task_soft_time_limit=os.environ['QUEUE_SOFT_TIME_LIMIT'],
    broker_pool_limit=0,
    broker_connection_retry_on_startup=True,
    include=['app.tasks'],
)


@signals.celeryd_init.connect
def init_sentry(**_kwargs: Any) -> None:  # pragma: no cover
    sentry_sdk.init(
        dsn=os.environ.get('MONITOR_SENTRY_DSN'),
        integrations=[CeleryIntegration()],
        traces_sample_rate=float(
            os.environ.get('MONITOR_SENTRY_SAMPLE_RATE', 0.0),
        ),
    )
