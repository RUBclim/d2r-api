import os
from collections.abc import Callable
from collections.abc import Coroutine
from functools import wraps
from typing import Any
from typing import ParamSpec
from typing import TypeVar

import sentry_sdk
from asgiref import sync
from celery import Celery
from celery import signals
from celery import Task
from sentry_sdk.integrations.celery import CeleryIntegration

P = ParamSpec('P')
R = TypeVar('R')


def async_task(app: Celery, *args: Any, **kwargs: Any) -> Task:
    # taken from: https://github.com/celery/celery/issues/6552
    def _decorator(func: Callable[P, Coroutine[Any, Any, R]]) -> Task:
        sync_call = sync.AsyncToSync(func)

        @app.task(*args, **kwargs)
        @wraps(func)
        def _decorated(*args: P.args, **kwargs: P.kwargs) -> R:
            return sync_call(*args, **kwargs)

        return _decorated

    return _decorator


celery_app = Celery(
    'd2r-api',
    broker=os.environ['CELERY_BROKER_URL'],
    task_soft_time_limit=os.environ['QUEUE_SOFT_TIME_LIMIT'],
    broker_pool_limit=0,
    broker_connection_retry_on_startup=True,
    include=['app.tasks'],
)
celery_app.conf.timezone = 'UTC'
celery_app.set_default()


@signals.celeryd_init.connect
def init_sentry(**_kwargs: Any) -> None:  # pragma: no cover
    sentry_sdk.init(
        dsn=os.environ.get('MONITOR_SENTRY_DSN'),
        integrations=[CeleryIntegration()],
        traces_sample_rate=float(
            os.environ.get('MONITOR_SENTRY_SAMPLE_RATE', 0.0),
        ),
    )


if __name__ == '__main__':
    celery_app.start()
