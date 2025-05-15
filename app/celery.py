import os
from collections.abc import Callable
from collections.abc import Coroutine
from datetime import timedelta
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

# https://github.com/sbdchd/celery-types/issues/80
Task.__class_getitem__ = classmethod(  # type: ignore [attr-defined]
    lambda cls, *args, **kwargs: cls,
)


def async_task(app: Celery, *args: Any, **kwargs: Any) -> Task[Any, Any]:
    """Decorator to convert an async function into a Celery task.

    :param app: The Celery app instance.
    :param args: Positional arguments to pass to the Celery task decorator.
    :param kwargs: Keyword arguments to pass to the Celery task decorator.
    :return: A Celery task decorator that wraps the async function.
    """
    # taken from: https://github.com/celery/celery/issues/6552
    def _decorator(func: Callable[P, Coroutine[Any, Any, R]]) -> Task[Any, Any]:
        # if we are running tests, we don't want this to be converted to a sync
        # function
        if 'PYTEST_VERSION' in os.environ:  # pragma: no branch
            # give the function it's .s attribute for testing
            func.s = func  # type: ignore[attr-defined]
            return func  # type: ignore[return-value]

        sync_call = sync.AsyncToSync(func)

        @app.task(*args, **kwargs)
        @wraps(func)
        def _decorated(*args: P.args, **kwargs: P.kwargs) -> R:  # pragma: no cover
            return sync_call(*args, **kwargs)

        return _decorated
    # TODO: remove this once we have the types figured out correctly
    return _decorator  # type: ignore[return-value]


celery_app = Celery(
    'd2r-api',
    broker=os.environ['CELERY_BROKER_URL'],
    backend=os.environ['CELERY_BROKER_URL'],
    task_soft_time_limit=int(os.environ['QUEUE_SOFT_TIME_LIMIT']),
    broker_connection_retry_on_startup=True,
    include=['app.tasks', 'app.tc_ingester'],
    result_expires=timedelta(600),  # expire after 10 minutes
)
celery_app.conf.timezone = 'UTC'
celery_app.set_default()


@signals.celeryd_init.connect
def init_sentry(**_kwargs: Any) -> None:  # pragma: no cover
    """Initialize Sentry for Celery tasks."""
    sentry_sdk.init(
        dsn=os.environ.get('MONITOR_SENTRY_DSN'),
        integrations=[CeleryIntegration()],
        traces_sample_rate=float(os.environ.get('SENTRY_SAMPLE_RATE', 0.0)),
    )


if __name__ == '__main__':
    celery_app.start()
