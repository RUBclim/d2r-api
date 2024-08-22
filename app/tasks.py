import time

from celery import shared_task


@shared_task(ignore_result=True)
def do_work() -> None:
    time.sleep(5)
