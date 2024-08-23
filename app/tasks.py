import os
import time

from celery import shared_task
from element import ElementApi


api = ElementApi(
    api_location='https://dew21.element-iot.com/api/v1/',
    api_key=os.environ['ELEMENT_API_KEY'],
)


@shared_task(ignore_result=True)
def do_work() -> None:
    time.sleep(5)
