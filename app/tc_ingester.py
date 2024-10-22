from __future__ import annotations

import os
import re
from typing import NamedTuple
from typing import TypedDict

import terracotta.exceptions
from terracotta.drivers import TerracottaDriver

from app.celery import celery_app

FNAME_REGEX = re.compile(
    r'^(?P<param>[A-Z]+)_(?P<method>[a-z]+)_(?P<resolution>\d+m)_(?P<version>v\d+\.\d+\.\d+)_(?P<year>\d{4})_(?P<doy>\d{1,3})_(?P<hour>\d{2})\.tif$',  # noqa: E501
)


class _RasterKeys(NamedTuple):
    param: str
    year: str
    doy: str
    hour: str

    @classmethod
    def from_string(cls, s: str) -> _RasterKeys:
        match = re.match(FNAME_REGEX, s)
        if match is not None:
            d = match.groupdict()
        else:
            raise ValueError(f"Filename {s} does not match the expected pattern")

        param = d['param']
        # TODO: maybe we want to add Ta and RH to here!
        if param not in {'UTCI', 'TMRT', 'PET'}:
            raise ValueError(f"Invalid param value {param}")

        return cls(
            param=d['param'],
            year=d['year'],
            doy=d['doy'],
            hour=d['hour'],
        )


class RasterInfo(TypedDict):
    key_values: _RasterKeys
    path: str
    override_path: str


def get_driver() -> TerracottaDriver:
    driver_path = (
        f"{os.environ['POSTGRES_USER']}:{os.environ['POSTGRES_PASSWORD']}@"
        f"{os.environ['TC_DATABASE_HOST']}:{os.environ['PGPORT']}/"
        f"{os.environ['TC_DATABASE_NAME']}"
    )

    driver = terracotta.get_driver(driver_path, provider='postgresql')

    # This is a stupid way of checking if the database exists, since terracotta does not
    # provide an interface for this and trying to create a database that already exists
    # will raise an error.
    db_exists = True
    try:
        driver.db_version
    except terracotta.exceptions.InvalidDatabaseError:
        db_exists = False

    if not db_exists:
        driver.create(_RasterKeys._fields)

    # check that the database has the same keys that we want to load, not sure what to
    # do if they are different
    if driver.key_names != _RasterKeys._fields:
        raise ValueError(
            f"Database keys do not match the expected keys: {driver.key_names} != "
            f"{_RasterKeys._fields}",
        )
    return driver


@celery_app.task(name='ingest-raster')
def ingest_raster(path: str, override_path: str = '') -> None:
    """Ingest a raster into terracotta.

    :param path: path to the raster file
    :param override_path: path to the raster file in the container. This will be
        prepended to the basename of ``path``.
    """
    base_name = os.path.basename(path)
    raster_info = RasterInfo(
        {
            'key_values': _RasterKeys.from_string(base_name),
            'path': path,
            'override_path': os.path.join(override_path, base_name),
        },
    )
    driver = get_driver()
    with driver.connect():
        driver.insert(
            keys=raster_info['key_values'],
            path=raster_info['path'],
            # we need to write it as an absolute path that is valid in the container
            override_path=raster_info['override_path'],
        )
