from __future__ import annotations

import os
import re
from typing import NamedTuple
from typing import TypedDict

import terracotta.exceptions
from terracotta.cog import check_raster_file
from terracotta.drivers import TerracottaDriver

from app.celery import celery_app

FNAME_REGEX = re.compile(
    r'^(?P<city>[A-Za-z]{2}(?=_))?_?(?P<param>[A-Za-z]+(?:\-class)?(?=_))_?(?P<method>[a-z]+(?=_))?_?(?P<resolution>\d+m(?=_))?_?(?P<version_a>v\d+\.\d+\.\d+(?=_))?_?(?P<year>\d{4}(?=_))?_(?P<doy>\d{1,3}(?=_))_?(?P<hour>\d{2})_?(?P<version_b>v\d+\.\d+\.\d+)?(?:_cog)?\.tif$',  # noqa: E501
)

VALID_PARAMS = {'MRT', 'PET', 'PET_CLASS', 'RH', 'TA', 'UTCI', 'UTCI_CLASS'}


class InvalidRasterError(ValueError):
    pass


class _RasterKeys(NamedTuple):
    param: str
    year: str
    doy: str
    hour: str
    city: str | None = None
    resolution: str | None = None
    version: str | None = None
    method: str | None = None

    @staticmethod
    def public_keys() -> tuple[str, str, str, str]:
        return ('param', 'year', 'doy', 'hour')

    @property
    def public_values(self) -> tuple[str, str, str, str]:
        return (self.param, self.year, self.doy, self.hour)

    @staticmethod
    def key_descriptions() -> dict[str, str]:
        return {
            'param': 'the parameter e.g. UTCI',
            'year': 'the year of the data',
            'doy': 'the day of the year',
            'hour': 'the hour of the day',
        }

    @classmethod
    def from_string(cls, s: str) -> _RasterKeys:
        match = re.match(FNAME_REGEX, s)
        err = False
        if match is not None:
            d = match.groupdict()
            version = d['version_a'] or d['version_b']
            if version is None:
                err = True
        else:
            err = True

        if err:
            raise ValueError(f"Filename {s} does not match the expected pattern")

        # there are different ways of naming Tmrt and the classes
        if d['param'] == 'Tmrt':
            d['param'] = 'MRT'
        if d['param'] == 'PET-class':
            d['param'] = 'PET_CLASS'
        if d['param'] == 'UTCI-class':
            d['param'] = 'UTCI_CLASS'

        if d['param'] not in VALID_PARAMS:
            raise ValueError(f"Invalid param value {d['param']}")

        return cls(
            param=d['param'],
            year=d['year'],
            doy=d['doy'],
            hour=d['hour'],
            city=d['city'],
            resolution=d['resolution'],
            version=version,
            method=d['method'],
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
        driver.create(
            keys=_RasterKeys.public_keys(),
            key_descriptions=_RasterKeys.key_descriptions(),
        )

    # check that the database has the same keys that we want to load, not sure what to
    # do if they are different
    if driver.key_names != _RasterKeys.public_keys():
        raise ValueError(
            f"Database keys do not match the expected keys: {driver.key_names} != "
            f"{_RasterKeys.public_keys()}",
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
    # validate that the raster we want to insert is ok
    errors, warnings, details = check_raster_file(path)
    if errors or warnings:
        print('there are errors or warnings')
        print(f'errors: {errors}')
        print(f'warnings ==> {warnings}')
        print(f'detail ==> {details}')
        raise InvalidRasterError(f"Invalid raster file {path}")

    raster_info = RasterInfo(
        {
            'key_values': _RasterKeys.from_string(base_name),
            'path': path,
            'override_path': os.path.join(override_path, base_name),
        },
    )
    driver = get_driver()
    with driver.connect():
        metadata = driver.compute_metadata(
            path=raster_info['path'],
            extra_metadata=raster_info['key_values']._asdict(),
        )
        driver.insert(
            keys=raster_info['key_values'].public_values,
            path=raster_info['path'],
            # we need to write it as an absolute path that is valid in the container
            override_path=raster_info['override_path'],
            metadata=metadata,
        )
