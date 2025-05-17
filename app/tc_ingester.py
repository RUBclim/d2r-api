from __future__ import annotations

import os
import re
from typing import NamedTuple
from typing import TypedDict

import terracotta.exceptions
from terracotta.cog import check_raster_file
from terracotta.drivers import TerracottaDriver

from app.celery import celery_app
from app.models import PET_STRESS_CATEGORIES
from app.models import UTCI_STRESS_CATEGORIES
from app.routers.v1 import compute_colormap_range
from app.schemas import PublicParamsBiomet
from app.schemas import VizParamSettings

FNAME_REGEX = re.compile(
    r'^(?P<city>[A-Za-z]{2}(?=_))?_?(?P<param>[A-Za-z]+(?:\-class)?(?=_))_?(?P<method>[a-z]+(?=_))?_?(?P<resolution>\d+m(?=_))?_?(?P<version_a>v\d+\.\d+\.\d+(?=_))?_?(?P<year>\d{4}(?=_))?_(?P<doy>\d{1,3}(?=_))_?(?P<hour>\d{2})_?(?P<version_b>v\d+\.\d+\.\d+)?(?:_cog)?\.tif$',  # noqa: E501
)

VIZ_PARAM_MAPPING = {
    'MRT': PublicParamsBiomet.mrt,
    'PET': PublicParamsBiomet.pet,
    'PET_CLASS': PublicParamsBiomet.pet_category,
    'RH': PublicParamsBiomet.relative_humidity,
    'TA': PublicParamsBiomet.air_temperature,
    'UTCI': PublicParamsBiomet.utci,
    'UTCI_CLASS': PublicParamsBiomet.utci_category,
}


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
    is_categorical: bool = False
    categories: dict[str, str] | None = None

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

        if d['param'] not in VIZ_PARAM_MAPPING:
            raise ValueError(f"Invalid param value {d['param']}")

        is_categorical = False
        categories = None
        if d['param'] == 'UTCI_CLASS':
            is_categorical = True
            # This simply numbers the categories from 0 to n (0-8 for PET and
            # 0-9 for UTCI). This means that 8 is extreme heat stress for PET and 9 is
            # extreme heat stress for UTCI. So this is inconsistent and likely a bit
            # harder to handle in the frontend.
            # the categories can be retrieved via the /metadata endpoint
            categories = {
                # the dictionaries are already ordered, so we can use the index as the
                # key for the raster category
                str(k): v.value for k, v in enumerate(UTCI_STRESS_CATEGORIES.values())
            }
        elif d['param'] == 'PET_CLASS':
            is_categorical = True
            categories = {
                str(k): v.value for k, v in enumerate(PET_STRESS_CATEGORIES.values())
            }
        return cls(
            param=d['param'],
            year=d['year'],
            doy=d['doy'],
            hour=d['hour'],
            city=d['city'],
            resolution=d['resolution'],
            version=version,
            method=d['method'],
            is_categorical=is_categorical,
            categories=categories,
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


@celery_app.task(name='ingest-raster', rate_limit='5/m')
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
        # add visualization suggestions to the metadata
        vmin, vmax = compute_colormap_range(
            data_min=metadata['range'][0],
            data_max=metadata['range'][1],
            param_setting=VizParamSettings.get(
                VIZ_PARAM_MAPPING[raster_info['key_values'].param],
            ),
        )
        metadata['metadata']['visualization'] = {'cmin': vmin, 'cmax': vmax}
        driver.insert(
            keys=raster_info['key_values'].public_values,
            path=raster_info['path'],
            # we need to write it as an absolute path that is valid in the container
            override_path=raster_info['override_path'],
            metadata=metadata,
        )
