import math
import random
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from functools import cached_property

import gevent
import sqlalchemy
from locust import between
from locust import FastHttpUser
from locust import task
from locust.contrib.fasthttp import LocustUserAgent

from app.database import DB_URL
from app.schemas import PublicParams


BIOMET_PARAMS = list(PublicParams)
TEMPRH_PARAMS = (
    'air_temperature',
    'relative_humidity',
    'dew_point',
    'absolute_humidity',
    'heat_index',
    'wet_bulb_temperature',
)

# TODO: add more params
RASTER_PARAMS = ('UTCI',)

# the date range we have data
MIN_DATE = datetime(2024, 8, 7, 13, tzinfo=timezone.utc)
MAX_DATE = datetime.now(tz=timezone.utc)

# rough extent of Dortmund
MIN_LAT = 51.47
MAX_LAT = 51.60
MIN_LON = 7.35
MAX_LON = 7.65

# we add 422, since it's frequently used in the API when checking if the station
# provides data for the requested parameter
LocustUserAgent.valid_response_codes = frozenset(
    (200, 201, 202, 203, 204, 205, 206, 207, 208, 226, 301, 302, 303, 304, 307, 422),
)

engine = sqlalchemy.create_engine(DB_URL)


def latlon_to_tile(lat: float, lon: float, zoom: int) -> tuple[int, int]:
    """
    Converts latitude and longitude to tile coordinates at a given zoom level.
    """
    n = 2 ** zoom
    x = int((lon + 180.0) / 360.0 * n)
    y = int(
        (
            1.0 - math.log(
                math.tan(math.radians(lat)) + 1 /
                math.cos(math.radians(lat)),
            ) / math.pi
        ) / 2.0 * n,
    )
    return x, y


class DashboardVisitor(FastHttpUser):
    wait_time = between(2, 15)
    # These were derived from what the dashboard does
    default_headers = {
        'Accept-Encoding': 'gzip, deflate, br, zstd',
        'Accept-Language': 'de,en-US;q=0.7,en;q=0.3',
        'Connection': 'keep-alive',
        'DNT': '1',
        'Priority': 'u=4',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'cross-site',
    }

    @cached_property
    def stations(self) -> list[dict[str, float | str]]:
        """get available stations via the API"""
        resp = self.client.get('/v1/stations/metadata', name='metadata')
        resp.raise_for_status()
        return resp.json()['data']

    @task(2)
    def initial_load_of_current_measurements_page(self) -> None:
        """The page that comes up when you click on 'Messwerte'"""
        # this drives the displayed map
        with self.rest(
            method='GET',
            url='/v1/stations/latest_data',
            params={'param': random.choice(BIOMET_PARAMS)},
            name='latest data',
        ):
            pass
        # this drives the date-slider
        # the default is three stations beings displayed, but it may be more or less
        nr_of_stations = random.randint(1, 6)
        stations = random.sample(self.stations, nr_of_stations)
        param = random.choice(BIOMET_PARAMS)
        random_date = MIN_DATE + timedelta(
            days=random.randint(0, (MAX_DATE - MIN_DATE).days),
        )
        for s in stations:
            name = s['name']
            with self.rest(
                method='GET',
                url=f'/v1/data/{name}',
                params={
                    'start_date': random_date,
                    'end_date': random_date + timedelta(days=1),
                    'param': param,
                    'scale': 'daily',
                },
                name='daily data',
            ):
                pass

        # TODO: display values for histogram - not implemented yet in the dashboard
        with self.rest(
            method='GET',
            url='/v1/network-snapshot',
            params={
                'param': param,
                'scale': 'hourly',
                'date': random_date + timedelta(hours=random.randint(0, 23)),
            },
            name='network-snapshot',
        ):
            pass

    @task(6)
    def compare_stations_for_one_point_in_time_per_hour(self) -> None:
        """Compare multiple stations on an hourly basis"""
        # this will drive the slider and bar plot.
        nr_of_stations = random.randint(2, 8)
        stations = random.sample(self.stations, nr_of_stations)
        param = random.choice(BIOMET_PARAMS)
        random_date = MIN_DATE + timedelta(
            days=random.randint(0, (MAX_DATE - MIN_DATE).days),
        ) + timedelta(hours=random.randint(0, 23)),
        for s in stations:
            name = s['name']
            with self.rest(
                method='GET',
                url=f'/v1/data/{name}',
                params={
                    'start_date': random_date,
                    'end_date': random_date,
                    'param': param,
                    'scale': 'hourly',
                },
                name='hourly data',
            ):
                pass

        # we need to update the histogram (this once per click)
        # TODO: not implemented yet in the dashboard
        with self.rest(
            method='GET',
            url='/v1/network-snapshot',
            params={
                'param': param,
                'scale': 'hourly',
                'date': random_date,
            },
            name='network-snapshot',
        ):
            pass

    @task(6)
    def compare_stations_for_one_point_in_time_per_day(self) -> None:
        """Compare multiple stations on a daily basis"""
        nr_of_stations = random.randint(2, 8)
        stations = random.sample(self.stations, nr_of_stations)

        param = random.choice(BIOMET_PARAMS)
        random_date = MIN_DATE + timedelta(
            days=random.randint(0, (MAX_DATE - MIN_DATE).days),
        )
        for s in stations:
            name = s['name']
            with self.rest(
                method='GET',
                url=f'/v1/data/{name}',
                params={
                    'start_date': random_date,
                    'end_date': random_date + timedelta(days=1),
                    'param': param,
                    'scale': 'daily',
                },
                name='daily data',
            ):
                pass

        # we need to update the histogram (this once per click)
        # TODO: not implemented yet in the dashboard
        with self.rest(
            method='GET',
            url='/v1/network-snapshot',
            params={
                'param': param,
                'scale': 'daily',
                'date': random_date,
            },
            name='network-snapshot',
        ):
            pass

    @task(4)
    def lineplot_time_range_multiple_stations(self) -> None:
        """Compare multiple (3) stations on an hourly basis as part of a line plot"""

        nr_of_stations = random.randint(2, 8)
        stations = random.sample(self.stations, nr_of_stations)
        param = random.choice(BIOMET_PARAMS)
        random_date = MIN_DATE + timedelta(
            days=random.randint(0, (MAX_DATE - MIN_DATE).days),
        )
        end_date = random_date + timedelta(days=random.randint(1, 30))
        for s in stations:
            name = s['name']
            with self.rest(
                method='GET',
                url=f'/v1/data/{name}',
                params={
                    'start_date': random_date,
                    'end_date': end_date,
                    'param': param,
                    'scale': 'hourly',
                },
                name='hourly data',
            ):
                pass

    @task(1)
    def view_metadata(self) -> None:
        """Display the metadata of all stations"""
        with self.rest(
            method='GET',
            url='/v1/stations/metadata',
            name='metadata',
        ):
            pass

    def make_tile_request_concurrent(
            self,
            param: str,
            year: int,
            doy: int,
            hour: int,
            z: int,
            x: int,
            y: int,
            name: str = 'tile',
    ) -> None:
        self.client.get(
            f'/tms/singleband/{param}/{year}/{doy}/{hour}/{z}/{x}/{y}.png?colormap=turbo&tile_size=[256,256]',  # noqa: E501
            name=name,
        )

    @task(2)
    def load_raster_tiles_initial_page_load(self) -> None:
        """the initial load of the raster tiles, which should almost always be cached"""
        year = 2024
        doy = 226
        hour = random.randint(0, 23)
        zoom_level = 12
        param = 'UTCI'
        XY_PAIRS = (
            (2132, 1362),
            (2132, 1361),
            (2133, 1362),
            (2133, 1361),
            (2131, 1362),
            (2131, 1361),
            (2132, 1363),
            (2132, 1360),
            (2133, 1363),
            (2133, 1360),
            (2134, 1362),
            (2134, 1361),
            (2131, 1363),
            (2131, 1360),
            (2134, 1363),
            (2134, 1360),
            (2130, 1362),
            (2130, 1361),
            (2135, 1362),
            (2135, 1361),
            (2130, 1363),
            (2130, 1360),
            (2135, 1363),
            (2135, 1360),
            (2129, 1362),
            (2129, 1361),
            (2136, 1362),
            (2129, 1363),
            (2136, 1361),
            (2129, 1360),
            (2136, 1363),
            (2136, 1360),
        )
        pool = gevent.pool.Pool()
        for x, y in XY_PAIRS:
            pool.spawn(
                self.make_tile_request_concurrent,
                param, year, doy, hour, zoom_level, x, y, 'initial tile',
            )

    @task(2)
    def load_raster_tiles_panning(self) -> None:
        """panning around the map at a consistent zoom level"""
        # depending on far we pan, a different number of tiles is requested
        num_tiles = random.randint(8, 40)
        # TODO
        year = random.randint(2024, 2024)
        doy = random.randint(226, 226)
        hour = random.randint(0, 23)
        z = random.randint(12, 17)
        # TODO: add different params
        param = random.choice(RASTER_PARAMS)

        pool = gevent.pool.Pool()
        for _ in range(num_tiles):
            # at the same zoom level, request random tiles
            x, y = latlon_to_tile(
                random.uniform(MIN_LAT, MAX_LAT),
                random.uniform(MIN_LON, MAX_LON),
                z,
            )
            pool.spawn(
                self.make_tile_request_concurrent,
                param, year, doy, hour, z, x, y, 'panning tile',
            )
        pool.join()
