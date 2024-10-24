from datetime import datetime

from locust import between
from locust import HttpUser
from locust import task


class DashboardVisitor(HttpUser):
    wait_time = between(2, 15)

    @task(2)
    def initial_load_of_current_measurements_page(self) -> None:
        """The page that comes up when you click on 'Aktuelle Messwerte'
        Corresponding to wire frame: 5-Messwerte-singlePoint-selected-default-utci.png
        """
        # this drives the displayed map
        self.client.get(
            '/v1/stations/latest_data',
            params={'param': 'utci'},
            name='latest data',
        )
        # this will drive the slider. For now we thought it would make sense to request
        # a window of one week so it is more performant and we only need to reload data
        # when we move out of this window.
        # we load a single station.
        self.client.get(
            '/v1/data/DEC005476',
            # Friedensplatz
            params={
                'start_date': datetime(2024, 9, 10, 0, 0),
                'end_date': datetime(2024, 9, 17, 0, 0),
                'param': 'utci',
                'scale': 'hourly',
            },
            name='hourly data',
        )
        # display values for histogram
        self.client.get(
            '/v1/network-snapshot',
            params={
                'param': 'utci',
                'scale': 'hourly',
                'date': datetime(2024, 9, 10, 10, 0),
            },
            name='network-snapshot',
        )

    @task(6)
    def compare_stations_for_one_point_in_time_per_hour(self) -> None:
        """Compare multiple (3) stations

        Corresponding to wire frame: 7-Messwerte-multiplePoints-selected-hourly.png
        """
        # this will drive the slider and bar blot. For now we thought it would make
        # sense to request a window of one week so it is more performant and we only
        # need to reload data when we move out of this window. we load a single station.
        # TODO: this may also be doable with the network snapshot, however, then each
        # click would trigger a request.
        # we select three stations for comparison

        # Friedensplatz
        self.client.get(
            '/v1/data/DEC005476',
            params={
                'start_date': datetime(2024, 9, 10, 0, 0),
                'end_date': datetime(2024, 9, 17, 0, 0),
                'param': 'utci',
                'scale': 'hourly',
            },
            name='hourly data',
        )
        # Hansaplatz
        self.client.get(
            '/v1/data/DEC005475',
            params={
                'start_date': datetime(2024, 9, 10, 0, 0),
                'end_date': datetime(2024, 9, 17, 0, 0),
                'param': 'utci',
                'scale': 'hourly',
            },
            name='hourly data',
        )
        # Droste-Hülshoff-Straße
        self.client.get(
            '/v1/data/DEC005470',
            params={
                'start_date': datetime(2024, 9, 10, 0, 0),
                'end_date': datetime(2024, 9, 17, 0, 0),
                'param': 'utci',
                'scale': 'hourly',
            },
            name='hourly data',
        )
        # we need to update the histogram (this once per click)
        self.client.get(
            '/v1/network-snapshot',
            params={
                'param': 'utci',
                'scale': 'hourly',
                'date': datetime(2024, 9, 10, 10, 0),
            },
            name='network-snapshot',
        )

    @task(6)
    def compare_stations_for_one_point_in_time_per_day(self) -> None:
        """Compare multiple (3) stations on a daily basis

        Corresponding to wire frame: 8-Messwerte-multiplePoints-selected-daily.png
        """
        # again, cache a few values (one year) for faster sliding
        self.client.get(
            '/v1/data/DEC005476',
            params={
                'start_date': datetime(2024, 1, 1, 0, 0),
                'end_date': datetime(2025, 1, 1, 0, 0),
                'param': 'utci',
                'scale': 'daily',
            },
            name='daily data',
        )
        # Hansaplatz
        self.client.get(
            '/v1/data/DEC005475',
            params={
                'start_date': datetime(2024, 1, 1, 0, 0),
                'end_date': datetime(2025, 1, 1, 0, 0),
                'param': 'utci',
                'scale': 'daily',
            },
            name='daily data',
        )
        # Droste-Hülshoff-Straße
        self.client.get(
            '/v1/data/DEC005470',
            params={
                'start_date': datetime(2024, 1, 1, 0, 0),
                'end_date': datetime(2025, 1, 1, 0, 0),
                'param': 'utci',
                'scale': 'daily',
            },
            name='daily data',
        )
        # also update the histogram
        self.client.get(
            '/v1/network-snapshot',
            params={
                'param': 'utci',
                'scale': 'daily',
                'date': datetime(2024, 9, 10),
            },
            name='network-snapshot',
        )

    @task(4)
    def lineplot_time_range_multiple_stations(self) -> None:
        """Compare multiple (3) stations on an hourly basis

        Corresponding to wire frame: 13-Messwerte-timeRange.png
        """
        # Friedensplatz
        self.client.get(
            '/v1/data/DEC005476',
            params={
                'start_date': datetime(2024, 9, 10, 0, 0),
                'end_date': datetime(2024, 9, 17, 0, 0),
                'param': 'utci',
                'scale': 'hourly',
            },
            name='hourly data',
        )
        # Hansaplatz
        self.client.get(
            '/v1/data/DEC005475',
            params={
                'start_date': datetime(2024, 9, 10, 0, 0),
                'end_date': datetime(2024, 9, 17, 0, 0),
                'param': 'utci',
                'scale': 'hourly',
            },
            name='hourly data',
        )
        # Droste-Hülshoff-Straße
        self.client.get(
            '/v1/data/DEC005470',
            params={
                'start_date': datetime(2024, 9, 10, 0, 0),
                'end_date': datetime(2024, 9, 17, 0, 0),
                'param': 'utci',
                'scale': 'hourly',
            },
            name='hourly data',
        )

    @task(1)
    def view_metadata(self) -> None:
        """Display the metadata of all stations

        Corresponding to wire frame: 14-MessStationen.png
        """
        self.client.get('/v1/stations/metadata', name='metadata')
