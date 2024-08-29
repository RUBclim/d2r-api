import pytest
from httpx import AsyncClient


@pytest.mark.anyio
@pytest.mark.parametrize('stations', [2], indirect=True)
async def test_get_station_metadata(stations: None, app: AsyncClient) -> None:
    resp = await app.get('/v1/stations/metadata')
    assert resp.status_code == 200
    assert resp.json() == [
        {
            'altitude': 100.0,
            'district': 'Innenstadt',
            'latitude': 51.446,
            'lcz': '2',
            'long_name': 'test-station-1',
            'longitude': 7.2627,
            'name': 'DEC1',
            'station_type': 'biomet',
        },
        {
            'altitude': 100.0,
            'district': 'Innenstadt',
            'latitude': 51.446,
            'lcz': '2',
            'long_name': 'test-station-2',
            'longitude': 7.2627,
            'name': 'DEC2',
            'station_type': 'biomet',
        },
    ]
