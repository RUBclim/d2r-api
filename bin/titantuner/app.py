from datetime import datetime
from datetime import timedelta
from datetime import timezone

import geopandas as gpd
import pandas as pd
from quart import jsonify
from quart import Quart
from quart import render_template
from quart import request
from quart import Response
from sqlalchemy import func
from sqlalchemy import literal
from sqlalchemy import select

from app.database import sessionmanager
from app.models import BiometData
from app.models import Station
from app.models import StationType
from app.models import TempRHData
from app.qc import apply_buddy_check
from app.qc import BUDDY_CHECK_COLUMNS
from app.qc import BuddyCheckConfig

app = Quart(__name__)

START = datetime(2024, 8, 12, 0, 0, tzinfo=timezone.utc)
END = datetime.now(tz=timezone.utc)


async def fetch_data(date: datetime) -> gpd.GeoDataFrame:
    rounded_time_biomet = func.to_timestamp(
        func.round(func.extract('epoch', BiometData.measured_at) / 300) * 300,
    )
    rounded_time_temp_rh = func.to_timestamp(
        func.round(func.extract('epoch', TempRHData.measured_at) / 300) * 300,
    )
    data_query = select(
        BiometData.measured_at,
        rounded_time_biomet.label('measured_at_rounded'),
        BiometData.station_id,
        Station.longitude,
        Station.latitude,
        Station.altitude,
        BiometData.air_temperature,
        BiometData.relative_humidity,
        BiometData.atmospheric_pressure,
    ).join(Station).where(
        BiometData.measured_at.between(
            date - timedelta(minutes=5), date + timedelta(minutes=5),
        ),
        rounded_time_biomet == date,
    ).union_all(
        select(
            TempRHData.measured_at,
            rounded_time_temp_rh.label('measured_at_rounded'),
            TempRHData.station_id,
            Station.longitude,
            Station.latitude,
            Station.altitude,
            TempRHData.air_temperature,
            TempRHData.relative_humidity,
            literal(None).label(BiometData.atmospheric_pressure.name),
        ).join(Station).where(
            TempRHData.measured_at.between(
                date - timedelta(minutes=5), date + timedelta(minutes=5),
            ),
            rounded_time_temp_rh == date,
            Station.station_type != StationType.double,
        ),
    )
    async with sessionmanager.session() as sess:
        con = await sess.connection()
        df: pd.DataFrame = await con.run_sync(
            lambda con: pd.read_sql(
                sql=data_query,
                con=con,
                index_col=['measured_at'],
            ),
        )
    df.reset_index(inplace=True)
    df = gpd.GeoDataFrame(
        df, geometry=gpd.points_from_xy(df.longitude, df.latitude),
        crs='EPSG:4326',
    )
    return df


@app.route('/')
async def index() -> str:
    steps = ((END - START) / 60).total_seconds()
    return await render_template(
        'index.html',
        steps=steps,
        start=START,
        end=END,
        # on page load populate them with what we currently have as settings
        buddy_check_columns=BUDDY_CHECK_COLUMNS['air_temperature'],
    )


@app.route('/get-default-config/<param>', methods=['GET'])
async def get_default_config(param: str) -> Response:
    """when we swtich parameters, we need to fetch the default config"""
    if param not in BUDDY_CHECK_COLUMNS:
        return jsonify({'error': 'Invalid parameter requested'}), 400

    config = BUDDY_CHECK_COLUMNS[param].copy()
    config.pop('callable')  # type: ignore[misc]
    return jsonify(config)


@app.route('/get-data', methods=['GET'])
async def get_data() -> Response:
    """get the data for the buddy check"""
    param = request.args['param']
    step = int(request.args.get('step'))

    # build a new configuration to try
    config: BuddyCheckConfig = {
        param: {  # type: ignore[misc]
            'radius': int(
                request.args.get('radius', BUDDY_CHECK_COLUMNS[param]['radius']),
            ),
            'num_min': int(
                request.args.get('num_min', BUDDY_CHECK_COLUMNS[param]['num_min']),
            ),
            'threshold': float(
                request.args.get('threshold', BUDDY_CHECK_COLUMNS[param]['threshold']),
            ),
            'max_elev_diff': float(
                request.args.get(
                    'max_elev_diff',
                    BUDDY_CHECK_COLUMNS[param]['max_elev_diff'],
                ),
            ),
            'elev_gradient': float(
                request.args.get(
                    'elev_gradient',
                    BUDDY_CHECK_COLUMNS[param]['elev_gradient'],
                ),
            ),
            'min_std': float(
                request.args.get('min_std', BUDDY_CHECK_COLUMNS[param]['min_std']),
            ),
            'num_iterations': int(
                request.args.get(
                    'num_iterations',
                    BUDDY_CHECK_COLUMNS[param]['num_iterations'],
                ),
            ),
        },
    }

    date = START + timedelta(minutes=step)
    df = await fetch_data(date)
    data_to_qc = df[df['measured_at_rounded'] == date][[
        'measured_at',
        'station_id', 'longitude', 'latitude', 'altitude', param,
    ]].copy()
    result = await apply_buddy_check(
        data_to_qc,
        config=config,  # type: ignore[arg-type]
    )
    data_to_qc.set_index(['measured_at', 'station_id'], inplace=True)
    result[param] = data_to_qc[param]
    result['geometry'] = df.set_index(['measured_at', 'station_id'])['geometry']
    result = result.rename(
        columns={
            param: 'value',
            f'{param}_qc_isolated_check': 'isolated_qc',
            f'{param}_qc_buddy_check': 'buddy_qc',
        },
    )
    result.reset_index(inplace=True)
    result = result.drop(columns=['measured_at'])
    result = gpd.GeoDataFrame(result, geometry='geometry', crs='EPSG:4326')
    geojson = result.to_json()
    return Response(geojson, mimetype='application/geo+json')


if __name__ == '__main__':
    app.run(debug=True)
