import csv
import io
import math
from collections.abc import AsyncGenerator
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from decimal import Decimal
from typing import Any
from typing import Literal
from typing import TypedDict

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Path
from fastapi import Query
from fastapi.responses import StreamingResponse
from sqlalchemy import and_
from sqlalchemy import cast
from sqlalchemy import CompoundSelect
from sqlalchemy import exists
from sqlalchemy import func
from sqlalchemy import not_
from sqlalchemy import Select
from sqlalchemy import select
from sqlalchemy import TIMESTAMP
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import InstrumentedAttribute

from app import schemas
from app.database import get_db_session
from app.database import sessionmanager
from app.models import BiometData
from app.models import BiometDataDaily
from app.models import BiometDataHourly
from app.models import BuddyCheckQc
from app.models import LatestData
from app.models import SensorDeployment
from app.models import Station
from app.models import StationType
from app.models import TempRHData
from app.models import TempRHDataDaily
from app.models import TempRHDataHourly
from app.schemas import NetworkValue
from app.schemas import ParamSettings
from app.schemas import PublicParams
from app.schemas import PublicParamsAggBiomet
from app.schemas import PublicParamsAggTempRH
from app.schemas import PublicParamsBiomet
from app.schemas import PublicParamsTempRH
from app.schemas import Response
from app.schemas import Trends
from app.schemas import TrendValue
from app.schemas import UNIT_MAPPING
from app.schemas import VisualizationSuggestion
from app.schemas import VizParamSettings
from app.schemas import VizResponse

router = APIRouter(prefix='/v1')

MAX_AGE_DESCRIPTION = '''\
The maximum age a measurement can have, until the station is omitted  from the results.
Setting this to a short time span, you can avoid  spatial inhomogeneity due to old data
when displaying values on a map.  The format is expected to be in
[ISO 8601](https://en.wikipedia.org/wiki/ISO_8601#Durations)
(see section 5.5.2 in the
[ISO-norm](https://www.iso.org/iso-8601-date-and-time-format.html)).
Which defines the format as:
- `["P"][i]["Y"][i]["M"][i]["D"]["T"][i]["H"][i]["M"][i]["S"]`
- `["P"][i]["W"]`

E.g. 1 hour corresponds to `PT1H`, 10 minutes to `PT10M`, 5 seconds  to `PT10S` 3 days
to `P3D`. For further explanation of the symbols see section 3.2 Symbols.
'''


@router.api_route('/healthcheck', include_in_schema=False, methods=['GET', 'HEAD'])
async def is_healthy(db: AsyncSession = Depends(get_db_session)) -> dict[str, str]:
    """API-endpoint to check whether the system is healthy. This is used by docker to
    check the container health. A HEAD request will not trigger a db-query, only a GET
    request will do that."""
    await db.execute(select(1))
    return {'message': "I'm healthy!"}


@router.get(
    '/stations/metadata',
    response_model=Response[list[schemas.StationMetadata]],
    response_model_exclude_unset=True,
    tags=['stations'],
)
async def get_stations_metadata(
        param: list[schemas.PublicStationMetadata] = Query(
            None,
            description=(
                'The parameter(s) of the metadata to get. If no parameter is set, all '
                'available parameters will be returned.'
            ),
        ),
        include_inactive: bool = Query(
            False,
            description=(
                'If True, also stations that currently do not have an active '
                'deployment (sensor mounted) are included, otherwise they are omitted.'
            ),
        ),
        db: AsyncSession = Depends(get_db_session),
) -> Any:
    """API-endpoint for retrieving metadata from all available stations. This does
    not take into account whether or not they currently have any up-to-date data."""
    # if no parameters are specified, send all available params
    if not param:
        columns = [getattr(Station, i.value) for i in schemas.PublicStationMetadata]
    else:
        columns = [getattr(Station, i) for i in param]
    query = select(Station.station_id, *columns)
    if include_inactive is False:
        query = query.where(
            exists().where(
                (SensorDeployment.station_id == Station.station_id) &
                # a deployment without a teardown_date is an active deployment
                (SensorDeployment.teardown_date.is_(None)),
            ),
        )

    data = (await db.execute(query.order_by(Station.station_id)))
    return Response(data=data.mappings().all())


@router.get(
    '/stations/latest_data',
    response_model=Response[list[schemas.StationParams]],
    response_model_exclude_unset=True,
    tags=['stations'],
)
async def get_stations_latest_data(
        param: list[PublicParamsBiomet] = Query(
            description=(
                'The parameter(-s) to get data for. Multiple parameters can be '
                'specified. Only data from stations that provide all specified values '
                'will be returned.'
            ),
        ),
        max_age: timedelta = Query(timedelta(hours=1), description=MAX_AGE_DESCRIPTION),
        db: AsyncSession = Depends(get_db_session),
) -> Any:
    """API-endpoint for getting the latest data from all available stations. Only
    stations that can provide all requested parameters are returned. The availability
    depends on the `StationType`. Stations of type `StationType.biomet` support **all**
    parameters, stations of type `StationType.temprh` only support a **subset** of
    parameters, that can be derived from `air_temperature` and `relative_humidity`
    which are:

    - `air_temperature`
    - `relative_humidity`
    - `dew_point`
    - `absolute_humidity`
    - `heat_index`
    - `wet_bulb_temperature`
    """
    if max_age.total_seconds() < 0:
        raise HTTPException(status_code=422, detail='max_age must be positive')

    columns: list[InstrumentedAttribute[Any]] = [getattr(LatestData, i) for i in param]
    # don't apply the criterion on the qc columns - the check might still be pending
    not_null_conditions = [c.isnot(None) for c in columns if 'qc' not in c.name]
    query = select(
        LatestData.station_id,
        LatestData.long_name,
        LatestData.latitude,
        LatestData.longitude,
        LatestData.altitude,
        LatestData.district,
        LatestData.measured_at,
        LatestData.lcz,
        LatestData.station_type,
        LatestData.measured_at,
        *columns,
    ).where(
        LatestData.measured_at > (datetime.now(tz=timezone.utc) - max_age),
        and_(True, *not_null_conditions),
    ).order_by(LatestData.station_id)
    data = await db.execute(query)
    return Response(data=data.mappings().all())


@router.get(
    '/trends/{param}',
    response_model=Response[schemas.Trends],
    tags=['stations'],
)
async def get_trends(
        param: PublicParamsAggBiomet | PublicParamsAggTempRH = Path(
            description=(
                'The parameter to get data for. Only data from stations that provide '
                'the value will be returned.'
            ),
        ),
        item_ids: list[str] = Query(description='The ids of the stations'),
        start_date: datetime = Query(
            description=(
                'Provide data only after this date and time (inclusive). The format '
                'must follow the '
                '[ISO 8601](https://www.iso.org/iso-8601-date-and-time-format.html) '
                'standard.'
            ),
        ),
        end_date: datetime | None = Query(
            None,
            description=(
                'Provide data only before this date and time (inclusive). If this is '
                'not specified, it will be set to `start_date` hence returning data '
                'for one exact date.  The format must follow the '
                '[ISO 8601](https://www.iso.org/iso-8601-date-and-time-format.html) '
                'standard.'
            ),
        ),
        hour: int = Query(
            ge=0,
            le=23,
            description=(
                'The hour (UTC) to get data for. Hours can be provided as an integer '
                'with or without a leading zero.'
            ),
        ),
        db: AsyncSession = Depends(get_db_session),
) -> Any:
    """Retrieve hourly data for a specific parameter across a time period, choosing
    either districts or stations. Data is based on hourly aggregates and refer to a
    specific hour of the day.
    """
    if end_date is None:
        end_date = start_date

    # check if the columns exists for both station types, biomet has all columns that
    # temp_rh has, hence we check this first
    column_biomet: InstrumentedAttribute[Any] = getattr(BiometDataHourly, param)
    column_temp_rh: InstrumentedAttribute[Any] | None = getattr(
        TempRHDataHourly,
        param,
        None,
    )

    # get the supported ids which are needed for the API return, probably for
    # possible comparison
    biomet_id_query = select(BiometDataHourly.station_id).distinct(
        BiometDataHourly.station_id,
    ).where(
        BiometDataHourly.measured_at.between(start_date, end_date) &
        (column_biomet.is_not(None)),
    )
    supported_biomet_ids = set((await db.execute(biomet_id_query)).scalars().all())

    # now get the data for the requested item_ids
    query = select(
        BiometDataHourly.measured_at,
        BiometDataHourly.station_id.label('key'),
        column_biomet.label('value'),
    ).where(
        BiometDataHourly.measured_at.between(start_date, end_date) &
        BiometDataHourly.station_id.in_(item_ids) &
        (func.extract('hour', BiometDataHourly.measured_at) == hour),
    ).order_by(BiometDataHourly.station_id, column_biomet)
    # if column_temp_rh is None, the entire station type is not supported, hence we
    # can start with a default of an empty set and change it if needed.
    supported_temp_rh_ids = set()
    if column_temp_rh is not None:
        temp_rh_id_query = select(TempRHDataHourly.station_id).distinct(
            TempRHDataHourly.station_id.label('key'),
        ).where(
            TempRHDataHourly.measured_at.between(start_date, end_date) &
            (column_temp_rh.is_not(None)),
        )
        supported_temp_rh_ids = set(
            (await db.execute(temp_rh_id_query)).scalars().all(),
        )

        # now get the data for the requested item_ids. We label this as value, so we
        # can create key-value pairs later on
        query_temp_rh = select(
            TempRHDataHourly.measured_at,
            TempRHDataHourly.station_id.label('key'),
            column_temp_rh.label('value'),
        ).where(
            TempRHDataHourly.measured_at.between(start_date, end_date) &
            TempRHDataHourly.station_id.in_(item_ids) &
            (func.extract('hour', TempRHDataHourly.measured_at) == hour),
        )
        # we can safely combine both queries since we have this parameter at both
        # types of stations
        sub_query = query.union_all(query_temp_rh).subquery()
        query = select(sub_query).order_by(sub_query.c.key, sub_query.c.value)

    supported_ids = sorted(supported_biomet_ids | supported_temp_rh_ids)
    data = await db.execute(query)

    # we now need to slightly change the format of the data for the schema we are
    # aiming for
    trends_data = [
        TrendValue({i['key']: i['value'], 'measured_at': i['measured_at']})
        for i in data.mappings()
    ]
    return Response(
        data=Trends(
            supported_ids=supported_ids,
            unit=UNIT_MAPPING[param],
            trends=trends_data,
        ),
    )


# we need this strongly typed, so the type checking works when switching
# between tables and params in get_data


class DataMappingMax(TypedDict):
    table: type[BiometData | TempRHData]
    allowed_params: type[PublicParamsBiomet | PublicParamsTempRH]


class DataMappingHourly(TypedDict):
    table: type[BiometDataHourly | TempRHDataHourly]
    allowed_params: type[PublicParamsAggBiomet | PublicParamsAggTempRH]


class DataMappingDaily(TypedDict):
    table: type[BiometDataDaily | TempRHDataDaily]
    allowed_params: type[PublicParamsAggBiomet | PublicParamsAggTempRH]


class TableMapping(TypedDict):
    max: DataMappingMax
    hourly: DataMappingHourly
    daily: DataMappingDaily


TABLE_MAPPING: dict[StationType, TableMapping] = {
    StationType.temprh: {
        'max': {
            'table': TempRHData,
            'allowed_params': PublicParamsTempRH,
        },
        'hourly': {
            'table': TempRHDataHourly,
            'allowed_params': PublicParamsAggTempRH,
        },
        'daily': {
            'table': TempRHDataDaily,
            'allowed_params': PublicParamsAggTempRH,
        },
    },
    StationType.biomet: {
        'max': {
            'table': BiometData,
            'allowed_params': PublicParamsBiomet,
        },
        'hourly': {
            'table': BiometDataHourly,
            'allowed_params': PublicParamsAggBiomet,
        },
        'daily': {
            'table': BiometDataDaily,
            'allowed_params': PublicParamsAggBiomet,
        },
    },
    # we simply treat a double station as a biomet station for now
    StationType.double: {
        'max': {
            'table': BiometData,
            'allowed_params': PublicParamsBiomet,
        },
        'hourly': {
            'table': BiometDataHourly,
            'allowed_params': PublicParamsAggBiomet,
        },
        'daily': {
            'table': BiometDataDaily,
            'allowed_params': PublicParamsAggBiomet,
        },
    },
}


@router.get(
    '/data/{station_id}',
    response_model=Response[
        list[schemas.StationData]
    ] | Response[list[schemas.StationDataAgg]],
    response_model_exclude_unset=True,
    tags=['stations'],
)
async def get_data(
        station_id: str = Path(
            description='The unique identifier of the station e.g. `DOBHAP`',
        ),
        start_date: datetime = Query(
            description='the start date of the data in UTC. The format must follow the '
            '[ISO 8601](https://www.iso.org/iso-8601-date-and-time-format.html) '
            'standard.',
        ),
        end_date: datetime = Query(
            description='the end date of the data in UTC. The format must follow the '
            '[ISO 8601](https://www.iso.org/iso-8601-date-and-time-format.html) '
            'standard.',
        ),
        param: list[PublicParams] = Query(
            description=(
                'The parameter(-s) to get data for. Multiple parameters can be '
                'specified. `_min` and `_max` parameters are only available for '
                'aggregates i.e. `hourly` and `daily`, but not `max`. Set `scale` '
                'accordingly.'
            ),
        ),
        scale: Literal['max', 'hourly', 'daily'] = Query(
            'max',
            description=(
                'The temporal scale to get data for. If using anything other than '
                '`max`, additional `_min` and `_max` values will be available.'
            ),
        ),
        fill_gaps: bool = Query(
            True,
            description=(
                'Fill gaps of missing timestamps in the result. In hourly and '
                'daily aggregates gaps are filled with `NULL` values to keep a '
                'consistent time step interval. To save on the amount of data '
                'transmitted this can be set to False, omitting filled gaps.'
            ),
        ),
        db: AsyncSession = Depends(get_db_session),
) -> Any:
    """API-endpoint for getting the data from any station for any time-span. A
    maximum of 31 days at 5-minute resolution, 365 days at hourly resolution and 3650
    days at daily resolution can be requested at once. If you need more data, please
    paginate your requests.

    **Note:** When requesting, you have to take the station type into account, not every
    `StationType` supports all parameters. So the combination of `scale` and
    `StationType` determines the values you are able to request. It is important to
    check the Error responses (`422`) for the correct subset. Generally `_min` and
    `_max` parameters are available when using `scale` as `daily` or `hourly`. Stations
    of type `StationType.biomet` will support the full set of parameters. Stations
    of type `StationType.temprh` only have parameters that can be derived from
    air-temperature and relative humidity measurements, which are:

    - `air_temperature`
    - `relative_humidity`
    - `dew_point`
    - `absolute_humidity`
    - `heat_index`
    - `wet_bulb_temperature`

    **Temporal Aggregation** (`scale` parameter): _Hourly_ values are **right**-labeled
    aggregates. Values from `2024-01-01 10:00:00+00:00` until
    `2024-01-01 10:59:59.999999+00:00` are labelled as `2024-01-01 11:00:00+00:00` and
    hence `start_date` and `end_date` have to take that into account when being used
    with `scale=hourly`.

    _Daily_ values are aggregated _internally_ based on the **UTC+1** timezone, meaning
    a daily average covers the period from `2024-01-10 23:00:00+00:00` to
    `2024-01-11 22:59:59.999999+00:00`. This approach was chosen for (annual)
    consistency, rather than aligning with true solar time.

    **There is no daylight savings time in UTC!**

    `_min` and `_max` parameters are calculated upon aggregation, deriving the
    minimum and maximum of each parameter from the raw data. This is done on a per-hour
    basis as well as a on daily basis, always deriving extremes from the raw values.
    Discrete values such as heat stress categories are aggregated using the mode
    (most common category in the time period).

    For the daily scale, aggregations for a station are set to `null` if less than 70 %
    of the expected values are present.
    """
    if start_date > end_date:
        raise HTTPException(
            status_code=422,
            detail='start_date must not be greater than end_date',
        )
    # we allow dynamic maximums depending on the scale to not try sending too much data
    if scale == 'max':
        delta = timedelta(days=31)
    elif scale == 'hourly':
        delta = timedelta(days=365)
    else:
        delta = timedelta(days=365*10)

    if end_date - start_date > delta:
        raise HTTPException(
            status_code=422,
            detail=(
                f'a maximum of {delta.total_seconds() / 60 / 60 / 24:.0f} '
                f'days is allowed per request'
            ),
        )

    station = (
        await db.execute(select(Station).where(Station.station_id == station_id))
    ).scalar_one_or_none()
    if station:
        table_info = TABLE_MAPPING[station.station_type][scale]
        table = table_info['table']
        allowed_params = table_info['allowed_params']
        has_buddy_check_cols = False
        for idx, p in enumerate(param):
            if p not in allowed_params or (
                not hasattr(table, p) and not hasattr(BuddyCheckQc, p)
            ):
                # try to mimic the usual validation error response
                allowed_vals = {e.name for e in allowed_params} & {
                    i.key for i in table.__table__.columns
                }
                # check that we have a table that has qc columns
                if 'air_temperature_qc_range_check' in allowed_vals:
                    # if we have qc columns, we need to add them to the allowed values
                    # since they are valid for BuddyCheckQc
                    allowed_buddy_check_vals = {
                        i.key for i in BuddyCheckQc.__table__.columns
                        if any(j for j in allowed_vals if j in i.key)
                    }
                    allowed_vals = allowed_vals | allowed_buddy_check_vals
                raise HTTPException(
                    status_code=422,
                    detail=[{
                        'type': 'enum',
                        'loc': ['query', 'param', idx],
                        'msg': (
                            f'This station is of type "{station.station_type}", hence '
                            f"the input should be: {', '.join(sorted(allowed_vals))}"
                        ),
                        'input': p,
                        'ctx': {
                            'expected': f"{', '.join(sorted(allowed_vals))}",
                        },
                    }],
                )
            has_buddy_check_cols |= hasattr(BuddyCheckQc, p)

        columns: list[InstrumentedAttribute[Any]] = [
            getattr(table, i) if hasattr(table, i) else getattr(BuddyCheckQc, i)
            for i in param
        ]
        # we need to cast to TIMESTAMPTZ here, since the view is in UTC but timescale
        # cannot keep it timezone aware AND make it right-labelled +1 hour
        query = select(
            cast(table.measured_at, TIMESTAMP(timezone=True)),
            *columns,
        ).where(
            table.measured_at.between(start_date, end_date) &
            (table.station_id == station.station_id),
        ).order_by(table.measured_at)

        if fill_gaps is False:
            null_condition = [c.is_(None) for c in columns]
            # a filled gap is defined by NULL values in all columns
            query = query.where(not_(and_(*null_condition)))
        if has_buddy_check_cols is True:
            query = query.join(
                BuddyCheckQc,
                and_(
                    BuddyCheckQc.station_id == table.station_id,
                    BuddyCheckQc.measured_at == table.measured_at,
                ),
                isouter=True,
            )
        data = (await db.execute(query))
        return Response(data=data.mappings().all())
    else:
        raise HTTPException(status_code=404, detail='Station not found')


def compute_colormap_range(
        *,
        data_min: float | Decimal | None,
        data_max: float | Decimal | None,
        param_setting: ParamSettings | None,
) -> tuple[float, float] | tuple[None, None]:
    """calculate a colormap range based on the data and the expected range of a
    parameter.

    :param data_min: the minimum value of the data
    :param data_max: the maximum value of the data
    :param param_setting: information on the specific parameter

    :return: the minimum and maximum value for the colormap
    """
    if data_min is None or data_max is None:
        return None, None

    # in case they are Decimals when returned from the db
    data_min = float(data_min)
    data_max = float(data_max)

    # if we have no info on the param, default to min/max scaling
    if param_setting is None:
        return data_min, data_max

    data_range = data_max - data_min
    expected_range = param_setting.percentile_95 - param_setting.percentile_5
    minimum_range = abs(expected_range * param_setting.fraction)
    # if the data range is greater than n-percent of the expected range simply use the
    # original data range
    vmin = data_min
    vmax = data_max
    # the range is smaller, we need to extend it
    if data_range < minimum_range:
        # some values may be all the same, then just extend it to the minimum range we
        # expect with the value centered
        if data_range == 0:
            vmin = data_min - (minimum_range / 2)
            vmax = data_max + (minimum_range / 2)
        else:
            val_diff = minimum_range - data_range
            vmin = data_min - (val_diff / 2)
            vmax = data_max + (val_diff / 2)

    # check if we are below or above the theoretical min and max ?
    if (vmin < param_setting.valid_min) and math.isfinite(param_setting.valid_min):
        vmin = param_setting.valid_min

    if (vmax > param_setting.valid_max) and math.isfinite(param_setting.valid_max):
        vmax = param_setting.valid_max

    return vmin, vmax


@router.get(
    '/network-snapshot',
    response_model=VizResponse[list[NetworkValue]],
    response_model_exclude_unset=True,
    tags=['stations'],
)
async def get_network_snapshot(
        param: list[PublicParamsAggBiomet] | list[PublicParamsAggTempRH] = Query(
            description=(
                'The parameter(-s) to get data for. Multiple parameters can be '
                'specified.'
            ),
            examples=['air_temperature'],
        ),
        scale: Literal['hourly', 'daily'] = Query(
            description='The temporal scale to get data for.',
            examples=['hourly'],
        ),
        date: datetime = Query(
            description=(
                'The date (and time when `scale=hourly`) to get data for. The format '
                'must follow the '
                '[ISO 8601](https://www.iso.org/iso-8601-date-and-time-format.html) '
                'standard. Everything provided above the hour precision is omitted, '
                'meaning `2024-01-01 10:15:13.12345` becomes `2024-01-01 10:00`.'
            ),
            examples=[datetime(2024, 1, 1, 10, 0)],
        ),
        suggest_viz: bool = Query(
            False,
            description=(
                'If True, a suggestion for the colormap range is returned based on '
                'the data returned'
            ),
            examples=[True],
        ),
        db: AsyncSession = Depends(get_db_session),
) -> Any:
    """API endpoint for retrieving data from all network stations at a specific time.

    You may specify multiple params. The availability of the param depends on the
    `StationType`. Stations of type `StationType.biomet` support **all**
    parameters, stations of type `StationType.temprh` only support a **subset** of
    parameters, that can be derived from `air_temperature` and `relative_humidity`
    which are:

    - `air_temperature`
    - `relative_humidity`
    - `dew_point`
    - `absolute_humidity`
    - `heat_index`
    - `wet_bulb_temperature`

    Since `StationType.temprh` only supports a subset, the unsupported parameters are
    set to `null` when multiple params are requested. If `StationType.temprh` does not
    support any of the requested parameters, these stations are completely omitted from
    the result.
    """
    # we remove any precision that exceeds "hour" since the max resolution is hourly
    date = date.replace(minute=0, second=0, microsecond=0)

    biomet_table: type[BiometDataHourly | BiometDataDaily]
    temp_rh_table: type[TempRHDataHourly | TempRHDataDaily]
    if scale == 'hourly':
        biomet_table = BiometDataHourly
        temp_rh_table = TempRHDataHourly
    elif scale == 'daily':
        biomet_table = BiometDataDaily
        temp_rh_table = TempRHDataDaily
    else:
        raise NotImplementedError('unknown scale')

    # extract the column attributes
    columns: list[InstrumentedAttribute[Any]] = [
        getattr(biomet_table, i) for i in param
    ]
    # temp_rh is always a subset of biomet
    columns_temp_rh: list[InstrumentedAttribute[Any] | None] = [
        getattr(temp_rh_table, i, None) for i in param
    ]
    query: Select[Any] | CompoundSelect[Any]
    query = select(
        biomet_table.measured_at,
        biomet_table.station_id,
        Station.station_type,
        *columns,
    ).join(
        Station, Station.station_id == biomet_table.station_id,
    ).where(biomet_table.measured_at == date).order_by(biomet_table.station_id)
    # check that not all of the temp_rh columns are None, if so, just don't query
    # them at all
    if any(columns_temp_rh):
        query = query.union_all(
            select(
                temp_rh_table.measured_at,
                temp_rh_table.station_id,
                Station.station_type,
                *columns_temp_rh,
            ).join(
                Station, Station.station_id == temp_rh_table.station_id,
            ).where(
                temp_rh_table.measured_at == date,
            ).order_by(temp_rh_table.station_id),
        )

    data = (await db.execute(query)).mappings().all()
    visualizations: dict[
        PublicParamsAggBiomet |
        PublicParamsAggTempRH, VisualizationSuggestion | None,
    ] | None = None
    if suggest_viz is True and data:
        # compute a suggestion for visualization based on the data
        visualizations = {}
        # now derive some statistics from the query
        stat_sub_query = query.cte('stat_sub_query')
        # compile the min/max functions per parameter
        agg_params = []
        for p in param:
            agg_params.append(func.min(stat_sub_query.c[p]).label(f'{p}_min'))
            agg_params.append(func.max(stat_sub_query.c[p]).label(f'{p}_max'))

        final = select(*agg_params)
        stat_data = (await db.execute(final)).mappings().one()
        for p in param:
            if 'category' in p:
                visualizations[p] = None
            else:
                param_min = stat_data[f'{p}_min']
                param_max = stat_data[f'{p}_max']
                cmin, cmax = compute_colormap_range(
                    data_min=param_min,
                    data_max=param_max,
                    param_setting=VizParamSettings.get(p),
                )
                visualizations[p] = VisualizationSuggestion(
                    cmin=cmin, cmax=cmax,
                    vmin=param_min, vmax=param_max,
                )

    return VizResponse(data=data, visualization=visualizations)


async def stream_results(stm: Select[Any]) -> AsyncGenerator[str]:
    """Stream the results of a query in batches of 250 rows as CSV. This is used to
    stream large amounts of data to the client without having to load everything into
    memory.

    :param stm: the database query to execute
    :param db: the database session to pass from the route
    :return: an (async) generator that yields CSV-formatted strings in batches
        of 250 rows.
    """
    async with sessionmanager.session() as db:
        # stream results in batches of 250 rows
        r = await db.stream(stm.execution_options(stream_results=True, yield_per=250))
        # we write to the buffer and yield it
        buffer = io.StringIO(newline='')
        # use the csv writer to write the header
        writer = csv.writer(buffer, dialect='excel')
        # write the header
        writer.writerow(r.keys())
        yield buffer.getvalue()
        buffer.seek(0)
        async for batch in r.partitions():
            # clear the previous buffer by truncating it
            buffer.truncate()
            # convert the row to csv-like string
            writer.writerows(batch)
            # set the pointer to the beginning of the buffer
            buffer.seek(0)
            # stream the buffer
            yield buffer.getvalue()
            buffer.seek(0)


@router.get('/download/{station_id}', tags=['stations'])
async def download_station_data(
        station_id: str = Path(
            description='The unique identifier of the station e.g. `DOBHAP`',
        ),
        start_date: datetime = Query(
            None,
            description='the start date of the data in UTC. The format must follow the '
            '[ISO 8601](https://www.iso.org/iso-8601-date-and-time-format.html) '
            'standard. This parameter is optional, and if not provided (the default), '
            'all available data will be returned.',
        ),
        end_date: datetime = Query(
            None,
            description='the end date of the data in UTC. The format must follow the '
            '[ISO 8601](https://www.iso.org/iso-8601-date-and-time-format.html) '
            'standard. This parameter is optional, and if not provided (the default), '
            'all available data up until now will be returned.',
        ),
        scale: Literal['max', 'hourly', 'daily'] = Query(
            'max',
            description=(
                'The temporal scale to get data for. If using anything other than '
                '`max`, additional `_min` and `_max` values will be available.'
            ),
        ),
        fill_gaps: bool = Query(
            False,
            description=(
                'Fill gaps of missing timestamps in the result. In hourly and '
                'daily aggregates gaps are filled with `NULL` values to keep a '
                'consistent time step interval. To save on the amount of data '
                'transmitted this can be set to False, omitting filled gaps. This only '
                'applies to hourly and daily scales and not the original data (`max`).'
            ),
        ),
        db: AsyncSession = Depends(get_db_session),
) -> StreamingResponse:
    """This endpoint allows for downloading large amounts of data on a per-station
    basis. The data is provided as a CSV-file and always includes all publicly available
    columns. The set of columns may vary depending on the type of station (`temprh` has
    a different set of columns than `biomet`) and the temporal scale (`max`, `hourly`,
    `daily`), where the last two will also provide extreme values (`_min` and `_max`).
    """
    station = (
        await db.execute(select(Station).where(Station.station_id == station_id))
    ).scalar_one_or_none()
    if station is None:
        raise HTTPException(status_code=404, detail='station not found')

    table = TABLE_MAPPING[station.station_type][scale]['table']
    columns = [
        getattr(table, i.value)
        if hasattr(table, i.value) else getattr(BuddyCheckQc, i.value)
        for i in TABLE_MAPPING[station.station_type][scale]['allowed_params']
    ]
    # sort the columns by name for a nicer output and the qc columns next to the param
    columns = sorted(columns, key=lambda c: c.name)
    if hasattr(table, 'qc_flagged'):
        # we want the qc_flagged column next to the parameter columns, so we move it
        # to the end of the list
        columns.remove(table.qc_flagged)
        columns.append(table.qc_flagged)

    stm = select(
        table.station_id,
        table.measured_at,
        *columns,
    ).join(
        BuddyCheckQc,
        and_(
            BuddyCheckQc.station_id == table.station_id,
            BuddyCheckQc.measured_at == table.measured_at,
        ),
        isouter=True,
    ).where(table.station_id == station_id).order_by(table.measured_at)
    if fill_gaps is False:
        null_condition = [c.is_(None) for c in table.__table__.columns]
        # a filled gap is defined by NULL values in all columns
        stm = stm.where(not_(and_(*null_condition)))
    # return data starting from that date up until now
    if start_date is not None and end_date is None:
        stm = stm.where(table.measured_at >= start_date)
    elif start_date is None and end_date is not None:
        stm = stm.where(table.measured_at <= end_date)
    elif start_date is not None and end_date is not None:
        stm = stm.where(table.measured_at.between(start_date, end_date))

    return StreamingResponse(
        stream_results(stm),
        media_type='text/csv',
        headers={
            'Content-Disposition': (
                f'attachment; filename="{station.station_id}_{scale}.csv"'
            ),
        },
    )
