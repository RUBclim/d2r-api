from datetime import datetime
from datetime import timedelta
from datetime import timezone
from typing import Any
from typing import Literal
from typing import TypedDict

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Path
from fastapi import Query
from sqlalchemy import and_
from sqlalchemy import cast
from sqlalchemy import Column
from sqlalchemy import CompoundSelect
from sqlalchemy import func
from sqlalchemy import Function
from sqlalchemy import Select
from sqlalchemy import select
from sqlalchemy import TIMESTAMP
from sqlalchemy import WithinGroup
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import InstrumentedAttribute

from app import schemas
from app.database import get_db_session
from app.models import BiometData
from app.models import BiometDataDaily
from app.models import BiometDataHourly
from app.models import LatestData
from app.models import Station
from app.models import StationType
from app.models import TempRHData
from app.models import TempRHDataDaily
from app.models import TempRHDataHourly
from app.schemas import PublicParams
from app.schemas import PublicParamsAggregates
from app.schemas import Response
from app.schemas import Trends
from app.schemas import TrendValue
from app.schemas import UNIT_MAPPING

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
    tags=['stations'],
)
async def get_stations_metadata(db: AsyncSession = Depends(get_db_session)) -> Any:
    """API-endpoint for retrieving metadata from all available stations. This does
    not take into account whether or not they currently have any up-to-date data."""
    data = (
        await db.execute(
            select(
                Station.name,
                Station.long_name,
                Station.latitude,
                Station.longitude,
                Station.altitude,
                Station.district,
                Station.lcz,
                Station.station_type,
            ).order_by(Station.name),
        )
    )
    return Response(data=data.mappings().all())


@router.get(
    '/stations/latest_data',
    response_model=Response[list[schemas.StationParams]],
    response_model_exclude_unset=True,
    tags=['stations'],
)
async def get_stations_latest_data(
        param: list[PublicParams] = Query(
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
    depends on the `StationType`. Stations of type `StationType.biomet` support all
    parameters, stations of type `StationType.temprh` only support a subset of
    parameters, that can be derived from `air_temperature` and `relative_humidity`.
    """
    if max_age.total_seconds() < 0:
        raise HTTPException(status_code=422, detail='max_age must be positive')

    columns: list[InstrumentedAttribute[Any]] = [getattr(LatestData, i) for i in param]
    not_null_conditions = [c.isnot(None) for c in columns]
    query = select(
        LatestData.name,
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
        and_(*not_null_conditions),
    ).order_by(LatestData.name)
    data = await db.execute(query)
    return Response(data=data.mappings().all())


@router.get(
    '/districts/latest_data',
    response_model=Response[list[schemas.DistrictParams]],
    response_model_exclude_unset=True,
    tags=['districts'],
)
async def get_districts_latest_data(
        param: list[PublicParams] = Query(
            description=(
                'The parameter(-s) to get data for. Multiple parameters can be '
                'specified. Only data from districts that provide all specified values '
                'will be returned.'
            ),
        ),
        max_age: timedelta = Query(timedelta(hours=1), description=MAX_AGE_DESCRIPTION),
        db: AsyncSession = Depends(get_db_session),
) -> Any:
    """API-endpoint for getting the latest data on a per-district level. Only
    districts that can provide all parameters are returned.
    """
    if max_age.total_seconds() < 0:
        raise HTTPException(status_code=422, detail='max_age must be positive')

    query_parts = []
    not_null_conditions = []
    for p in param:
        column: InstrumentedAttribute[Any] = getattr(LatestData, p)
        query_part = get_aggregator(col=column, area_average=True).label(p)
        not_null_conditions.append(column.isnot(None))
        query_parts.append(query_part)

    query = select(LatestData.district, *query_parts).where(
        (LatestData.measured_at > datetime.now(tz=timezone.utc) - max_age) &
        (LatestData.district.isnot(None)),
        and_(*not_null_conditions),
    ).group_by(LatestData.district).order_by(LatestData.district)
    data = await db.execute(query)
    return Response(data=data.mappings().all())


def get_aggregator(
        col: Column[Any] | InstrumentedAttribute[Any],
        area_average: bool = False,
) -> Function[Any] | WithinGroup[Any]:
    """choose an appropriate aggregator based on the column name"""
    if 'max' in col.name:
        return func.max(col)
    elif 'min' in col.name:
        return func.min(col)
    elif 'category' in col.name:
        return func.mode().within_group(col.asc())
    elif 'direction' in col.name:
        return func.avg_angle(col)
    elif ('sum' in col.name or 'count' in col.name) and not area_average:
        return func.sum(col)
    else:
        return func.avg(col)


@router.get(
    '/trends/{param}',
    response_model=Response[schemas.Trends],
    tags=['districts', 'stations'],
)
async def get_trends(
        param: PublicParamsAggregates = Path(
            description=(
                'The parameter to get data for. Only data from districts that provide '
                'the value will be returned.'
            ),
        ),
        spatial_level: Literal['stations', 'districts'] = Query(
            description=(
                'Whether to get data for specific stations or all stations in a '
                'district, aggregating them spatially.'
            ),
        ),
        item_ids: list[str] = Query(
            description='Either names of the districts or names of the stations',
        ),
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

    # we need to to do this completely different depending on whether stations or
    # districts are requested
    query: Select[Any] | CompoundSelect
    if spatial_level == 'stations':
        # get the supported ids which are needed for the API return, probably for
        # possible comparison
        biomet_id_query = select(BiometDataHourly.name).distinct(
            BiometDataHourly.name,
        ).where(
            BiometDataHourly.measured_at.between(start_date, end_date) &
            (column_biomet.is_not(None)),
        )
        supported_biomet_ids = set((await db.execute(biomet_id_query)).scalars().all())

        # now get the data for the requested item_ids
        query = select(
            cast(BiometDataHourly.measured_at, TIMESTAMP(timezone=True)),
            BiometDataHourly.name.label('key'),
            column_biomet.label('value'),
        ).where(
            BiometDataHourly.measured_at.between(start_date, end_date) &
            BiometDataHourly.name.in_(item_ids) &
            (func.extract('hour', BiometDataHourly.measured_at) == hour),
        ).order_by(BiometDataHourly.name, column_biomet)
        # if column_temp_rh is None, the entire station type is not supported, hence we
        # can start with a default of an empty list and change it if needed.
        supported_temp_rh_ids = set()
        if column_temp_rh is not None:
            temp_rh_id_query = select(TempRHDataHourly.name).distinct(
                TempRHDataHourly.name.label('key'),
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
                cast(TempRHDataHourly.measured_at, TIMESTAMP(timezone=True)),
                TempRHDataHourly.name.label('key'),
                column_temp_rh.label('value'),
            ).where(
                TempRHDataHourly.measured_at.between(start_date, end_date) &
                TempRHDataHourly.name.in_(item_ids) &
                (func.extract('hour', TempRHDataHourly.measured_at) == hour),
            )
            # we can safely combine both queries since we have this parameter at both
            # types of stations
            sub_query = query.union_all(query_temp_rh).subquery()
            query = select(sub_query).order_by(sub_query.c.key, sub_query.c.value)

        supported_ids = sorted(supported_biomet_ids | supported_temp_rh_ids)
        data = await db.execute(query)
    else:
        # no the only other option are districts
        # get the supported district names
        biomet_districts_query = select(Station.district).distinct(
            Station.district,
        ).join(
            BiometDataHourly, Station.name == BiometDataHourly.name,
        ).where(
            BiometDataHourly.measured_at.between(start_date, end_date) &
            (column_biomet.is_not(None)),
        )
        supported_biomet_districts = set((
            await db.execute(biomet_districts_query)
        ).scalars().all())

        # start with the biomet, since this type supports all params
        biomet = select(
            cast(BiometDataHourly.measured_at, TIMESTAMP(timezone=True)),
            Station.district,
            column_biomet.label('value'),
        ).join(
            Station, Station.name == BiometDataHourly.name, isouter=True,
        ).where(
            BiometDataHourly.measured_at.between(start_date, end_date) &
            Station.district.in_(item_ids) &
            (func.extract('hour', BiometDataHourly.measured_at) == hour),
        )

        supported_temp_rh_districts = set()
        if column_temp_rh is not None:
            temp_rh_districts_query = select(Station.district).distinct(
                Station.district,
            ).join(
                TempRHDataHourly, Station.name == TempRHDataHourly.name,
            ).where(
                TempRHDataHourly.measured_at.between(start_date, end_date) &
                (column_temp_rh.is_not(None)),
            )
            supported_temp_rh_districts = set((
                await db.execute(temp_rh_districts_query)
            ).scalars().all())
            # since we have data from both station types, we have to combine
            # both datasets
            temp_rh = select(
                cast(TempRHDataHourly.measured_at, TIMESTAMP(timezone=True)),
                Station.district,
                column_temp_rh.label('value'),
            ).join(Station, Station.name == TempRHDataHourly.name, isouter=True).where(
                TempRHDataHourly.measured_at.between(start_date, end_date) &
                Station.district.in_(item_ids) &
                (func.extract('hour', TempRHDataHourly.measured_at) == hour),
            )
            # both have the same columns, so we can simply union both queries
            all_data = biomet.union_all(temp_rh).subquery()
            # finalize and aggregate on a district level based on the combined data
            query = select(
                all_data.c.measured_at,
                all_data.c.district.label('key'),
                func.avg(all_data.c.value).label('value'),
            ).group_by(all_data.c.district, all_data.c.measured_at).order_by(
                all_data.c.district,
                all_data.c.measured_at,
            )
        else:
            # we have no data from a temp_rh station, just query biomet
            biomet_subquery = biomet.subquery()
            agg_col: WithinGroup[Any] | Function[Any]
            if 'max' in param:
                agg_col = func.max(biomet_subquery.c.value)
            elif 'category' in param:
                agg_col = func.mode().within_group(biomet_subquery.c.value.asc())
            elif 'direction' in param:
                agg_col = func.avg_angle(biomet_subquery.c.value)
            else:
                agg_col = func.avg(biomet_subquery.c.value)

            query = select(
                biomet_subquery.c.measured_at,
                biomet_subquery.c.district.label('key'),
                agg_col.label('value'),
            ).group_by(
                biomet_subquery.c.district, biomet_subquery.c.measured_at,
            ).order_by(biomet_subquery.c.district, biomet_subquery.c.measured_at)

        supported_ids = sorted(supported_biomet_districts | supported_temp_rh_districts)
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
    allowed_params: type[PublicParams]


class DataMappingHourly(TypedDict):
    table: type[BiometDataHourly | TempRHDataHourly]
    allowed_params: type[PublicParamsAggregates]


class DataMappingDaily(TypedDict):
    table: type[BiometDataDaily | TempRHDataDaily]
    allowed_params: type[PublicParamsAggregates]


class TableMapping(TypedDict):
    max: DataMappingMax
    hourly: DataMappingHourly
    daily: DataMappingDaily


TABLE_MAPPING: dict[StationType, TableMapping] = {
    StationType.temprh: {
        'max': {
            'table': TempRHData,
            'allowed_params': PublicParams,
        },
        'hourly': {
            'table': TempRHDataHourly,
            'allowed_params': PublicParamsAggregates,
        },
        'daily': {
            'table': TempRHDataDaily,
            'allowed_params': PublicParamsAggregates,
        },
    },
    StationType.biomet: {
        'max': {
            'table': BiometData,
            'allowed_params': PublicParams,
        },
        'hourly': {
            'table': BiometDataHourly,
            'allowed_params': PublicParamsAggregates,
        },
        'daily': {
            'table': BiometDataDaily,
            'allowed_params': PublicParamsAggregates,
        },
    },
}


@router.get(
    '/data/{name}',
    response_model=Response[
        list[schemas.StationData]
    ] | Response[list[schemas.StationDataAgg]],
    response_model_exclude_unset=True,
    tags=['stations'],
)
async def get_data(
        name: str = Path(
            description='The unique name of the station e.g. `DEC005476`',
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
        param: list[PublicParamsAggregates] = Query(
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
        db: AsyncSession = Depends(get_db_session),
) -> Any:
    """API-endpoint for getting the data from any station for any time-span. A
    maximum (31 days at 5-minute resolution, 365 days at hourly resolution and 3650 days
    at daily resolution) can be requested at once. If you need more data, please
    paginate your requests.

    **Note:** When requesting, you have to take the station type into account, not every
    `StationType` supports all parameters. So the combination of `scale` and
    `StationType` determines the values you are able to request. It is important to
    check the Error responses (`422`) for the correct subset. Generally `_min` and
    `_max` parameters are available when using `scale` as `daily` or `hourly`. Stations
    of type `StationType.temprh` only have parameters that can be derived from
    air-temperature and relative humidity measurements. Stations of type
    `StationType.biomet` will support the full set of parameters.
    """
    if start_date > end_date:
        raise HTTPException(status_code=422, detail='start_date must not be > end_date')
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
        await db.execute(select(Station).where(Station.name == name))
    ).scalar_one_or_none()
    if station:
        table_info = TABLE_MAPPING[station.station_type][scale]
        table = table_info['table']
        allowed_params = table_info['allowed_params']

        for idx, p in enumerate(param):
            if p not in allowed_params or not hasattr(table, p):
                # try to mimic the usual validation error response
                allowed_vals = sorted(
                    {e.name for e in allowed_params} & {
                        i.key for i in table.__table__.columns
                    },
                )
                raise HTTPException(
                    status_code=422,
                    detail=[{
                        'type': 'enum',
                        'loc': ['query', 'param', idx],
                        'msg': (
                            f'This station is of type "{station.station_type}", hence '
                            f"the input should be: {', '.join(list(allowed_vals))}"
                        ),
                        'input': p,
                        'ctx': {
                            'expected': f"{', '.join(list(allowed_vals))}",
                        },
                    }],
                )

        columns = [getattr(table, i) for i in param]
        data = (
            await db.execute(
                # we need to cast to TIMESTAMPTZ here, since the view is in UTC but
                # timescale cannot keep it timezone aware AND make it right-labelled
                # + 1 hour
                select(
                    cast(table.measured_at, TIMESTAMP(timezone=True)),
                    *columns,
                ).where(
                    table.measured_at.between(start_date, end_date) &
                    (table.name == station.name),
                ).order_by(table.measured_at),
            )
        )
        return Response(data=data.mappings().all())
    else:
        raise HTTPException(status_code=404, detail='Station not found')
