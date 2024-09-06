from datetime import datetime
from datetime import timedelta
from datetime import timezone
from typing import Annotated
from typing import Any
from typing import Literal

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Path
from fastapi import Query
from sqlalchemy import and_
from sqlalchemy import CompoundSelect
from sqlalchemy import func
from sqlalchemy import Function
from sqlalchemy import Select
from sqlalchemy import select
from sqlalchemy import WithinGroup
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import InstrumentedAttribute

from app import schemas
from app.database import get_db_session
from app.models import BiometData
from app.models import BiometDataHourly
from app.models import LatestData
from app.models import Station
from app.models import StationType
from app.models import TempRHData
from app.models import TempRHDataHourly
from app.schemas import _Trends
from app.schemas import GenericReturn
from app.schemas import PublicParams
from app.schemas import Trends
from app.schemas import UNIT_MAPPING

router = APIRouter()


@router.get(
    '/stations/metadata',
    response_model=GenericReturn[list[schemas.StationMetadata]],
    tags=['stations'],
)
async def get_station_metadata(db: AsyncSession = Depends(get_db_session)) -> Any:
    """API-endpoint for retrieving metadata from all available stations."""
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
    return GenericReturn(data=data.mappings().all())


@router.get(
    '/stations/latest_data',
    response_model=GenericReturn[list[schemas.StationParams]],
    response_model_exclude_unset=True,
    tags=['stations'],
)
async def get_station_latest_data(
        param: list[PublicParams] = Query(
            description=(
                'The parameter(s) to get data for. Multiple parameters can be '
                'specified. Only data from stations that provide both values is '
                'returned.'
            ),
        ),
        max_age: timedelta = Query(
            timedelta(hours=1),
            description=(
                'The maximum age a measurement can have, until the station is omitted '
                'from the results. Setting this to a short time span, you can avoid '
                'spatial inhomogeneity when displaying values on a map.'
            ),
        ),
        db: AsyncSession = Depends(get_db_session),
) -> Any:
    """API-endpoint for getting the latest data from all available stations. Only
    stations that can provide all requested parameters are returned.
    """
    columns: list[InstrumentedAttribute[Any]] = [
        getattr(LatestData, i) for i in param
    ]
    cut_off_date = datetime.now(tz=timezone.utc) - max_age
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
        LatestData.measured_at > cut_off_date,
        and_(*not_null_conditions),
    ).order_by(LatestData.name)
    data = await db.execute(query)
    return GenericReturn(data=data.mappings().all())


@router.get(
    '/districts/latest_data',
    response_model=GenericReturn[list[schemas.DistrictParams]],
    response_model_exclude_unset=True,
    tags=['districts'],
)
async def get_districts(
        param: list[PublicParams] = Query(
            description=(
                'The parameter(s) to get data for. Multiple parameters can be '
                'specified. Only data from districts that provide both values is '
                'returned.'
            ),
        ),
        max_age: timedelta = Query(
            timedelta(hours=1),
            description=(
                'The maximum age a measurement can have, until the station is omitted '
                'from the results. Setting this to a short time span, you can avoid '
                'spatial inhomogeneity when displaying values on a map.'
            ),
        ),
        db: AsyncSession = Depends(get_db_session),
) -> Any:
    """API-endpoint for getting the latest data on a per-district level. Only
    districts that can provide all parameters are returned.
    """
    query_parts = []
    not_null_conditions = []
    cut_off_date = datetime.now(tz=timezone.utc) - max_age
    for p in param:
        column: InstrumentedAttribute[Any] = getattr(LatestData, p)
        query_part = get_aggregator(col=column).label(p)
        not_null_conditions.append(column.isnot(None))
        query_parts.append(query_part)

    query = select(LatestData.district, *query_parts).where(
        (LatestData.measured_at > cut_off_date) & (
            LatestData.district.isnot(None)
        ),
        and_(*not_null_conditions),
    ).group_by(LatestData.district).order_by(LatestData.district)
    data = await db.execute(query)
    return GenericReturn(data=data.mappings().all())


def get_aggregator(col: InstrumentedAttribute[Any]) -> Function[Any] | WithinGroup[Any]:
    if '_max' in col.name:
        return func.max(col)
    elif 'category' in col.name:
        return func.mode().within_group(col.asc())
    elif 'direction' in col.name:
        return func.avg_angle(col)
    else:
        return func.avg(col)


@router.get(
    '/trends/{param}',
    response_model=GenericReturn[schemas.Trends],
)
async def get_historic_data(
        param: PublicParams,
        item_type: Literal['stations', 'districts'] = Query(),
        item_ids: list[str] = Query(),
        start_date: datetime = Query(),
        end_date: datetime | None = Query(),
        hour: int = Query(ge=0, le=23),
        db: AsyncSession = Depends(get_db_session),
) -> Any:
    if end_date is None:
        end_date = start_date

    # check if the columns exists for both station types, biomet has all columns that
    # temp_rh has, hence we check this first
    column_biomet: InstrumentedAttribute[Any] = getattr(
        BiometDataHourly, param,
    )
    column_temp_rh: InstrumentedAttribute[Any] | None = getattr(
        TempRHDataHourly,
        param,
        None,
    )

    # we need to to do this completely different depending on whether stations or
    # districts are requested
    query: Select[Any] | CompoundSelect
    if item_type == 'stations':
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
            BiometDataHourly.measured_at,
            BiometDataHourly.name.label('key'),
            column_biomet.label('value'),
        ).where(
            BiometDataHourly.measured_at.between(start_date, end_date) &
            BiometDataHourly.name.in_(item_ids) &
            (func.extract('hour', BiometDataHourly.measured_at) == hour),
        )
        # if column_temp_rh is None, the entire station type is not supported, hence we
        # can start with a default of an empty list and change it if needed.
        if column_temp_rh is not None:
            temp_rh_id_query = select(TempRHDataHourly.name).distinct(
                TempRHDataHourly.name.label('key'),
            ).where(
                TempRHDataHourly.measured_at.between(start_date, end_date) &
                (column_temp_rh.is_not(None)),
            )
            supported_temprh_ids = set(
                (await db.execute(temp_rh_id_query)).scalars().all(),
            )

            # now get the data for the requested item_ids. We label this as value, so we
            # can create key-value pairs later on
            query_temp_rh = select(
                TempRHDataHourly.measured_at,
                TempRHDataHourly.name.label('key'),
                column_temp_rh.label('value'),
            ).where(
                TempRHDataHourly.measured_at.between(start_date, end_date) &
                TempRHDataHourly.name.in_(item_ids) &
                (func.extract('hour', TempRHDataHourly.measured_at) == hour),
            )
            # we can safely combine both queries since we have this parameter at both
            # types of stations
            query = query.union_all(query_temp_rh)

        supported_ids = supported_biomet_ids | supported_temprh_ids
        data = await db.execute(query.order_by(query.c.key, query.c.value))
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
            BiometDataHourly.measured_at,
            Station.district,
            column_biomet.label('value'),
        ).join(
            Station, Station.name == BiometDataHourly.name, isouter=True,
        ).where(
            BiometDataHourly.measured_at.between(start_date, end_date) &
            Station.district.in_(item_ids) &
            (func.extract('hour', BiometDataHourly.measured_at) == hour),
        )
        # check what type of parameter we have an what aggregator we need for that
        agg = get_aggregator(col=column_biomet)

        if column_temp_rh is not None:
            temp_rh_districts_query = select(Station.district).distinct(
                Station.district,
            ).join(
                TempRHDataHourly, Station.name == TempRHDataHourly.name,
            ).where(
                TempRHDataHourly.measured_at.between(start_date, end_date) &
                (column_biomet.is_not(None)),
            )
            supported_temp_rh_districts = set((
                await db.execute(temp_rh_districts_query)
            ).scalars().all())
            # since we have data from both station types, we have to combine
            # both datasets
            temp_rh = select(
                TempRHDataHourly.measured_at,
                Station.district,
                column_temp_rh.label('value'),
            ).join(Station, Station.name == TempRHDataHourly.name, isouter=True).where(
                TempRHDataHourly.measured_at.between(start_date, end_date) &
                Station.district.in_(item_ids) &
                (func.extract('hour', TempRHDataHourly.measured_at) == hour),
            )
            # both have the same columns, so we can simply union both queries
            all_data = biomet.union_all(temp_rh)

            # again, we need to figure out the aggregate, this time, however, based on
            # the union of both queries
            agg_col: WithinGroup[Any] | Function[Any] | Any
            if '_max' in param:
                agg_col = func.max(all_data.c.value)
            elif 'category' in param:
                agg_col = func.mode().within_group(all_data.c.value.asc())
            elif 'direction' in param:
                agg_col = func.avg_angle(all_data.c.value)
            else:
                agg_col = func.avg(all_data.c.value)

            # finalize and aggregate on a district level based on the combined data
            query = select(
                all_data.c.measured_at,
                all_data.c.district.label('key'),
                agg_col.label('value'),
            ).group_by(all_data.c.district, all_data.c.measured_at).order_by(
                all_data.c.district,
                all_data.c.measured_at,
            )
        else:
            # we have no data from a temp_rh station, just query biomet
            query = select(
                biomet.c.measured_at,
                biomet.c.district.label('key'),
                agg.label('value'),
            ).group_by(
                biomet.c.district, biomet.c.measured_at,
            ).order_by(biomet.c.district, biomet.c.measured_at)

        supported_ids = supported_biomet_districts | supported_temp_rh_districts
        data = await db.execute(query)

    # we now need to slightly change the format of the data for the schema we are
    # aiming for
    trends_data = [
        _Trends({i['key']: i['value'], 'measured_at': i['measured_at']})
        for i in data.mappings()
    ]
    return GenericReturn(
        data=Trends(
            supported_ids=supported_ids,
            unit=UNIT_MAPPING[param],
            trends=trends_data,
        ),
    )


@router.get('/stats/{param}')
async def get_stats(
        param: PublicParams,
        item_type: Literal['stations', 'districts'],
        item_id: str,
        start_date: datetime,
        end_date: datetime,
        hour: Annotated[int, Query(ge=0, le=23)],
        value_type: Literal['min', 'max', 'mean'],
        db: AsyncSession = Depends(get_db_session),
) -> Any:
    ...


@router.get(
    '/data/{name}',
    response_model=GenericReturn[list[schemas.StationData]],
    response_model_exclude_unset=True,
    tags=['stations'],
)
async def get_data(
        name: str = Path(
            description='The unique name of the station e.g. `DEC005476`',
        ),
        start_date: datetime = Query(
            description='the start date of the data in UTC',
        ),
        end_date: datetime = Query(
            description='the end date of the data in UTC',
        ),
        param: list[PublicParams] = Query(
            description=(
                'The parameter(s) to get data for. Multiple parameters can be '
                'specified. Only data from districts that provide both values is '
                'returned.'
            ),
        ),
        db: AsyncSession = Depends(get_db_session),
) -> Any:
    """API-endpoint for getting the data from any station for any time-span. A
    maximum of 30 days can be requested at once. If you need more data, please
    paginate your requests.
    """
    if start_date > end_date:
        raise HTTPException(
            status_code=400,
            detail='start_date must not be > end_date',
        )

    if end_date - start_date > timedelta(days=30):
        raise HTTPException(
            status_code=400,
            detail='a maximum of 30 days is allowed per request',
        )

    station = (
        await db.execute(select(Station).where(Station.name == name))
    ).scalar_one_or_none()
    if station:
        table: type[BiometData | TempRHData]
        if station.station_type == StationType.biomet:
            table = BiometData
        else:
            table = TempRHData

        columns = [getattr(table, i) for i in param]
        data = (
            await db.execute(
                select(table.measured_at, *columns).where(
                    table.measured_at.between(start_date, end_date) &
                    (table.name == station.name),
                ),
            )
        )
        return GenericReturn(data=data.mappings().all())
    else:
        raise HTTPException(status_code=404, detail='Station not found')
