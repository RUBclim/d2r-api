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
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import InstrumentedAttribute

from app import schemas
from app.database import get_db_session
from app.models import BiometData
from app.models import LatestData
from app.models import Station
from app.models import StationType
from app.models import TempRHData
from app.schemas import GenericReturn
from app.schemas import PublicParams

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
        if '_max' in p:
            query_part = func.max(column).label(p)
        elif 'category' in p:
            query_part = func.mode().within_group(column.asc()).label(p)
        elif 'direction' in p:
            query_part = func.avg_angle(column).label(p)
        else:
            query_part = func.avg(column).label(p)

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


@router.get('/trends/{param}')
async def get_historic_data(
        param: PublicParams,
        item_type: Literal['stations', 'districts'],
        item_id: str,
        start_date: datetime | None,
        end_date: datetime,
        hour: Annotated[int, Query(ge=0, le=23)],
        value_type: Literal['min', 'max', 'mean'],
        db: AsyncSession = Depends(get_db_session),
) -> Any:
    ...


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
