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
from psycopg.sql import Identifier
from psycopg.sql import SQL
from sqlalchemy import select
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app import schemas
from app.database import get_db_session
from app.models import BiometData
from app.models import Station
from app.models import StationType
from app.models import TempRHData
from app.schemas import PublicParams

router = APIRouter()


@router.get(
    '/stations/metadata',
    response_model=list[schemas.StationMetadata],
    tags=['stations'],
)
async def get_station_metadata(db: AsyncSession = Depends(get_db_session)) -> Any:
    """API-endpoint for retrieving metadata from all available stations."""
    return (
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


@router.get(
    '/stations/latest_data',
    response_model=list[schemas.StationParams],
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
    # we need ot query a view here so it is fast!
    identifiers = [Identifier(p.value) for p in param]
    conditions = [SQL('AND {f} IS NOT NULL').format(f=i) for i in identifiers]
    query = SQL(
        '''\
        SELECT
            name,
            long_name,
            latitude,
            longitude,
            altitude,
            district,
            lcz,
            station_type,
            measured_at,
            {fields}
        FROM latest_data
        WHERE measured_at > {cut_off_date} {conditions}
        ORDER BY name
        ''',
    ).format(
        fields=SQL(',').join(identifiers),
        conditions=SQL(' ').join(conditions),
        cut_off_date=datetime.now(tz=timezone.utc) - max_age,
    )
    return await db.execute(text(query.as_string()))


@router.get(
    '/districts/latest_data',
    response_model=list[schemas.DistrictParams],
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
    conditions = []
    for p in param:
        if '_max' in param:
            query_part = SQL('MAX({p}) AS {p}')
        elif 'category' in param:
            query_part = SQL('MODE() WITHIN GROUP (ORDER BY {p}) AS {p}')
        elif 'direction' in param:
            pass
        else:
            query_part = SQL('AVG({p}) AS {p}')

        conditions.append(SQL('AND {f} IS NOT NULL').format(f=Identifier(p)))
        query_parts.append(query_part.format(p=Identifier(p)))

    query = SQL(
        '''\
            SELECT
                district,
                {query_part},
            FROM latest_data
            WHERE
                district IS NOT NULL AND
                measured_at > {cut_off_date}
                {conditions}
            GROUP BY district
        ''',
    ).format(
        query_part=SQL(', ').join(query_parts),
        conditions=SQL(' ').join(conditions),
        cut_off_date=datetime.now(tz=timezone.utc) - max_age,
    )
    return await db.execute(text(query.as_string()))


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
    response_model=list[schemas.StationData],
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
        ).all()
        return data
    else:
        raise HTTPException(status_code=404, detail='Station not found')
