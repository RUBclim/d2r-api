from datetime import datetime
from typing import Annotated
from typing import Any
from typing import Literal

from fastapi import APIRouter
from fastapi import Depends
from fastapi import Query
from psycopg.sql import Identifier
from psycopg.sql import SQL
from sqlalchemy import select
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app import schemas
from app.database import get_db_session
from app.models import Station
from app.schemas import PublicParams

router = APIRouter()


@router.get('/stations/metadata', response_model=list[schemas.Station])
async def get_station_metadata(db: AsyncSession = Depends(get_db_session)) -> Any:
    return (
        await db.execute(
            select(
                Station.name,
                Station.latitude,
                Station.longitude,
                Station.altitude,
                Station.station_type,
            ),
        )
    )


@router.get('/stations/{param}', response_model=list[schemas.OneParam])
async def get_station_latest_data(
        param: PublicParams,
        db: AsyncSession = Depends(get_db_session),
) -> Any:
    # we need ot query a view here so it is fast!
    # we limit to measured_on, so we do not accidentally provide very old data
    # which may seem (spatially) out of context
    # TODO: This should also use the ORM, once we implemented this
    query = SQL(
        '''\
        SELECT
            name,
            long_name,
            latitude,
            longitude,
            altitude,
            measured_at,
            {param_name} AS value
        FROM latest_data
        WHERE measured_at > NOW() - '2hours'::INTERVAL
        AND {param_name} IS NOT NULL
        ORDER BY name
        ''',
    ).format(param_name=Identifier(param))
    return await db.execute(text(query.as_string()))


@router.get('/districts')
async def get_districts(
        param: PublicParams = PublicParams.utci_category,
        db: AsyncSession = Depends(get_db_session),
) -> Any:
    ...


@router.get('/trends/{param}')
async def get_historic_data(
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
