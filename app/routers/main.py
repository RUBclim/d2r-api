import random
import string
from datetime import datetime
from datetime import timezone
from typing import Any

from fastapi import APIRouter
from fastapi import Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app import schemas
from app.database import get_db_session
from app.models import Station

router = APIRouter()


@router.get('/stations/metadata', response_model=list[schemas.Station])
async def get_station(db: AsyncSession = Depends(get_db_session)) -> Any:
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


@router.get('/new-station')
async def new_station(db: AsyncSession = Depends(get_db_session)) -> Any:
    # randomly create a new station (for testing)
    station = Station(
        name=''.join(
            random.choices(
                string.ascii_uppercase + string.digits, k=8,
            ),
        ),
        device_id=12345678,
        long_name='some long station name',
        latitude=54,
        longitude=10,
        altitude=1000,
        leuchtennummer=12345,
        setup_date=datetime(2024, 8, 23, 15, 0, tzinfo=timezone.utc),
        station_type='blg',
    )
    db.add(station)
    await db.commit()
    return {'message': 'created new station!'}
