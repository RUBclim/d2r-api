import asyncio

from celery import chord
from celery import group
from sqlalchemy import delete
from sqlalchemy import select

from app.database import sessionmanager
from app.models import BiometData
from app.models import BuddyCheckQc
from app.models import Station
from app.models import StationType
from app.models import TempRHData
from app.tasks import calculate_biomet
from app.tasks import calculate_temp_rh
from app.tasks import perform_spatial_buddy_check
from app.tasks import refresh_all_views


async def amain() -> int:
    async with sessionmanager.session() as sess:
        # 1. delete the data
        print('Deleting all data from BiometData, TempRHData, BuddyCheckQc')
        await sess.execute(delete(BiometData))
        await sess.execute(delete(TempRHData))
        await sess.execute(delete(BuddyCheckQc))
        await sess.commit()

        # 2. recompute the data
        print('Recomputing data for all stations (enqueuing tasks)')
        stations = (
            await sess.execute(select(Station).order_by(Station.station_id))
        ).scalars().all()
        tasks = []
        for station in stations:
            match station.station_type:
                case StationType.biomet:
                    tasks.append(calculate_biomet.s(station.station_id))
                case StationType.temprh:
                    tasks.append(calculate_temp_rh.s(station.station_id))
                case StationType.double:
                    tasks.append(calculate_biomet.s(station.station_id))
                    tasks.append(calculate_temp_rh.s(station.station_id))
                case _:
                    raise NotImplementedError

        task_group = group(tasks)
        task_chord = chord(task_group)
        result = task_chord(refresh_all_views.s())
        print('Waiting for calculation tasks (incl. refresh) to complete...')
        result.get()
        # after we recalculated the data, we need to apply the buddy check -
        print('Applying spatial buddy check')
        result = perform_spatial_buddy_check.apply_async()
        print('Waiting for spatial buddy check to complete...')
        result.get()
        return 0

if __name__ == '__main__':
    asyncio.run(amain())
