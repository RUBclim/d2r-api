import contextlib
import os
from collections.abc import AsyncGenerator
from collections.abc import AsyncIterator
from typing import Any

from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.ext.asyncio import AsyncConnection
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


# Heavily inspired by
# https://github.com/ThomasAitken/demo-fastapi-async-sqlalchemy/blob/1d5d5b1789944ab2d9659af94b60568a49fe62cc/backend/app/database.py
# https://praciano.com.br/fastapi-and-async-sqlalchemy-20-with-pytest-done-right.html

class DatabaseSessionManager:
    def __init__(self, host: str, engine_kwargs: dict[str, Any] = {}):
        self._engine = create_async_engine(
            host,
            **engine_kwargs,
        )
        self._sessionmaker = async_sessionmaker(
            autocommit=False,
            bind=self._engine,
            expire_on_commit=False,
        )

    async def close(self) -> None:
        await self._engine.dispose()

    @contextlib.asynccontextmanager
    async def connect(
            self,
            as_transaction: bool = True,
    ) -> AsyncGenerator[AsyncConnection]:
        if self._engine is None:
            raise Exception('DatabaseSessionManager is not initialized')

        if as_transaction:
            async with self._engine.begin() as connection:
                try:
                    yield connection
                except Exception:
                    await connection.rollback()
                    raise

        else:
            async with self._engine.connect() as connection:
                await connection.execution_options(isolation_level='AUTOCOMMIT')
                yield connection

    @contextlib.asynccontextmanager
    async def session(self) -> AsyncIterator[AsyncSession]:
        if self._sessionmaker is None:
            raise Exception('DatabaseSessionManager is not initialized')

        session = self._sessionmaker()
        try:
            yield session
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


DB_URL = (
    f"{os.environ['DB_PROVIDER']}://{os.environ['POSTGRES_USER']}:"
    f"{os.environ['POSTGRES_PASSWORD']}@{os.environ['DB_HOST']}:"
    f"{os.environ['DB_PORT']}/{os.environ['POSTGRES_DB']}"
)

sessionmanager = DatabaseSessionManager(DB_URL)


async def get_db_session() -> AsyncGenerator[AsyncSession]:
    async with sessionmanager.session() as session:
        yield session

# the shared object is already in the docker image!
# https://github.com/jkittner/postgres-angle-avg/blob/master/angle_avg.sql
angle_avg_funcs = '''\
CREATE OR REPLACE FUNCTION complex_angle_accum(float8[], DOUBLE PRECISION)
RETURNS float8[]
    AS '$libdir/angle_avg', 'complex_angle_accum'
    LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION complex_angle_avg(float8[])
RETURNS DOUBLE PRECISION
    AS '$libdir/angle_avg', 'complex_angle_avg'
    LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION complex_angle_combine(float8[], float8[])
RETURNS float8[]
    AS '$libdir/angle_avg', 'complex_angle_combine'
    LANGUAGE C STRICT;

CREATE OR REPLACE AGGREGATE avg_angle(DOUBLE PRECISION)
(
    SFUNC = complex_angle_accum,
    STYPE = float8[],
    FINALFUNC = complex_angle_avg,
    initcond = '{0,0,0}',
    COMBINEFUNC = complex_angle_combine,
    PARALLEL = SAFE
);
'''
