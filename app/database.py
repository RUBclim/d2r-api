import contextlib
import os
from collections.abc import AsyncGenerator
from collections.abc import AsyncIterator
from typing import Any

from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.ext.asyncio import AsyncConnection
from sqlalchemy.ext.asyncio import AsyncEngine
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
        self._engine: AsyncEngine | None = create_async_engine(
            host,
            **engine_kwargs,
        )
        self._sessionmaker: async_sessionmaker[AsyncSession] | None = async_sessionmaker(  # noqa: E501
            autocommit=False,
            bind=self._engine,
            expire_on_commit=False,
        )

    async def close(self) -> None:
        if self._engine is None:
            raise Exception('DatabaseSessionManager is not initialized')
        await self._engine.dispose()

        self._engine = None
        self._sessionmaker = None

    @contextlib.asynccontextmanager
    async def connect(self) -> AsyncGenerator[AsyncConnection]:
        if self._engine is None:
            raise Exception('DatabaseSessionManager is not initialized')

        async with self._engine.begin() as connection:
            try:
                yield connection
            except Exception:
                await connection.rollback()
                raise

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


sessionmanager = DatabaseSessionManager(
    f"{os.environ['DB_PROVIDER']}://{os.environ['POSTGRES_USER']}:"
    f"{os.environ['POSTGRES_PASSWORD']}@{os.environ['DB_HOST']}:"
    f"{os.environ['DB_PORT']}/{os.environ['POSTGRES_DB']}",
)


async def get_db_session() -> AsyncGenerator[AsyncSession]:
    async with sessionmanager.session() as session:
        yield session
