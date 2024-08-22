import os
from collections.abc import Generator

from sqlmodel import create_engine
from sqlmodel import Session
from sqlmodel import SQLModel


engine = create_engine(os.environ['DB_URI'])


def init_db() -> None:
    SQLModel.metadata.create_all(engine)


def get_session() -> Generator[Session]:
    with Session(engine) as session:
        yield session
