import asyncio
import functools
import os

import pytest
import sqlalchemy

from databases import Database

# from starlette.applications import Starlette
# from starlette.database import transaction
# from starlette.datastructures import CommaSeparatedStrings, DatabaseURL
# from starlette.middleware.database import DatabaseMiddleware
# from starlette.responses import JSONResponse
# from starlette.testclient import TestClient

assert "TEST_DATABASE_URLS" in os.environ, "TEST_DATABASE_URLS is not set."

DATABASE_URL = os.environ["TEST_DATABASE_URLS"]


metadata = sqlalchemy.MetaData()

notes = sqlalchemy.Table(
    "notes",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("text", sqlalchemy.String(length=100)),
    sqlalchemy.Column("completed", sqlalchemy.Boolean),
)


@pytest.fixture(autouse=True, scope="module")
def create_test_database():
    engine = sqlalchemy.create_engine(DATABASE_URL)
    metadata.create_all(engine)
    yield
    engine.execute("DROP TABLE notes")


def async_adapter(wrapped_func):
    @functools.wraps(wrapped_func)
    def run_sync(*args, **kwargs):
        loop = asyncio.get_event_loop()
        task = wrapped_func(*args, **kwargs)
        return loop.run_until_complete(task)
    return run_sync


@async_adapter
async def test_databases():
    database = Database(DATABASE_URL)
    await database.connect()
    session = database.session()
    query = notes.select()
    results = await session.fetchall(query=query)
    assert len(results) == 0
