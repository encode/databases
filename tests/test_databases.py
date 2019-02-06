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
async def test_queries():
    async with Database(DATABASE_URL) as database:
        async with database.session(rollback_isolation=True) as session:
            # execute()
            query = notes.insert().values(text="example1", completed=True)
            await session.execute(query)

            # executemany()
            query = notes.insert()
            data = [
                {"text": "example2", "completed": False},
                {"text": "example3", "completed": True},
            ]
            await session.executemany(query, data)

            # fetchall()
            query = notes.select()
            results = await session.fetchall(query=query)
            assert len(results) == 3
            assert results[0]["text"] == "example1"
            assert results[0]["completed"] == True
            assert results[1]["text"] == "example2"
            assert results[1]["completed"] == False
            assert results[2]["text"] == "example3"
            assert results[2]["completed"] == True

            # fetchone()
            query = notes.select()
            result = await session.fetchone(query=query)
            assert result["text"] == "example1"
            assert result["completed"] == True


@async_adapter
async def test_rollback_isolation():
    async with Database(DATABASE_URL) as database:
        # Perform some INSERT operations on the database.
        async with database.session(rollback_isolation=True) as session:
            query = notes.insert().values(text="example1", completed=True)
            await session.execute(query)

        # Ensure INSERT operations have been rolled back.
        async with database.session() as session:
            query = notes.select()
            results = await session.fetchall(query=query)
            assert len(results) == 0
