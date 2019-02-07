import asyncio
import functools
import os

import pytest
import sqlalchemy

from databases import Database, DatabaseURL

# from starlette.applications import Starlette
# from starlette.database import transaction
# from starlette.datastructures import CommaSeparatedStrings, DatabaseURL
# from starlette.middleware.database import DatabaseMiddleware
# from starlette.responses import JSONResponse
# from starlette.testclient import TestClient

assert "TEST_DATABASE_URLS" in os.environ, "TEST_DATABASE_URLS is not set."

DATABASE_URLS = [url.strip() for url in os.environ["TEST_DATABASE_URLS"].split(",")]


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
    for url in DATABASE_URLS:
        database_url = DatabaseURL(url)
        if database_url.dialect == "mysql":
            url = str(database_url.replace(driver="pymysql"))
        engine = sqlalchemy.create_engine(url)
        metadata.create_all(engine)
    yield
    for url in DATABASE_URLS:
        database_url = DatabaseURL(url)
        if database_url.dialect == "mysql":
            url = str(database_url.replace(driver="pymysql"))
        engine = sqlalchemy.create_engine(url)
        metadata.drop_all(engine)


def async_adapter(wrapped_func):
    @functools.wraps(wrapped_func)
    def run_sync(*args, **kwargs):
        loop = asyncio.get_event_loop()
        task = wrapped_func(*args, **kwargs)
        return loop.run_until_complete(task)

    return run_sync


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@async_adapter
async def test_queries(database_url):
    async with Database(database_url) as database:
        # fetchall()
        query = notes.select()
        results = await database.fetchall(query=query)
        assert len(results) == 0

        async with database.transaction(force_rollback=True):
            # execute()
            query = notes.insert().values(text="example1", completed=True)
            await database.execute(query)

            # executemany()
            query = notes.insert()
            data = [
                {"text": "example2", "completed": False},
                {"text": "example3", "completed": True},
            ]
            await database.executemany(query, data)

            # fetchall()
            query = notes.select()
            results = await database.fetchall(query=query)
            assert len(results) == 3
            assert results[0]["text"] == "example1"
            assert results[0]["completed"] == True
            assert results[1]["text"] == "example2"
            assert results[1]["completed"] == False
            assert results[2]["text"] == "example3"
            assert results[2]["completed"] == True

            # fetchone()
            query = notes.select()
            result = await database.fetchone(query=query)
            assert result["text"] == "example1"
            assert result["completed"] == True

        # fetchall()
        query = notes.select()
        results = await database.fetchall(query=query)
        assert len(results) == 0


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@async_adapter
async def test_rollback_isolation(database_url):
    async with Database(database_url) as database:
        # Perform some INSERT operations on the database.
        async with database.transaction(force_rollback=True):
            query = notes.insert().values(text="example1", completed=True)
            await database.execute(query)

        # Ensure INSERT operations have been rolled back.
        query = notes.select()
        results = await database.fetchall(query=query)
        assert len(results) == 0


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@async_adapter
async def test_transaction_commit(database_url):
    async with Database(database_url) as database:
        async with database.transaction(force_rollback=True):
            async with database.transaction():
                query = notes.insert().values(text="example1", completed=True)
                await database.execute(query)

            query = notes.select()
            results = await database.fetchall(query=query)
            assert len(results) == 1


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@async_adapter
async def test_transaction_rollback(database_url):
    async with Database(database_url) as database:
        async with database.transaction(force_rollback=True):
            try:
                async with database.transaction():
                    query = notes.insert().values(text="example1", completed=True)
                    await database.execute(query)
                    raise RuntimeError()
            except RuntimeError:
                pass

            query = notes.select()
            results = await database.fetchall(query=query)
            assert len(results) == 0
