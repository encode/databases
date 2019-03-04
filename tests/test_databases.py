import asyncio
import datetime
import functools
import os

import pytest
import sqlalchemy

from databases import Database, DatabaseURL

assert "TEST_DATABASE_URLS" in os.environ, "TEST_DATABASE_URLS is not set."

DATABASE_URLS = [url.strip() for url in os.environ["TEST_DATABASE_URLS"].split(",")]


class MyEpochType(sqlalchemy.types.TypeDecorator):
    impl = sqlalchemy.Integer

    epoch = datetime.date(1970, 1, 1)

    def process_bind_param(self, value, dialect):
        return (value - self.epoch).days

    def process_result_value(self, value, dialect):
        return self.epoch + datetime.timedelta(days=value)


metadata = sqlalchemy.MetaData()

notes = sqlalchemy.Table(
    "notes",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("text", sqlalchemy.String(length=100)),
    sqlalchemy.Column("completed", sqlalchemy.Boolean),
)

# Used to test DateTime
articles = sqlalchemy.Table(
    "articles",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("title", sqlalchemy.String(length=100)),
    sqlalchemy.Column("published", sqlalchemy.DateTime),
)

# Used to test JSON
session = sqlalchemy.Table(
    "session",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("data", sqlalchemy.JSON),
)

# Used to test custom column types
custom_date = sqlalchemy.Table(
    "custom_date",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("title", sqlalchemy.String(length=100)),
    sqlalchemy.Column("published", MyEpochType),
)


@pytest.fixture(autouse=True, scope="module")
def create_test_database():
    # Create test databases
    for url in DATABASE_URLS:
        database_url = DatabaseURL(url)
        if database_url.dialect == "mysql":
            url = str(database_url.replace(driver="pymysql"))
        engine = sqlalchemy.create_engine(url)
        metadata.create_all(engine)

    # Run the test suite
    yield

    # Drop test databases
    for url in DATABASE_URLS:
        database_url = DatabaseURL(url)
        if database_url.dialect == "mysql":
            url = str(database_url.replace(driver="pymysql"))
        engine = sqlalchemy.create_engine(url)
        metadata.drop_all(engine)


def async_adapter(wrapped_func):
    """
    Decorator used to run async test cases.
    """

    @functools.wraps(wrapped_func)
    def run_sync(*args, **kwargs):
        loop = asyncio.get_event_loop()
        task = wrapped_func(*args, **kwargs)
        return loop.run_until_complete(task)

    return run_sync


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@async_adapter
async def test_queries(database_url):
    """
    Test that the basic `execute()`, `execute_many()`, `fetch_all()``, and
    `fetch_one()` interfaces are all supported.
    """
    async with Database(database_url) as database:
        async with database.transaction(force_rollback=True):
            # execute()
            query = notes.insert()
            values = {"text": "example1", "completed": True}
            await database.execute(query, values)

            # execute_many()
            query = notes.insert()
            values = [
                {"text": "example2", "completed": False},
                {"text": "example3", "completed": True},
            ]
            await database.execute_many(query, values)

            # fetch_all()
            query = notes.select()
            results = await database.fetch_all(query=query)
            assert len(results) == 3
            assert results[0]["text"] == "example1"
            assert results[0]["completed"] == True
            assert results[1]["text"] == "example2"
            assert results[1]["completed"] == False
            assert results[2]["text"] == "example3"
            assert results[2]["completed"] == True

            # fetch_one()
            query = notes.select()
            result = await database.fetch_one(query=query)
            assert result["text"] == "example1"
            assert result["completed"] == True

            # iterate()
            query = notes.select()
            iterate_results = []
            async for result in database.iterate(query=query):
                iterate_results.append(result)
            assert len(iterate_results) == 3
            assert iterate_results[0]["text"] == "example1"
            assert iterate_results[0]["completed"] == True
            assert iterate_results[1]["text"] == "example2"
            assert iterate_results[1]["completed"] == False
            assert iterate_results[2]["text"] == "example3"
            assert iterate_results[2]["completed"] == True


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@async_adapter
async def test_results_support_mapping_interface(database_url):
    """
    Casting results to a dict should work, since the interface defines them
    as supporting the mapping interface.
    """
    async with Database(database_url) as database:
        async with database.transaction(force_rollback=True):
            # execute()
            query = notes.insert()
            values = {"text": "example1", "completed": True}
            await database.execute(query, values)

            # fetch_all()
            query = notes.select()
            results = await database.fetch_all(query=query)
            results_as_dicts = [dict(item) for item in results]

            assert len(results[0]) == 3
            assert len(results_as_dicts[0]) == 3

            assert isinstance(results_as_dicts[0]["id"], int)
            assert results_as_dicts[0]["text"] == "example1"
            assert results_as_dicts[0]["completed"] == True


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@async_adapter
async def test_fetch_one_returning_no_results(database_url):
    """
    fetch_one should return `None` when no results match.
    """
    async with Database(database_url) as database:
        async with database.transaction(force_rollback=True):
            # fetch_all()
            query = notes.select()
            result = await database.fetch_one(query=query)
            assert result is None


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@async_adapter
async def test_execute_return_val(database_url):
    """
    Test using return value from `execute()` to get an inserted primary key.
    """
    async with Database(database_url) as database:
        async with database.transaction(force_rollback=True):
            query = notes.insert()
            values = {"text": "example1", "completed": True}
            pk = await database.execute(query, values)

            assert isinstance(pk, int)
            query = notes.select().where(notes.c.id == pk)
            result = await database.fetch_one(query)
            assert result["text"] == "example1"
            assert result["completed"] == True


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@async_adapter
async def test_rollback_isolation(database_url):
    """
    Ensure that `database.transaction(force_rollback=True)` provides strict isolation.
    """

    async with Database(database_url) as database:
        # Perform some INSERT operations on the database.
        async with database.transaction(force_rollback=True):
            query = notes.insert().values(text="example1", completed=True)
            await database.execute(query)

        # Ensure INSERT operations have been rolled back.
        query = notes.select()
        results = await database.fetch_all(query=query)
        assert len(results) == 0


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@async_adapter
async def test_transaction_commit(database_url):
    """
    Ensure that transaction commit is supported.
    """
    async with Database(database_url) as database:
        async with database.transaction(force_rollback=True):
            async with database.transaction():
                query = notes.insert().values(text="example1", completed=True)
                await database.execute(query)

            query = notes.select()
            results = await database.fetch_all(query=query)
            assert len(results) == 1


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@async_adapter
async def test_transaction_rollback(database_url):
    """
    Ensure that transaction rollback is supported.
    """

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
            results = await database.fetch_all(query=query)
            assert len(results) == 0


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@async_adapter
async def test_transaction_commit_low_level(database_url):
    """
    Ensure that an explicit `await transaction.commit()` is supported.
    """

    async with Database(database_url) as database:
        async with database.transaction(force_rollback=True):
            transaction = await database.transaction()
            try:
                query = notes.insert().values(text="example1", completed=True)
                await database.execute(query)
            except:  # pragma: no cover
                await transaction.rollback()
            else:
                await transaction.commit()

            query = notes.select()
            results = await database.fetch_all(query=query)
            assert len(results) == 1


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@async_adapter
async def test_transaction_rollback_low_level(database_url):
    """
    Ensure that an explicit `await transaction.rollback()` is supported.
    """

    async with Database(database_url) as database:
        async with database.transaction(force_rollback=True):
            transaction = await database.transaction()
            try:
                query = notes.insert().values(text="example1", completed=True)
                await database.execute(query)
                raise RuntimeError()
            except:
                await transaction.rollback()
            else:  # pragma: no cover
                await transaction.commit()

            query = notes.select()
            results = await database.fetch_all(query=query)
            assert len(results) == 0


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@async_adapter
async def test_transaction_decorator(database_url):
    """
    Ensure that @database.transaction() is supported.
    """
    async with Database(database_url, force_rollback=True) as database:

        @database.transaction()
        async def insert_data(raise_exception):
            query = notes.insert().values(text="example", completed=True)
            await database.execute(query)
            if raise_exception:
                raise RuntimeError()

        with pytest.raises(RuntimeError):
            await insert_data(raise_exception=True)

        query = notes.select()
        results = await database.fetch_all(query=query)
        assert len(results) == 0

        await insert_data(raise_exception=False)

        query = notes.select()
        results = await database.fetch_all(query=query)
        assert len(results) == 1


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@async_adapter
async def test_datetime_field(database_url):
    """
    Test DataTime columns, to ensure records are coerced to/from proper Python types.
    """

    async with Database(database_url) as database:
        async with database.transaction(force_rollback=True):
            now = datetime.datetime.now().replace(microsecond=0)

            # execute()
            query = articles.insert()
            values = {"title": "Hello, world", "published": now}
            await database.execute(query, values)

            # fetch_all()
            query = articles.select()
            results = await database.fetch_all(query=query)
            assert len(results) == 1
            assert results[0]["title"] == "Hello, world"
            assert results[0]["published"] == now


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@async_adapter
async def test_json_field(database_url):
    """
    Test JSON columns, to ensure correct cross-database support.
    """

    async with Database(database_url) as database:
        async with database.transaction(force_rollback=True):
            # execute()
            query = session.insert()
            values = {"data": {"text": "hello", "boolean": True, "int": 1}}
            await database.execute(query, values)

            # fetch_all()
            query = session.select()
            results = await database.fetch_all(query=query)
            assert len(results) == 1
            assert results[0]["data"] == {"text": "hello", "boolean": True, "int": 1}


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@async_adapter
async def test_custom_field(database_url):
    """
    Test custom column types.
    """

    async with Database(database_url) as database:
        async with database.transaction(force_rollback=True):
            today = datetime.date.today()

            # execute()
            query = custom_date.insert()
            values = {"title": "Hello, world", "published": today}
            await database.execute(query, values)

            # fetch_all()
            query = custom_date.select()
            results = await database.fetch_all(query=query)
            assert len(results) == 1
            assert results[0]["title"] == "Hello, world"
            assert results[0]["published"] == today


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@async_adapter
async def test_connections_isolation(database_url):
    """
    Ensure that changes are visible between different connections.
    To check this we have to not create a transaction, so that
    each query ends up on a different connection from the pool.
    """

    async with Database(database_url) as database:
        try:
            query = notes.insert().values(text="example1", completed=True)
            await database.execute(query)

            query = notes.select()
            results = await database.fetch_all(query=query)
            assert len(results) == 1
        finally:
            query = notes.delete()
            await database.execute(query)


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@async_adapter
async def test_commit_on_root_transaction(database_url):
    """
    Because our tests are generally wrapped in rollback-islation, they
    don't have coverage for commiting the root transaction.

    Deal with this here, and delete the records rather than rolling back.
    """

    async with Database(database_url) as database:
        try:
            async with database.transaction():
                query = notes.insert().values(text="example1", completed=True)
                await database.execute(query)

            query = notes.select()
            results = await database.fetch_all(query=query)
            assert len(results) == 1
        finally:
            query = notes.delete()
            await database.execute(query)


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@async_adapter
async def test_connect_and_disconnect(database_url):
    """
    Test explicit connect() and disconnect().
    """
    database = Database(database_url)

    assert not database.is_connected
    await database.connect()
    assert database.is_connected
    await database.disconnect()
    assert not database.is_connected


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@async_adapter
async def test_connection_context(database_url):
    """
    Test connection contexts are task-local.
    """
    async with Database(database_url) as database:
        async with database.connection() as connection_1:
            async with database.connection() as connection_2:
                assert connection_1 is connection_2

    async with Database(database_url) as database:
        connection_1 = None
        connection_2 = None
        test_complete = asyncio.Event()

        async def get_connection_1():
            nonlocal connection_1

            async with database.connection() as connection:
                connection_1 = connection
                await test_complete.wait()

        async def get_connection_2():
            nonlocal connection_2

            async with database.connection() as connection:
                connection_2 = connection
                await test_complete.wait()

        loop = asyncio.get_event_loop()
        task_1 = loop.create_task(get_connection_1())
        task_2 = loop.create_task(get_connection_2())
        while connection_1 is None or connection_2 is None:
            await asyncio.sleep(0.000001)
        assert connection_1 is not connection_2
        test_complete.set()
        await task_1
        await task_2


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@async_adapter
async def test_connection_context_with_raw_connection(database_url):
    """
    Test connection contexts with respect to the raw connection.
    """
    async with Database(database_url) as database:
        async with database.connection() as connection_1:
            async with database.connection() as connection_2:
                assert connection_1 is connection_2
                assert connection_1.raw_connection is connection_2.raw_connection


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@async_adapter
async def test_queries_with_expose_backend_connection(database_url):
    """
    Replication of `execute()`, `execute_many()`, `fetch_all()``, and
    `fetch_one()` using the raw driver interface.
    """
    async with Database(database_url) as database:
        async with database.connection() as connection:
            async with database.transaction(force_rollback=True):
                # Get the raw connection
                raw_connection = connection.raw_connection

                # Insert query
                if str(database_url).startswith("mysql"):
                    insert_query = "INSERT INTO notes (text, completed) VALUES (%s, %s)"
                else:
                    insert_query = "INSERT INTO notes (text, completed) VALUES ($1, $2)"

                # execute()
                values = ("example1", True)

                if str(database_url).startswith("postgresql"):
                    await raw_connection.execute(insert_query, *values)
                elif str(database_url).startswith("mysql"):
                    cursor = await raw_connection.cursor()
                    await cursor.execute(insert_query, values)
                elif str(database_url).startswith("sqlite"):
                    await raw_connection.execute(insert_query, values)

                # execute_many()
                values = [("example2", False), ("example3", True)]

                if str(database_url).startswith("mysql"):
                    cursor = await raw_connection.cursor()
                    await cursor.executemany(insert_query, values)
                else:
                    await raw_connection.executemany(insert_query, values)

                # Select query
                select_query = "SELECT notes.id, notes.text, notes.completed FROM notes"

                # fetch_all()
                if str(database_url).startswith("postgresql"):
                    results = await raw_connection.fetch(select_query)
                elif str(database_url).startswith("mysql"):
                    cursor = await raw_connection.cursor()
                    await cursor.execute(select_query)
                    results = await cursor.fetchall()
                elif str(database_url).startswith("sqlite"):
                    results = await raw_connection.execute_fetchall(select_query)

                assert len(results) == 3
                # Raw output for the raw request
                assert results[0][1] == "example1"
                assert results[0][2] == True
                assert results[1][1] == "example2"
                assert results[1][2] == False
                assert results[2][1] == "example3"
                assert results[2][2] == True

                # fetch_one()
                if str(database_url).startswith("postgresql"):
                    result = await raw_connection.fetchrow(select_query)
                else:
                    cursor = await raw_connection.cursor()
                    await cursor.execute(select_query)
                    result = await cursor.fetchone()

                # Raw output for the raw request
                assert result[1] == "example1"
                assert result[2] == True
