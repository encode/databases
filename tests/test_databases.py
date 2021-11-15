import asyncio
import datetime
import decimal
import functools
import os
import re
import sys
from unittest.mock import MagicMock, patch

import pytest
import sqlalchemy

from databases import Database, DatabaseURL

assert "TEST_DATABASE_URLS" in os.environ, "TEST_DATABASE_URLS is not set."

DATABASE_URLS = [url.strip() for url in os.environ["TEST_DATABASE_URLS"].split(",")]


def mysql_versions(wrapped_func):
    """
    Decorator used to handle multiple versions of Python for mysql drivers
    """

    @functools.wraps(wrapped_func)
    def check(*args, **kwargs):
        url = DatabaseURL(kwargs["database_url"])
        if url.scheme in ["mysql", "mysql+aiomysql"] and sys.version_info >= (3, 10):
            pytest.skip("aiomysql supports python 3.9 and lower")
        if url.scheme == "mysql+asyncmy" and sys.version_info < (3, 7):
            pytest.skip("asyncmy supports python 3.7 and higher")
        return wrapped_func(*args, **kwargs)

    return check


class AsyncMock(MagicMock):
    async def __call__(self, *args, **kwargs):
        return super(AsyncMock, self).__call__(*args, **kwargs)


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

# Used to test Numeric
prices = sqlalchemy.Table(
    "prices",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("price", sqlalchemy.Numeric(precision=30, scale=20)),
)


@pytest.fixture(autouse=True, scope="function")
def create_test_database():
    # Create test databases with tables creation
    for url in DATABASE_URLS:
        database_url = DatabaseURL(url)
        if database_url.scheme in ["mysql", "mysql+aiomysql", "mysql+asyncmy"]:
            url = str(database_url.replace(driver="pymysql"))
        elif database_url.scheme in [
            "postgresql+aiopg",
            "sqlite+aiosqlite",
            "postgresql+asyncpg",
        ]:
            url = str(database_url.replace(driver=None))
        engine = sqlalchemy.create_engine(url)
        metadata.create_all(engine)

    # Run the test suite
    yield

    # Drop test databases
    for url in DATABASE_URLS:
        database_url = DatabaseURL(url)
        if database_url.scheme in ["mysql", "mysql+aiomysql", "mysql+asyncmy"]:
            url = str(database_url.replace(driver="pymysql"))
        elif database_url.scheme in [
            "postgresql+aiopg",
            "sqlite+aiosqlite",
            "postgresql+asyncpg",
        ]:
            url = str(database_url.replace(driver=None))
        engine = sqlalchemy.create_engine(url)
        metadata.drop_all(engine)


def async_adapter(wrapped_func):
    """
    Decorator used to run async test cases.
    """

    @functools.wraps(wrapped_func)
    def run_sync(*args, **kwargs):
        loop = asyncio.new_event_loop()
        task = wrapped_func(*args, **kwargs)
        return loop.run_until_complete(task)

    return run_sync


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@mysql_versions
@async_adapter
async def test_queries(database_url):
    """
    Test that the basic `execute()`, `execute_many()`, `fetch_all()``, and
    `fetch_one()` interfaces are all supported (using SQLAlchemy core).
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

            # fetch_val()
            query = sqlalchemy.sql.select([notes.c.text])
            result = await database.fetch_val(query=query)
            assert result == "example1"

            # fetch_val() with no rows
            query = sqlalchemy.sql.select([notes.c.text]).where(
                notes.c.text == "impossible"
            )
            result = await database.fetch_val(query=query)
            assert result is None

            # fetch_val() with a different column
            query = sqlalchemy.sql.select([notes.c.id, notes.c.text])
            result = await database.fetch_val(query=query, column=1)
            assert result == "example1"

            # row access (needed to maintain test coverage for Record.__getitem__ in postgres backend)
            query = sqlalchemy.sql.select([notes.c.text])
            result = await database.fetch_one(query=query)
            assert result["text"] == "example1"
            assert result[0] == "example1"

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
@mysql_versions
@async_adapter
async def test_queries_raw(database_url):
    """
    Test that the basic `execute()`, `execute_many()`, `fetch_all()``, and
    `fetch_one()` interfaces are all supported (raw queries).
    """
    async with Database(database_url) as database:
        async with database.transaction(force_rollback=True):
            # execute()
            query = "INSERT INTO notes(text, completed) VALUES (:text, :completed)"
            values = {"text": "example1", "completed": True}
            await database.execute(query, values)

            # execute_many()
            query = "INSERT INTO notes(text, completed) VALUES (:text, :completed)"
            values = [
                {"text": "example2", "completed": False},
                {"text": "example3", "completed": True},
            ]
            await database.execute_many(query, values)

            # fetch_all()
            query = "SELECT * FROM notes WHERE completed = :completed"
            results = await database.fetch_all(query=query, values={"completed": True})
            assert len(results) == 2
            assert results[0]["text"] == "example1"
            assert results[0]["completed"] == True
            assert results[1]["text"] == "example3"
            assert results[1]["completed"] == True

            # fetch_one()
            query = "SELECT * FROM notes WHERE completed = :completed"
            result = await database.fetch_one(query=query, values={"completed": False})
            assert result["text"] == "example2"
            assert result["completed"] == False

            # fetch_val()
            query = "SELECT completed FROM notes WHERE text = :text"
            result = await database.fetch_val(query=query, values={"text": "example1"})
            assert result == True
            query = "SELECT * FROM notes WHERE text = :text"
            result = await database.fetch_val(
                query=query, values={"text": "example1"}, column="completed"
            )
            assert result == True

            # iterate()
            query = "SELECT * FROM notes"
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
@mysql_versions
@async_adapter
async def test_ddl_queries(database_url):
    """
    Test that the built-in DDL elements such as `DropTable()`,
    `CreateTable()` are supported (using SQLAlchemy core).
    """
    async with Database(database_url) as database:
        async with database.transaction(force_rollback=True):
            # DropTable()
            query = sqlalchemy.schema.DropTable(notes)
            await database.execute(query)

            # CreateTable()
            query = sqlalchemy.schema.CreateTable(notes)
            await database.execute(query)


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@mysql_versions
@async_adapter
async def test_queries_after_error(database_url):
    """
    Test that the basic `execute()` works after a previous error.
    """

    class DBException(Exception):
        pass

    async with Database(database_url) as database:
        with patch.object(
            database.connection()._connection,
            "acquire",
            new=AsyncMock(side_effect=DBException),
        ):
            with pytest.raises(DBException):
                query = notes.select()
                await database.fetch_all(query)

        query = notes.select()
        await database.fetch_all(query)


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@mysql_versions
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
@mysql_versions
@async_adapter
async def test_results_support_column_reference(database_url):
    """
    Casting results to a dict should work, since the interface defines them
    as supporting the mapping interface.
    """
    async with Database(database_url) as database:
        async with database.transaction(force_rollback=True):
            now = datetime.datetime.now().replace(microsecond=0)
            today = datetime.date.today()

            # execute()
            query = articles.insert()
            values = {"title": "Hello, world Article", "published": now}
            await database.execute(query, values)

            query = custom_date.insert()
            values = {"title": "Hello, world Custom", "published": today}
            await database.execute(query, values)

            # fetch_all()
            query = sqlalchemy.select([articles, custom_date])
            results = await database.fetch_all(query=query)
            assert len(results) == 1
            assert results[0][articles.c.title] == "Hello, world Article"
            assert results[0][articles.c.published] == now
            assert results[0][custom_date.c.title] == "Hello, world Custom"
            assert results[0][custom_date.c.published] == today


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@mysql_versions
@async_adapter
async def test_result_values_allow_duplicate_names(database_url):
    """
    The values of a result should respect when two columns are selected
    with the same name.
    """
    async with Database(database_url) as database:
        async with database.transaction(force_rollback=True):
            query = "SELECT 1 AS id, 2 AS id"
            row = await database.fetch_one(query=query)

            assert list(row._mapping.keys()) == ["id", "id"]
            assert list(row._mapping.values()) == [1, 2]


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@mysql_versions
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
@mysql_versions
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

            # Apparently for `aiopg` it's OID that will always 0 in this case
            # As it's only one action within this cursor life cycle
            # It's recommended to use the `RETURNING` clause
            # For obtaining the record id
            if database.url.scheme == "postgresql+aiopg":
                assert pk == 0
            else:
                query = notes.select().where(notes.c.id == pk)
                result = await database.fetch_one(query)
                assert result["text"] == "example1"
                assert result["completed"] == True


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@mysql_versions
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
@mysql_versions
@async_adapter
async def test_rollback_isolation_with_contextmanager(database_url):
    """
    Ensure that `database.force_rollback()` provides strict isolation.
    """

    database = Database(database_url)

    with database.force_rollback():
        async with database:
            # Perform some INSERT operations on the database.
            query = notes.insert().values(text="example1", completed=True)
            await database.execute(query)

        async with database:
            # Ensure INSERT operations have been rolled back.
            query = notes.select()
            results = await database.fetch_all(query=query)
            assert len(results) == 0


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@mysql_versions
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
@mysql_versions
@async_adapter
async def test_transaction_commit_serializable(database_url):
    """
    Ensure that serializable transaction commit via extra parameters is supported.
    """

    database_url = DatabaseURL(database_url)

    if database_url.scheme not in ["postgresql", "postgresql+asyncpg"]:
        pytest.skip("Test (currently) only supports asyncpg")

    if database_url.scheme == "postgresql+asyncpg":
        database_url = database_url.replace(driver=None)

    def insert_independently():
        engine = sqlalchemy.create_engine(str(database_url))
        conn = engine.connect()

        query = notes.insert().values(text="example1", completed=True)
        conn.execute(query)

    def delete_independently():
        engine = sqlalchemy.create_engine(str(database_url))
        conn = engine.connect()

        query = notes.delete()
        conn.execute(query)

    async with Database(database_url) as database:
        async with database.transaction(force_rollback=True, isolation="serializable"):
            query = notes.select()
            results = await database.fetch_all(query=query)
            assert len(results) == 0

            insert_independently()

            query = notes.select()
            results = await database.fetch_all(query=query)
            assert len(results) == 0

            delete_independently()


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@mysql_versions
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
@mysql_versions
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
@mysql_versions
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
@mysql_versions
@async_adapter
async def test_transaction_decorator(database_url):
    """
    Ensure that @database.transaction() is supported.
    """
    database = Database(database_url, force_rollback=True)

    @database.transaction()
    async def insert_data(raise_exception):
        query = notes.insert().values(text="example", completed=True)
        await database.execute(query)
        if raise_exception:
            raise RuntimeError()

    async with database:
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
@mysql_versions
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
@mysql_versions
@async_adapter
async def test_decimal_field(database_url):
    """
    Test Decimal (NUMERIC) columns, to ensure records are coerced to/from proper Python types.
    """

    async with Database(database_url) as database:
        async with database.transaction(force_rollback=True):
            price = decimal.Decimal("0.700000000000001")

            # execute()
            query = prices.insert()
            values = {"price": price}
            await database.execute(query, values)

            # fetch_all()
            query = prices.select()
            results = await database.fetch_all(query=query)
            assert len(results) == 1
            if database_url.startswith("sqlite"):
                # aiosqlite does not support native decimals --> a roud-off error is expected
                assert results[0]["price"] == pytest.approx(price)
            else:
                assert results[0]["price"] == price


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@mysql_versions
@async_adapter
async def test_json_field(database_url):
    """
    Test JSON columns, to ensure correct cross-database support.
    """

    async with Database(database_url) as database:
        async with database.transaction(force_rollback=True):
            # execute()
            data = {"text": "hello", "boolean": True, "int": 1}
            values = {"data": data}
            query = session.insert()
            await database.execute(query, values)

            # fetch_all()
            query = session.select()
            results = await database.fetch_all(query=query)
            assert len(results) == 1
            assert results[0]["data"] == {"text": "hello", "boolean": True, "int": 1}


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@mysql_versions
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
@mysql_versions
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
@mysql_versions
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
@mysql_versions
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

    # connect and disconnect idempotence
    await database.connect()
    await database.connect()
    assert database.is_connected
    await database.disconnect()
    await database.disconnect()
    assert not database.is_connected


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@mysql_versions
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
@mysql_versions
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
@mysql_versions
@async_adapter
async def test_queries_with_expose_backend_connection(database_url):
    """
    Replication of `execute()`, `execute_many()`, `fetch_all()``, and
    `fetch_one()` using the raw driver interface.
    """
    async with Database(database_url) as database:
        async with database.connection() as connection:
            async with connection.transaction(force_rollback=True):
                # Get the raw connection
                raw_connection = connection.raw_connection

                # Insert query
                if database.url.scheme in [
                    "mysql",
                    "mysql+asyncmy",
                    "mysql+aiomysql",
                    "postgresql+aiopg",
                ]:
                    insert_query = "INSERT INTO notes (text, completed) VALUES (%s, %s)"
                else:
                    insert_query = "INSERT INTO notes (text, completed) VALUES ($1, $2)"

                # execute()
                values = ("example1", True)

                if database.url.scheme in [
                    "mysql",
                    "mysql+aiomysql",
                    "postgresql+aiopg",
                ]:
                    cursor = await raw_connection.cursor()
                    await cursor.execute(insert_query, values)
                elif database.url.scheme == "mysql+asyncmy":
                    async with raw_connection.cursor() as cursor:
                        await cursor.execute(insert_query, values)
                elif database.url.scheme in ["postgresql", "postgresql+asyncpg"]:
                    await raw_connection.execute(insert_query, *values)
                elif database.url.scheme in ["sqlite", "sqlite+aiosqlite"]:
                    await raw_connection.execute(insert_query, values)

                # execute_many()
                values = [("example2", False), ("example3", True)]

                if database.url.scheme in ["mysql", "mysql+aiomysql"]:
                    cursor = await raw_connection.cursor()
                    await cursor.executemany(insert_query, values)
                elif database.url.scheme == "mysql+asyncmy":
                    async with raw_connection.cursor() as cursor:
                        await cursor.executemany(insert_query, values)
                elif database.url.scheme == "postgresql+aiopg":
                    cursor = await raw_connection.cursor()
                    # No async support for `executemany`
                    for value in values:
                        await cursor.execute(insert_query, value)
                else:
                    await raw_connection.executemany(insert_query, values)

                # Select query
                select_query = "SELECT notes.id, notes.text, notes.completed FROM notes"

                # fetch_all()
                if database.url.scheme in [
                    "mysql",
                    "mysql+aiomysql",
                    "postgresql+aiopg",
                ]:
                    cursor = await raw_connection.cursor()
                    await cursor.execute(select_query)
                    results = await cursor.fetchall()
                elif database.url.scheme == "mysql+asyncmy":
                    async with raw_connection.cursor() as cursor:
                        await cursor.execute(select_query)
                        results = await cursor.fetchall()
                elif database.url.scheme in ["postgresql", "postgresql+asyncpg"]:
                    results = await raw_connection.fetch(select_query)
                elif database.url.scheme in ["sqlite", "sqlite+aiosqlite"]:
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
                if database.url.scheme in ["postgresql", "postgresql+asyncpg"]:
                    result = await raw_connection.fetchrow(select_query)
                elif database.url.scheme == "mysql+asyncmy":
                    async with raw_connection.cursor() as cursor:
                        await cursor.execute(select_query)
                        result = await cursor.fetchone()
                else:
                    cursor = await raw_connection.cursor()
                    await cursor.execute(select_query)
                    result = await cursor.fetchone()

                # Raw output for the raw request
                assert result[1] == "example1"
                assert result[2] == True


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@mysql_versions
@async_adapter
async def test_database_url_interface(database_url):
    """
    Test that Database instances expose a `.url` attribute.
    """
    async with Database(database_url) as database:
        assert isinstance(database.url, DatabaseURL)
        assert database.url == database_url


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@async_adapter
async def test_concurrent_access_on_single_connection(database_url):
    database_url = DatabaseURL(database_url)
    if database_url.dialect != "postgresql":
        pytest.skip("Test requires `pg_sleep()`")

    async with Database(database_url, force_rollback=True) as database:

        async def db_lookup():
            await database.fetch_one("SELECT pg_sleep(1)")

        await asyncio.gather(db_lookup(), db_lookup())


@pytest.mark.parametrize("database_url", DATABASE_URLS)
def test_global_connection_is_initialized_lazily(database_url):
    """
    Ensure that global connection is initialized at latest possible time
    so it's _query_lock will belong to same event loop that async_adapter has
    initialized.

    See https://github.com/encode/databases/issues/157 for more context.
    """

    database_url = DatabaseURL(database_url)
    if database_url.dialect != "postgresql":
        pytest.skip("Test requires `pg_sleep()`")

    database = Database(database_url, force_rollback=True)

    @async_adapter
    async def run_database_queries():
        async with database:

            async def db_lookup():
                await database.fetch_one("SELECT pg_sleep(1)")

            await asyncio.gather(db_lookup(), db_lookup())

    run_database_queries()


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@async_adapter
async def test_iterate_outside_transaction_with_values(database_url):
    """
    Ensure `iterate()` works even without a transaction on all drivers.
    The asyncpg driver relies on server-side cursors without hold
    for iteration, which requires a transaction to be created.
    This is mentionned in both their documentation and their test suite.
    """

    database_url = DatabaseURL(database_url)
    if database_url.dialect == "mysql":
        pytest.skip("MySQL does not support `FROM (VALUES ...)` (F641)")

    async with Database(database_url) as database:
        query = "SELECT * FROM (VALUES (1), (2), (3), (4), (5)) as t"
        iterate_results = []

        async for result in database.iterate(query=query):
            iterate_results.append(result)

        assert len(iterate_results) == 5


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@mysql_versions
@async_adapter
async def test_iterate_outside_transaction_with_temp_table(database_url):
    """
    Same as test_iterate_outside_transaction_with_values but uses a
    temporary table instead of a list of values.
    """

    database_url = DatabaseURL(database_url)
    if database_url.dialect == "sqlite":
        pytest.skip("SQLite interface does not work with temporary tables.")

    async with Database(database_url) as database:
        query = "CREATE TEMPORARY TABLE no_transac(num INTEGER)"
        await database.execute(query)

        query = "INSERT INTO no_transac(num) VALUES (1), (2), (3), (4), (5)"
        await database.execute(query)

        query = "SELECT * FROM no_transac"
        iterate_results = []

        async for result in database.iterate(query=query):
            iterate_results.append(result)

        assert len(iterate_results) == 5


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@pytest.mark.parametrize("select_query", [notes.select(), "SELECT * FROM notes"])
@mysql_versions
@async_adapter
async def test_column_names(database_url, select_query):
    """
    Test that column names are exposed correctly through `._mapping.keys()` on each row.
    """
    async with Database(database_url) as database:
        async with database.transaction(force_rollback=True):
            # insert values
            query = notes.insert()
            values = {"text": "example1", "completed": True}
            await database.execute(query, values)
            # fetch results
            results = await database.fetch_all(query=select_query)
            assert len(results) == 1

            assert sorted(results[0]._mapping.keys()) == ["completed", "id", "text"]
            assert results[0]["text"] == "example1"
            assert results[0]["completed"] == True


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@mysql_versions
@async_adapter
async def test_parallel_transactions(database_url):
    """
    Test parallel transaction execution.
    """

    async def test_task(db):
        async with db.transaction():
            await db.fetch_one("SELECT 1")

    async with Database(database_url) as database:
        await database.fetch_one("SELECT 1")

        tasks = [test_task(database) for i in range(4)]
        await asyncio.gather(*tasks)


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@async_adapter
async def test_posgres_interface(database_url):
    """
    Since SQLAlchemy 1.4, `Row.values()` is removed and `Row.keys()` is deprecated.
    Custom postgres interface mimics more or less this behaviour by deprecating those
    two methods
    """
    database_url = DatabaseURL(database_url)

    if database_url.scheme not in ["postgresql", "postgresql+asyncpg"]:
        pytest.skip("Test is only for asyncpg")

    async with Database(database_url) as database:
        async with database.transaction(force_rollback=True):
            query = notes.insert()
            values = {"text": "example1", "completed": True}
            await database.execute(query, values)

            query = notes.select()
            result = await database.fetch_one(query=query)

            with pytest.warns(
                DeprecationWarning,
                match=re.escape(
                    "The `Row.keys()` method is deprecated to mimic SQLAlchemy behaviour, "
                    "use `Row._mapping.keys()` instead."
                ),
            ):
                assert (
                    list(result.keys())
                    == [k for k in result]
                    == ["id", "text", "completed"]
                )

            with pytest.warns(
                DeprecationWarning,
                match=re.escape(
                    "The `Row.values()` method is deprecated to mimic SQLAlchemy behaviour, "
                    "use `Row._mapping.values()` instead."
                ),
            ):
                # avoid checking `id` at index 0 since it may change depending on the launched tests
                assert list(result.values())[1:] == ["example1", True]


@pytest.mark.parametrize("database_url", DATABASE_URLS)
@mysql_versions
@async_adapter
async def test_postcompile_queries(database_url):
    """
    Since SQLAlchemy 1.4, IN operators needs to do render_postcompile
    """
    async with Database(database_url) as database:
        query = notes.insert()
        values = {"text": "example1", "completed": True}
        await database.execute(query, values)

        query = notes.select().where(notes.c.id.in_([2, 3]))
        results = await database.fetch_all(query=query)

        assert len(results) == 0
