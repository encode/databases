"""
Unit tests for the backend connection arguments.
"""
from contextlib import suppress
from urllib.parse import urlparse

import pytest

from databases import Database
from databases.backends.mysql import MySQLBackend
from databases.backends.postgres import PostgresBackend
from tests.test_databases import DATABASE_URLS, async_adapter

POSTGRES_URLS = [url for url in DATABASE_URLS if urlparse(url).scheme == "postgresql"]


def test_postgres_pool_size():
    backend = PostgresBackend("postgres://localhost/database?min_size=1&max_size=20")
    kwargs = backend._get_connection_kwargs()
    assert kwargs == {"min_size": 1, "max_size": 20}


def test_postgres_explicit_pool_size():
    backend = PostgresBackend("postgres://localhost/database", min_size=1, max_size=20)
    kwargs = backend._get_connection_kwargs()
    assert kwargs == {"min_size": 1, "max_size": 20}


def test_postgres_ssl():
    backend = PostgresBackend("postgres://localhost/database?ssl=true")
    kwargs = backend._get_connection_kwargs()
    assert kwargs == {"ssl": True}


def test_postgres_explicit_ssl():
    backend = PostgresBackend("postgres://localhost/database", ssl=True)
    kwargs = backend._get_connection_kwargs()
    assert kwargs == {"ssl": True}


urls_with_options = [
    (f"{POSTGRES_URLS[0]}?min_size=1&max_size=20", suppress()),
    (f"{POSTGRES_URLS[0]}?min_size=0&max_size=0", pytest.raises(ValueError)),
    (f"{POSTGRES_URLS[0]}?min_size=10&max_size=0", pytest.raises(ValueError)),
]


@pytest.mark.parametrize("database_url, expectation", urls_with_options)
@async_adapter
async def test_postgres_pool_size_connect(database_url, expectation):
    with expectation:
        database = Database(database_url)
        await database.connect()
        assert database.is_connected
        await database.disconnect()
        assert not database.is_connected


def test_mysql_pool_size():
    backend = MySQLBackend("mysql://localhost/database?min_size=1&max_size=20")
    kwargs = backend._get_connection_kwargs()
    assert kwargs == {"minsize": 1, "maxsize": 20}


def test_mysql_explicit_pool_size():
    backend = MySQLBackend("mysql://localhost/database", min_size=1, max_size=20)
    kwargs = backend._get_connection_kwargs()
    assert kwargs == {"minsize": 1, "maxsize": 20}


def test_mysql_ssl():
    backend = MySQLBackend("mysql://localhost/database?ssl=true")
    kwargs = backend._get_connection_kwargs()
    assert kwargs == {"ssl": True}


def test_mysql_explicit_ssl():
    backend = MySQLBackend("mysql://localhost/database", ssl=True)
    kwargs = backend._get_connection_kwargs()
    assert kwargs == {"ssl": True}
