"""
Unit tests for the backend connection arguments.
"""
import sys

import pytest

from databases.backends.aiopg import AiopgBackend
from databases.backends.postgres import PostgresBackend
from databases.core import DatabaseURL
from tests.test_databases import DATABASE_URLS, async_adapter

if sys.version_info >= (3, 7):  # pragma: no cover
    from databases.backends.asyncmy import AsyncMyBackend


if sys.version_info < (3, 10):  # pragma: no cover
    from databases.backends.mysql import MySQLBackend


def test_postgres_pool_size():
    backend = PostgresBackend("postgres://localhost/database?min_size=1&max_size=20")
    kwargs = backend._get_connection_kwargs()
    assert kwargs == {"min_size": 1, "max_size": 20}


@async_adapter
async def test_postgres_pool_size_connect():
    for url in DATABASE_URLS:
        if DatabaseURL(url).dialect != "postgresql":
            continue
        backend = PostgresBackend(url + "?min_size=1&max_size=20")
        await backend.connect()
        await backend.disconnect()


def test_postgres_explicit_pool_size():
    backend = PostgresBackend("postgres://localhost/database", min_size=1, max_size=20)
    kwargs = backend._get_connection_kwargs()
    assert kwargs == {"min_size": 1, "max_size": 20}


def test_postgres_ssl():
    backend = PostgresBackend("postgres://localhost/database?ssl=true")
    kwargs = backend._get_connection_kwargs()
    assert kwargs == {"ssl": True}


def test_postgres_ssl_verify_full():
    backend = PostgresBackend("postgres://localhost/database?ssl=verify-full")
    kwargs = backend._get_connection_kwargs()
    assert kwargs == {"ssl": "verify-full"}


def test_postgres_explicit_ssl():
    backend = PostgresBackend("postgres://localhost/database", ssl=True)
    kwargs = backend._get_connection_kwargs()
    assert kwargs == {"ssl": True}


def test_postgres_explicit_ssl_verify_full():
    backend = PostgresBackend("postgres://localhost/database", ssl="verify-full")
    kwargs = backend._get_connection_kwargs()
    assert kwargs == {"ssl": "verify-full"}


def test_postgres_no_extra_options():
    backend = PostgresBackend("postgres://localhost/database")
    kwargs = backend._get_connection_kwargs()
    assert kwargs == {}


def test_postgres_password_as_callable():
    def gen_password():
        return "Foo"

    backend = PostgresBackend(
        "postgres://:password@localhost/database", password=gen_password
    )
    kwargs = backend._get_connection_kwargs()
    assert kwargs == {"password": gen_password}
    assert kwargs["password"]() == "Foo"


@pytest.mark.skipif(sys.version_info >= (3, 10), reason="requires python3.9 or lower")
def test_mysql_pool_size():
    backend = MySQLBackend("mysql://localhost/database?min_size=1&max_size=20")
    kwargs = backend._get_connection_kwargs()
    assert kwargs == {"minsize": 1, "maxsize": 20}


@pytest.mark.skipif(sys.version_info >= (3, 10), reason="requires python3.9 or lower")
def test_mysql_unix_socket():
    backend = MySQLBackend(
        "mysql+aiomysql://username:password@/testsuite?unix_socket=/tmp/mysqld/mysqld.sock"
    )
    kwargs = backend._get_connection_kwargs()
    assert kwargs == {"unix_socket": "/tmp/mysqld/mysqld.sock"}


@pytest.mark.skipif(sys.version_info >= (3, 10), reason="requires python3.9 or lower")
def test_mysql_explicit_pool_size():
    backend = MySQLBackend("mysql://localhost/database", min_size=1, max_size=20)
    kwargs = backend._get_connection_kwargs()
    assert kwargs == {"minsize": 1, "maxsize": 20}


@pytest.mark.skipif(sys.version_info >= (3, 10), reason="requires python3.9 or lower")
def test_mysql_ssl():
    backend = MySQLBackend("mysql://localhost/database?ssl=true")
    kwargs = backend._get_connection_kwargs()
    assert kwargs == {"ssl": True}


@pytest.mark.skipif(sys.version_info >= (3, 10), reason="requires python3.9 or lower")
def test_mysql_explicit_ssl():
    backend = MySQLBackend("mysql://localhost/database", ssl=True)
    kwargs = backend._get_connection_kwargs()
    assert kwargs == {"ssl": True}


@pytest.mark.skipif(sys.version_info >= (3, 10), reason="requires python3.9 or lower")
def test_mysql_pool_recycle():
    backend = MySQLBackend("mysql://localhost/database?pool_recycle=20")
    kwargs = backend._get_connection_kwargs()
    assert kwargs == {"pool_recycle": 20}


@pytest.mark.skipif(sys.version_info < (3, 7), reason="requires python3.7 or higher")
def test_asyncmy_pool_size():
    backend = AsyncMyBackend(
        "mysql+asyncmy://localhost/database?min_size=1&max_size=20"
    )
    kwargs = backend._get_connection_kwargs()
    assert kwargs == {"minsize": 1, "maxsize": 20}


@pytest.mark.skipif(sys.version_info < (3, 7), reason="requires python3.7 or higher")
def test_asyncmy_unix_socket():
    backend = AsyncMyBackend(
        "mysql+asyncmy://username:password@/testsuite?unix_socket=/tmp/mysqld/mysqld.sock"
    )
    kwargs = backend._get_connection_kwargs()
    assert kwargs == {"unix_socket": "/tmp/mysqld/mysqld.sock"}


@pytest.mark.skipif(sys.version_info < (3, 7), reason="requires python3.7 or higher")
def test_asyncmy_explicit_pool_size():
    backend = AsyncMyBackend("mysql://localhost/database", min_size=1, max_size=20)
    kwargs = backend._get_connection_kwargs()
    assert kwargs == {"minsize": 1, "maxsize": 20}


@pytest.mark.skipif(sys.version_info < (3, 7), reason="requires python3.7 or higher")
def test_asyncmy_ssl():
    backend = AsyncMyBackend("mysql+asyncmy://localhost/database?ssl=true")
    kwargs = backend._get_connection_kwargs()
    assert kwargs == {"ssl": True}


@pytest.mark.skipif(sys.version_info < (3, 7), reason="requires python3.7 or higher")
def test_asyncmy_explicit_ssl():
    backend = AsyncMyBackend("mysql+asyncmy://localhost/database", ssl=True)
    kwargs = backend._get_connection_kwargs()
    assert kwargs == {"ssl": True}


@pytest.mark.skipif(sys.version_info < (3, 7), reason="requires python3.7 or higher")
def test_asyncmy_pool_recycle():
    backend = AsyncMyBackend("mysql+asyncmy://localhost/database?pool_recycle=20")
    kwargs = backend._get_connection_kwargs()
    assert kwargs == {"pool_recycle": 20}


def test_aiopg_pool_size():
    backend = AiopgBackend(
        "postgresql+aiopg://localhost/database?min_size=1&max_size=20"
    )
    kwargs = backend._get_connection_kwargs()
    assert kwargs == {"minsize": 1, "maxsize": 20}


def test_aiopg_explicit_pool_size():
    backend = AiopgBackend(
        "postgresql+aiopg://localhost/database", min_size=1, max_size=20
    )
    kwargs = backend._get_connection_kwargs()
    assert kwargs == {"minsize": 1, "maxsize": 20}


def test_aiopg_ssl():
    backend = AiopgBackend("postgresql+aiopg://localhost/database?ssl=true")
    kwargs = backend._get_connection_kwargs()
    assert kwargs == {"ssl": True}


def test_aiopg_explicit_ssl():
    backend = AiopgBackend("postgresql+aiopg://localhost/database", ssl=True)
    kwargs = backend._get_connection_kwargs()
    assert kwargs == {"ssl": True}
