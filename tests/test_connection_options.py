"""
Unit tests for the backend connection arguments.
"""

from databases.backends.mysql import MySQLBackend
from databases.backends.postgres import PostgresBackend


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
