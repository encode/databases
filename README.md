# Morcilla

<p>
<a href="https://github.com/athenianco/morcilla/actions">
    <img src="https://github.com/athenianco/morcilla/workflows/Test%20Suite/badge.svg" alt="Test Suite">
</a>
<a href="https://pypi.org/project/morcilla/">
    <img src="https://badge.fury.io/py/morcilla.svg" alt="Package version">
</a>
</p>

Morcilla gives you simple and high-performant asyncio support for a range of databases.
The project is a hard fork of [encode/databases](https://github.com/encode/databases).

It allows you to make queries using the powerful [SQLAlchemy Core][sqlalchemy-core]
expression language, and provides support for PostgreSQL, MySQL, and SQLite.

Databases is suitable for integrating against any async Web framework.

**Requirements**: Python 3.8+

---

## Installation

```shell
$ pip install morcilla
```

You can install the required database drivers with:

```shell
$ pip install morcilla[postgresql]
$ pip install morcilla[mysql]
$ pip install morcilla[sqlite]
```

Default driver support is provided using one of [asyncpg][asyncpg], [aiomysql][aiomysql], or [aiosqlite][aiosqlite].

You can also use other database drivers supported by `morcilla`:

```shel
$ pip install morcilla[postgresql+aiopg]
$ pip install morcilla[mysql+asyncmy]
```

Note that if you are using any synchronous SQLAlchemy functions such as `engine.create_all()` or [alembic][alembic] migrations then you still have to install a synchronous DB driver: [psycopg2][psycopg2] for PostgreSQL and [pymysql][pymysql] for MySQL.

---

## Quickstart

For this example we'll create a very simple SQLite database to run some
queries against.

```shell
$ pip install morcilla[sqlite]
$ pip install ipython
```

We can now run a simple example from the console.

Note that we want to use `ipython` here, because it supports using `await`
expressions directly from the console.

```python
# Create a database instance, and connect to it.
from morcilla import Database
database = Database('sqlite:///example.db')
await database.connect()

# Create a table.
query = """CREATE TABLE HighScores (id INTEGER PRIMARY KEY, name VARCHAR(100), score INTEGER)"""
await database.execute(query=query)

# Insert some data.
query = "INSERT INTO HighScores(name, score) VALUES (:name, :score)"
values = [
    {"name": "Daisy", "score": 92},
    {"name": "Neil", "score": 87},
    {"name": "Carol", "score": 43},
]
await database.execute_many(query=query, values=values)

# Run a database query.
query = "SELECT * FROM HighScores"
rows = await database.fetch_all(query=query)
print('High Scores:', rows)
```

Check out the documentation on [making database queries](https://www.encode.io/morcilla/database_queries/)
for examples of how to start using morcilla together with SQLAlchemy core expressions.

## Why hard fork?

Morcilla satisfies one particular requirement that Athenian has: to provide the best performance
at any cost, while sacrificing as little developer experience as possible. Hence it rejects
the uniform Record interface that `encode/databases` provides in favor of native backend objects.
Thus there is no guarantee that the same code will work equally successful for all the supported
DB backends. Besides, we have optimized Morcilla for asyncpg, so e.g. asyncpg performs JSON serialization and
deserialization instead of sqlalchemy for performance boost. The character of the changes is
very much breaking the existing code, and they should not be submitted upstream.

Finally, we are going to add new backends such as Clickhouse in the future.


[sqlalchemy-core]: https://docs.sqlalchemy.org/en/latest/core/
[sqlalchemy-core-tutorial]: https://docs.sqlalchemy.org/en/latest/core/tutorial.html
[alembic]: https://alembic.sqlalchemy.org/en/latest/
[psycopg2]: https://www.psycopg.org/
[pymysql]: https://github.com/PyMySQL/PyMySQL
[asyncpg]: https://github.com/MagicStack/asyncpg
[aiomysql]: https://github.com/aio-libs/aiomysql
[aiosqlite]: https://github.com/jreese/aiosqlite
