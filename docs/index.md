# Databases

<p>
<a href="https://github.com/encode/databases/actions">
    <img src="https://github.com/encode/databases/workflows/Test%20Suite/badge.svg" alt="Test Suite">
</a>
<a href="https://pypi.org/project/databases/">
    <img src="https://badge.fury.io/py/databases.svg" alt="Package version">
</a>
</p>

Databases gives you simple asyncio support for a range of databases.

It allows you to make queries using the powerful [SQLAlchemy Core][sqlalchemy-core]
expression language, and provides support for PostgreSQL, MySQL, and SQLite.

Databases is suitable for integrating against any async Web framework, such as [Starlette][starlette],
[Sanic][sanic], [Responder][responder], [Quart][quart], [aiohttp][aiohttp], [Tornado][tornado], or [FastAPI][fastapi].

**Community**: [https://discuss.encode.io/c/databases](https://discuss.encode.io/c/databases)

**Requirements**: Python 3.6+

---

## Installation

```shell
$ pip install databases
```

You can install the required database drivers with:

```shell
$ pip install databases[postgresql]
$ pip install databases[mysql]
$ pip install databases[sqlite]
```

Driver support is providing using one of [asyncpg][asyncpg], [aiomysql][aiomysql], or [aiosqlite][aiosqlite].

---

## Quickstart

For this example we'll create a very simple SQLite database to run some
queries against.

```shell
$ pip install databases[sqlite]
$ pip install ipython
```

We can now run a simple example from the console.

Note that we want to use `ipython` here, because it supports using `await`
expressions directly from the console.

```python
# Create a database instance, and connect to it.
from databases import Database
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

Check out the documentation on [making database queries](database_queries.md)
for examples of how to start using databases together with SQLAlchemy core expressions.


<p align="center">&mdash; ⭐️ &mdash;</p>
<p align="center"><i>Databases is <a href="https://github.com/encode/databases/blob/master/LICENSE.md">BSD licensed</a> code. Designed & built in Brighton, England.</i></p>

[sqlalchemy-core]: https://docs.sqlalchemy.org/en/latest/core/
[sqlalchemy-core-tutorial]: https://docs.sqlalchemy.org/en/latest/core/tutorial.html
[alembic]: https://alembic.sqlalchemy.org/en/latest/
[asyncpg]: https://github.com/MagicStack/asyncpg
[aiomysql]: https://github.com/aio-libs/aiomysql
[aiosqlite]: https://github.com/jreese/aiosqlite

[starlette]: https://github.com/encode/starlette
[sanic]: https://github.com/huge-success/sanic
[responder]: https://github.com/kennethreitz/responder
[quart]: https://gitlab.com/pgjones/quart
[aiohttp]: https://github.com/aio-libs/aiohttp
[tornado]: https://github.com/tornadoweb/tornado
[fastapi]: https://github.com/tiangolo/fastapi
