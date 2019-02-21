# Databases

<p>
<a href="https://travis-ci.org/encode/databases">
    <img src="https://travis-ci.org/encode/databases.svg?branch=master" alt="Build Status">
</a>
<a href="https://codecov.io/gh/encode/databases">
    <img src="https://codecov.io/gh/encode/databases/branch/master/graph/badge.svg" alt="Coverage">
</a>
<a href="https://pypi.org/project/databases/">
    <img src="https://badge.fury.io/py/databases.svg" alt="Package version">
</a>
</p>

Databases gives you simple asyncio support for a range of databases.

It allows you to make queries using the powerful [SQLAlchemy Core][sqlalchemy-core]
expression language, and provides support for PostgreSQL, MySQL, and SQLite.

Databases is suitable for integrating against any async Web framework, such as [Starlette][starlette],
[Sanic][sanic], [Responder][responder], [Quart][quart], [aiohttp][aiohttp], [FastAPI][fastapi], or [Bocadillo][bocadillo].

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

## Getting started

Declare your tables using SQLAlchemy:

```python
import sqlalchemy


metadata = sqlalchemy.MetaData()

notes = sqlalchemy.Table(
    "notes",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("text", sqlalchemy.String(length=100)),
    sqlalchemy.Column("completed", sqlalchemy.Boolean),
)
```


You can use any of the sqlalchemy column types such as `sqlalchemy.JSON`, or
custom column types.

## Queries

You can now use any [SQLAlchemy core][sqlalchemy-core] queries:

```python
from databases import Database

database = Database('postgresql://localhost/example')


# Establish the connection pool
await database.connect()

# Execute
query = notes.insert()
values = {"text": "example1", "completed": True}
await database.execute(query, values)

# Execute many
query = notes.insert()
values = [
    {"text": "example2", "completed": False},
    {"text": "example3", "completed": True},
]
await database.execute_many(query, values)

# Fetch multiple rows
query = notes.select()
rows = await database.fetch_all(query)

# Fetch single row
query = notes.select()
row = await database.fetch_one(query)

# Fetch multiple rows without loading them all into memory at once
query = notes.select()
async for row in database.iterate(query):
    ...

# Close all connection in the connection pool
await database.disconnect()
```

Connections are managed as task-local state, with driver implementations
transparently using connection pooling behind the scenes.

## Transactions

Transactions are managed by async context blocks:

```python
async with database.transaction():
    ...
```

For a lower-level transaction API:

```python
transaction = await database.transaction()
try:
    ...
except:
    transaction.rollback()
else:
    transaction.commit()
```

You can also use `.transaction()` as a function decorator on any async function:

```python
@database.transaction()
async def create_users(request):
    ...
```

Transaction blocks are managed as task-local state. Nested transactions
are fully supported, and are implemented using database savepoints.

## Connecting and disconnecting

You can control the database connect/disconnect, by using it as a async context manager.

```python
async with Database(DATABASE_URL) as database:
    ...
```

Or by using explicit connection and disconnection:

```python
database = Database(DATABASE_URL)
await database.connect()
...
await database.disconnect()
```

If you're integrating against a web framework, then you'll probably want
to hook into framework startup or shutdown events. For example, with
[Starlette][starlette] you would use the following:

```python
@app.on_event("startup")
async def startup():
    await database.connect()

@app.on_event("shutdown")
async def shutdown():
    await database.disconnect()
```

## Connection options

The PostgreSQL and MySQL backends provide a few connection options for SSL
and for configuring the connection pool.

```python
# Use an SSL connection.
database = Database('postgresql://localhost/example?ssl=true')

# Use a connection pool of between 5-20 connections.
database = Database('mysql://localhost/example?min_size=5&max_size=20')
```

## Test isolation

For strict test isolation you will always want to rollback the test database
to a clean state between each test case:

```python
database = Database(DATABASE_URL, force_rollback=True)
```

This will ensure that all database connections are run within a transaction
that rollbacks once the database is disconnected.

If you're integrating against a web framework you'll typically want to
use something like the following pattern:

```python
if TESTING:
    database = Database(TEST_DATABASE_URL, force_rollback=True)
else:
    database = Database(DATABASE_URL)
```

This will give you test cases that run against a different database to
the development database, with strict test isolation so long as you make sure
to connect and disconnect to the database between test cases.

For a lower level API you can explicitly create force-rollback transactions:

```python
async with database.transaction(force_rollback=True):
    ...
```

## Migrations

Because `databases` uses SQLAlchemy core, you can integrate with [Alembic][alembic]
for database migration support.

```shell
$ pip install alembic
$ alembic init migrations
```

You'll want to set things up so that Alembic references the configured
`DATABASE_URL`, and uses your table metadata.

In `alembic.ini` remove the following line:

```shell
sqlalchemy.url = driver://user:pass@localhost/dbname
```

In `migrations/env.py`, you need to set the ``'sqlalchemy.url'`` configuration key,
and the `target_metadata` variable. You'll want something like this:

```python
# The Alembic Config object.
config = context.config

# Configure Alembic to use our DATABASE_URL and our table definitions.
# These are just examples - the exact setup will depend on whatever
# framework you're integrating against.
from myapp.settings import DATABASE_URL
from myapp.tables import metadata

config.set_main_option('sqlalchemy.url', str(DATABASE_URL))
target_metadata = metadata

...
```

Note that migrations will use a standard synchronous database driver,
rather than using the async drivers that `databases` provides support for.

This will also be the case if you're using SQLAlchemy's standard tooling, such
as using `metadata.create_all(engine)` to setup the database tables.

**Note for MySQL**:

For MySQL you'll probably need to explicitly specify the `pymysql` dialect when
using Alembic since the default MySQL dialect does not support Python 3.

If you're using the `databases.DatabaseURL` datatype, you can obtain this using
`DATABASE_URL.replace(dialect="pymysql")`

<p align="center">&mdash; ⭐️ &mdash;</p>
<p align="center"><i>Databases is <a href="https://github.com/encode/databases/blob/master/LICENSE.md">BSD licensed</a> code. Designed & built in Brighton, England.</i></p>

[sqlalchemy-core]: https://docs.sqlalchemy.org/en/latest/core/
[alembic]: https://alembic.sqlalchemy.org/en/latest/
[asyncpg]: https://github.com/MagicStack/asyncpg
[aiomysql]: https://github.com/aio-libs/aiomysql
[aiosqlite]: https://github.com/jreese/aiosqlite

[starlette]: https://github.com/encode/starlette
[sanic]: https://github.com/huge-success/sanic
[responder]: https://github.com/kennethreitz/responder
[quart]: https://gitlab.com/pgjones/quart
[aiohttp]: https://github.com/aio-libs/aiohttp
[fastapi]: https://github.com/tiangolo/fastapi
[bocadillo]: https://github.com/bocadilloproject/bocadillo
