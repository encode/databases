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

Currently PostgreSQL and MySQL are supported.

**Requirements**: Python 3.6+

## Installation

```shell
$ pip install databases
```

You can install the required database drivers with:

```shell
$ pip install databases[postgresql]
$ pip install databases[mysql]
```

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

You can use any of the SQLAlchemy column types such as `sqlalchemy.JSON`, or
use custom column types. Marshalling between python datatypes and the underlying
database encoding will be correctly handled for you.

## Queries

You can now use [SQLAlchemy core](https://docs.sqlalchemy.org/en/latest/core/) queries:

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
```

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

For strict test isolation you will always want to rollback the test database
to a clean state between each test case:

```python
async with database.transaction(force_rollback=True):
    ...
```

Transaction blocks are managed as task-local state. Nested transactions
are fully supported, and are implemented using database savepoints.
