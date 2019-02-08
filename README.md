# Databases

Databases gives you simple asyncio support for a range of databases.
Currently PostgreSQL and MySQL are supported.

```shell
$ pip install databases
```

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

You can now use SQLAlchemy core queries:

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
rows = await database.fetch_all()


# Fetch single row
query = notes.select()
row = await database.fetch_one()
```

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
