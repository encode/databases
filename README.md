# Databases

**NOT YET READY FOR USE**

Databases gives you asyncio support for making SQLAlchemy core queries against
a range of databases.

* Postgres, MySQL, and SQLite support.
* Queries using SQLAlchemy core.
* Migrations using Alembic.

```shell
$ pip install databases
```

```python
from databases import Database


database = Database('postgresql://localhost/example')

query = ...
rows = await database.execute()

query = ...
rows = await database.fetchall()

query = ...
row = await database.fetchone()

query = ...
value = await database.fetchval()
```

Transactions

```python
async with database.transaction():
    ...
```
