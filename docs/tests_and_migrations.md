# Tests and Migrations

Databases is designed to allow you to fully integrate with production
ready services, with API support for test isolation, and integration
with [Alembic][alembic] for database migrations.

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

[alembic]: https://alembic.sqlalchemy.org/en/latest/
