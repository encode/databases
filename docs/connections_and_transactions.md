# Connections and Transactions

Databases handles database connection pooling and transaction management
with minimal fuss. It'll automatically deal with acquiring and releasing
connections to the pool as needed, and supports a simple transaction API
that transparently handles the use of either transactions or savepoints.

## Connecting and disconnecting

You can control the database connection pool with an async context manager:

```python
async with Database(DATABASE_URL) as database:
    ...
```

Or by using the explicit `.connect()` and `.disconnect()` methods:

```python
database = Database(DATABASE_URL)
await database.connect()
...
await database.disconnect()
```

Connections within this connection pool are acquired for each new `asyncio.Task`.

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
database = Database('postgresql+asyncpg://localhost/example?ssl=true')

# Use a connection pool of between 5-20 connections.
database = Database('mysql+aiomysql://localhost/example?min_size=5&max_size=20')
```

You can also use keyword arguments to pass in any connection options.
Available keyword arguments may differ between database backends.

```python
database = Database('postgresql+asyncpg://localhost/example', ssl=True, min_size=5, max_size=20)
```

## Transactions

Transactions are managed by async context blocks.

A transaction can be acquired from the database connection pool:

```python
async with database.transaction():
    ...
```

It can also be acquired from a specific database connection:

```python
async with database.connection() as connection:
    async with connection.transaction():
        ...
```

For a lower-level transaction API:

```python
transaction = await database.transaction()
try:
    ...
except:
    await transaction.rollback()
else:
    await transaction.commit()
```

You can also use `.transaction()` as a function decorator on any async function:

```python
@database.transaction()
async def create_users(request):
    ...
```

Transaction state is tied to the connection used in the currently executing asynchronous task.
If you would like to influence an active transaction from another task, the connection must be
shared. This state is _inherited_ by tasks that share the same connection:

```python
async def add_excitement(connnection: databases.core.Connection, id: int):
    await connection.execute(
        "UPDATE notes SET text = CONCAT(text, '!!!') WHERE id = :id",
        {"id": id}
    )


async with Database(database_url) as database:
    async with database.transaction():
        # This note won't exist until the transaction closes...
        await database.execute(
            "INSERT INTO notes(id, text) values (1, 'databases is cool')"
        )
        # ...but child tasks can use this connection now!
        await asyncio.create_task(add_excitement(database.connection(), id=1))

    await database.fetch_val("SELECT text FROM notes WHERE id=1")
    # ^ returns: "databases is cool!!!"
```

Nested transactions are fully supported, and are implemented using database savepoints:

```python
async with databases.Database(database_url) as db:
    async with db.transaction() as outer:
        # Do something in the outer transaction
        ...

        # Suppress to prevent influence on the outer transaction
        with contextlib.suppress(ValueError):
            async with db.transaction():
                # Do something in the inner transaction
                ...

                raise ValueError('Abort the inner transaction')

    # Observe the results of the outer transaction,
    # without effects from the inner transaction.
    await db.fetch_all('SELECT * FROM ...')
```

Transaction isolation-level can be specified if the driver backend supports that:

```python
async with database.transaction(isolation="serializable"):
    ...
```

[starlette]: https://github.com/encode/starlette
