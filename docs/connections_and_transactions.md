# Connections and Transactions

Databases handles database connection pooling and transaction management
with minimal fuss. It'll automatically deal with acquiring and releasing
connections to the pool as needed, and supports a simple transaction API
that transparently handles the use of either transactions or savepoints.

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

Transaction blocks are managed as task-local state. Nested transactions
are fully supported, and are implemented using database savepoints.

Transaction isolation-level can be specified if the driver backend supports that:

```python
async with database.transaction(isolation="serializable"):
    ...
```

[starlette]: https://github.com/encode/starlette
