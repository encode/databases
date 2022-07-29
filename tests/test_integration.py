import pytest
import sqlalchemy
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.testclient import TestClient

from databases import Database, DatabaseURL
from tests.test_databases import DATABASE_URLS

metadata = sqlalchemy.MetaData()

notes = sqlalchemy.Table(
    "notes",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("text", sqlalchemy.String(length=100)),
    sqlalchemy.Column("completed", sqlalchemy.Boolean),
)


@pytest.fixture(autouse=True, scope="module")
def create_test_database():
    # Create test databases
    for url in DATABASE_URLS:
        database_url = DatabaseURL(url)
        if database_url.scheme in ["mysql", "mysql+aiomysql", "mysql+asyncmy"]:
            url = str(database_url.replace(driver="pymysql"))
        elif database_url.scheme in [
            "postgresql+aiopg",
            "sqlite+aiosqlite",
            "postgresql+asyncpg",
        ]:
            url = str(database_url.replace(driver=None))
        engine = sqlalchemy.create_engine(url)
        metadata.create_all(engine)

    # Run the test suite
    yield

    for url in DATABASE_URLS:
        database_url = DatabaseURL(url)
        if database_url.scheme in ["mysql", "mysql+aiomysql", "mysql+asyncmy"]:
            url = str(database_url.replace(driver="pymysql"))
        elif database_url.scheme in [
            "postgresql+aiopg",
            "sqlite+aiosqlite",
            "postgresql+asyncpg",
        ]:
            url = str(database_url.replace(driver=None))
        engine = sqlalchemy.create_engine(url)
        metadata.drop_all(engine)


def get_app(database_url):
    database = Database(database_url, force_rollback=True)
    app = Starlette()

    @app.on_event("startup")
    async def startup():
        await database.connect()

    @app.on_event("shutdown")
    async def shutdown():
        await database.disconnect()

    @app.route("/notes", methods=["GET"])
    async def list_notes(request):
        query = notes.select()
        results = await database.fetch_all(query)
        content = [
            {"text": result["text"], "completed": result["completed"]}
            for result in results
        ]
        return JSONResponse(content)

    @app.route("/notes", methods=["POST"])
    async def add_note(request):
        data = await request.json()
        query = notes.insert().values(text=data["text"], completed=data["completed"])
        await database.execute(query)
        return JSONResponse({"text": data["text"], "completed": data["completed"]})

    return app


@pytest.mark.parametrize("database_url", DATABASE_URLS)
def test_integration(database_url):
    app = get_app(database_url)

    with TestClient(app) as client:
        response = client.post("/notes", json={"text": "example", "completed": True})
        assert response.status_code == 200
        assert response.json() == {"text": "example", "completed": True}

        response = client.get("/notes")
        assert response.status_code == 200
        assert response.json() == [{"text": "example", "completed": True}]

    with TestClient(app) as client:
        # Ensure sessions are isolated
        response = client.get("/notes")
        assert response.status_code == 200
        assert response.json() == []
