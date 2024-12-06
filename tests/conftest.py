import os
import json
from contextlib import asynccontextmanager

import psycopg
import pytest
from pytest_postgresql.janitor import DatabaseJanitor

from fastapi.testclient import TestClient
from fastapi import FastAPI

from api.models import HealthCheckResponse

SQL_DIR = os.path.join(os.path.dirname(__file__), "sql")

@pytest.fixture(scope="session")
def database(postgresql_proc):
    """Create Database Fixture."""
    with DatabaseJanitor(
        user=postgresql_proc.user,
        host=postgresql_proc.host,
        port=postgresql_proc.port,
        dbname="data",
        version=postgresql_proc.version,
        password="password",
    ) as jan:
        yield jan


def _get_sql(source: str) -> str:
    with open(source, "r") as fd:
        to_run = fd.readlines()

    return "\n".join(to_run)

@pytest.fixture(scope="session")
def database_wrapper(database):
    """add data to the database fixture"""
    db_url = f"postgresql://{database.user}:{database.password}@{database.host}:{database.port}/{database.dbname}"
    with psycopg.connect(db_url, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(f"ALTER DATABASE {database.dbname} SET TIMEZONE='UTC';")
            cur.execute("SET TIME ZONE 'UTC';")

            cur.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
            assert cur.execute(
                "SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname='postgis' LIMIT 1);"
            ).fetchone()[0]

            cur.execute("CREATE SCHEMA IF NOT EXISTS public;")
            cur.execute(_get_sql(os.path.join(SQL_DIR, "states.sql")))

    return database

def create_app(database) -> FastAPI:
    """Create Application."""

    from api.routers.upload_anything import router as upload_anything_router

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        """
        FastAPI lifespan event to initialize the application.

        This event is called on application startup and shutdown. On startup, it reads the
        geographies.json file to initialize the list of geographies.
        """
        if os.path.exists("api/geographies.json") is False:
            raise Exception("api/geographies.json not found")
        with open("api/geographies.json", "r") as f:
            app.state.geographies = json.loads(f.read())
        yield

    app = FastAPI(
        title="pg-upload-anything-api",
        description="API for uploading any geographic data to a PostgreSQL database.",
        lifespan=lifespan,
    )
    app.include_router(
        upload_anything_router.router,
        prefix="/api/v1/upload_anything",
        tags=["Upload Anything"],
    )

    @app.get("/api/v1/health_check", tags=["Health"], response_model=HealthCheckResponse)
    async def health():
        """
        Method used to verify server is healthy.
        """

        return {"status": "UP"}

    app.state.dbname = database.dbname
    app.state.dbuser = database.user
    app.state.dbpass = database.password
    app.state.dbhost = database.host
    app.state.dbport = database.port

    return app

@pytest.fixture
def app(database_wrapper):
    """Create APP with only custom functions."""

    app = create_app(database_wrapper)   

    with TestClient(app) as client:
        yield client