import json
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api import config
from api.models import HealthCheckResponse
from api.routers.upload_anything import router as upload_anything_router
from api.version import __version__


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
    version=__version__,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(
    upload_anything_router.router,
    prefix="/api/v1/upload_anything",
    tags=["Upload Anything"],
)

app.state.dbname = config.DB_NAME
app.state.dbuser = config.DB_USER
app.state.dbpass = config.DB_PASSWORD
app.state.dbhost = config.DB_HOST
app.state.dbport = config.DB_PORT


@app.get("/api/v1/health_check", tags=["Health"], response_model=HealthCheckResponse)
async def health():
    """
    Method used to verify server is healthy.
    """

    return {"status": "UP"}
