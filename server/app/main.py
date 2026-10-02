from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config.settings import settings
from app.config.database import (
    close_mongodb,
    connect_to_mongodb,
    create_database_indexes,
    mongodb_status,
)
from app.websocket import router as websocket_router
from app.history import router as history_router


@asynccontextmanager
async def lifespan(_: FastAPI):
    await connect_to_mongodb()
    if mongodb_status() == "connected":
        await create_database_indexes()
    yield
    await close_mongodb()


app = FastAPI(
    title=settings.app_name,
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[settings.frontend_origin],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(websocket_router)
app.include_router(history_router)


@app.get("/")
async def root() -> dict[str, str]:
    return {"message": settings.app_name}


@app.get("/api/health")
async def health() -> dict[str, str]:
    return {
        "status": "ok",
        "environment": settings.app_env,
        "database": mongodb_status(),
    }
