from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config.settings import settings
from app.config.database import close_mongodb, connect_to_mongodb, mongodb_status
from app.services.slots import seed_fake_slots
from app.services.tracking import seed_fake_deliveries
from app.websocket import router as websocket_router


@asynccontextmanager
async def lifespan(_: FastAPI):
    await connect_to_mongodb()
    await seed_fake_deliveries()
    await seed_fake_slots()
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
