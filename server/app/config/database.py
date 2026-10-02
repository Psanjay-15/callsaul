import logging

from pymongo import AsyncMongoClient
from pymongo.errors import PyMongoError

from app.config.settings import settings


logger = logging.getLogger(__name__)

_client: AsyncMongoClient | None = None
_database = None
_status = "not_configured"


async def connect_to_mongodb() -> None:
    global _client, _database, _status

    if not settings.mongodb_uri:
        _status = "not_configured"
        logger.warning("MONGODB_URI is not configured")
        return

    _client = AsyncMongoClient(
        settings.mongodb_uri,
        serverSelectionTimeoutMS=2_000,
    )
    _database = _client[settings.mongodb_db_name]

    try:
        await _client.admin.command("ping")
        _status = "connected"
        logger.info("Connected to MongoDB database '%s'", settings.mongodb_db_name)
    except PyMongoError as error:
        _status = "unavailable"
        logger.warning("MongoDB is unavailable: %s", error)


async def close_mongodb() -> None:
    global _client, _database, _status

    if _client is not None:
        await _client.close()

    _client = None
    _database = None
    _status = "not_configured"


async def create_database_indexes() -> None:
    database = get_database()
    await database.deliveries.create_index("tracking_id", unique=True)
    await database.delivery_slots.create_index("slot_id", unique=True)
    await database.delivery_slots.create_index(
        "booked_tracking_id",
        unique=True,
        sparse=True,
    )
    await database.delivery_slots.create_index(
        "booking_idempotency_key",
        unique=True,
        sparse=True,
    )
    await database.bookings.create_index("idempotency_key", unique=True)
    await database.chat_sessions.create_index("session_id", unique=True)
    await database.chat_sessions.create_index("last_message_at")
    await database.chat_messages.create_index(
        [("session_id", 1), ("created_at", 1)]
    )


def mongodb_status() -> str:
    return _status


def get_database():
    if _status != "connected" or _database is None:
        raise RuntimeError("MongoDB is not connected")
    return _database
