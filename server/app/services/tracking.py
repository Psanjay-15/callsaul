import re

from app.config.database import get_database


TRACKING_ID_PATTERN = re.compile(r"^[A-Z]{2}\d{6}$")

FAKE_DELIVERIES = [
    {
        "tracking_id": "BD418207",
        "status": "out_for_delivery",
        "customer_name": "Sanjay",
    },
    {
        "tracking_id": "CS123456",
        "status": "scheduled",
        "customer_name": "Alex",
    },
    {
        "tracking_id": "AX654321",
        "status": "scheduled",
        "customer_name": "Taylor",
    },
]


def normalize_tracking_id(value: str) -> str:
    return "".join(character for character in value.upper() if character.isalnum())


def is_valid_tracking_id(tracking_id: str) -> bool:
    return bool(TRACKING_ID_PATTERN.fullmatch(tracking_id))


def tracking_id_for_speech(tracking_id: str) -> str:
    return ", ".join(tracking_id)


async def find_delivery(tracking_id: str) -> dict | None:
    database = get_database()
    return await database.deliveries.find_one(
        {"tracking_id": tracking_id},
        {"_id": 0},
    )


async def seed_fake_deliveries() -> None:
    try:
        database = get_database()
    except RuntimeError:
        return

    await database.deliveries.create_index("tracking_id", unique=True)

    for delivery in FAKE_DELIVERIES:
        await database.deliveries.update_one(
            {"tracking_id": delivery["tracking_id"]},
            {"$setOnInsert": delivery},
            upsert=True,
        )
