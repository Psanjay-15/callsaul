import asyncio
from datetime import date, timedelta

from app.config.database import (
    close_mongodb,
    connect_to_mongodb,
    create_database_indexes,
    get_database,
    mongodb_status,
)
from app.services.slots import create_slot


DELIVERIES = [
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


def demo_slots() -> list[dict]:
    tomorrow = date.today() + timedelta(days=1)
    following_day = date.today() + timedelta(days=2)
    return [
        create_slot(tomorrow, "10:00", "12:00", "10 AM to 12 PM"),
        create_slot(tomorrow, "14:00", "16:00", "2 PM to 4 PM"),
        create_slot(following_day, "16:00", "18:00", "4 PM to 6 PM"),
    ]


async def seed_data() -> None:
    await connect_to_mongodb()
    if mongodb_status() != "connected":
        raise RuntimeError("MongoDB is not connected. Check MONGODB_URI in .env.")

    await create_database_indexes()
    database = get_database()

    for delivery in DELIVERIES:
        await database.deliveries.update_one(
            {"tracking_id": delivery["tracking_id"]},
            {"$setOnInsert": delivery},
            upsert=True,
        )

    for slot in demo_slots():
        await database.delivery_slots.update_one(
            {"slot_id": slot["slot_id"]},
            {"$setOnInsert": slot},
            upsert=True,
        )

    print(f"Seeded {len(DELIVERIES)} deliveries and {len(demo_slots())} slots.")
    await close_mongodb()


if __name__ == "__main__":
    asyncio.run(seed_data())
