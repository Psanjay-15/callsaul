from datetime import date, datetime, timedelta, timezone

from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError

from app.config.database import get_database


def build_fake_slots() -> list[dict]:
    tomorrow = date.today() + timedelta(days=1)
    following_day = date.today() + timedelta(days=2)

    return [
        create_slot(tomorrow, "10:00", "12:00", "10 AM to 12 PM"),
        create_slot(tomorrow, "14:00", "16:00", "2 PM to 4 PM"),
        create_slot(following_day, "16:00", "18:00", "4 PM to 6 PM"),
    ]


def create_slot(
    slot_date: date,
    start_time: str,
    end_time: str,
    time_label: str,
) -> dict:
    date_label = f"{slot_date:%A, %B} {slot_date.day}"
    return {
        "slot_id": f"{slot_date.isoformat()}-{start_time}",
        "date": slot_date.isoformat(),
        "start_time": start_time,
        "end_time": end_time,
        "label": f"{date_label}, {time_label}",
        "available": True,
    }


async def seed_fake_slots() -> None:
    try:
        database = get_database()
    except RuntimeError:
        return

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

    for slot in build_fake_slots():
        await database.delivery_slots.update_one(
            {"slot_id": slot["slot_id"]},
            {"$setOnInsert": slot},
            upsert=True,
        )


async def get_available_slots(limit: int = 3) -> list[dict]:
    database = get_database()
    today = date.today().isoformat()
    cursor = (
        database.delivery_slots.find(
            {
                "available": True,
                "date": {"$gte": today},
            },
            {"_id": 0},
        )
        .sort([("date", 1), ("start_time", 1)])
        .limit(limit)
    )
    return await cursor.to_list(length=limit)


async def book_delivery_slot(
    tracking_id: str,
    slot_id: str,
    idempotency_key: str,
) -> dict:
    database = get_database()
    booked_at = datetime.now(timezone.utc)

    try:
        slot = await database.delivery_slots.find_one_and_update(
            {
                "slot_id": slot_id,
                "$or": [
                    {"available": True},
                    {"booking_idempotency_key": idempotency_key},
                ],
            },
            {
                "$set": {
                    "available": False,
                    "booked_tracking_id": tracking_id,
                    "booking_idempotency_key": idempotency_key,
                    "booked_at": booked_at,
                }
            },
            projection={"_id": 0},
            return_document=ReturnDocument.AFTER,
        )
    except DuplicateKeyError:
        slot = await database.delivery_slots.find_one(
            {
                "$or": [
                    {"booked_tracking_id": tracking_id},
                    {"booking_idempotency_key": idempotency_key},
                ]
            },
            {"_id": 0},
        )

        if slot is None:
            return {"status": "unavailable"}

        if slot["slot_id"] != slot_id:
            return {"status": "already_booked", "slot": slot}

    if slot is None:
        return {"status": "unavailable"}

    await database.bookings.update_one(
        {"idempotency_key": idempotency_key},
        {
            "$setOnInsert": {
                "idempotency_key": idempotency_key,
                "tracking_id": tracking_id,
                "slot_id": slot_id,
                "slot_label": slot["label"],
                "status": "confirmed",
                "created_at": booked_at,
            }
        },
        upsert=True,
    )

    await database.deliveries.update_one(
        {"tracking_id": tracking_id},
        {
            "$set": {
                "status": "rescheduled",
                "delivery_slot_id": slot_id,
                "delivery_slot_label": slot["label"],
            }
        },
    )

    return {"status": "booked", "slot": slot}
