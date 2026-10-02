from datetime import date, datetime, timezone

from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError

from app.config.database import get_database
from app.services.fake_backend import simulate_backend_behavior


class SlotUnavailableError(Exception):
    pass


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


async def get_available_slots(limit: int = 3) -> list[dict]:
    await simulate_backend_behavior("get_available_slots")
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


async def get_booked_slot(tracking_id: str) -> dict | None:
    database = get_database()
    return await database.delivery_slots.find_one(
        {"booked_tracking_id": tracking_id},
        {"_id": 0},
    )


async def book_delivery_slot(
    tracking_id: str,
    slot_id: str,
    idempotency_key: str,
) -> dict:
    await simulate_backend_behavior("book_delivery_slot")
    database = get_database()
    booked_at = datetime.now(timezone.utc)

    async def save_booking(session) -> dict:
        previous_booking = await database.bookings.find_one(
            {"idempotency_key": idempotency_key},
            {"_id": 0},
            session=session,
        )
        if previous_booking is not None:
            current_slot = await database.delivery_slots.find_one(
                {"booked_tracking_id": tracking_id},
                {"_id": 0},
                session=session,
            )
            if current_slot is None:
                raise SlotUnavailableError
            return {"status": "already_booked", "slot": current_slot}

        current_slot = await database.delivery_slots.find_one(
            {"booked_tracking_id": tracking_id},
            {"_id": 0},
            session=session,
        )

        if current_slot is not None and current_slot["slot_id"] == slot_id:
            return {"status": "already_booked", "slot": current_slot}

        requested_slot = await database.delivery_slots.find_one(
            {"slot_id": slot_id, "available": True},
            {"_id": 0},
            session=session,
        )
        if requested_slot is None:
            raise SlotUnavailableError

        if current_slot is not None:
            await database.delivery_slots.update_one(
                {
                    "slot_id": current_slot["slot_id"],
                    "booked_tracking_id": tracking_id,
                },
                {
                    "$set": {"available": True},
                    "$unset": {
                        "booked_tracking_id": "",
                        "booking_idempotency_key": "",
                        "booked_at": "",
                    },
                },
                session=session,
            )

        slot = await database.delivery_slots.find_one_and_update(
            {
                "slot_id": slot_id,
                "available": True,
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
            session=session,
        )
        if slot is None:
            raise SlotUnavailableError

        await database.bookings.update_many(
            {
                "tracking_id": tracking_id,
                "status": "confirmed",
            },
            {
                "$set": {
                    "status": "superseded",
                    "superseded_at": booked_at,
                }
            },
            session=session,
        )
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
            session=session,
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
            session=session,
        )

        status = "rescheduled" if current_slot is not None else "booked"
        return {"status": status, "slot": slot}

    try:
        async with database.client.start_session() as session:
            return await session.with_transaction(save_booking)
    except SlotUnavailableError:
        return {"status": "unavailable"}
    except DuplicateKeyError:
        existing_slot = await database.delivery_slots.find_one(
            {"booked_tracking_id": tracking_id},
            {"_id": 0},
        )
        if existing_slot is not None and existing_slot["slot_id"] == slot_id:
            return {"status": "already_booked", "slot": existing_slot}
        return {"status": "unavailable"}
