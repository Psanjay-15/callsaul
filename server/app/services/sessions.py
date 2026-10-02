from datetime import datetime, timezone
from uuid import UUID, uuid4

from app.config.database import get_database
from app.conversation import ConversationState


DEFAULT_SESSION_TITLE = "New delivery chat"


async def create_chat_session() -> dict:
    database = get_database()
    now = datetime.now(timezone.utc)
    session = {
        "session_id": str(uuid4()),
        "title": DEFAULT_SESSION_TITLE,
        "state": ConversationState().to_dict(),
        "created_at": now,
        "updated_at": now,
        "last_message_at": now,
    }
    await database.chat_sessions.insert_one(session)
    return session


async def get_chat_session(session_id: str) -> dict | None:
    try:
        UUID(session_id)
    except (TypeError, ValueError, AttributeError):
        return None

    database = get_database()
    return await database.chat_sessions.find_one(
        {"session_id": session_id},
        {"_id": 0},
    )


async def save_conversation_state(
    session_id: str,
    state: ConversationState,
) -> None:
    database = get_database()
    await database.chat_sessions.update_one(
        {"session_id": session_id},
        {
            "$set": {
                "state": state.to_dict(),
                "updated_at": datetime.now(timezone.utc),
            }
        },
    )


async def append_chat_message(
    session_id: str,
    role: str,
    content: str,
) -> dict:
    database = get_database()
    now = datetime.now(timezone.utc)
    message = {
        "message_id": str(uuid4()),
        "session_id": session_id,
        "role": role,
        "content": content,
        "created_at": now,
    }
    await database.chat_messages.insert_one(message)
    await database.chat_sessions.update_one(
        {"session_id": session_id},
        {
            "$set": {
                "last_message_at": now,
                "updated_at": now,
            }
        },
    )
    if role == "user":
        title = " ".join(content.split())
        if len(title) > 48:
            title = f"{title[:45]}..."
        await database.chat_sessions.update_one(
            {
                "session_id": session_id,
                "$or": [
                    {"title": DEFAULT_SESSION_TITLE},
                    {"title": {"$exists": False}},
                ],
            },
            {"$set": {"title": title or DEFAULT_SESSION_TITLE}},
        )
    return message


async def list_chat_messages(session_id: str) -> list[dict]:
    database = get_database()
    cursor = database.chat_messages.find(
        {"session_id": session_id},
        {"_id": 0},
    ).sort("created_at", 1)
    return await cursor.to_list(length=None)


async def list_chat_sessions(limit: int = 50) -> list[dict]:
    database = get_database()
    cursor = (
        database.chat_sessions.find({}, {"_id": 0})
        .sort("last_message_at", -1)
        .limit(limit)
    )
    return await cursor.to_list(length=limit)


async def delete_chat_session(session_id: str) -> bool:
    session = await get_chat_session(session_id)
    if session is None:
        return False

    database = get_database()
    await database.chat_messages.delete_many({"session_id": session_id})
    result = await database.chat_sessions.delete_one({"session_id": session_id})
    return result.deleted_count == 1


def messages_for_llm(messages: list[dict]) -> list[dict]:
    return [
        {
            "role": message["role"],
            "content": message["content"],
        }
        for message in messages
    ]


def message_for_client(message: dict) -> dict:
    created_at = message.get("created_at")
    return {
        "id": message["message_id"],
        "role": message["role"],
        "text": message["content"],
        "created_at": created_at.isoformat() if created_at else None,
    }


def session_for_client(session: dict) -> dict:
    last_message_at = session.get("last_message_at")
    created_at = session.get("created_at")
    state = session.get("state") or {}
    return {
        "session_id": session["session_id"],
        "title": session.get("title") or DEFAULT_SESSION_TITLE,
        "stage": state.get("stage", "collecting_tracking_id"),
        "last_message_at": (
            last_message_at.isoformat() if last_message_at else None
        ),
        "created_at": created_at.isoformat() if created_at else None,
    }
