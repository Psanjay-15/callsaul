from fastapi import APIRouter, HTTPException

from app.services.sessions import (
    delete_chat_session,
    get_chat_session,
    list_chat_messages,
    list_chat_sessions,
    message_for_client,
    session_for_client,
)


router = APIRouter(prefix="/api/history", tags=["history"])


@router.get("")
async def get_history() -> dict:
    sessions = await list_chat_sessions()
    return {
        "sessions": [session_for_client(session) for session in sessions]
    }


@router.get("/{session_id}")
async def get_session_history(session_id: str) -> dict:
    session = await get_chat_session(session_id)
    if session is None:
        raise HTTPException(status_code=404, detail="Chat session not found")

    messages = await list_chat_messages(session_id)
    return {
        "session": session_for_client(session),
        "state": session.get("state") or {},
        "messages": [message_for_client(message) for message in messages],
    }


@router.delete("/{session_id}")
async def delete_session_history(session_id: str) -> dict[str, bool]:
    deleted = await delete_chat_session(session_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Chat session not found")
    return {"deleted": True}
