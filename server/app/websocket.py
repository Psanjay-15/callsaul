from fastapi import APIRouter, WebSocket

from app.services.transcription import TranscriptionService


router = APIRouter()


@router.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket) -> None:
    await websocket.accept()
    session_id = websocket.query_params.get("session_id")
    service = TranscriptionService(websocket, session_id=session_id)
    await service.run()
