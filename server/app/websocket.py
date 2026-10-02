from fastapi import APIRouter, WebSocket

from app.services.transcription import TranscriptionService


router = APIRouter()


@router.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket) -> None:
    await websocket.accept()
    service = TranscriptionService(websocket)
    await service.run()
