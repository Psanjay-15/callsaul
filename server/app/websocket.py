import json

from fastapi import APIRouter, WebSocket, WebSocketDisconnect


router = APIRouter()


@router.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket) -> None:
    await websocket.accept()
    audio_active = False
    audio_chunks = 0
    audio_bytes = 0

    await websocket.send_json(
        {
            "type": "connected",
            "message": "WebSocket connection established.",
        }
    )

    try:
        while True:
            event = await websocket.receive()
            if event.get("type") == "websocket.disconnect":
                return

            audio = event.get("bytes")
            if audio is not None:
                if not audio_active:
                    await websocket.send_json(
                        {
                            "type": "error",
                            "message": "Start the microphone before sending audio.",
                        }
                    )
                    continue

                audio_chunks += 1
                audio_bytes += len(audio)
                if audio_chunks == 1 or audio_chunks % 10 == 0:
                    await websocket.send_json(
                        {
                            "type": "audio_received",
                            "chunks": audio_chunks,
                            "bytes": audio_bytes,
                        }
                    )
                continue

            raw_message = event.get("text")
            if raw_message is None:
                continue

            try:
                message = json.loads(raw_message)
            except json.JSONDecodeError:
                await websocket.send_json(
                    {"type": "error", "message": "WebSocket message must be valid JSON."}
                )
                continue

            message_type = message.get("type")

            if message_type == "ping":
                await websocket.send_json({"type": "pong"})
                continue

            if message_type == "message":
                text = message.get("text")
                if not isinstance(text, str) or not text.strip() or len(text.strip()) > 300:
                    await websocket.send_json(
                        {
                            "type": "error",
                            "message": "Messages must contain between 1 and 300 characters.",
                        }
                    )
                    continue

                await websocket.send_json(
                    {
                        "type": "message",
                        "message": f"Server received: {text.strip()}",
                    }
                )
                continue

            if message_type == "audio_start":
                audio_active = True
                audio_chunks = 0
                audio_bytes = 0
                await websocket.send_json(
                    {
                        "type": "audio_started",
                        "mime_type": message.get("mime_type", "unknown"),
                    }
                )
                continue

            if message_type == "audio_stop":
                audio_active = False
                await websocket.send_json(
                    {
                        "type": "audio_stopped",
                        "chunks": audio_chunks,
                        "bytes": audio_bytes,
                    }
                )
                continue

            if message_type == "stop":
                await websocket.close(code=1000)
                return

            await websocket.send_json(
                {"type": "error", "message": "Unsupported WebSocket message type."}
            )
    except WebSocketDisconnect:
        return
