import asyncio
import json
from contextlib import suppress

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from app.providers.deepgram_stt import DeepgramFluxSTT


router = APIRouter()


@router.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket) -> None:
    await websocket.accept()
    send_lock = asyncio.Lock()
    audio_active = False
    audio_chunks = 0
    audio_bytes = 0
    stt: DeepgramFluxSTT | None = None

    async def send(payload: dict) -> None:
        async with send_lock:
            await websocket.send_json(payload)

    await send(
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
                    await send(
                        {
                            "type": "error",
                            "message": "Start the microphone before sending audio.",
                        }
                    )
                    continue

                audio_chunks += 1
                audio_bytes += len(audio)
                if stt is not None:
                    try:
                        await stt.send_audio(audio)
                    except Exception as error:
                        await send(
                            {
                                "type": "stt_error",
                                "message": f"Could not send audio to Deepgram: {error}",
                            }
                        )
                        await stt.close()
                        stt = None

                if audio_chunks == 1 or audio_chunks % 10 == 0:
                    await send(
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
                await send(
                    {"type": "error", "message": "WebSocket message must be valid JSON."}
                )
                continue

            message_type = message.get("type")

            if message_type == "ping":
                await send({"type": "pong"})
                continue

            if message_type == "message":
                text = message.get("text")
                if not isinstance(text, str) or not text.strip() or len(text.strip()) > 300:
                    await send(
                        {
                            "type": "error",
                            "message": "Messages must contain between 1 and 300 characters.",
                        }
                    )
                    continue

                await send(
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

                if stt is not None:
                    await stt.close()

                stt = DeepgramFluxSTT(send)
                try:
                    await stt.start()
                except Exception as error:
                    await send(
                        {
                            "type": "stt_error",
                            "message": f"Could not connect to Deepgram: {error}",
                        }
                    )
                    await stt.close()
                    stt = None

                await send(
                    {
                        "type": "audio_started",
                        "mime_type": message.get("mime_type", "unknown"),
                    }
                )
                continue

            if message_type == "audio_stop":
                audio_active = False
                if stt is not None:
                    await stt.close()
                    stt = None

                await send(
                    {
                        "type": "audio_stopped",
                        "chunks": audio_chunks,
                        "bytes": audio_bytes,
                    }
                )
                continue

            if message_type == "stop":
                if stt is not None:
                    await stt.close()
                    stt = None
                await websocket.close(code=1000)
                return

            await send(
                {"type": "error", "message": "Unsupported WebSocket message type."}
            )
    except WebSocketDisconnect:
        return
    finally:
        if stt is not None:
            with suppress(Exception):
                await stt.close()
