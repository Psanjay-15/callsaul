import asyncio
import json
from contextlib import suppress

from deepgram.core.events import EventType
from fastapi import WebSocket, WebSocketDisconnect

from app.config.settings import settings
from app.services.llm import LLMService
from app.stt.deepgram_client import create_deepgram_client


class TranscriptionService:
    def __init__(self, websocket: WebSocket) -> None:
        self.websocket = websocket
        self.send_lock = asyncio.Lock()
        self.llm = LLMService()
        self.connection_context = None
        self.connection = None
        self.listener_task: asyncio.Task | None = None
        self.audio_active = False
        self.audio_chunks = 0
        self.audio_bytes = 0

    async def run(self) -> None:
        await self.send(
            {
                "type": "connected",
                "message": "WebSocket connection established.",
            }
        )

        try:
            while True:
                event = await self.websocket.receive()
                if event.get("type") == "websocket.disconnect":
                    break

                audio = event.get("bytes")
                if audio is not None:
                    await self.forward_audio(audio)
                    continue

                text = event.get("text")
                if text is not None and await self.handle_control(text):
                    break
        except WebSocketDisconnect:
            pass
        finally:
            await self.close_deepgram_stream()

    async def handle_control(self, raw_message: str) -> bool:
        try:
            message = json.loads(raw_message)
        except json.JSONDecodeError:
            await self.send(
                {"type": "error", "message": "WebSocket message must be valid JSON."}
            )
            return False

        message_type = message.get("type")

        if message_type == "audio_start":
            await self.start_deepgram_stream(message.get("mime_type", "unknown"))
        elif message_type == "audio_stop":
            await self.stop_audio()
        elif message_type == "ping":
            await self.send({"type": "pong"})
        elif message_type == "message":
            await self.send_to_llm(message.get("text"))
        elif message_type == "stop":
            await self.websocket.close(code=1000)
            return True
        else:
            await self.send(
                {"type": "error", "message": "Unsupported WebSocket message type."}
            )

        return False

    async def start_deepgram_stream(self, mime_type: str) -> None:
        await self.close_deepgram_stream()
        self.audio_active = True
        self.audio_chunks = 0
        self.audio_bytes = 0

        try:
            client = create_deepgram_client()
            self.connection_context = client.listen.v2.connect(
                model=settings.deepgram_stt_model,
                numerals="true",
            )
            self.connection = await self.connection_context.__aenter__()
            self.connection.on(EventType.MESSAGE, self.handle_deepgram_message)
            self.connection.on(EventType.ERROR, self.handle_deepgram_error)
            self.listener_task = asyncio.create_task(
                self.connection.start_listening()
            )

            await self.send({"type": "stt_status", "status": "connected"})
            await self.send({"type": "audio_started", "mime_type": mime_type})
        except Exception as error:
            await self.send(
                {
                    "type": "stt_error",
                    "message": f"Could not connect to Deepgram: {error}",
                }
            )
            await self.close_deepgram_stream()

    async def forward_audio(self, audio: bytes) -> None:
        if not self.audio_active:
            await self.send(
                {
                    "type": "error",
                    "message": "Start the microphone before sending audio.",
                }
            )
            return

        self.audio_chunks += 1
        self.audio_bytes += len(audio)

        if self.connection is not None:
            try:
                await self.connection.send_media(audio)
            except Exception as error:
                await self.send(
                    {
                        "type": "stt_error",
                        "message": f"Could not send audio to Deepgram: {error}",
                    }
                )
                await self.close_deepgram_stream()

        if self.audio_chunks == 1 or self.audio_chunks % 10 == 0:
            await self.send(
                {
                    "type": "audio_received",
                    "chunks": self.audio_chunks,
                    "bytes": self.audio_bytes,
                }
            )

    async def handle_deepgram_message(self, message) -> None:
        if getattr(message, "type", None) != "TurnInfo":
            return

        turn_event = str(message.event)
        await self.send(
            {
                "type": "transcript",
                "event": turn_event,
                "text": message.transcript,
                "is_final": turn_event == "EndOfTurn",
                "turn_index": message.turn_index,
            }
        )

        if turn_event == "EndOfTurn" and message.transcript.strip():
            await self.send_to_llm(message.transcript)

    async def handle_deepgram_error(self, error) -> None:
        await self.send(
            {
                "type": "stt_error",
                "message": f"Deepgram transcription failed: {error}",
            }
        )

    async def stop_audio(self) -> None:
        self.audio_active = False
        await self.close_deepgram_stream()
        await self.send(
            {
                "type": "audio_stopped",
                "chunks": self.audio_chunks,
                "bytes": self.audio_bytes,
            }
        )

    async def close_deepgram_stream(self) -> None:
        connection = self.connection
        listener_task = self.listener_task
        connection_context = self.connection_context

        self.connection = None
        self.listener_task = None
        self.connection_context = None

        if connection is not None:
            with suppress(Exception):
                await connection.send_close_stream()

        if listener_task is not None:
            try:
                await asyncio.wait_for(listener_task, timeout=2)
            except TimeoutError:
                listener_task.cancel()
                with suppress(asyncio.CancelledError):
                    await listener_task

        if connection_context is not None:
            with suppress(Exception):
                await connection_context.__aexit__(None, None, None)

    async def send_to_llm(self, text) -> None:
        if not isinstance(text, str) or not text.strip():
            await self.send({"type": "error", "message": "Message cannot be empty."})
            return

        await self.send({"type": "llm_status", "status": "thinking"})
        try:
            response = await self.llm.get_response(text.strip())
            await self.send({"type": "llm_response", "message": response})
        except Exception as error:
            await self.send(
                {
                    "type": "llm_error",
                    "message": f"Could not get an OpenAI response: {error}",
                }
            )
        finally:
            await self.send({"type": "llm_status", "status": "idle"})

    async def send(self, payload: dict) -> None:
        async with self.send_lock:
            await self.websocket.send_json(payload)
