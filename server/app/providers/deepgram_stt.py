import asyncio
from collections.abc import Awaitable, Callable
from contextlib import suppress

from deepgram import AsyncDeepgramClient
from deepgram.core.events import EventType

from app.config import settings


EventHandler = Callable[[dict], Awaitable[None]]


class DeepgramFluxSTT:
    def __init__(self, on_event: EventHandler) -> None:
        self._on_event = on_event
        self._connection_context = None
        self._connection = None
        self._listener_task: asyncio.Task | None = None

    async def start(self) -> None:
        if not settings.deepgram_api_key:
            raise RuntimeError("DEEPGRAM_API_KEY is not configured")

        client = AsyncDeepgramClient(api_key=settings.deepgram_api_key)
        self._connection_context = client.listen.v2.connect(
            model=settings.deepgram_stt_model,
            numerals="true",
        )
        self._connection = await self._connection_context.__aenter__()
        self._connection.on(EventType.MESSAGE, self._handle_message)
        self._connection.on(EventType.ERROR, self._handle_error)
        self._listener_task = asyncio.create_task(self._connection.start_listening())

        await self._on_event({"type": "stt_status", "status": "connected"})

    async def send_audio(self, audio: bytes) -> None:
        if self._connection is not None:
            await self._connection.send_media(audio)

    async def close(self) -> None:
        connection = self._connection
        listener_task = self._listener_task
        connection_context = self._connection_context

        self._connection = None
        self._listener_task = None
        self._connection_context = None

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

    async def _handle_message(self, message) -> None:
        if getattr(message, "type", None) != "TurnInfo":
            return

        event = str(message.event)
        await self._on_event(
            {
                "type": "transcript",
                "event": event,
                "text": message.transcript,
                "is_final": event == "EndOfTurn",
                "turn_index": message.turn_index,
            }
        )

    async def _handle_error(self, error) -> None:
        await self._on_event(
            {
                "type": "stt_error",
                "message": f"Deepgram transcription failed: {error}",
            }
        )

