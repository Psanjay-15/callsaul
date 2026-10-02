import asyncio
from collections.abc import Awaitable, Callable
from contextlib import suppress

from deepgram.core.events import EventType
from deepgram.speak.v2.types import SpeakV2Interrupt, SpeakV2Speak

from app.clients.deepgram import create_deepgram_client
from app.config.settings import settings


AudioHandler = Callable[[bytes], Awaitable[None]]
EventHandler = Callable[[dict], Awaitable[None]]


class TTSService:
    def __init__(self, on_audio: AudioHandler, on_event: EventHandler) -> None:
        self.on_audio = on_audio
        self.on_event = on_event
        self.connection_context = None
        self.connection = None
        self.listener_task: asyncio.Task | None = None
        self.active_turn = False
        self.discard_audio = False

    async def connect(self) -> None:
        if self.connection is not None:
            return

        client = create_deepgram_client()
        self.connection_context = client.speak.v2.connect(
            model=settings.deepgram_tts_model,
            encoding="linear16",
            sample_rate=settings.deepgram_tts_sample_rate,
        )
        self.connection = await self.connection_context.__aenter__()
        self.connection.on(EventType.MESSAGE, self.handle_message)
        self.connection.on(EventType.ERROR, self.handle_error)
        self.listener_task = asyncio.create_task(self.connection.start_listening())
        await self.on_event({"type": "tts_status", "status": "connected"})

    async def send_text(self, text: str) -> None:
        if self.connection is not None and text:
            self.active_turn = True
            self.discard_audio = False
            await self.connection.send_speak(SpeakV2Speak(text=text))

    async def flush(self) -> None:
        if self.connection is not None:
            await self.connection.send_flush()

    async def interrupt(self) -> bool:
        if self.connection is None or not self.active_turn:
            return False

        self.active_turn = False
        self.discard_audio = True
        await self.connection.send_interrupt(SpeakV2Interrupt())
        await self.on_event({"type": "tts_status", "status": "interrupted"})
        return True

    async def close(self) -> None:
        connection = self.connection
        listener_task = self.listener_task
        connection_context = self.connection_context

        self.connection = None
        self.listener_task = None
        self.connection_context = None
        self.active_turn = False
        self.discard_audio = False

        if connection is not None:
            with suppress(Exception):
                await connection.send_close()

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

    async def handle_message(self, message) -> None:
        if isinstance(message, bytes):
            if not self.discard_audio:
                await self.on_audio(message)
            return

        message_type = str(getattr(message, "type", ""))
        if message_type == "SpeechStarted":
            self.active_turn = True
            await self.on_event({"type": "tts_status", "status": "speaking"})
        elif message_type == "SpeechMetadata":
            self.active_turn = False
            self.discard_audio = False
            await self.on_event({"type": "tts_status", "status": "connected"})
        elif message_type == "SpeechInterrupted":
            self.active_turn = False
            self.discard_audio = False
            await self.on_event({"type": "tts_status", "status": "interrupted"})

    async def handle_error(self, error) -> None:
        await self.on_event(
            {
                "type": "tts_error",
                "message": f"Deepgram speech generation failed: {error}",
            }
        )
