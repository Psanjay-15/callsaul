from collections.abc import AsyncIterator

from app.clients.openai import create_openai_client
from app.config.settings import settings


class LLMService:
    def __init__(self) -> None:
        self.client = create_openai_client()

    async def stream_response(self, transcription: str) -> AsyncIterator[str]:
        stream = await self.client.responses.create(
            model=settings.openai_model,
            instructions=(
                "You are a helpful voice assistant. "
                "Answer clearly and keep the response brief."
            ),
            input=transcription,
            stream=True,
        )

        async for event in stream:
            if event.type == "response.output_text.delta":
                yield event.delta
