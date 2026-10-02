from app.config.settings import settings
from app.llm.openai import create_openai_client


class LLMService:
    def __init__(self) -> None:
        self.client = create_openai_client()

    async def get_response(self, transcription: str) -> str:
        response = await self.client.responses.create(
            model=settings.openai_model,
            instructions=(
                "You are a helpful voice assistant. "
                "Answer clearly and keep the response brief."
            ),
            input=transcription,
        )

        return response.output_text.strip()
