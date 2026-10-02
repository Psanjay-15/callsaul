from openai import AsyncOpenAI

from app.config.settings import settings


def create_openai_client() -> AsyncOpenAI:
    if not settings.openai_api_key:
        raise RuntimeError("OPENAI_API_KEY is not configured")

    return AsyncOpenAI(api_key=settings.openai_api_key)
