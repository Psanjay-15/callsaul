from deepgram import AsyncDeepgramClient

from app.config.settings import settings


def create_deepgram_client() -> AsyncDeepgramClient:
    if not settings.deepgram_api_key:
        raise RuntimeError("DEEPGRAM_API_KEY is not configured")

    return AsyncDeepgramClient(api_key=settings.deepgram_api_key)
