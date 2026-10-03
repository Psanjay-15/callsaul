from deepgram import AsyncDeepgramClient
from deepgram.listen.v2 import client as listen_v2_client
from deepgram.speak.v2 import client as speak_v2_client
from websockets.asyncio.client import connect as websocket_connect

from app.config.settings import settings


def _connect_websocket(url: str, extra_headers=None):
    """Use websockets' current asyncio transport for Deepgram streams.

    Deepgram 7.7 imports the legacy transport by default. That transport can
    fail its keepalive task while a stream is closing on Python 3.13.
    """
    return websocket_connect(
        url,
        additional_headers=extra_headers,
        proxy=None,
    )


# Deepgram's generated v2 clients keep their transport in module-level names.
# Replace only the two transports used by this application.
listen_v2_client.websockets_client_connect = _connect_websocket
speak_v2_client.websockets_client_connect = _connect_websocket


def create_deepgram_client() -> AsyncDeepgramClient:
    if not settings.deepgram_api_key:
        raise RuntimeError("DEEPGRAM_API_KEY is not configured")

    return AsyncDeepgramClient(api_key=settings.deepgram_api_key)
