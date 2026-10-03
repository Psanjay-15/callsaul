from pathlib import Path

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


PROJECT_ROOT = Path(__file__).resolve().parents[3]


class Settings(BaseSettings):
    app_name: str = "CallSaul Voice Agent"
    app_env: str = "development"
    frontend_origin: str = "http://localhost:5173"
    mongodb_uri: str = Field(
        default="",
        validation_alias=AliasChoices("MONGODB_URI"),
    )
    mongodb_db_name: str = "callsaul"
    deepgram_api_key: str = ""
    deepgram_stt_model: str = "flux-general-en"
    deepgram_eot_threshold: float = 0.7
    deepgram_eot_timeout_ms: int = 2000
    voice_turn_commit_delay_ms: int = 250
    voice_short_turn_commit_delay_ms: int = 1000
    deepgram_tts_model: str = "flux-alexis-en"
    deepgram_tts_sample_rate: int = 24000
    fake_backend_mode: str = "cycle"
    fake_backend_slow_seconds: float = 6.0
    fake_backend_progress_seconds: float = 2.5
    conversation_idle_timeout_seconds: float = 60.0
    openai_api_key: str = ""
    openai_model: str = "gpt-4.1-mini-2025-04-14"

    model_config = SettingsConfigDict(
        env_file=PROJECT_ROOT / ".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


settings = Settings()
