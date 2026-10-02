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

    model_config = SettingsConfigDict(
        env_file=PROJECT_ROOT / ".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


settings = Settings()
