import os
import json
from pydantic_settings import BaseSettings, SettingsConfigDict


class Config(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=os.path.join(os.path.dirname(__file__), '.env'),
        extra="ignore"
    )

    BOT_TOKEN: str = ""
    ADMIN_CHAT_ID: int = 0
    MODERATION_CHANNEL_ID: int = 0

    SOURCE_TARGET_MAP_RAW: str = "{}"
    PUBLISH_INTERVALS_RAW: str = "{}"
    MEDIA_AGGREGATION_TIMEOUT: int = 10

    DATABASE_URL: str = ""
    REDIS_URL: str = "redis://localhost:6379/0"

    OPENROUTER_API_KEY: str = ""
    MODEL_NAME: str = "deepseek/deepseek-chat"
    MODEL_TEMPERATURE: float = 0.7
    MODEL_MAX_TOKENS: int = 1024
    LLM_MAX_RETRIES: int = 3
    MAX_RETRIES: int = 10

    TELETHON_API_ID: int = 0
    TELETHON_API_HASH: str = ""
    TELETHON_PHONE: str = ""

    @property
    def SOURCE_TARGET_MAP(self) -> dict[int, list[int]]:
        raw = json.loads(self.SOURCE_TARGET_MAP_RAW)
        return {int(k): v for k, v in raw.items()}

    @property
    def PUBLISH_INTERVALS(self) -> dict[int, int]:
        raw = json.loads(self.PUBLISH_INTERVALS_RAW)
        return {int(k): int(v) for k, v in raw.items()}


config = Config()
