import os
from dataclasses import dataclass

from dotenv import load_dotenv


load_dotenv()


def _required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


@dataclass(frozen=True)
class Settings:
    api_id: int
    api_hash: str
    session_name: str
    source_channel_ids: tuple[int, ...]
    destination_channel_id: int
    bot_token: str | None
    log_level: str

    @classmethod
    def from_env(cls) -> "Settings":
        api_id_raw = _required_env("API_ID")
        destination_raw = _required_env("DESTINATION_CHANNEL_ID")

        try:
            api_id = int(api_id_raw)
            destination_channel_id = int(destination_raw)
        except ValueError as exc:
            raise ValueError(
                "API_ID and DESTINATION_CHANNEL_ID must be integers."
            ) from exc

        source_channel_ids_raw = os.getenv("SOURCE_CHANNEL_IDS")
        source_channel_id_raw = os.getenv("SOURCE_CHANNEL_ID")
        if source_channel_ids_raw:
            try:
                source_channel_ids = tuple(
                    int(item.strip()) for item in source_channel_ids_raw.split(",") if item.strip()
                )
            except ValueError as exc:
                raise ValueError("SOURCE_CHANNEL_IDS must be a comma-separated list of integers.") from exc
            if not source_channel_ids:
                raise ValueError("SOURCE_CHANNEL_IDS is set but empty.")
        elif source_channel_id_raw:
            try:
                source_channel_ids = (int(source_channel_id_raw),)
            except ValueError as exc:
                raise ValueError("SOURCE_CHANNEL_ID must be an integer.") from exc
        else:
            raise ValueError(
                "Missing required environment variable: SOURCE_CHANNEL_ID or SOURCE_CHANNEL_IDS"
            )

        session_name = os.getenv("SESSION_NAME", "telegram_copier")
        bot_token = os.getenv("BOT_TOKEN")
        log_level = os.getenv("LOG_LEVEL", "INFO").upper()

        return cls(
            api_id=api_id,
            api_hash=_required_env("API_HASH"),
            session_name=session_name,
            source_channel_ids=source_channel_ids,
            destination_channel_id=destination_channel_id,
            bot_token=bot_token,
            log_level=log_level,
        )
