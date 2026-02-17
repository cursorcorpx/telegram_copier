import pytest

try:
    from telegram_copier.config import Settings
except ModuleNotFoundError:
    from config import Settings


REQUIRED_ENV_KEYS = (
    "API_ID",
    "API_HASH",
    "SESSION_NAME",
    "SOURCE_CHANNEL_ID",
    "SOURCE_CHANNEL_IDS",
    "DESTINATION_CHANNEL_ID",
    "BOT_TOKEN",
    "LOG_LEVEL",
)


def clear_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in REQUIRED_ENV_KEYS:
        monkeypatch.delenv(key, raising=False)


def test_from_env_success(monkeypatch: pytest.MonkeyPatch) -> None:
    clear_env(monkeypatch)
    monkeypatch.setenv("API_ID", "123456")
    monkeypatch.setenv("API_HASH", "hash_value")
    monkeypatch.setenv("SESSION_NAME", "session_name")
    monkeypatch.setenv("SOURCE_CHANNEL_ID", "-1001000000001")
    monkeypatch.setenv("DESTINATION_CHANNEL_ID", "-1001000000002")
    monkeypatch.setenv("BOT_TOKEN", "123:abc")
    monkeypatch.setenv("LOG_LEVEL", "debug")

    settings = Settings.from_env()

    assert settings.api_id == 123456
    assert settings.api_hash == "hash_value"
    assert settings.session_name == "session_name"
    assert settings.source_channel_ids == (-1001000000001,)
    assert settings.destination_channel_id == -1001000000002
    assert settings.bot_token == "123:abc"
    assert settings.log_level == "DEBUG"


def test_from_env_missing_required(monkeypatch: pytest.MonkeyPatch) -> None:
    clear_env(monkeypatch)
    monkeypatch.setenv("API_HASH", "hash_value")
    monkeypatch.setenv("DESTINATION_CHANNEL_ID", "-1001000000002")

    with pytest.raises(ValueError, match="Missing required environment variable: API_ID"):
        Settings.from_env()


def test_from_env_invalid_integer(monkeypatch: pytest.MonkeyPatch) -> None:
    clear_env(monkeypatch)
    monkeypatch.setenv("API_ID", "invalid")
    monkeypatch.setenv("API_HASH", "hash_value")
    monkeypatch.setenv("SOURCE_CHANNEL_ID", "-1001000000001")
    monkeypatch.setenv("DESTINATION_CHANNEL_ID", "-1001000000002")

    with pytest.raises(ValueError, match="API_ID and DESTINATION_CHANNEL_ID must be integers."):
        Settings.from_env()


def test_from_env_multiple_source_channel_ids(monkeypatch: pytest.MonkeyPatch) -> None:
    clear_env(monkeypatch)
    monkeypatch.setenv("API_ID", "123456")
    monkeypatch.setenv("API_HASH", "hash_value")
    monkeypatch.setenv("SOURCE_CHANNEL_IDS", "-1001000000001, -1001000000002,-1001000000003")
    monkeypatch.setenv("DESTINATION_CHANNEL_ID", "-1002000000001")

    settings = Settings.from_env()

    assert settings.source_channel_ids == (-1001000000001, -1001000000002, -1001000000003)


def test_from_env_invalid_source_channel_ids(monkeypatch: pytest.MonkeyPatch) -> None:
    clear_env(monkeypatch)
    monkeypatch.setenv("API_ID", "123456")
    monkeypatch.setenv("API_HASH", "hash_value")
    monkeypatch.setenv("SOURCE_CHANNEL_IDS", "-1001000000001,not-an-int")
    monkeypatch.setenv("DESTINATION_CHANNEL_ID", "-1002000000001")

    with pytest.raises(ValueError, match="SOURCE_CHANNEL_IDS must be a comma-separated list of integers."):
        Settings.from_env()


def test_from_env_missing_source_channel_variables(monkeypatch: pytest.MonkeyPatch) -> None:
    clear_env(monkeypatch)
    monkeypatch.setenv("API_ID", "123456")
    monkeypatch.setenv("API_HASH", "hash_value")
    monkeypatch.setenv("DESTINATION_CHANNEL_ID", "-1002000000001")

    with pytest.raises(
        ValueError, match="Missing required environment variable: SOURCE_CHANNEL_ID or SOURCE_CHANNEL_IDS"
    ):
        Settings.from_env()
