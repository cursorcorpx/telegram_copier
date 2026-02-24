from __future__ import annotations

import importlib.util
import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest


MODULE_PATH = (
    Path(__file__).resolve().parents[1] / "appwrite" / "functions" / "webhook" / "src" / "main.py"
)


def load_module():
    spec = importlib.util.spec_from_file_location("appwrite_user_sync_main", MODULE_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class FakeRes:
    def __init__(self) -> None:
        self.payload = None
        self.status = None

    def json(self, payload, status=200):  # noqa: ANN001
        self.payload = payload
        self.status = status
        return payload


class FakeSentMessage:
    def __init__(self, message_id: int) -> None:
        self.id = message_id


def clear_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in (
        "API_ID",
        "API_HASH",
        "SESSION_STRING",
        "SOURCE_CHANNEL_ID",
        "SOURCE_CHANNEL_IDS",
        "DESTINATION_CHANNEL_ID",
        "LIMIT_PER_SOURCE",
    ):
        monkeypatch.delenv(key, raising=False)


def test_load_settings_requires_session_string(monkeypatch: pytest.MonkeyPatch) -> None:
    module = load_module()
    clear_env(monkeypatch)
    monkeypatch.setenv("API_ID", "123456")
    monkeypatch.setenv("API_HASH", "hash")
    monkeypatch.setenv("SOURCE_CHANNEL_ID", "-1001")
    monkeypatch.setenv("DESTINATION_CHANNEL_ID", "-1002")

    with pytest.raises(ValueError, match="SESSION_STRING is required"):
        module._load_settings_from_env()


def test_load_settings_with_multiple_sources(monkeypatch: pytest.MonkeyPatch) -> None:
    module = load_module()
    clear_env(monkeypatch)
    monkeypatch.setenv("API_ID", "123456")
    monkeypatch.setenv("API_HASH", "hash")
    monkeypatch.setenv("SESSION_STRING", "session")
    monkeypatch.setenv("SOURCE_CHANNEL_IDS", "-1001,-1002,-1003")
    monkeypatch.setenv("DESTINATION_CHANNEL_ID", "-2001")
    monkeypatch.setenv("LIMIT_PER_SOURCE", "25")

    settings = module._load_settings_from_env()

    assert settings.api_id == 123456
    assert settings.source_channel_ids == (-1001, -1002, -1003)
    assert settings.destination_channel_id == -2001
    assert settings.limit_per_source == 25


def test_group_message_ids_keeps_album_boundaries() -> None:
    module = load_module()
    messages = [
        SimpleNamespace(id=1, grouped_id=None),
        SimpleNamespace(id=2, grouped_id=10),
        SimpleNamespace(id=3, grouped_id=10),
        SimpleNamespace(id=4, grouped_id=None),
        SimpleNamespace(id=5, grouped_id=11),
    ]

    grouped = module._group_message_ids(messages)

    assert grouped == [(False, [1]), (True, [2, 3]), (False, [4]), (True, [5])]


def test_sanitize_message_text_removes_telegram_links() -> None:
    module = load_module()
    text = "visit https://t.me/some/path and https://t.me/+qKumi0QiYOsyMDY9 now"
    assert module._sanitize_message_text(text) == "visit and now"


def test_sanitize_message_text_removes_telegram_link_variants() -> None:
    module = load_module()
    text = (
        "one t.me/somechannel two telegram.me/joinchat/abc "
        "three www.t.me/+invitecode four https://www.telegram.me/c/123/456"
    )
    assert module._sanitize_message_text(text) == "one two three four"


def test_sanitize_message_text_removes_bad_words_and_youtube() -> None:
    module = load_module()
    text = "hello MC world bc see https://youtube.com/watch?v=1 and youtube.com/test"
    assert module._sanitize_message_text(text) == "hello world see and"


def test_should_skip_message_text_blocks_ad_markers(monkeypatch: pytest.MonkeyPatch) -> None:
    module = load_module()
    assert module._should_skip_message_text("#ad InsideAds (https://t.me/InsideAds_bot/open?startapp=abc)")
    assert module._should_skip_message_text("insideAds special")
    assert not module._should_skip_message_text("this is an ad")
    monkeypatch.setenv("BLOCK_GENERIC_AD_WORD", "1")
    assert module._should_skip_message_text("this is an ad")
    assert not module._should_skip_message_text("this is a normal update")


def test_is_gif_message_detects_flag() -> None:
    module = load_module()
    assert module._is_gif_message(SimpleNamespace(gif=True))
    assert not module._is_gif_message(SimpleNamespace(gif=False, media=None))


def test_is_gif_message_detects_mp4_animation_variant() -> None:
    module = load_module()

    class DocumentAttributeVideo:
        def __init__(self) -> None:
            self.supports_streaming = False

    media = SimpleNamespace(
        document=SimpleNamespace(
            mime_type="video/mp4",
            attributes=[DocumentAttributeVideo()],
        )
    )
    assert module._is_gif_message(SimpleNamespace(gif=False, media=media))


def test_main_returns_error_payload_on_invalid_env(monkeypatch: pytest.MonkeyPatch) -> None:
    module = load_module()
    clear_env(monkeypatch)
    res = FakeRes()
    context = SimpleNamespace(req=SimpleNamespace(), res=res)

    module.main(context)

    assert res.status == 500
    assert res.payload["ok"] is False


def test_fetch_recent_messages_respects_cutoff_not_last_seen() -> None:
    module = load_module()
    now = datetime.now(timezone.utc)

    class FakeClient:
        async def iter_messages(self, source_id):  # noqa: ANN001
            assert source_id == -1001
            yield SimpleNamespace(id=110, date=now - timedelta(minutes=5))
            yield SimpleNamespace(id=109, date=now - timedelta(minutes=50))
            yield SimpleNamespace(id=100, date=now - timedelta(minutes=1))
            yield SimpleNamespace(id=108, date=now - timedelta(minutes=70))

    result = asyncio.run(
        module._fetch_recent_messages(
            client=FakeClient(),
            source_id=-1001,
            last_seen_id=100,
            cutoff_dt=now - timedelta(minutes=60),
        )
    )

    assert [msg.id for msg in result] == [100, 109, 110]


def test_copy_source_messages_forwards_each_group(monkeypatch: pytest.MonkeyPatch) -> None:
    module = load_module()

    async def fake_load_state(client):  # noqa: ANN001
        return {"sources": {}}, 1

    async def fake_save_state(client, state, state_message_id):  # noqa: ANN001
        return 1, True

    async def fake_fetch_recent_messages(client, source_id, last_seen_id, cutoff_dt):  # noqa: ANN001
        return [SimpleNamespace(id=1, message="", grouped_id=10), SimpleNamespace(id=2, message="", grouped_id=11)]

    forward_mock = AsyncMock(return_value=(1, 0, (1,)))
    run_snapshot_mock = AsyncMock(return_value=[99])

    monkeypatch.setattr(module, "_load_state", fake_load_state)
    monkeypatch.setattr(module, "_save_state", fake_save_state)
    monkeypatch.setattr(module, "_fetch_recent_messages", fake_fetch_recent_messages)
    monkeypatch.setattr(module, "_load_recent_run_snapshot_ids", AsyncMock(return_value={}))
    monkeypatch.setattr(module, "_forward_with_fallback", forward_mock)
    monkeypatch.setattr(module, "_save_run_window_ids", run_snapshot_mock)

    settings = module.AppwriteSettings(
        api_id=1,
        api_hash="x",
        session_string="y",
        source_channel_ids=(-1001,),
        destination_channel_id=-2002,
        limit_per_source=50,
        lookback_minutes=60,
    )

    result = asyncio.run(module._copy_source_messages(client=SimpleNamespace(), settings=settings))

    assert forward_mock.await_count == 2
    assert result["copied"] == 2


def test_copy_source_messages_skips_ad_messages(monkeypatch: pytest.MonkeyPatch) -> None:
    module = load_module()

    async def fake_load_state(client):  # noqa: ANN001
        return {"sources": {}}, 1

    async def fake_save_state(client, state, state_message_id):  # noqa: ANN001
        return 1, True

    now = datetime.now(timezone.utc)

    async def fake_fetch_recent_messages(client, source_id, last_seen_id, cutoff_dt):  # noqa: ANN001
        return [
            SimpleNamespace(id=1, message="#ad InsideAds link", grouped_id=None, date=now),
            SimpleNamespace(id=2, message="normal message", grouped_id=None, date=now),
        ]

    forward_mock = AsyncMock(return_value=(1, 0, (2,)))
    run_snapshot_mock = AsyncMock(return_value=[99])

    monkeypatch.setattr(module, "_load_state", fake_load_state)
    monkeypatch.setattr(module, "_save_state", fake_save_state)
    monkeypatch.setattr(module, "_fetch_recent_messages", fake_fetch_recent_messages)
    monkeypatch.setattr(module, "_load_recent_run_snapshot_ids", AsyncMock(return_value={}))
    monkeypatch.setattr(module, "_forward_with_fallback", forward_mock)
    monkeypatch.setattr(module, "_save_run_window_ids", run_snapshot_mock)

    settings = module.AppwriteSettings(
        api_id=1,
        api_hash="x",
        session_string="y",
        source_channel_ids=(-1001,),
        destination_channel_id=-2002,
        limit_per_source=50,
        lookback_minutes=60,
    )

    result = asyncio.run(module._copy_source_messages(client=SimpleNamespace(), settings=settings))

    assert forward_mock.await_count == 1
    assert result["copied"] == 1
    assert result["skipped_invalid"] == 1
    assert result["skipped_blocked_ad"] == 1
    assert result["skipped_blocked_gif"] == 0


def test_copy_source_messages_skips_gif_messages(monkeypatch: pytest.MonkeyPatch) -> None:
    module = load_module()

    async def fake_load_state(client):  # noqa: ANN001
        return {"sources": {}}, 1

    async def fake_save_state(client, state, state_message_id):  # noqa: ANN001
        return 1, True

    now = datetime.now(timezone.utc)

    async def fake_fetch_recent_messages(client, source_id, last_seen_id, cutoff_dt):  # noqa: ANN001
        return [
            SimpleNamespace(id=1, message="gif here", grouped_id=None, date=now, gif=True),
            SimpleNamespace(id=2, message="normal message", grouped_id=None, date=now, gif=False),
        ]

    forward_mock = AsyncMock(return_value=(1, 0, (2,)))
    run_snapshot_mock = AsyncMock(return_value=[99])

    monkeypatch.setattr(module, "_load_state", fake_load_state)
    monkeypatch.setattr(module, "_save_state", fake_save_state)
    monkeypatch.setattr(module, "_fetch_recent_messages", fake_fetch_recent_messages)
    monkeypatch.setattr(module, "_load_recent_run_snapshot_ids", AsyncMock(return_value={}))
    monkeypatch.setattr(module, "_forward_with_fallback", forward_mock)
    monkeypatch.setattr(module, "_save_run_window_ids", run_snapshot_mock)

    settings = module.AppwriteSettings(
        api_id=1,
        api_hash="x",
        session_string="y",
        source_channel_ids=(-1001,),
        destination_channel_id=-2002,
        limit_per_source=50,
        lookback_minutes=60,
    )

    result = asyncio.run(module._copy_source_messages(client=SimpleNamespace(), settings=settings))

    assert forward_mock.await_count == 1
    assert result["copied"] == 1
    assert result["skipped_invalid"] == 1
    assert result["skipped_blocked_gif"] == 1


def test_build_state_payload_compacts_when_too_large() -> None:
    module = load_module()
    huge_errors = {f"-100{i}": "x" * 1200 for i in range(20)}
    state = {
        "sources": {
            "-1001": {"last_id": 123, "recent": list(range(1, 100))},
            "-1002": {"last_id": 456, "recent": list(range(1, 120))},
        },
        "meta": {
            "status": "completed_with_errors",
            "last_run_ts_ms": 1234567890,
            "source_errors": huge_errors,
        },
    }

    payload = module._build_state_payload(state)

    assert payload.startswith(module.STATE_MARKER)
    assert len(payload) <= module.MAX_STATE_PAYLOAD_CHARS
    parsed = module._extract_state(payload)
    assert parsed["sources"]["-1001"]["last_id"] == 123
    assert len(parsed["sources"]["-1001"]["recent"]) <= module.MAX_RECENT_IDS


def test_save_run_window_ids_creates_messages_and_chunks() -> None:
    module = load_module()

    class FakeClient:
        def __init__(self) -> None:
            self.messages: list[str] = []

        async def send_message(self, entity, message):  # noqa: ANN001
            assert entity == "me"
            self.messages.append(message)
            return FakeSentMessage(len(self.messages))

    client = FakeClient()
    ids_by_source = {"-1001": list(range(1, 3000))}

    sent_ids = asyncio.run(
        module._save_run_window_ids(
            client=client,
            run_ts_ms=123,
            lookback_minutes=60,
            run_status="completed",
            ids_by_source=ids_by_source,
        )
    )

    assert len(sent_ids) >= 2
    assert len(client.messages) == len(sent_ids)
    assert all(message.startswith(module.RUN_MARKER) for message in client.messages)
    assert all(len(message) <= module.MAX_RUN_MESSAGE_CHARS for message in client.messages)


def test_parse_run_snapshot_message() -> None:
    module = load_module()
    text = (
        "telegram_copier_run_v1:"
        "run_ts_ms=123;status=completed;lookback_minutes=60;source_id=-1001;part=1/1;ids=11,12,13"
    )
    parsed = module._parse_run_snapshot_message(text)
    assert parsed == (123, "-1001", [11, 12, 13])


def test_load_recent_run_snapshot_ids_filters_by_time() -> None:
    module = load_module()
    now_ms = 1_700_000_000_000

    class FakeClient:
        async def iter_messages(self, entity, search=None, limit=None):  # noqa: ANN001
            assert entity == "me"
            assert search == module.RUN_MARKER
            yield SimpleNamespace(
                message=(
                    f"{module.RUN_MARKER}"
                    f"run_ts_ms={now_ms};status=completed;lookback_minutes=60;"
                    "source_id=-1001;part=1/1;ids=1,2"
                )
            )
            yield SimpleNamespace(
                message=(
                    f"{module.RUN_MARKER}"
                    f"run_ts_ms={now_ms - (70 * 60 * 1000)};status=completed;lookback_minutes=60;"
                    "source_id=-1001;part=1/1;ids=3,4"
                )
            )

    original_time = module.time.time
    module.time.time = lambda: now_ms / 1000
    try:
        result = asyncio.run(module._load_recent_run_snapshot_ids(client=FakeClient(), lookback_minutes=60))
    finally:
        module.time.time = original_time

    assert result == {"-1001": {1, 2}}


def test_copy_source_messages_includes_run_snapshot(monkeypatch: pytest.MonkeyPatch) -> None:
    module = load_module()

    async def fake_load_state(client):  # noqa: ANN001
        return {"sources": {}}, 1

    async def fake_save_state(client, state, state_message_id):  # noqa: ANN001
        return 1, True

    now = datetime.now(timezone.utc)

    async def fake_fetch_recent_messages(client, source_id, last_seen_id, cutoff_dt):  # noqa: ANN001
        return [SimpleNamespace(id=101, message="hello", grouped_id=None, date=now)]

    forward_mock = AsyncMock(return_value=(1, 0, (101,)))
    run_snapshot_mock = AsyncMock(return_value=[11, 12])

    monkeypatch.setattr(module, "_load_state", fake_load_state)
    monkeypatch.setattr(module, "_save_state", fake_save_state)
    monkeypatch.setattr(module, "_fetch_recent_messages", fake_fetch_recent_messages)
    monkeypatch.setattr(module, "_load_recent_run_snapshot_ids", AsyncMock(return_value={}))
    monkeypatch.setattr(module, "_forward_with_fallback", forward_mock)
    monkeypatch.setattr(module, "_save_run_window_ids", run_snapshot_mock)

    settings = module.AppwriteSettings(
        api_id=1,
        api_hash="x",
        session_string="y",
        source_channel_ids=(-1001,),
        destination_channel_id=-2002,
        limit_per_source=50,
        lookback_minutes=60,
    )

    result = asyncio.run(module._copy_source_messages(client=SimpleNamespace(), settings=settings))

    assert run_snapshot_mock.await_count == 1
    assert result["run_snapshot_count"] == 2
    assert result["run_snapshot_message_ids"] == [11, 12]


def test_copy_source_messages_merges_snapshot_ids_with_live_fetch(monkeypatch: pytest.MonkeyPatch) -> None:
    module = load_module()

    async def fake_load_state(client):  # noqa: ANN001
        return {"sources": {}}, 1

    async def fake_save_state(client, state, state_message_id):  # noqa: ANN001
        return 1, True

    now = datetime.now(timezone.utc)

    async def fake_fetch_recent_messages(client, source_id, last_seen_id, cutoff_dt):  # noqa: ANN001
        return [SimpleNamespace(id=101, message="live", grouped_id=None, date=now)]

    async def fake_load_recent_run_snapshot_ids(client, lookback_minutes):  # noqa: ANN001
        return {"-1001": {101, 102}}

    async def fake_get_messages(source_id, ids):  # noqa: ANN001
        assert source_id == -1001
        assert ids == [102]
        return [SimpleNamespace(id=102, message="recovered", grouped_id=None, date=now)]

    forward_mock = AsyncMock(side_effect=[(1, 0, (101,)), (1, 0, (102,))])
    run_snapshot_mock = AsyncMock(return_value=[9])

    client = SimpleNamespace(get_messages=fake_get_messages)

    monkeypatch.setattr(module, "_load_state", fake_load_state)
    monkeypatch.setattr(module, "_save_state", fake_save_state)
    monkeypatch.setattr(module, "_fetch_recent_messages", fake_fetch_recent_messages)
    monkeypatch.setattr(module, "_load_recent_run_snapshot_ids", fake_load_recent_run_snapshot_ids)
    monkeypatch.setattr(module, "_forward_with_fallback", forward_mock)
    monkeypatch.setattr(module, "_save_run_window_ids", run_snapshot_mock)

    settings = module.AppwriteSettings(
        api_id=1,
        api_hash="x",
        session_string="y",
        source_channel_ids=(-1001,),
        destination_channel_id=-2002,
        limit_per_source=50,
        lookback_minutes=60,
    )

    result = asyncio.run(module._copy_source_messages(client=client, settings=settings))

    assert forward_mock.await_count == 2
    assert result["copied"] == 2


def test_copy_source_messages_skips_ids_at_or_below_last_seen(monkeypatch: pytest.MonkeyPatch) -> None:
    module = load_module()
    monkeypatch.setenv("ENABLE_CURSOR_GATE", "1")

    async def fake_load_state(client):  # noqa: ANN001
        return {"sources": {"-1001": {"last_id": 200, "recent": []}}}, 1

    async def fake_save_state(client, state, state_message_id):  # noqa: ANN001
        return 1, True

    now = datetime.now(timezone.utc)

    async def fake_fetch_recent_messages(client, source_id, last_seen_id, cutoff_dt):  # noqa: ANN001
        return [
            SimpleNamespace(id=190, message="old", grouped_id=None, date=now),
            SimpleNamespace(id=200, message="edge", grouped_id=None, date=now),
            SimpleNamespace(id=201, message="new", grouped_id=None, date=now),
        ]

    forward_mock = AsyncMock(return_value=(1, 0, (201,)))
    run_snapshot_mock = AsyncMock(return_value=[9])

    monkeypatch.setattr(module, "_load_state", fake_load_state)
    monkeypatch.setattr(module, "_save_state", fake_save_state)
    monkeypatch.setattr(module, "_fetch_recent_messages", fake_fetch_recent_messages)
    monkeypatch.setattr(module, "_load_recent_run_snapshot_ids", AsyncMock(return_value={}))
    monkeypatch.setattr(module, "_forward_with_fallback", forward_mock)
    monkeypatch.setattr(module, "_save_run_window_ids", run_snapshot_mock)

    settings = module.AppwriteSettings(
        api_id=1,
        api_hash="x",
        session_string="y",
        source_channel_ids=(-1001,),
        destination_channel_id=-2002,
        limit_per_source=50,
        lookback_minutes=60,
    )

    result = asyncio.run(module._copy_source_messages(client=SimpleNamespace(), settings=settings))

    assert forward_mock.await_count == 1
    assert result["copied"] == 1


def test_copy_source_messages_allows_recovered_id_below_last_seen(monkeypatch: pytest.MonkeyPatch) -> None:
    module = load_module()

    async def fake_load_state(client):  # noqa: ANN001
        return {"sources": {"-1001": {"last_id": 200, "recent": []}}}, 1

    async def fake_save_state(client, state, state_message_id):  # noqa: ANN001
        return 1, True

    now = datetime.now(timezone.utc)

    async def fake_fetch_recent_messages(client, source_id, last_seen_id, cutoff_dt):  # noqa: ANN001
        return [SimpleNamespace(id=201, message="new", grouped_id=None, date=now)]

    async def fake_load_recent_run_snapshot_ids(client, lookback_minutes):  # noqa: ANN001
        return {"-1001": {199}}

    async def fake_get_messages(source_id, ids):  # noqa: ANN001
        assert ids == [199]
        return [SimpleNamespace(id=199, message="recovered", grouped_id=None, date=now)]

    forward_mock = AsyncMock(side_effect=[(1, 0, (199,)), (1, 0, (201,))])
    run_snapshot_mock = AsyncMock(return_value=[9])

    client = SimpleNamespace(get_messages=fake_get_messages)

    monkeypatch.setattr(module, "_load_state", fake_load_state)
    monkeypatch.setattr(module, "_save_state", fake_save_state)
    monkeypatch.setattr(module, "_fetch_recent_messages", fake_fetch_recent_messages)
    monkeypatch.setattr(module, "_load_recent_run_snapshot_ids", fake_load_recent_run_snapshot_ids)
    monkeypatch.setattr(module, "_forward_with_fallback", forward_mock)
    monkeypatch.setattr(module, "_save_run_window_ids", run_snapshot_mock)

    settings = module.AppwriteSettings(
        api_id=1,
        api_hash="x",
        session_string="y",
        source_channel_ids=(-1001,),
        destination_channel_id=-2002,
        limit_per_source=50,
        lookback_minutes=60,
    )

    result = asyncio.run(module._copy_source_messages(client=client, settings=settings))

    assert forward_mock.await_count == 2
    assert result["copied"] == 2


def test_copy_source_messages_self_heals_when_cursor_ahead(monkeypatch: pytest.MonkeyPatch) -> None:
    module = load_module()
    monkeypatch.setenv("ENABLE_CURSOR_GATE", "1")

    async def fake_load_state(client):  # noqa: ANN001
        return {"sources": {"-1001": {"last_id": 500, "recent": []}}}, 1

    async def fake_save_state(client, state, state_message_id):  # noqa: ANN001
        return 1, True

    now = datetime.now(timezone.utc)

    async def fake_fetch_recent_messages(client, source_id, last_seen_id, cutoff_dt):  # noqa: ANN001
        return [
            SimpleNamespace(id=101, message="a", grouped_id=None, date=now),
            SimpleNamespace(id=102, message="b", grouped_id=None, date=now),
        ]

    forward_mock = AsyncMock(side_effect=[(1, 0, (101,)), (1, 0, (102,))])
    run_snapshot_mock = AsyncMock(return_value=[9])

    monkeypatch.setattr(module, "_load_state", fake_load_state)
    monkeypatch.setattr(module, "_save_state", fake_save_state)
    monkeypatch.setattr(module, "_fetch_recent_messages", fake_fetch_recent_messages)
    monkeypatch.setattr(module, "_load_recent_run_snapshot_ids", AsyncMock(return_value={}))
    monkeypatch.setattr(module, "_forward_with_fallback", forward_mock)
    monkeypatch.setattr(module, "_save_run_window_ids", run_snapshot_mock)

    settings = module.AppwriteSettings(
        api_id=1,
        api_hash="x",
        session_string="y",
        source_channel_ids=(-1001,),
        destination_channel_id=-2002,
        limit_per_source=50,
        lookback_minutes=60,
    )

    result = asyncio.run(module._copy_source_messages(client=SimpleNamespace(), settings=settings))

    assert result["fetched"] == 2
    assert result["copied"] == 2
    assert forward_mock.await_count == 2
