import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from telethon.errors import FloodWaitError

try:
    from telegram_copier import main
except ModuleNotFoundError:
    import main


def test_copy_single_message_calls_telethon_copy() -> None:
    client = AsyncMock()

    asyncio.run(
        main.copy_single_message(
            client=client,
            source_channel_id=-1001,
            destination_channel_id=-2002,
            message_id=42,
        )
    )

    client.forward_messages.assert_awaited_once_with(
        entity=-2002,
        messages=42,
        from_peer=-1001,
        drop_author=True,
        drop_media_captions=False,
    )


def test_copy_media_group_calls_telethon_copy_with_list() -> None:
    client = AsyncMock()
    messages = [SimpleNamespace(id=1), SimpleNamespace(id=2), SimpleNamespace(id=3)]

    asyncio.run(
        main.copy_media_group(
            client=client,
            source_channel_id=-1001,
            destination_channel_id=-2002,
            messages=messages,
        )
    )

    client.forward_messages.assert_awaited_once_with(
        entity=-2002,
        messages=[1, 2, 3],
        from_peer=-1001,
        as_album=True,
        drop_author=True,
        drop_media_captions=False,
    )


def test_process_new_message_event_skips_grouped_messages(monkeypatch: pytest.MonkeyPatch) -> None:
    copy_single_message_mock = AsyncMock()
    monkeypatch.setattr(main, "copy_single_message", copy_single_message_mock)

    grouped_message = SimpleNamespace(id=7, grouped_id=999)

    asyncio.run(
        main.process_new_message_event(
            client=AsyncMock(),
            source_channel_id=-1001,
            destination_channel_id=-2002,
            message=grouped_message,
        )
    )

    copy_single_message_mock.assert_not_called()


def test_process_new_message_event_calls_copy_for_regular_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    copy_single_message_mock = AsyncMock()
    monkeypatch.setattr(main, "copy_single_message", copy_single_message_mock)

    regular_message = SimpleNamespace(id=8, grouped_id=None)
    client = AsyncMock()

    asyncio.run(
        main.process_new_message_event(
            client=client,
            source_channel_id=-1001,
            destination_channel_id=-2002,
            message=regular_message,
        )
    )

    copy_single_message_mock.assert_awaited_once_with(
        client=client,
        source_channel_id=-1001,
        destination_channel_id=-2002,
        message_id=8,
    )


def test_process_album_event_calls_copy_media_group(monkeypatch: pytest.MonkeyPatch) -> None:
    copy_media_group_mock = AsyncMock()
    monkeypatch.setattr(main, "copy_media_group", copy_media_group_mock)

    messages = [SimpleNamespace(id=1), SimpleNamespace(id=2)]
    client = AsyncMock()

    asyncio.run(
        main.process_album_event(
            client=client,
            source_channel_id=-1001,
            destination_channel_id=-2002,
            messages=messages,
        )
    )

    copy_media_group_mock.assert_awaited_once_with(
        client=client,
        source_channel_id=-1001,
        destination_channel_id=-2002,
        messages=messages,
    )


def test_process_new_message_event_handles_floodwait(monkeypatch: pytest.MonkeyPatch) -> None:
    flood_wait = FloodWaitError.__new__(FloodWaitError)
    flood_wait.seconds = 2

    async def raise_flood_wait(*args, **kwargs):  # noqa: ANN002, ANN003
        raise flood_wait

    sleep_mock = AsyncMock()
    monkeypatch.setattr(main, "copy_single_message", raise_flood_wait)
    monkeypatch.setattr(main.asyncio, "sleep", sleep_mock)

    message = SimpleNamespace(id=8, grouped_id=None)

    asyncio.run(
        main.process_new_message_event(
            client=AsyncMock(),
            source_channel_id=-1001,
            destination_channel_id=-2002,
            message=message,
        )
    )

    sleep_mock.assert_awaited_once_with(2)


def test_process_new_message_event_end_to_end_in_process() -> None:
    client = AsyncMock()
    message = SimpleNamespace(id=101, grouped_id=None)

    asyncio.run(
        main.process_new_message_event(
            client=client,
            source_channel_id=-1001,
            destination_channel_id=-2002,
            message=message,
        )
    )

    client.forward_messages.assert_awaited_once_with(
        entity=-2002,
        messages=101,
        from_peer=-1001,
        drop_author=True,
        drop_media_captions=False,
    )


def test_process_album_event_end_to_end_in_process() -> None:
    client = AsyncMock()
    messages = [SimpleNamespace(id=11), SimpleNamespace(id=12)]

    asyncio.run(
        main.process_album_event(
            client=client,
            source_channel_id=-1001,
            destination_channel_id=-2002,
            messages=messages,
        )
    )

    client.forward_messages.assert_awaited_once_with(
        entity=-2002,
        messages=[11, 12],
        from_peer=-1001,
        as_album=True,
        drop_author=True,
        drop_media_captions=False,
    )
