import asyncio
import logging
import signal
import sys
from collections.abc import Sequence

from telethon import TelegramClient, events
from telethon.errors import FloodWaitError, RPCError
from telethon.tl.custom.message import Message

try:
    from .config import Settings
except ImportError:  # pragma: no cover
    from config import Settings


logger = logging.getLogger("telegram_copier")


def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


async def copy_single_message(
    client: TelegramClient, source_channel_id: int, destination_channel_id: int, message_id: int
) -> None:
    await client.forward_messages(
        entity=destination_channel_id,
        messages=message_id,
        from_peer=source_channel_id,
        drop_author=True,
        drop_media_captions=False,
    )


async def copy_media_group(
    client: TelegramClient,
    source_channel_id: int,
    destination_channel_id: int,
    messages: Sequence[Message],
) -> None:
    message_ids = [msg.id for msg in messages]
    await client.forward_messages(
        entity=destination_channel_id,
        messages=message_ids,
        from_peer=source_channel_id,
        as_album=True,
        drop_author=True,
        drop_media_captions=False,
    )


async def process_album_event(
    client: TelegramClient, source_channel_id: int, destination_channel_id: int, messages: Sequence[Message]
) -> None:
    try:
        await copy_media_group(
            client=client,
            source_channel_id=source_channel_id,
            destination_channel_id=destination_channel_id,
            messages=messages,
        )
        logger.info("Copied media group with %s items.", len(messages))
    except FloodWaitError as exc:
        logger.warning("FloodWaitError in album handler; sleeping for %s seconds.", exc.seconds)
        await asyncio.sleep(exc.seconds)
    except RPCError:
        logger.exception("Telegram RPC error while copying media group.")
    except Exception:
        logger.exception("Unexpected error while copying media group.")


async def process_new_message_event(
    client: TelegramClient,
    source_channel_id: int,
    destination_channel_id: int,
    message: Message,
) -> None:
    if getattr(message, "grouped_id", None):
        return

    try:
        await copy_single_message(
            client=client,
            source_channel_id=source_channel_id,
            destination_channel_id=destination_channel_id,
            message_id=message.id,
        )
        logger.info("Copied message id=%s", message.id)
    except FloodWaitError as exc:
        logger.warning("FloodWaitError in message handler; sleeping for %s seconds.", exc.seconds)
        await asyncio.sleep(exc.seconds)
    except RPCError:
        logger.exception("Telegram RPC error while copying message id=%s", message.id)
    except Exception:
        logger.exception("Unexpected error while copying message id=%s", message.id)


async def start_client(client: TelegramClient, settings: Settings) -> None:
    if settings.bot_token:
        await client.start(bot_token=settings.bot_token)
        logger.info("Started Telethon client in bot mode.")
        return

    has_tty = bool(getattr(sys.stdin, "isatty", lambda: False)())
    if has_tty:
        try:
            await client.start()
            logger.info("Started Telethon client in user mode.")
            return
        except EOFError as exc:
            raise RuntimeError(
                "Interactive login is not available in this environment. "
                "Set BOT_TOKEN in .env, or mount a pre-authenticated .session file."
            ) from exc

    # Non-interactive mode: do not prompt. Use existing authorized session only.
    await client.connect()
    if await client.is_user_authorized():
        logger.info("Started Telethon client in user mode using existing session.")
        return

    raise RuntimeError(
        "Non-interactive environment with no authorized user session. "
        "Set BOT_TOKEN in .env, or pre-create/mount a valid .session file."
    )


async def run() -> None:
    settings = Settings.from_env()
    setup_logging(settings.log_level)

    client = TelegramClient(settings.session_name, settings.api_id, settings.api_hash)
    await start_client(client, settings)

    logger.info(
        "Listening for messages from sources=%s and copying to destination=%s",
        list(settings.source_channel_ids),
        settings.destination_channel_id,
    )

    @client.on(events.Album(chats=list(settings.source_channel_ids)))
    async def album_handler(event: events.Album.Event) -> None:
        source_channel_id = event.chat_id
        if source_channel_id is None:
            logger.warning("Album event has no chat_id; skipping copy.")
            return
        await process_album_event(
            client=client,
            source_channel_id=source_channel_id,
            destination_channel_id=settings.destination_channel_id,
            messages=event.messages,
        )

    @client.on(events.NewMessage(chats=list(settings.source_channel_ids)))
    async def message_handler(event: events.NewMessage.Event) -> None:
        source_channel_id = event.chat_id
        if source_channel_id is None:
            logger.warning("Message event id=%s has no chat_id; skipping copy.", event.message.id)
            return
        await process_new_message_event(
            client=client,
            source_channel_id=source_channel_id,
            destination_channel_id=settings.destination_channel_id,
            message=event.message,
        )

    stop_event = asyncio.Event()

    def _signal_handler(*_: object) -> None:
        logger.info("Shutdown signal received.")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for signame in ("SIGINT", "SIGTERM"):
        sig = getattr(signal, signame, None)
        if sig is not None:
            try:
                loop.add_signal_handler(sig, _signal_handler)
            except NotImplementedError:
                pass

    await stop_event.wait()
    logger.info("Disconnecting Telegram client.")
    await client.disconnect()


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass
