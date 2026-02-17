import asyncio
import os
from dotenv import load_dotenv
from telethon import TelegramClient

load_dotenv()


async def main() -> None:
    client = TelegramClient(
        os.getenv("SESSION_NAME", "telegram_copier"),
        int(os.environ["API_ID"]),
        os.environ["API_HASH"],
    )
    await client.start(bot_token=os.getenv("BOT_TOKEN") or None)
    async for dialog in client.iter_dialogs():
        print(f"{dialog.name} => {dialog.id}")
    await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())