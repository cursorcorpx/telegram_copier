import asyncio
import os

from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.sessions import StringSession


def _required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


async def _generate() -> str:
    load_dotenv()
    api_id = int(_required_env("API_ID"))
    api_hash = _required_env("API_HASH")

    print("Starting Telegram login flow for StringSession generation...")
    print("Enter phone/code/2FA details when prompted.")

    client = TelegramClient(StringSession(), api_id, api_hash)
    await client.start()
    session_string = client.session.save()
    await client.disconnect()
    return session_string


def main() -> None:
    try:
        session_string = asyncio.run(_generate())
    except KeyboardInterrupt:
        print("\nCancelled by user.")
        return
    except Exception as exc:
        print(f"Error: {exc}")
        return

    print("\nSESSION_STRING generated successfully:\n")
    print(session_string)
    print("\nStore this value in Appwrite Function environment variable: SESSION_STRING")


if __name__ == "__main__":
    main()
