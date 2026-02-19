# Telegram Channel Copier (Telethon)

Production-ready async Telegram copier that listens to a source channel and copies messages/media to a destination channel without forward attribution.

## Features

- Async architecture (Python 3.11+)
- Telethon-based listener
- Copies:
  - text
  - photos
  - videos
  - documents
  - audio
  - voice
  - media groups (albums)
- Uses `forward_messages(..., drop_author=True)` to copy without forward attribution
- Preserves captions/formatting/media content
- Logging and graceful shutdown
- Flood wait handling and RPC error handling

## Project Structure

```text
telegram_copier/
├── main.py
├── config.py
├── requirements.txt
├── .env
└── README.md
```

## Prerequisites

- Python 3.11+
- Telegram API credentials from https://my.telegram.org
- Source and destination channel IDs (`-100...` format)
- Permissions:
  - Source: account/bot must be able to read channel posts
  - Destination: account/bot must be able to post messages

## Environment Variables

Create or update `.env`:

```env
API_ID=
API_HASH=
SESSION_NAME=telegram_copier
SOURCE_CHANNEL_ID=
# Optional for multiple sources (comma-separated). If set, it takes precedence.
# SOURCE_CHANNEL_IDS=-1001111111111,-1002222222222
DESTINATION_CHANNEL_ID=
BOT_TOKEN=
LOG_LEVEL=INFO
```

Notes:
- `BOT_TOKEN` is optional.  
  - If set: client starts in bot mode.
  - If not set: client starts in user mode.
- Channel IDs must be integers, typically negative supergroup/channel IDs like `-1001234567890`.
- Multi-source support:
  - Use `SOURCE_CHANNEL_ID` for one source.
  - Use `SOURCE_CHANNEL_IDS` for multiple sources (comma-separated).
  - If both are set, `SOURCE_CHANNEL_IDS` is used.

## Find `DESTINATION_CHANNEL_ID` (and `SOURCE_CHANNEL_ID`)

Use one of these methods.

1. Telegram Desktop link method (quick)

- Open the destination channel in Telegram Desktop.
- Right-click any post and click `Copy Message Link`.
- If link looks like `https://t.me/c/1234567890/15`, the channel ID is:
  - `-1001234567890`
- Same process for source channel.

2. Telethon script method (most reliable)

Create a temp file `print_dialog_ids.py` inside `telegram_copier/`:

```python
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
```

Run it:

```bash
python print_dialog_ids.py
```

- Find your destination channel name in output.
- Use its numeric value as `DESTINATION_CHANNEL_ID`.
- Use the source channel value as `SOURCE_CHANNEL_ID`.
- For multiple source channels, put them in `SOURCE_CHANNEL_IDS` separated by commas.

Example:

```env
SOURCE_CHANNEL_IDS=-1001111111111,-1002222222222,-1003333333333
```

## Setup

1. Install dependencies

```bash
python -m venv .venv
source .venv/bin/activate   # Linux/macOS
# .venv\Scripts\activate    # Windows PowerShell
pip install -r requirements.txt
```

2. First-time login

- User mode (`BOT_TOKEN` empty):
  - Run script and enter phone/code (and 2FA password if enabled).
  - Telethon stores the session in a local `.session` file using `SESSION_NAME`.
- Bot mode (`BOT_TOKEN` set):
  - Ensure bot is admin in destination channel and has access to source channel posts.

3. Run script

```bash
python main.py
```

## Run Locally (Windows PowerShell)

From repo root (`f:\telegram-cp`):

```powershell
cd .\telegram_copier
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python .\main.py
```

If you prefer running from repo root without `cd`:

```powershell
.\telegram_copier\.venv\Scripts\Activate.ps1
python .\telegram_copier\main.py
```

## Tests

Install test dependencies:

```bash
pip install -r requirements-dev.txt
```

Run tests:

```bash
python -m pytest -q
```

## Docker (Multi-stage)

Build image from project root:

```bash
docker build -t telegram-copier:latest ./telegram_copier
```

Run with `.env`:

```bash
docker run --rm --name telegram-copier --env-file ./telegram_copier/.env telegram-copier:latest
```

If you are doing first-time user login in Docker, run interactively:

```bash
docker run -it --rm --name telegram-copier --env-file ./telegram_copier/.env telegram-copier:latest
```

If you use user-mode login (no `BOT_TOKEN`), persist session file:

```bash
docker run --rm --name telegram-copier \
  --env-file ./telegram_copier/.env \
  -v $(pwd)/telegram_copier:/app \
  telegram-copier:latest
```

Notes:
- `BOT_TOKEN` mode is recommended for container/cloud deployments.
- Startup behavior:
  - If `BOT_TOKEN` is set: runs as bot.
  - If `BOT_TOKEN` is empty and an authorized `.session` exists: runs as user session.
  - If `BOT_TOKEN` is empty and no authorized `.session` exists: startup fails with clear error.
- `.dockerignore` excludes `.env`, `.session`, tests, and local caches to keep image small.

4. Deploy on Linux (systemd example)

Create `/etc/systemd/system/telegram-copier.service`:

```ini
[Unit]
Description=Telegram Copier Bot
After=network.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/opt/telegram_copier
EnvironmentFile=/opt/telegram_copier/.env
ExecStart=/opt/telegram_copier/.venv/bin/python /opt/telegram_copier/main.py
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

Enable and start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable telegram-copier
sudo systemctl start telegram-copier
sudo systemctl status telegram-copier
```

## How It Avoids "Forwarded From"

The app uses Telethon `forward_messages(..., drop_author=True, drop_media_captions=False)`, which removes forward attribution while preserving caption/media content.

## Operational Notes

- If you miss events while the process is down, this script does not backfill history by default.
- Add monitoring (for example via systemd status checks, log shipping, or health checks) in production.
- Keep `.env` and `.session` files secure.
