# Appwrite Deployment (User Session Mode)

This guide deploys the Appwrite function-based copier using **user session** (no bot token).

## 1. Prerequisites

- Appwrite Cloud project created.
- `appwrite.json` already has your correct `projectId`.
- Python 3.11+ locally.
- Appwrite CLI installed and logged in.

## 2. Generate `SESSION_STRING`

From project root:

```bash
cd telegram_copier
python -m venv .venv
source .venv/bin/activate   # Linux/macOS
# .venv\Scripts\Activate.ps1   # Windows PowerShell
pip install -r requirements.txt
python generate_session_string.py
```

Copy the printed value. This is your `SESSION_STRING`.

## 3. Configure Function Environment Variables

In Appwrite Cloud -> Functions -> `Telegram Copier User Sync` -> Variables, set:

- `API_ID`
- `API_HASH`
- `SESSION_STRING`
- `DESTINATION_CHANNEL_ID`
- `SOURCE_CHANNEL_ID` or `SOURCE_CHANNEL_IDS`
- `LIMIT_PER_SOURCE` (optional, default `50`)

Notes:
- Keep `SESSION_STRING` secret.
- Use channel numeric IDs (`-100...`).

## 4. Deploy Functions

From `telegram_copier/`:

```bash
appwrite login
appwrite push functions
```

## 5. Verify Scheduled Execution

- In Appwrite Cloud, confirm function `Telegram Copier User Sync` is enabled.
- Schedule should be `*/1 * * * *`.
- Open function logs and verify successful runs.

## 6. Validate Behavior

- Post messages in source channel(s).
- Confirm they appear in destination channel with no forward attribution.
- Check Appwrite logs for copied counts.

## 7. Troubleshooting

- `SESSION_STRING is not authorized`: regenerate with `generate_session_string.py`.
- `Set SOURCE_CHANNEL_ID or SOURCE_CHANNEL_IDS`: missing source IDs.
- `API_ID/API_HASH` errors: verify values from https://my.telegram.org.
