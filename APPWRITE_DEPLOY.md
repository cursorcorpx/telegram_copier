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
- `LOOKBACK_MINUTES` (optional, default `60`)
- `APPWRITE_DATABASE_ID` (**required** for persistent logs/state)
- `APPWRITE_LOGS_COLLECTION_ID` (recommended: `telegram_copier_logs`)   
- `APPWRITE_STATE_COLLECTION_ID` (recommended: `telegram_copier_state`)  

Notes:
- Keep `SESSION_STRING` secret.
- Use channel numeric IDs (`-100...`).
- If you want link/content filtering:
  - `STRIP_T_LINKS=1`
  - `STRIP_YOUTUBE_LINKS=1`
  - `FILTER_BAD_WORDS=mc,bc`

## 4. Create Database + Collections (Appwrite Console)

Go to **Databases** in Appwrite Cloud and create (or reuse) one database.

1. Create database
- Name: `telegram-copier-db` (any name is fine)
- Copy the database ID and set it as `APPWRITE_DATABASE_ID`

2. Create collection: `telegram_copier_logs`
- Collection ID: `telegram_copier_logs`
- Attributes:
  - `function_name` (string, size 128, required)
  - `run_ts_ms` (integer, required)
  - `status` (string, size 64, required)
  - `lookback_minutes` (integer, required)
  - `source_id` (integer, required)
  - `part` (string, size 32, required)
  - `message_ids_json` (json, optional but recommended)
  - `message_ids` (string, size 100000, required)
  - `created_at` (string, size 64, required)

3. Create collection: `telegram_copier_state`
- Collection ID: `telegram_copier_state`
- Attributes:
  - `state_key` (string, size 64, required)
  - `payload_json` (json, optional but recommended)
  - `payload` (string, size 100000, required)
  - `updated_at` (string, size 64, required)
  - `run_ts_ms` (integer, required)

4. Permissions
- Keep default function access or ensure your function has:
  - `databases.read`
  - `databases.write`

5. Optional indexes (recommended)
- On `telegram_copier_logs`: index on `run_ts_ms`, `source_id`
- On `telegram_copier_state`: unique index on `state_key`

## 5. Deploy Functions

From `telegram_copier/`:

```bash
appwrite login
appwrite push functions
```

## 6. Verify Scheduled Execution

- In Appwrite Cloud, confirm function `Telegram Copier User Sync` is enabled.
- Schedule in `appwrite.json` is currently `*/10 * * * *` (every 10 minutes).
- Open function logs and verify successful runs.

## 7. Validate Behavior

- Post messages in source channel(s).
- Confirm they appear in destination channel with no forward attribution.
- Check execution logs for copied counts.
- Check `telegram_copier_logs` documents are being inserted each run.
- Check `telegram_copier_state` has a `state_key=global` document and updates every run.
- Check JSON fields are populated when present:
  - `telegram_copier_logs.message_ids_json`
  - `telegram_copier_state.payload_json`
- Telegram Saved Messages should no longer receive run snapshot log messages.

## 8. Troubleshooting

- `SESSION_STRING is not authorized`: regenerate with `generate_session_string.py`.
- `Set SOURCE_CHANNEL_ID or SOURCE_CHANNEL_IDS`: missing source IDs.
- `API_ID/API_HASH` errors: verify values from https://my.telegram.org.
- No DB logs created:
  - Verify `APPWRITE_DATABASE_ID` and collection IDs.
  - Verify function scopes include `databases.read` and `databases.write`.
  - Check function variables are set on the active deployment.
- DB write fails with `Unknown attribute`:
  - Add missing fields in Appwrite table schema exactly as named:
    - `message_ids_json` in `telegram_copier_logs`
    - `payload_json` in `telegram_copier_state`
  - If you do not want JSON fields, keep string fields (`message_ids`, `payload`) only; the function will fallback automatically.
- DB write fails with `400 Bad Request`:
  - Check attribute types:
    - `run_ts_ms`, `lookback_minutes`, `source_id` -> integer
    - `message_ids_json`, `payload_json` -> json
    - `message_ids`, `payload` -> string with enough max size
- Function runs but no copy:
  - Validate source/destination channel IDs.
  - Ensure the user session account has access to source channels and posting rights on destination.
