# Telegram Copier (Appwrite + Telethon User Session)

Production setup is Appwrite Function-based user-session sync.

## What It Does

- Copies messages from one or many source channels to a destination channel.
- Works on a scheduled run in Appwrite.
- Uses a Telethon `SESSION_STRING` (user account session), not a bot token.
- Tries to avoid duplicates while still covering the last lookback window.

## Current Runtime Behavior

- Lookback window:
  - Controlled by `LOOKBACK_MINUTES` (default `60`).
  - Fetches source messages by timestamp in that window.
- Recovery:
  - Reads per-run snapshot IDs from Saved Messages (`telegram_copier_run_v1`) and merges with live fetch.
- Dedupe:
  - Uses source cursor (`last_id`) and `recent` IDs to avoid repeats.
  - Includes cursor self-heal if cursor is ahead of fetched range.
- Sanitization:
  - Removes Telegram links (`t.me/...`, `telegram.me/...`) and `youtube.com` links.
  - Removes words `mc` and `bc`.
- Blocking:
  - Skips ad markers (`#ad`, `insideads*`).
  - Optional stricter ad blocking with `BLOCK_GENERIC_AD_WORD=1` for standalone `ad`.
  - Skips GIF messages/media.
- Protected channels:
  - Falls back to repost when forwarding is restricted.
- State safety:
  - Compacts oversized state payloads to avoid Telegram `MessageTooLongError`.
- Session safety:
  - Handles `AuthKeyDuplicatedError` with explicit actionable error.

## Saved Messages Markers

- State/checkpoint:
  - `telegram_copier_state_v1:...`
- Per-run snapshot:
  - `telegram_copier_run_v1:...`

## Key Metrics in Logs/Response

- Core:
  - `copied`, `fetched`, `skipped_invalid`, `filtered_links`, `sources_with_new_messages`
- Skip breakdown:
  - `skipped_invalid_runtime`
  - `skipped_duplicate_recent`
  - `skipped_cursor_gate`
  - `skipped_blocked_ad`
  - `skipped_blocked_gif`
- State/run:
  - `state_saved`, `state_message_id`, `updated_sources`
  - `run_snapshot_count`, `run_snapshot_message_ids`

## Project Structure

```text
telegram_copier/
├── appwrite.json
├── APPWRITE_DEPLOY.md
├── AGENTS.md
├── appwrite/
│   └── functions/
│       ├── webhook/
│       │   ├── requirements.txt
│       │   └── src/main.py
│       └── album_flush/
├── tests/
├── generate_session_string.py
└── main.py
```

## Environment Variables (Appwrite Function: `telegram-copier-webhook`)

- Required:
  - `API_ID`
  - `API_HASH`
  - `SESSION_STRING`
  - `DESTINATION_CHANNEL_ID`
  - `SOURCE_CHANNEL_ID` or `SOURCE_CHANNEL_IDS`
- Optional:
  - `LIMIT_PER_SOURCE` (default `50`, range `1..200`)
  - `LOOKBACK_MINUTES` (default `60`, range `1..1440`)
  - `BLOCK_GENERIC_AD_WORD` (`1`/`true` to block standalone `ad`)

## Deploy to Appwrite

1. Generate user session string:

```bash
cd telegram_copier
python -m venv .venv
source .venv/bin/activate   # Linux/macOS
# .venv\Scripts\Activate.ps1 # Windows PowerShell
pip install -r requirements.txt
python generate_session_string.py
```

2. In Appwrite Cloud, set function variables for `Telegram Copier User Sync`.

3. Deploy functions:

```bash
appwrite login
appwrite push functions
```

4. Trigger one manual run and inspect logs.

## Operational Troubleshooting

- `AuthKeyDuplicatedError`:
  - Current `SESSION_STRING` is invalidated by Telegram.
  - Generate a NEW session string, update Appwrite variable, redeploy.
  - Ensure this session is used only by this runtime.
- `copied=0` with `fetched>0`:
  - Check skip counters in logs to see exact reason.
  - Validate content is not blocked by ad/gif/sanitization rules.
- State message too long:
  - Already handled by compaction, should not crash current build.

## Local Testing

```bash
cd telegram_copier
pip install -r requirements-dev.txt
python -m pytest -q
```

## Contributing

1. Create a branch from latest mainline.
2. Make focused changes (prefer small PRs).
3. Add/update tests for behavior changes.
4. Run full tests:

```bash
python -m pytest -q
```

5. Update docs when runtime behavior changes:
  - `README.md`
  - `AGENTS.md`
  - `APPWRITE_DEPLOY.md` (if deployment steps changed)
6. Open PR with:
  - Problem statement
  - Behavior change summary
  - Test evidence
  - Deployment impact/notes

## Security Notes

- Keep `SESSION_STRING`, `API_HASH`, and channel IDs private.
- Do not commit `.env` secrets.
- Rotate session if compromise is suspected.
