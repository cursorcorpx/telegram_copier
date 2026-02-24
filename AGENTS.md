# Handover Notes (Telegram Copier - Appwrite User Sync)

## Scope
- This project supports two runtime paths:
  - Local/docker long-running Telethon client (`main.py`)
  - Appwrite function-based scheduled sync (user session) in:
    - `appwrite/functions/webhook/src/main.py`
- Current production focus is Appwrite scheduled user-session sync.

## Appwrite Function Behavior
- Uses `SESSION_STRING` (Telethon StringSession), not bot token.
- Schedule is configured in `appwrite.json` (`telegram-copier-webhook` function).
- Reads source channels and copies/reposts into destination channel.
- Maintains dedupe/checkpoint state in Telegram Saved Messages with marker:
  - `telegram_copier_state_v1:...`
- Creates per-run snapshot messages in Telegram Saved Messages with marker:
  - `telegram_copier_run_v1:...` (contains last lookback-window IDs by source, chunked if large)

## State Model (Saved Messages)
- `sources.<channel_id>.last_id`
- `sources.<channel_id>.recent` (last processed IDs window)
- `sources.<channel_id>.updated_at_ist` (primary timestamp)
- `sources.<channel_id>.updated_at_utc` (legacy compatibility)
- `meta.last_run_ts_ms`
- `meta.last_run_at_ist` (primary timestamp)
- `meta.last_run_at_utc` (legacy compatibility)
- `meta.status` (`running`, `completed`, `completed_with_errors`, `halted`)
- Optional run counters in `meta` (`copied`, `fetched`, etc.)

## Key Runtime Rules
- Dedupe:
  - Skip already processed IDs (`recent`) and older IDs (`last_id`).
- Lookback:
  - Controlled by `LOOKBACK_MINUTES` (default 60).
  - Source fetch is strict time-window based (last `LOOKBACK_MINUTES` by message timestamp) for coverage/snapshoting.
  - Forward/repost processing is gated by source cursor (`last_id`) and recent dedupe to prevent duplicate pushes.
  - Processing set is built from:
    - live fetch from source channels for lookback window, plus
    - IDs recovered from Saved Messages run snapshots (`telegram_copier_run_v1`) from the same lookback window.
  - This recovery merge reduces risk of missing messages between runs.
  - Cursor self-heal:
    - If source cursor is ahead of all fetched IDs for a run, cursor gating is temporarily relaxed for that source in that run (still deduped by `recent`) to restore flow.
- Flood wait:
  - Halts gracefully and returns `flood_wait_seconds` + `halted=1`.
- Protected/forward-restricted chats:
  - Fallback repost path attempts direct send (text/media) without forward.
- Sanitization:
  - Removes:
    - Telegram links (`t.me/...`, `telegram.me/...`, invite links)
    - `youtube.com` URLs/tokens
    - words `mc`, `bc` (case-insensitive whole words)
- Hard skips (not copied/forwarded):
  - Any message text matching ad markers (`#ad`, `insideads*`)
  - Optional stricter rule: standalone `ad` only when `BLOCK_GENERIC_AD_WORD=1`
  - GIF messages/media (`.gif`)
- Media fallback safety:
  - `MessageMediaWebPage` is treated as non-sendable media and sent as text.
- State save safety:
  - Oversized state payloads are compacted automatically before save to avoid `MessageTooLongError`.
- Snapshot-recovery safety:
  - Loading run snapshots from Saved Messages is best-effort; failures do not block main forwarding.
- Auth/session safety:
  - `AuthKeyDuplicatedError` means Telegram invalidated the session; rotate `SESSION_STRING` and keep it exclusive to this runtime.

## Critical Env Vars (Appwrite Function)
- `API_ID`
- `API_HASH`
- `SESSION_STRING`
- `DESTINATION_CHANNEL_ID`
- `SOURCE_CHANNEL_ID` or `SOURCE_CHANNEL_IDS`
- `LIMIT_PER_SOURCE` (1..200)
- `LOOKBACK_MINUTES` (1..1440)
- `BLOCK_GENERIC_AD_WORD` (optional; `1` enables standalone `ad` blocking)

## Runtime Metrics
- Core:
  - `copied`, `fetched`, `skipped_invalid`, `filtered_links`, `sources_with_new_messages`, `source_errors_count`
- State/run tracking:
  - `state_saved`, `state_message_id`, `updated_sources`
  - `run_snapshot_count`, `run_snapshot_message_ids`
- Skip breakdown:
  - `skipped_invalid_runtime`
  - `skipped_duplicate_recent`
  - `skipped_cursor_gate`
  - `skipped_blocked_ad`
  - `skipped_blocked_gif`

## Recent Fixes
- Added robust source error reporting (`source_errors`, `source_errors_count`).
- Added reliable state lookup via search in Saved Messages.
- Added explicit `state_message_id` + `state_saved` metrics.
- Added fallback for `MessageMediaWebPage` so it no longer crashes on repost.
- Fixed lookback fetch traversal to honor full `LOOKBACK_MINUTES` window.
- Fixed loop bug that could process only the last grouped item in some runs.
- Added Telegram link removal for full `t.me/`/`telegram.me/` URLs.
- Added ad-message blocking.
- Added GIF-message blocking.
- Added per-run Saved Messages snapshot (`telegram_copier_run_v1`) with 60-min IDs by source.
- Added merge logic to combine live fetch + recent run snapshot IDs (last 60 min) before processing.
- Added state compaction on save to prevent 500 errors from oversized state message payloads.
- Added best-effort snapshot loading (failure won’t block main forwarding).
- Added cursor self-heal when `last_id` is ahead of fetched range.
- Added explicit `AuthKeyDuplicatedError` handling with actionable runtime error.
- Added detailed skip-reason counters in run result/logs.

## Deployment Status
- As of 2026-02-24: code updated locally in workspace, tests passing.
- Deployment from this workspace is pending (run `appwrite push functions` to publish).

## Validation
- Local tests:
  - `python -m pytest -q`
- Latest baseline in this workspace: all tests passing.

## Ops Checklist
1. `appwrite push functions`
2. Ensure latest deployment is active.
3. Verify function variables are set correctly.
4. Run one manual execution and inspect logs:
   - `copied`, `fetched`, `source_errors_count`, `state_saved`, `state_message_id`
   - skip breakdown: `skipped_invalid_runtime`, `skipped_duplicate_recent`, `skipped_cursor_gate`, `skipped_blocked_ad`, `skipped_blocked_gif`
5. Confirm destination messages and Saved Messages state updates.
