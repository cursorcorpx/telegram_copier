import asyncio
import concurrent.futures
import json
import os
import re
import time
import traceback
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any


STATE_MARKER = "telegram_copier_state_v1:"
RUN_MARKER = "telegram_copier_run_v1:"
TELEGRAM_LINK_PATTERN = re.compile(r"(?:https?://)?(?:www\.)?(?:t\.me|telegram\.me)/\S+", re.IGNORECASE)
YOUTUBE_PATTERN = re.compile(r"(?:https?://)?(?:www\.)?youtube\.com[^\s]*", re.IGNORECASE)
BAD_WORDS_PATTERN = re.compile(r"\b(?:mc|bc)\b", re.IGNORECASE)
AD_BLOCK_STRICT_PATTERN = re.compile(r"(?:#ad\b|insideads[\w-]*)", re.IGNORECASE)
AD_BLOCK_GENERIC_WORD_PATTERN = re.compile(r"\bad\b", re.IGNORECASE)
IST_TIMEZONE = timezone(timedelta(hours=5, minutes=30))


@dataclass(frozen=True)
class AppwriteSettings:
    api_id: int
    api_hash: str
    session_string: str
    source_channel_ids: tuple[int, ...]
    destination_channel_id: int
    limit_per_source: int
    lookback_minutes: int


@dataclass(frozen=True)
class FloodHalt(Exception):
    seconds: int
    processed_ids: tuple[int, ...]
    copied: int
    skipped: int


# Keep a wider recent window so strict lookback processing does not re-copy on normal schedules.
MAX_RECENT_IDS = 2000
MAX_STATE_PAYLOAD_CHARS = 3800
MAX_RUN_MESSAGE_CHARS = 3800


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _now_ist_iso() -> str:
    return datetime.now(IST_TIMEZONE).isoformat()


def _state_payload_text(state: dict[str, Any]) -> str:
    return STATE_MARKER + json.dumps(state, separators=(",", ":"), sort_keys=True)


def _copy_state(state: dict[str, Any]) -> dict[str, Any]:
    try:
        return json.loads(json.dumps(state))
    except Exception:
        return {"sources": {}}


def _compact_state_for_save(state: dict[str, Any]) -> dict[str, Any]:
    compact = _copy_state(state)

    meta = compact.get("meta")
    if isinstance(meta, dict):
        meta.pop("source_errors", None)

    sources = compact.get("sources")
    if isinstance(sources, dict):
        for source_key, source_entry in sources.items():
            if not isinstance(source_entry, dict):
                sources[source_key] = {"last_id": 0, "recent": []}
                continue
            recent = source_entry.get("recent", [])
            if isinstance(recent, list):
                source_entry["recent"] = [
                    int(mid) for mid in recent if isinstance(mid, int)
                ][-MAX_RECENT_IDS:]

    return compact


def _build_state_payload(state: dict[str, Any]) -> str:
    payload = _state_payload_text(state)
    if len(payload) <= MAX_STATE_PAYLOAD_CHARS:
        return payload

    compact = _compact_state_for_save(state)
    payload = _state_payload_text(compact)
    if len(payload) <= MAX_STATE_PAYLOAD_CHARS:
        return payload

    # Last-resort compact form to avoid runtime failure due to Telegram message limits.
    minimal_sources: dict[str, dict[str, int]] = {}
    for source_key, source_entry in (compact.get("sources") or {}).items():
        if not isinstance(source_entry, dict):
            continue
        last_id = source_entry.get("last_id", 0)
        minimal_sources[str(source_key)] = {"last_id": int(last_id) if isinstance(last_id, int) else 0}

    minimal_state = {
        "sources": minimal_sources,
        "meta": {
            "status": ((compact.get("meta") or {}).get("status") if isinstance(compact.get("meta"), dict) else None),
            "last_run_ts_ms": (
                (compact.get("meta") or {}).get("last_run_ts_ms") if isinstance(compact.get("meta"), dict) else None
            ),
            "state_compacted": 1,
            "compacted_at_ist": _now_ist_iso(),
            "compacted_at_utc": _now_utc_iso(),
        },
    }
    return _state_payload_text(minimal_state)


def _split_id_tokens(tokens: list[str], max_chars: int) -> list[list[str]]:
    if not tokens:
        return [[]]
    parts: list[list[str]] = []
    current: list[str] = []
    current_len = 0
    for token in tokens:
        token_len = len(token) + (1 if current else 0)
        if current and current_len + token_len > max_chars:
            parts.append(current)
            current = [token]
            current_len = len(token)
            continue
        current.append(token)
        current_len += token_len
    if current:
        parts.append(current)
    return parts


async def _save_run_window_ids(
    client: Any,
    run_ts_ms: int,
    lookback_minutes: int,
    run_status: str,
    ids_by_source: dict[str, list[int]],
) -> list[int]:
    sent_ids: list[int] = []
    for source_key in sorted(ids_by_source.keys()):
        raw_ids = ids_by_source.get(source_key, [])
        tokens = [str(int(mid)) for mid in raw_ids if isinstance(mid, int)]
        if not tokens:
            continue
        parts = _split_id_tokens(tokens, max_chars=2800)
        total_parts = len(parts)

        for idx, part_tokens in enumerate(parts, start=1):
            prefix = (
                f"{RUN_MARKER}"
                f"run_ts_ms={run_ts_ms};status={run_status};"
                f"lookback_minutes={lookback_minutes};source_id={source_key};"
                f"part={idx}/{total_parts};ids="
            )
            ids_text = ",".join(part_tokens)
            message_text = prefix + ids_text
            if len(message_text) > MAX_RUN_MESSAGE_CHARS:
                ids_text = ids_text[: max(1, MAX_RUN_MESSAGE_CHARS - len(prefix))]
                message_text = prefix + ids_text
            sent = await client.send_message("me", message_text)
            message_id = getattr(sent, "id", None)
            if isinstance(message_id, int):
                sent_ids.append(message_id)
    return sent_ids


def _parse_run_snapshot_message(text: str) -> tuple[int, str, list[int]] | None:
    if not text.startswith(RUN_MARKER):
        return None
    raw = text[len(RUN_MARKER) :]
    parts = raw.split(";")
    fields: dict[str, str] = {}
    for part in parts:
        if "=" not in part:
            continue
        key, value = part.split("=", 1)
        fields[key.strip()] = value.strip()
    run_ts_raw = fields.get("run_ts_ms")
    source_id = fields.get("source_id")
    ids_raw = fields.get("ids", "")
    if not run_ts_raw or not source_id:
        return None
    try:
        run_ts_ms = int(run_ts_raw)
    except ValueError:
        return None
    parsed_ids: list[int] = []
    if ids_raw and ids_raw != "-":
        for token in ids_raw.split(","):
            token = token.strip()
            if not token:
                continue
            try:
                parsed_ids.append(int(token))
            except ValueError:
                continue
    return run_ts_ms, str(source_id), parsed_ids


async def _load_recent_run_snapshot_ids(client: Any, lookback_minutes: int) -> dict[str, set[int]]:
    now_ms = int(time.time() * 1000)
    cutoff_ms = now_ms - (lookback_minutes * 60 * 1000)
    by_source: dict[str, set[int]] = {}

    try:
        async for message in client.iter_messages("me", search=RUN_MARKER, limit=500):
            text = (getattr(message, "message", "") or "").strip()
            parsed = _parse_run_snapshot_message(text)
            if parsed is None:
                continue
            run_ts_ms, source_key, ids = parsed
            if run_ts_ms < cutoff_ms:
                continue
            if source_key not in by_source:
                by_source[source_key] = set()
            by_source[source_key].update(ids)
    except Exception:
        # Snapshot recovery is best-effort and should never block main forwarding.
        return {}
    return by_source


def _parse_source_ids() -> tuple[int, ...]:
    source_ids_raw = os.getenv("SOURCE_CHANNEL_IDS", "").strip()
    source_id_raw = os.getenv("SOURCE_CHANNEL_ID", "").strip()

    if source_ids_raw:
        return tuple(int(part.strip()) for part in source_ids_raw.split(",") if part.strip())
    if source_id_raw:
        return (int(source_id_raw),)
    return ()


def _load_settings_from_env() -> AppwriteSettings:
    api_id_raw = os.getenv("API_ID", "").strip()
    api_hash = os.getenv("API_HASH", "").strip()
    session_string = os.getenv("SESSION_STRING", "").strip()
    destination_raw = os.getenv("DESTINATION_CHANNEL_ID", "").strip()
    limit_raw = os.getenv("LIMIT_PER_SOURCE", "50").strip()
    lookback_raw = os.getenv("LOOKBACK_MINUTES", "60").strip()

    if not api_id_raw:
        raise ValueError("API_ID is required.")
    if not api_hash:
        raise ValueError("API_HASH is required.")
    if not session_string:
        raise ValueError("SESSION_STRING is required for user-session mode.")
    if not destination_raw:
        raise ValueError("DESTINATION_CHANNEL_ID is required.")

    source_channel_ids = _parse_source_ids()
    if not source_channel_ids:
        raise ValueError("Set SOURCE_CHANNEL_ID or SOURCE_CHANNEL_IDS.")

    try:
        api_id = int(api_id_raw)
        destination_channel_id = int(destination_raw)
        limit_per_source = int(limit_raw)
        lookback_minutes = int(lookback_raw)
    except ValueError as exc:
        raise ValueError(
            "API_ID, DESTINATION_CHANNEL_ID, LIMIT_PER_SOURCE, and LOOKBACK_MINUTES must be integers."
        ) from exc

    if limit_per_source < 1 or limit_per_source > 200:
        raise ValueError("LIMIT_PER_SOURCE must be between 1 and 200.")
    if lookback_minutes < 1 or lookback_minutes > 1440:
        raise ValueError("LOOKBACK_MINUTES must be between 1 and 1440.")

    return AppwriteSettings(
        api_id=api_id,
        api_hash=api_hash,
        session_string=session_string,
        source_channel_ids=source_channel_ids,
        destination_channel_id=destination_channel_id,
        limit_per_source=limit_per_source,
        lookback_minutes=lookback_minutes,
    )


def _respond_json(context: Any, payload: dict[str, Any], status_code: int = 200):
    res = context.res
    try:
        return res.json(payload, status_code)
    except TypeError:
        try:
            return res.json(payload)
        except TypeError:
            try:
                return res.send(json.dumps(payload), status_code)
            except Exception:
                return payload


def _safe_log(context: Any, message: str, is_error: bool = False) -> None:
    logger = getattr(context, "error", None) if is_error else getattr(context, "log", None)
    if callable(logger):
        try:
            logger(message)
        except Exception:
            pass
    # Native logs fallback (shown by Appwrite as "Native logs detected")
    print(message, flush=True)


def _extract_state(raw_text: str) -> dict[str, Any]:
    if not raw_text.startswith(STATE_MARKER):
        return {}
    try:
        payload = json.loads(raw_text[len(STATE_MARKER) :])
    except json.JSONDecodeError:
        return {}

    # Backward compatibility:
    # - old format: {"-100123": 456}
    # - new format: {"sources": {"-100123": {"last_id": 456, "recent": [..]}}}
    if isinstance(payload, dict) and all(isinstance(v, int) for v in payload.values()):
        return {"sources": {str(k): {"last_id": int(v), "recent": []} for k, v in payload.items()}}
    if isinstance(payload, dict) and isinstance(payload.get("sources"), dict):
        return payload
    return {"sources": {}}


def _get_source_state(state: dict[str, Any], source_key: str) -> tuple[int, list[int]]:
    sources = state.setdefault("sources", {})
    entry = sources.get(source_key)
    if not isinstance(entry, dict):
        return 0, []
    last_id = int(entry.get("last_id", 0)) if isinstance(entry.get("last_id", 0), int) else 0
    recent_raw = entry.get("recent", [])
    recent = [int(x) for x in recent_raw if isinstance(x, int)]
    return last_id, recent


def _set_source_state(state: dict[str, Any], source_key: str, last_id: int, recent: list[int]) -> None:
    dedup_recent: list[int] = []
    seen: set[int] = set()
    for mid in recent:
        if mid in seen:
            continue
        seen.add(mid)
        dedup_recent.append(mid)
    if len(dedup_recent) > MAX_RECENT_IDS:
        dedup_recent = dedup_recent[-MAX_RECENT_IDS:]
    state.setdefault("sources", {})[source_key] = {
        "last_id": int(last_id),
        "recent": dedup_recent,
        "updated_at_ist": _now_ist_iso(),
        "updated_at_utc": _now_utc_iso(),
    }


def _set_run_meta(state: dict[str, Any], **kwargs: Any) -> None:
    meta = state.setdefault("meta", {})
    for key, value in kwargs.items():
        meta[key] = value


def _sanitize_message_text(text: str) -> str:
    # Remove Telegram links (including invite links).
    cleaned = TELEGRAM_LINK_PATTERN.sub("", text)
    # Remove youtube.com URLs/tokens.
    cleaned = YOUTUBE_PATTERN.sub("", cleaned)
    # Remove configured abusive keywords.
    cleaned = BAD_WORDS_PATTERN.sub("", cleaned)
    # Normalize spaces introduced by removals.
    cleaned = re.sub(r"[ \t]{2,}", " ", cleaned)
    cleaned = re.sub(r" *\n *", "\n", cleaned)
    return cleaned.strip()


def _should_skip_message_text(text: str) -> bool:
    value = text or ""
    if AD_BLOCK_STRICT_PATTERN.search(value):
        return True
    block_generic_ad_word = os.getenv("BLOCK_GENERIC_AD_WORD", "0").strip().lower() in ("1", "true", "yes", "on")
    if block_generic_ad_word and AD_BLOCK_GENERIC_WORD_PATTERN.search(value):
        return True
    return False


def _is_cursor_gate_enabled() -> bool:
    return os.getenv("ENABLE_CURSOR_GATE", "1").strip().lower() in ("1", "true", "yes", "on")


def _is_snapshot_recovery_enabled() -> bool:
    return os.getenv("ENABLE_SNAPSHOT_RECOVERY", "0").strip().lower() in ("1", "true", "yes", "on")


async def _fetch_recent_messages(
    client: Any,
    source_id: int,
    last_seen_id: int,
    cutoff_dt: datetime,
) -> list[Any]:
    messages: list[Any] = []
    async for msg in client.iter_messages(source_id):
        msg_id = int(getattr(msg, "id", 0))
        if msg_id <= 0:
            continue
        msg_date = getattr(msg, "date", None)
        if msg_date is None:
            continue
        if msg_date.tzinfo is None:
            msg_date = msg_date.replace(tzinfo=timezone.utc)
        # Dates are descending as well; once below cutoff, the rest are out of range.
        if msg_date < cutoff_dt:
            break
        messages.append(msg)

    messages.sort(key=lambda m: int(getattr(m, "id", 0)))
    return messages


def _has_sendable_media(message: Any) -> bool:
    media = getattr(message, "media", None)
    if not media:
        return False
    # Web page previews are represented as media but cannot be sent via send_file.
    if media.__class__.__name__ == "MessageMediaWebPage":
        return False
    return True


def _is_gif_message(message: Any) -> bool:
    if bool(getattr(message, "gif", False)):
        return True
    media = getattr(message, "media", None)
    if not media:
        return False
    document = getattr(media, "document", None)
    if not document:
        return False
    mime_type = (getattr(document, "mime_type", "") or "").lower()
    if mime_type == "image/gif":
        return True
    has_video_attr = False
    has_animated_attr = False
    video_supports_streaming = None
    for attr in (getattr(document, "attributes", None) or []):
        if attr.__class__.__name__ == "DocumentAttributeAnimated":
            has_animated_attr = True
            return True
        if attr.__class__.__name__ == "DocumentAttributeVideo":
            has_video_attr = True
            video_supports_streaming = bool(getattr(attr, "supports_streaming", False))
        file_name = (getattr(attr, "file_name", "") or "").lower()
        if file_name.endswith(".gif"):
            return True
    # Telegram GIFs are frequently delivered as non-streaming MP4 animations.
    if mime_type == "video/mp4" and (has_animated_attr or (has_video_attr and video_supports_streaming is False)):
        return True
    return False


async def _load_state(client: Any) -> tuple[dict[str, Any], int | None]:
    # Reliable lookup: search by marker across Saved Messages history.
    try:
        async for message in client.iter_messages("me", search=STATE_MARKER, limit=1):
            text = (message.message or "").strip()
            if text.startswith(STATE_MARKER):
                return _extract_state(text), message.id
    except Exception:
        pass

    # Fallback for runtimes where search may be restricted.
    messages = await client.get_messages("me", limit=200)
    for message in messages:
        text = (message.message or "").strip()
        if text.startswith(STATE_MARKER):
            return _extract_state(text), message.id
    return {}, None


async def _save_state(client: Any, state: dict[str, Any], state_message_id: int | None) -> tuple[int | None, bool]:
    from telethon.errors.rpcerrorlist import MessageNotModifiedError

    state_payload = _build_state_payload(state)
    if state_message_id:
        try:
            await client.edit_message("me", state_message_id, state_payload)
            return state_message_id, True
        except MessageNotModifiedError:
            return state_message_id, False
        except Exception:
            # Fallback: create a fresh state message if editing fails.
            sent = await client.send_message("me", state_payload)
            return getattr(sent, "id", None), True

    sent = await client.send_message("me", state_payload)
    return getattr(sent, "id", None), True


def _group_message_ids(messages: list[Any]) -> list[tuple[bool, list[int]]]:
    grouped: list[tuple[bool, list[int]]] = []
    current_album_key = None
    current_album_ids: list[int] = []

    def flush_album() -> None:
        nonlocal current_album_key, current_album_ids
        if current_album_ids:
            grouped.append((True, current_album_ids))
            current_album_key = None
            current_album_ids = []

    for message in messages:
        grouped_id = getattr(message, "grouped_id", None)
        message_id = int(getattr(message, "id", 0))
        if message_id <= 0:
            continue

        if grouped_id is None:
            flush_album()
            grouped.append((False, [message_id]))
            continue

        if current_album_key is None or current_album_key != grouped_id:
            flush_album()
            current_album_key = grouped_id
            current_album_ids = [message_id]
            continue

        current_album_ids.append(message_id)

    flush_album()
    return grouped


async def _copy_source_messages(client: Any, settings: AppwriteSettings) -> dict[str, Any]:
    copied_total = 0
    skipped_invalid_total = 0
    skipped_invalid_runtime_total = 0
    skipped_duplicate_recent_total = 0
    skipped_cursor_gate_total = 0
    skipped_blocked_ad_total = 0
    skipped_blocked_gif_total = 0
    filtered_links_total = 0
    fetched_total = 0
    sources_with_new_messages = 0
    state, state_message_id = await _load_state(client)
    if "sources" not in state:
        state = {"sources": {}}
    run_ts_ms = int(time.time() * 1000)
    _set_run_meta(
        state,
        last_run_ts_ms=run_ts_ms,
        last_run_at_ist=_now_ist_iso(),
        last_run_at_utc=_now_utc_iso(),
        status="running",
    )
    state_updated = False
    updated_source_keys: list[str] = []
    source_errors: dict[str, str] = {}
    use_cursor_gate = _is_cursor_gate_enabled()
    use_snapshot_recovery = _is_snapshot_recovery_enabled()
    run_window_ids_by_source: dict[str, list[int]] = {str(source_id): [] for source_id in settings.source_channel_ids}
    recent_snapshot_ids_by_source = (
        await _load_recent_run_snapshot_ids(client=client, lookback_minutes=settings.lookback_minutes)
        if use_snapshot_recovery
        else {}
    )
    # Force save every run so Saved Messages always reflects latest state metadata.
    changed = True
    state_message_id, did_update = await _save_state(client=client, state=state, state_message_id=state_message_id)
    state_updated = state_updated or did_update

    cutoff_dt = datetime.now(timezone.utc) - timedelta(minutes=settings.lookback_minutes)

    for source_id in settings.source_channel_ids:
        key = str(source_id)
        try:
            last_seen_id, recent_ids = _get_source_state(state, key)
            recent_set = set(recent_ids)
            cursor_id = last_seen_id
            source_state_dirty = False
            # Fetch recent messages by traversing backward until lookback cutoff.
            messages = await _fetch_recent_messages(
                client=client,
                source_id=source_id,
                last_seen_id=last_seen_id,
                cutoff_dt=cutoff_dt,
            )
            live_message_ids = {int(getattr(msg, "id", 0)) for msg in messages if int(getattr(msg, "id", 0)) > 0}
            snapshot_ids = sorted(
                mid for mid in recent_snapshot_ids_by_source.get(key, set()) if isinstance(mid, int) and mid > 0
            )
            missing_snapshot_ids = [
                mid for mid in snapshot_ids if mid not in live_message_ids and mid > last_seen_id
            ][: settings.limit_per_source]
            recovered_id_set: set[int] = set()
            if missing_snapshot_ids:
                recovered = await client.get_messages(source_id, ids=missing_snapshot_ids)
                recovered_list = list(recovered) if isinstance(recovered, (list, tuple)) else [recovered]
                recovered_valid = [
                    msg
                    for msg in recovered_list
                    if msg is not None and int(getattr(msg, "id", 0)) > 0
                ]
                recovered_id_set = {int(getattr(msg, "id", 0)) for msg in recovered_valid if int(getattr(msg, "id", 0)) > 0}
                messages.extend(recovered_valid)
            dedup_by_id: dict[int, Any] = {}
            for msg in messages:
                mid = int(getattr(msg, "id", 0))
                if mid > 0:
                    dedup_by_id[mid] = msg
            messages = sorted(dedup_by_id.values(), key=lambda m: int(getattr(m, "id", 0)))
            max_fetched_id = max((int(getattr(msg, "id", 0)) for msg in messages), default=0)
            # Self-heal: if cursor is ahead of all fetched IDs, fall back to recent-set dedupe for this run.
            cursor_gate_disabled = max_fetched_id > 0 and last_seen_id > max_fetched_id
            run_window_ids_by_source[key] = [int(getattr(msg, "id", 0)) for msg in messages if int(getattr(msg, "id", 0)) > 0]
            fetched_total += len(messages)
            if not messages:
                continue
            sources_with_new_messages += 1

            for is_album, message_ids in _group_message_ids(messages):
                original_group_ids = list(message_ids)
                # Prevent re-copy loops: process only new IDs for this source cursor.
                # Snapshot recovery still works for missed IDs that are newer than last_seen_id.
                message_ids = [
                    mid
                    for mid in message_ids
                    if (
                        (not use_cursor_gate)
                        or cursor_gate_disabled
                        or mid > last_seen_id
                        or (mid in recovered_id_set and mid > last_seen_id)
                    )
                ]
                skipped_cursor_gate_total += len(original_group_ids) - len(message_ids)
                if not message_ids:
                    continue
                # Skip IDs already processed in recent window to avoid duplicates.
                before_recent_filter = len(message_ids)
                message_ids = [mid for mid in message_ids if mid not in recent_set]
                skipped_duplicate_recent_total += before_recent_filter - len(message_ids)
                if not message_ids:
                    continue
                group_by_id = {int(getattr(msg, "id", 0)): msg for msg in messages}

                # Drop ad/promotional messages entirely.
                blocked_ad_ids = [
                    mid
                    for mid in message_ids
                    if _should_skip_message_text((getattr(group_by_id.get(mid), "message", "") or "").strip())
                ]
                blocked_gif_ids = [mid for mid in message_ids if _is_gif_message(group_by_id.get(mid))]
                blocked_ids = sorted(set(blocked_ad_ids) | set(blocked_gif_ids))
                if blocked_ids:
                    skipped_blocked_ad_total += len(blocked_ad_ids)
                    skipped_blocked_gif_total += len(blocked_gif_ids)
                    recent_ids.extend(blocked_ids)
                    recent_set.update(blocked_ids)
                    cursor_id = max(cursor_id, max(blocked_ids))
                    source_state_dirty = True
                    skipped_invalid_total += len(blocked_ids)
                    message_ids = [mid for mid in message_ids if mid not in set(blocked_ids)]
                    if not message_ids:
                        continue

                if is_album:
                    album_ids = tuple(message_ids)
                    album_messages = [msg for msg in messages if int(getattr(msg, "id", 0)) in set(album_ids)]
                    if any(
                        _sanitize_message_text(getattr(msg, "message", "") or "")
                        != (getattr(msg, "message", "") or "").strip()
                        for msg in album_messages
                    ):
                        for msg in album_messages:
                            try:
                                copied_count, skipped_count, filtered_count, processed_ids = (
                                    await _copy_single_with_optional_sanitize(
                                        client=client,
                                        destination_channel_id=settings.destination_channel_id,
                                        source_id=source_id,
                                        message=msg,
                                    )
                                )
                                copied_total += copied_count
                                skipped_invalid_total += skipped_count
                                skipped_invalid_runtime_total += skipped_count
                                filtered_links_total += filtered_count
                                if processed_ids:
                                    recent_ids.extend(processed_ids)
                                    recent_set.update(processed_ids)
                                    cursor_id = max(cursor_id, max(processed_ids))
                                    source_state_dirty = True
                            except FloodHalt as halt:
                                copied_total += halt.copied
                                skipped_invalid_total += halt.skipped
                                skipped_invalid_runtime_total += halt.skipped
                                if halt.processed_ids:
                                    recent_ids.extend(halt.processed_ids)
                                    recent_set.update(halt.processed_ids)
                                    cursor_id = max(cursor_id, max(halt.processed_ids))
                                    source_state_dirty = True
                                if cursor_id > last_seen_id or source_state_dirty:
                                    _set_source_state(state, key, cursor_id, recent_ids)
                                    updated_source_keys.append(key)
                                    changed = True
                                if changed:
                                    _set_run_meta(
                                        state,
                                        status="halted",
                                        halted_at_ist=_now_ist_iso(),
                                        halted_at_utc=_now_utc_iso(),
                                        copied=copied_total,
                                        skipped_invalid=skipped_invalid_total,
                                        filtered_links=filtered_links_total,
                                        fetched=fetched_total,
                                        sources_with_new_messages=sources_with_new_messages,
                                    )
                                    state_message_id, did_update = await _save_state(
                                        client=client, state=state, state_message_id=state_message_id
                                    )
                                    state_updated = state_updated or did_update
                                run_snapshot_ids = await _save_run_window_ids(
                                    client=client,
                                    run_ts_ms=run_ts_ms,
                                    lookback_minutes=settings.lookback_minutes,
                                    run_status="halted",
                                    ids_by_source=run_window_ids_by_source,
                                )
                                return {
                                    "copied": copied_total,
                                    "skipped_invalid": skipped_invalid_total,
                                    "skipped_invalid_runtime": skipped_invalid_runtime_total,
                                    "skipped_duplicate_recent": skipped_duplicate_recent_total,
                                    "skipped_cursor_gate": skipped_cursor_gate_total,
                                    "skipped_blocked_ad": skipped_blocked_ad_total,
                                    "skipped_blocked_gif": skipped_blocked_gif_total,
                                    "filtered_links": filtered_links_total,
                                    "fetched": fetched_total,
                                    "sources_with_new_messages": sources_with_new_messages,
                                    "sources": len(settings.source_channel_ids),
                                    "flood_wait_seconds": int(halt.seconds),
                                    "halted": 1,
                                    "state_saved": int(state_updated),
                                    "state_message_id": state_message_id,
                                    "run_snapshot_count": len(run_snapshot_ids),
                                    "run_snapshot_message_ids": run_snapshot_ids,
                                    "updated_sources": sorted(set(updated_source_keys)),
                                }
                        continue
                else:
                    single_id = message_ids[0]
                    single_message = next((msg for msg in messages if int(getattr(msg, "id", 0)) == single_id), None)
                    if single_message is not None:
                        try:
                            copied_count, skipped_count, filtered_count, processed_ids = (
                                await _copy_single_with_optional_sanitize(
                                    client=client,
                                    destination_channel_id=settings.destination_channel_id,
                                    source_id=source_id,
                                    message=single_message,
                                )
                            )
                            copied_total += copied_count
                            skipped_invalid_total += skipped_count
                            skipped_invalid_runtime_total += skipped_count
                            filtered_links_total += filtered_count
                            if processed_ids:
                                recent_ids.extend(processed_ids)
                                recent_set.update(processed_ids)
                                cursor_id = max(cursor_id, max(processed_ids))
                                source_state_dirty = True
                        except FloodHalt as halt:
                            copied_total += halt.copied
                            skipped_invalid_total += halt.skipped
                            skipped_invalid_runtime_total += halt.skipped
                            if halt.processed_ids:
                                recent_ids.extend(halt.processed_ids)
                                recent_set.update(halt.processed_ids)
                                cursor_id = max(cursor_id, max(halt.processed_ids))
                                source_state_dirty = True
                            if cursor_id > last_seen_id or source_state_dirty:
                                _set_source_state(state, key, cursor_id, recent_ids)
                                updated_source_keys.append(key)
                                changed = True
                            if changed:
                                _set_run_meta(
                                    state,
                                    status="halted",
                                    halted_at_ist=_now_ist_iso(),
                                    halted_at_utc=_now_utc_iso(),
                                    copied=copied_total,
                                    skipped_invalid=skipped_invalid_total,
                                    filtered_links=filtered_links_total,
                                    fetched=fetched_total,
                                    sources_with_new_messages=sources_with_new_messages,
                                )
                                state_message_id, did_update = await _save_state(
                                    client=client, state=state, state_message_id=state_message_id
                                )
                                state_updated = state_updated or did_update
                            run_snapshot_ids = await _save_run_window_ids(
                                client=client,
                                run_ts_ms=run_ts_ms,
                                lookback_minutes=settings.lookback_minutes,
                                run_status="halted",
                                ids_by_source=run_window_ids_by_source,
                            )
                            return {
                                "copied": copied_total,
                                "skipped_invalid": skipped_invalid_total,
                                "skipped_invalid_runtime": skipped_invalid_runtime_total,
                                "skipped_duplicate_recent": skipped_duplicate_recent_total,
                                "skipped_cursor_gate": skipped_cursor_gate_total,
                                "skipped_blocked_ad": skipped_blocked_ad_total,
                                "skipped_blocked_gif": skipped_blocked_gif_total,
                                "filtered_links": filtered_links_total,
                                "fetched": fetched_total,
                                "sources_with_new_messages": sources_with_new_messages,
                                "sources": len(settings.source_channel_ids),
                                "flood_wait_seconds": int(halt.seconds),
                                "halted": 1,
                                "state_saved": int(state_updated),
                                "state_message_id": state_message_id,
                                "run_snapshot_count": len(run_snapshot_ids),
                                "run_snapshot_message_ids": run_snapshot_ids,
                                "updated_sources": sorted(set(updated_source_keys)),
                            }
                        continue

                try:
                    copied_count, skipped_count, processed_ids = await _forward_with_fallback(
                        client=client,
                        destination_channel_id=settings.destination_channel_id,
                        source_id=source_id,
                        message_ids=message_ids,
                        as_album=is_album and len(message_ids) > 1,
                    )
                    copied_total += copied_count
                    skipped_invalid_total += skipped_count
                    skipped_invalid_runtime_total += skipped_count
                    if processed_ids:
                        recent_ids.extend(processed_ids)
                        recent_set.update(processed_ids)
                        cursor_id = max(cursor_id, max(processed_ids))
                        source_state_dirty = True
                except FloodHalt as halt:
                    copied_total += halt.copied
                    skipped_invalid_total += halt.skipped
                    skipped_invalid_runtime_total += halt.skipped
                    if halt.processed_ids:
                        recent_ids.extend(halt.processed_ids)
                        recent_set.update(halt.processed_ids)
                        cursor_id = max(cursor_id, max(halt.processed_ids))
                        source_state_dirty = True
                    if cursor_id > last_seen_id or source_state_dirty:
                        _set_source_state(state, key, cursor_id, recent_ids)
                        updated_source_keys.append(key)
                        changed = True
                    if changed:
                        _set_run_meta(
                            state,
                            status="halted",
                            halted_at_ist=_now_ist_iso(),
                            halted_at_utc=_now_utc_iso(),
                            copied=copied_total,
                            skipped_invalid=skipped_invalid_total,
                            filtered_links=filtered_links_total,
                            fetched=fetched_total,
                            sources_with_new_messages=sources_with_new_messages,
                        )
                        state_message_id, did_update = await _save_state(
                            client=client, state=state, state_message_id=state_message_id
                        )
                        state_updated = state_updated or did_update
                    run_snapshot_ids = await _save_run_window_ids(
                        client=client,
                        run_ts_ms=run_ts_ms,
                        lookback_minutes=settings.lookback_minutes,
                        run_status="halted",
                        ids_by_source=run_window_ids_by_source,
                    )
                    return {
                        "copied": copied_total,
                        "skipped_invalid": skipped_invalid_total,
                        "skipped_invalid_runtime": skipped_invalid_runtime_total,
                        "skipped_duplicate_recent": skipped_duplicate_recent_total,
                        "skipped_cursor_gate": skipped_cursor_gate_total,
                        "skipped_blocked_ad": skipped_blocked_ad_total,
                        "skipped_blocked_gif": skipped_blocked_gif_total,
                        "filtered_links": filtered_links_total,
                        "fetched": fetched_total,
                        "sources_with_new_messages": sources_with_new_messages,
                        "sources": len(settings.source_channel_ids),
                        "flood_wait_seconds": int(halt.seconds),
                        "halted": 1,
                        "state_saved": int(state_updated),
                        "state_message_id": state_message_id,
                        "run_snapshot_count": len(run_snapshot_ids),
                        "run_snapshot_message_ids": run_snapshot_ids,
                        "updated_sources": sorted(set(updated_source_keys)),
                    }

            if cursor_id > last_seen_id or source_state_dirty:
                _set_source_state(state, key, cursor_id, recent_ids)
                updated_source_keys.append(key)
                changed = True
        except Exception as source_exc:
            source_errors[key] = f"{type(source_exc).__name__}: {source_exc}"
            continue

    if changed:
        run_status = "completed_with_errors" if source_errors else "completed"
        _set_run_meta(
            state,
            status=run_status,
            completed_at_ist=_now_ist_iso(),
            completed_at_utc=_now_utc_iso(),
            copied=copied_total,
            skipped_invalid=skipped_invalid_total,
            filtered_links=filtered_links_total,
            fetched=fetched_total,
            sources_with_new_messages=sources_with_new_messages,
            source_errors=source_errors,
        )
        state_message_id, did_update = await _save_state(client=client, state=state, state_message_id=state_message_id)
        state_updated = state_updated or did_update
    else:
        run_status = "completed_with_errors" if source_errors else "completed"

    run_snapshot_ids = await _save_run_window_ids(
        client=client,
        run_ts_ms=run_ts_ms,
        lookback_minutes=settings.lookback_minutes,
        run_status=run_status,
        ids_by_source=run_window_ids_by_source,
    )

    return {
        "copied": copied_total,
        "skipped_invalid": skipped_invalid_total,
        "skipped_invalid_runtime": skipped_invalid_runtime_total,
        "skipped_duplicate_recent": skipped_duplicate_recent_total,
        "skipped_cursor_gate": skipped_cursor_gate_total,
        "skipped_blocked_ad": skipped_blocked_ad_total,
        "skipped_blocked_gif": skipped_blocked_gif_total,
        "filtered_links": filtered_links_total,
        "fetched": fetched_total,
        "sources_with_new_messages": sources_with_new_messages,
        "sources": len(settings.source_channel_ids),
        "source_errors_count": len(source_errors),
        "source_errors": source_errors,
        "state_saved": int(state_updated),
        "state_message_id": state_message_id,
        "run_snapshot_count": len(run_snapshot_ids),
        "run_snapshot_message_ids": run_snapshot_ids,
        "updated_sources": sorted(set(updated_source_keys)),
    }


async def _copy_single_with_optional_sanitize(
    client: Any,
    destination_channel_id: int,
    source_id: int,
    message: Any,
) -> tuple[int, int, int, tuple[int, ...]]:
    from telethon.errors.rpcerrorlist import FloodWaitError

    message_id = int(getattr(message, "id", 0))
    if message_id <= 0:
        return 0, 1, 0, tuple()
    if _is_gif_message(message):
        return 0, 1, 0, (message_id,)

    original_text = (getattr(message, "message", None) or "").strip()
    cleaned_text = _sanitize_message_text(original_text)
    has_media = _has_sendable_media(message)
    is_sanitized = cleaned_text != original_text

    if is_sanitized:
        try:
            if has_media:
                await client.send_file(
                    entity=destination_channel_id,
                    file=message.media,
                    caption=cleaned_text or None,
                )
                return 1, 0, 1, (message_id,)

            if cleaned_text:
                await client.send_message(
                    entity=destination_channel_id,
                    message=cleaned_text,
                    link_preview=False,
                )
                return 1, 0, 1, (message_id,)

            # Text became empty after removing https://t.* links.
            return 0, 1, 1, (message_id,)
        except FloodWaitError as exc:
            raise FloodHalt(seconds=int(exc.seconds), processed_ids=tuple(), copied=0, skipped=0) from exc

    copied, skipped, processed_ids = await _forward_with_fallback(
        client=client,
        destination_channel_id=destination_channel_id,
        source_id=source_id,
        message_ids=[message_id],
        as_album=False,
    )
    return copied, skipped, 0, processed_ids


async def _forward_with_fallback(
    client: Any,
    destination_channel_id: int,
    source_id: int,
    message_ids: list[int],
    as_album: bool,
) -> tuple[int, int, tuple[int, ...]]:
    from telethon.errors import RPCError
    from telethon.errors.rpcerrorlist import ChatForwardsRestrictedError
    from telethon.errors.rpcerrorlist import FloodWaitError
    from telethon.errors.rpcerrorlist import MessageIdInvalidError

    def is_forwards_restricted(exc: Exception) -> bool:
        if isinstance(exc, ChatForwardsRestrictedError):
            return True
        if isinstance(exc, RPCError):
            text = f"{exc.__class__.__name__}: {exc}".upper()
            return "CHATFORWARDSRESTRICTED" in text or "CHAT_FORWARDS_RESTRICTED" in text
        return False

    def is_skippable_forward_error(exc: Exception) -> bool:
        if isinstance(exc, MessageIdInvalidError):
            return True
        if isinstance(exc, ChatForwardsRestrictedError):
            return True
        if isinstance(exc, RPCError):
            text = f"{exc.__class__.__name__}: {exc}".upper()
            return (
                "MESSAGEIDINVALID" in text
                or "MESSAGE_ID_INVALID" in text
                or "CHATFORWARDSRESTRICTED" in text
                or "CHAT_FORWARDS_RESTRICTED" in text
            )
        return False

    try:
        await client.forward_messages(
            entity=destination_channel_id,
            messages=message_ids if as_album else message_ids[0],
            from_peer=source_id,
            as_album=as_album if as_album else None,
            drop_author=True,
            drop_media_captions=False,
        )
        return len(message_ids), 0, tuple(message_ids)
    except Exception as exc:
        if isinstance(exc, FloodWaitError):
            raise FloodHalt(seconds=int(exc.seconds), processed_ids=tuple(), copied=0, skipped=0) from exc
        if is_forwards_restricted(exc):
            # Fallback: repost content directly (no forward) for protected chats.
            return await _repost_messages_without_forward(
                client=client,
                destination_channel_id=destination_channel_id,
                source_id=source_id,
                message_ids=message_ids,
            )
        if not is_skippable_forward_error(exc):
            raise
        # Some message IDs cannot be forwarded/copied (deleted/unsupported/restricted).
        # For albums, degrade to per-item forwarding so valid items can still pass through.
        if as_album and len(message_ids) > 1:
            copied = 0
            skipped = 0
            processed: list[int] = []
            for mid in message_ids:
                try:
                    await client.forward_messages(
                        entity=destination_channel_id,
                        messages=mid,
                        from_peer=source_id,
                        drop_author=True,
                        drop_media_captions=False,
                    )
                    copied += 1
                    processed.append(mid)
                except Exception as inner_exc:
                    if isinstance(inner_exc, FloodWaitError):
                        raise FloodHalt(
                            seconds=int(inner_exc.seconds),
                            processed_ids=tuple(processed),
                            copied=copied,
                            skipped=skipped,
                        ) from inner_exc
                    if not is_skippable_forward_error(inner_exc):
                        raise
                    skipped += 1
                    processed.append(mid)
            return copied, skipped, tuple(processed)
        # Single message that is invalid/restricted: skip and mark as processed for dedupe.
        return 0, len(message_ids), tuple(message_ids)


async def _repost_messages_without_forward(
    client: Any,
    destination_channel_id: int,
    source_id: int,
    message_ids: list[int],
) -> tuple[int, int, tuple[int, ...]]:
    from telethon.errors.rpcerrorlist import FloodWaitError

    fetched = await client.get_messages(source_id, ids=message_ids)
    messages = list(fetched) if isinstance(fetched, (list, tuple)) else [fetched]
    by_id = {int(getattr(msg, "id", 0)): msg for msg in messages if getattr(msg, "id", None)}

    copied = 0
    skipped = 0
    processed: list[int] = []

    for mid in message_ids:
        msg = by_id.get(int(mid))
        if msg is None:
            skipped += 1
            processed.append(int(mid))
            continue
        if _is_gif_message(msg):
            skipped += 1
            processed.append(int(mid))
            continue

        text = (getattr(msg, "message", None) or "").strip()
        cleaned_text = _sanitize_message_text(text)
        has_media = _has_sendable_media(msg)
        try:
            if has_media:
                await client.send_file(
                    entity=destination_channel_id,
                    file=msg.media,
                    caption=cleaned_text or None,
                )
                copied += 1
                processed.append(int(mid))
                continue

            if cleaned_text:
                await client.send_message(
                    entity=destination_channel_id,
                    message=cleaned_text,
                    link_preview=False,
                )
                copied += 1
                processed.append(int(mid))
                continue

            skipped += 1
            processed.append(int(mid))
        except FloodWaitError as inner_exc:
            raise FloodHalt(
                seconds=int(inner_exc.seconds),
                processed_ids=tuple(processed),
                copied=copied,
                skipped=skipped,
            ) from inner_exc

    return copied, skipped, tuple(processed)


async def _run_once(settings: AppwriteSettings) -> dict[str, Any]:
    try:
        from telethon import TelegramClient
        from telethon.errors.rpcerrorlist import AuthKeyDuplicatedError
        from telethon.sessions import StringSession
    except Exception as exc:
        raise RuntimeError(
            "Telethon runtime dependency is unavailable. "
            "Verify function build/deploy installed requirements.txt correctly."
        ) from exc

    client = TelegramClient(StringSession(settings.session_string), settings.api_id, settings.api_hash)
    try:
        await client.connect()
    except AuthKeyDuplicatedError as exc:
        raise RuntimeError(
            "SESSION_STRING invalidated by Telegram (AuthKeyDuplicatedError). "
            "Generate a NEW session string and set it in Appwrite. "
            "Ensure the same session is not used from multiple IPs/runtimes."
        ) from exc
    try:
        if not await client.is_user_authorized():
            raise RuntimeError("SESSION_STRING is not authorized. Create a valid user session string first.")
        return await _copy_source_messages(client=client, settings=settings)
    finally:
        await client.disconnect()


def _run_once_sync(settings: AppwriteSettings) -> dict[str, Any]:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(_run_once(settings))

    # If host runtime already has an active loop, run in an isolated worker thread.
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(lambda: asyncio.run(_run_once(settings)))
        return future.result()


def main(context: Any):
    _safe_log(context, "User sync execution started.")
    try:
        settings = _load_settings_from_env()
        _safe_log(
            context,
            "Starting user sync run "
            f"(sources={len(settings.source_channel_ids)}, destination={settings.destination_channel_id})",
        )
        result = _run_once_sync(settings)
        _safe_log(
            context,
            "Sync completed successfully: "
            f"copied={result.get('copied', 0)}, "
            f"fetched={result.get('fetched', 0)}, "
            f"skipped_invalid={result.get('skipped_invalid', 0)}, "
            f"skipped_invalid_runtime={result.get('skipped_invalid_runtime', 0)}, "
            f"skipped_duplicate_recent={result.get('skipped_duplicate_recent', 0)}, "
            f"skipped_cursor_gate={result.get('skipped_cursor_gate', 0)}, "
            f"skipped_blocked_ad={result.get('skipped_blocked_ad', 0)}, "
            f"skipped_blocked_gif={result.get('skipped_blocked_gif', 0)}, "
            f"filtered_links={result.get('filtered_links', 0)}, "
            f"sources_with_new_messages={result.get('sources_with_new_messages', 0)}, "
            f"source_errors_count={result.get('source_errors_count', 0)}, "
            f"halted={result.get('halted', 0)}, "
            f"flood_wait_seconds={result.get('flood_wait_seconds', 0)}, "
            f"state_saved={result.get('state_saved', 0)}, "
            f"run_snapshot_count={result.get('run_snapshot_count', 0)}, "
            f"updated_sources={len(result.get('updated_sources', []))}, "
            f"state_message_id={result.get('state_message_id')}",
        )
        if result.get("source_errors"):
            _safe_log(context, f"Source errors: {json.dumps(result.get('source_errors'), ensure_ascii=True)}", is_error=True)
        return _respond_json(context, {"ok": True, **result})
    except Exception as exc:
        _safe_log(context, f"Sync failed: {type(exc).__name__}: {exc}", is_error=True)
        _safe_log(context, traceback.format_exc(), is_error=True)
        return _respond_json(context, {"ok": False, "error": str(exc)}, 500)
