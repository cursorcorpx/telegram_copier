import asyncio
import concurrent.futures
import json
import os
import re
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any


STATE_MARKER = "telegram_copier_state_v1:"
RUN_MARKER = "telegram_copier_run_v1:"
TELEGRAM_LINK_PATTERN = re.compile(r"(?:https?://)?(?:www\.)?(?:t\.me|telegram\.me)/\S+", re.IGNORECASE)
YOUTUBE_PATTERN = re.compile(r"(?:https?://)?(?:www\.)?youtube\.com[^\s]*", re.IGNORECASE)
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
DEFAULT_FUNCTION_NAME = "telegram_copier_run_v1"
BUILD_VERSION = "2026-03-09-inline-entity-preserve-v6"
DB_LOG_LAST_ERROR = ""
DB_STATE_LAST_ERROR = ""
STATE_ROW_ID = "state_global_v1"


def _appwrite_endpoint() -> str:
    return (
        os.getenv("APPWRITE_FUNCTION_API_ENDPOINT", "").strip()
        or os.getenv("APPWRITE_API_ENDPOINT", "").strip()
        or os.getenv("APPWRITE_ENDPOINT", "").strip()
    )


def _appwrite_project_id() -> str:
    return os.getenv("APPWRITE_FUNCTION_PROJECT_ID", "").strip() or os.getenv("APPWRITE_PROJECT_ID", "").strip()


def _appwrite_api_key() -> str:
    return (
        os.getenv("APPWRITE_FUNCTION_API_KEY", "").strip()
        or os.getenv("APPWRITE_KEY", "").strip()
        or os.getenv("APPWRITE_API_KEY", "").strip()
    )


def _appwrite_jwt() -> str:
    return os.getenv("APPWRITE_FUNCTION_JWT", "").strip() or os.getenv("APPWRITE_JWT", "").strip()


def _has_appwrite_auth() -> bool:
    return bool(_appwrite_api_key() or _appwrite_jwt())


def _appwrite_auth_mode() -> str:
    if _appwrite_api_key():
        return "api_key"
    if _appwrite_jwt():
        return "jwt"
    return "none"


def _log_database_id() -> str:
    return os.getenv("APPWRITE_LOGS_DATABASE_ID", "").strip() or os.getenv("APPWRITE_DATABASE_ID", "").strip()


def _log_collection_id() -> str:
    return os.getenv("APPWRITE_LOGS_COLLECTION_ID", "").strip() or "telegram_copier_logs"


def _state_database_id() -> str:
    return os.getenv("APPWRITE_STATE_DATABASE_ID", "").strip() or _log_database_id()


def _state_collection_id() -> str:
    return os.getenv("APPWRITE_STATE_COLLECTION_ID", "").strip() or "telegram_copier_state"


def _is_db_logging_enabled() -> bool:
    return bool(_appwrite_endpoint() and _has_appwrite_auth() and _log_database_id())


def _is_db_state_enabled() -> bool:
    return bool(_appwrite_endpoint() and _has_appwrite_auth() and _state_database_id())


def _db_logging_disable_reason() -> str:
    missing: list[str] = []
    if not _appwrite_endpoint():
        missing.append("endpoint")
    if not _has_appwrite_auth():
        missing.append("auth")
    if not _log_database_id():
        missing.append("database_id")
    if not missing:
        return ""
    return "db_logging_disabled_missing_" + ",".join(missing)


def _db_state_disable_reason() -> str:
    missing: list[str] = []
    if not _appwrite_endpoint():
        missing.append("endpoint")
    if not _has_appwrite_auth():
        missing.append("auth")
    if not _state_database_id():
        missing.append("database_id")
    if not missing:
        return ""
    return "db_state_disabled_missing_" + ",".join(missing)


def _appwrite_env_diagnostics() -> dict[str, Any]:
    return {
        "build_version": BUILD_VERSION,
        "appwrite_endpoint_set": int(bool(_appwrite_endpoint())),
        "appwrite_project_id_set": int(bool(_appwrite_project_id())),
        "appwrite_auth_set": int(bool(_has_appwrite_auth())),
        "appwrite_database_id_set": int(bool(_log_database_id())),
        "appwrite_state_database_id_set": int(bool(_state_database_id())),
        "appwrite_logs_collection_id": _log_collection_id(),
        "appwrite_state_collection_id": _state_collection_id(),
    }


def _appwrite_headers() -> dict[str, str]:
    headers = {"Content-Type": "application/json"}
    project_id = _appwrite_project_id()
    if project_id:
        headers["X-Appwrite-Project"] = project_id
    api_key = _appwrite_api_key()
    jwt = _appwrite_jwt()
    if api_key:
        headers["X-Appwrite-Key"] = api_key
    elif jwt:
        headers["X-Appwrite-JWT"] = jwt
    return headers


def _appwrite_build_url(path: str, queries: list[str] | None = None) -> str:
    base = _appwrite_endpoint().rstrip("/")
    # Support endpoints both with and without trailing /v1.
    if base.endswith("/v1"):
        url = f"{base}{path}"
    else:
        url = f"{base}/v1{path}"
    if queries:
        query_pairs = [("queries[]", query) for query in queries]
        url += "?" + urllib.parse.urlencode(query_pairs)
    return url


def _appwrite_request_json(method: str, path: str, payload: dict[str, Any] | None = None, queries: list[str] | None = None) -> dict[str, Any]:
    body = json.dumps(payload).encode("utf-8") if payload is not None else None
    request = urllib.request.Request(
        _appwrite_build_url(path=path, queries=queries),
        data=body,
        headers=_appwrite_headers(),
        method=method,
    )
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            raw = response.read().decode("utf-8") or "{}"
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                return parsed
            return {}
    except urllib.error.HTTPError as exc:
        details = ""
        try:
            details = (exc.read() or b"").decode("utf-8", errors="replace").strip()
        except Exception:
            details = ""
        if details:
            raise RuntimeError(f"HTTP {exc.code} {exc.reason}: {details[:600]}") from exc
        raise RuntimeError(f"HTTP {exc.code} {exc.reason}") from exc


def _appwrite_create_document(database_id: str, collection_id: str, data: dict[str, Any]) -> dict[str, Any]:
    payload = {"documentId": "unique()", "data": data}
    try:
        return _appwrite_request_json(
            method="POST",
            path=f"/databases/{database_id}/collections/{collection_id}/documents",
            payload=payload,
        )
    except Exception:
        # TablesDB fallback for newer Appwrite projects.
        try:
            # Prefer server-generated row ID to avoid collisions.
            return _appwrite_request_json(
                method="POST",
                path=f"/tablesdb/{database_id}/tables/{collection_id}/rows",
                payload={"data": data},
            )
        except Exception:
            pass
        last_exc: Exception | None = None
        for _ in range(5):
            tables_payload = {"rowId": uuid.uuid4().hex, "data": data}
            try:
                return _appwrite_request_json(
                    method="POST",
                    path=f"/tablesdb/{database_id}/tables/{collection_id}/rows",
                    payload=tables_payload,
                )
            except Exception as exc:
                last_exc = exc
                if "row_already_exists" in str(exc).lower() or "already exists" in str(exc).lower():
                    continue
                raise
        if last_exc is not None:
            raise last_exc
        raise RuntimeError("Failed to create Appwrite row after retries.")


def _appwrite_list_documents(database_id: str, collection_id: str, queries: list[str]) -> list[dict[str, Any]]:
    try:
        response = _appwrite_request_json(
            method="GET",
            path=f"/databases/{database_id}/collections/{collection_id}/documents",
            queries=queries,
        )
    except Exception:
        response = _appwrite_request_json(
            method="GET",
            path=f"/tablesdb/{database_id}/tables/{collection_id}/rows",
            queries=queries,
        )
    documents = response.get("documents", [])
    if isinstance(documents, list) and documents:
        return [doc for doc in documents if isinstance(doc, dict)]
    rows = response.get("rows", [])
    if isinstance(rows, list):
        return [row for row in rows if isinstance(row, dict)]
    if isinstance(documents, list):
        return [doc for doc in documents if isinstance(doc, dict)]
    return []


def _serialize_message_ids(ids: list[int]) -> str:
    return ",".join(str(int(mid)) for mid in ids if isinstance(mid, int))


def _parse_serialized_message_ids(raw: str) -> list[int]:
    parsed: list[int] = []
    for token in (raw or "").split(","):
        token = token.strip()
        if not token:
            continue
        try:
            parsed.append(int(token))
        except ValueError:
            continue
    return parsed


def _parse_message_ids_json_value(value: Any) -> list[int]:
    if isinstance(value, list):
        return [int(mid) for mid in value if isinstance(mid, int)]
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return [int(mid) for mid in parsed if isinstance(mid, int)]
        except Exception:
            return []
    return []


def _strip_unknown_attribute_from_error(payload: dict[str, Any], error_text: str) -> bool:
    normalized = (error_text or "").replace('\\"', '"')
    match = re.search(r'Unknown attribute:\s*"([^"]+)"', normalized, re.IGNORECASE)
    if not match:
        return False
    attr = match.group(1)
    if attr in payload:
        payload.pop(attr, None)
        return True
    return False


def _coerce_json_like_fields_to_text(payload: dict[str, Any], fields: tuple[str, ...]) -> bool:
    changed = False
    for field in fields:
        if field not in payload:
            continue
        value = payload.get(field)
        if value is None or isinstance(value, str):
            continue
        try:
            payload[field] = json.dumps(value, separators=(",", ":"), ensure_ascii=True)
            changed = True
        except Exception:
            continue
    return changed


def _adjust_json_field_by_error(payload: dict[str, Any], field: str, error_text: str) -> bool:
    normalized = (error_text or "").replace('\\"', '"').lower()
    if field not in payload:
        return False

    value = payload.get(field)
    if field == "message_ids_json" and "message_ids_json['" in normalized and "valid string" in normalized:
        if isinstance(value, list):
            payload[field] = [str(item) for item in value]
            return True
        return False

    if f'"{field.lower()}" must be an array' in normalized:
        if isinstance(value, list):
            return False
        if isinstance(value, str):
            try:
                parsed = json.loads(value)
                if isinstance(parsed, list):
                    payload[field] = parsed
                    return True
            except Exception:
                parsed_ids = _parse_serialized_message_ids(value)
                payload[field] = parsed_ids
                return True
        payload[field] = []
        return True

    if (
        f'"{field.lower()}" must be a string' in normalized
        or f'"{field.lower()}" must be text' in normalized
        or (field.lower() in normalized and "valid string" in normalized)
    ):
        if isinstance(value, str):
            return False
        try:
            payload[field] = json.dumps(value, separators=(",", ":"), ensure_ascii=True)
            return True
        except Exception:
            return False

    return False


def _build_log_part_payload(
    run_ts_ms: int,
    lookback_minutes: int,
    run_status: str,
    source_key: str,
    part: str,
    message_ids: list[int],
) -> dict[str, Any]:
    return {
        "function_name": DEFAULT_FUNCTION_NAME,
        "run_ts_ms": int(run_ts_ms),
        "status": run_status,
        "lookback_minutes": int(lookback_minutes),
        "source_id": int(source_key),
        "part": part,
        "message_ids_json": [int(mid) for mid in message_ids if isinstance(mid, int)],
        "message_ids": _serialize_message_ids(message_ids),
        "created_at": _now_utc_iso(),
    }


def log_copier_execution(log_data: dict[str, Any]) -> bool:
    """Persist log to Appwrite DB. Never raises and retries once."""
    global DB_LOG_LAST_ERROR
    if not _is_db_logging_enabled():
        DB_LOG_LAST_ERROR = _db_logging_disable_reason() or "db_logging_disabled_missing_env_or_auth"
        return False

    database_id = _log_database_id()
    collection_id = _log_collection_id()
    current = dict(log_data)
    for attempt in (1, 2, 3):
        try:
            _appwrite_create_document(database_id=database_id, collection_id=collection_id, data=current)
            DB_LOG_LAST_ERROR = ""
            return True
        except Exception as exc:
            if _strip_unknown_attribute_from_error(current, str(exc)):
                continue
            if _adjust_json_field_by_error(current, "message_ids_json", str(exc)):
                continue
            if attempt == 1:
                # Compact payload for strict schemas (small varchar / type mismatch).
                current = {
                    "function_name": str(current.get("function_name", DEFAULT_FUNCTION_NAME))[:120],
                    "run_ts_ms": int(current.get("run_ts_ms", int(time.time() * 1000))),
                    "status": str(current.get("status", "completed"))[:60],
                    "lookback_minutes": int(current.get("lookback_minutes", 60)),
                    "source_id": int(current.get("source_id", 0)),
                    "part": str(current.get("part", "1/1"))[:24],
                    "message_ids_json": [],
                    "message_ids": str(current.get("message_ids", ""))[:240],
                    "created_at": _now_utc_iso(),
                }
                time.sleep(0.25)
                continue
            DB_LOG_LAST_ERROR = f"{type(exc).__name__}: {exc}"
            return False
    return False


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


def _build_state_payload_with_limit(state: dict[str, Any], max_chars: int) -> str:
    payload = _build_state_payload(state)
    if len(payload) <= max_chars:
        return payload

    sources = state.get("sources", {}) if isinstance(state.get("sources"), dict) else {}
    minimal_sources: dict[str, dict[str, int]] = {}
    for source_key in sorted(sources.keys()):
        source_entry = sources.get(source_key)
        if not isinstance(source_entry, dict):
            continue
        last_id = source_entry.get("last_id", 0)
        minimal_sources[str(source_key)] = {"last_id": int(last_id) if isinstance(last_id, int) else 0}

    candidate = _state_payload_text({"sources": minimal_sources, "meta": {"state_compacted": 1}})
    if len(candidate) <= max_chars:
        return candidate

    source_keys = sorted(minimal_sources.keys())
    while source_keys:
        current = {k: minimal_sources[k] for k in source_keys}
        candidate = _state_payload_text({"sources": current, "meta": {"state_compacted": 1, "truncated": 1}})
        if len(candidate) <= max_chars:
            return candidate
        source_keys.pop(0)

    return _state_payload_text({"sources": {}, "meta": {"state_compacted": 1, "truncated": 1}})


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
    saved_markers: list[int] = []
    marker = 0
    ids_max_chars_raw = os.getenv("DB_LOG_IDS_MAX_CHARS", "800").strip()
    try:
        ids_max_chars = int(ids_max_chars_raw)
    except ValueError:
        ids_max_chars = 800
    ids_max_chars = max(64, min(2800, ids_max_chars))
    for source_key in sorted(ids_by_source.keys()):
        raw_ids = ids_by_source.get(source_key, [])
        tokens = [str(int(mid)) for mid in raw_ids if isinstance(mid, int)]
        if not tokens:
            continue
        parts = _split_id_tokens(tokens, max_chars=ids_max_chars)
        total_parts = len(parts)

        for idx, part_tokens in enumerate(parts, start=1):
            part_info = f"{idx}/{total_parts}"
            part_ids = [int(token) for token in part_tokens if token]
            payload = _build_log_part_payload(
                run_ts_ms=run_ts_ms,
                lookback_minutes=lookback_minutes,
                run_status=run_status,
                source_key=source_key,
                part=part_info,
                message_ids=part_ids,
            )
            if log_copier_execution(payload):
                marker += 1
                saved_markers.append(marker)
    return saved_markers


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
        if _is_db_logging_enabled():
            documents = _appwrite_list_documents(
                database_id=_log_database_id(),
                collection_id=_log_collection_id(),
                queries=[
                    "orderDesc($createdAt)",
                    "limit(500)",
                    f'equal("function_name",["{DEFAULT_FUNCTION_NAME}"])',
                ],
            )
            for doc in documents:
                run_ts_ms = doc.get("run_ts_ms")
                source_id = doc.get("source_id")
                ids_raw = doc.get("message_ids", "")
                ids_json = doc.get("message_ids_json")
                if not isinstance(run_ts_ms, int) or source_id is None:
                    continue
                if run_ts_ms < cutoff_ms:
                    continue
                source_key = str(source_id)
                if source_key not in by_source:
                    by_source[source_key] = set()
                parsed_ids_json = _parse_message_ids_json_value(ids_json)
                if parsed_ids_json:
                    by_source[source_key].update(parsed_ids_json)
                else:
                    by_source[source_key].update(_parse_serialized_message_ids(str(ids_raw)))
            return by_source
    except Exception:
        # Snapshot recovery is best-effort and should never block main forwarding.
        pass

    # Backward compatibility fallback: read previous snapshots from Saved Messages.
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
    cleaned = text
    modified = False
    strip_t_links = os.getenv("STRIP_T_LINKS", "0").strip().lower() in ("1", "true", "yes", "on")
    strip_youtube = os.getenv("STRIP_YOUTUBE_LINKS", "0").strip().lower() in ("1", "true", "yes", "on")
    bad_words_csv = os.getenv("FILTER_BAD_WORDS", "").strip()

    # Optional filters; defaults preserve links exactly as in source.
    if strip_t_links:
        updated = TELEGRAM_LINK_PATTERN.sub("", cleaned)
        modified = modified or updated != cleaned
        cleaned = updated
    if strip_youtube:
        updated = YOUTUBE_PATTERN.sub("", cleaned)
        modified = modified or updated != cleaned
        cleaned = updated
    if bad_words_csv:
        bad_words = [re.escape(part.strip()) for part in bad_words_csv.split(",") if part.strip()]
        if bad_words:
            bad_words_pattern = re.compile(r"\b(?:" + "|".join(bad_words) + r")\b", re.IGNORECASE)
            updated = bad_words_pattern.sub("", cleaned)
            modified = modified or updated != cleaned
            cleaned = updated

    if not modified:
        return text

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


def _is_telegram_state_fallback_enabled() -> bool:
    return os.getenv("ENABLE_TELEGRAM_STATE_FALLBACK", "0").strip().lower() in ("1", "true", "yes", "on")


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


async def _load_state(client: Any) -> tuple[dict[str, Any], str | int | None]:
    if _is_db_state_enabled():
        try:
            documents = _appwrite_list_documents(
                database_id=_state_database_id(),
                collection_id=_state_collection_id(),
                queries=['equal("state_key",["global"])', "limit(1)"],
            )
            if documents:
                document = documents[0]
                payload_json = document.get("payload_json")
                if isinstance(payload_json, str):
                    try:
                        payload_json = json.loads(payload_json)
                    except Exception:
                        payload_json = None
                if isinstance(payload_json, dict):
                    if isinstance(payload_json.get("sources"), dict):
                        document_id = document.get("$id")
                        return payload_json, str(document_id) if isinstance(document_id, str) else None
                payload_text = document.get("payload", "")
                if isinstance(payload_text, str) and payload_text:
                    parsed = _extract_state(payload_text)
                    if parsed:
                        document_id = document.get("$id")
                        return parsed, str(document_id) if isinstance(document_id, str) else None
        except Exception:
            # Non-fatal: fallback to legacy Telegram state.
            pass

    if not _is_telegram_state_fallback_enabled():
        return {}, None

    # Legacy fallback for migration safety (opt-in).
    try:
        async for message in client.iter_messages("me", search=STATE_MARKER, limit=1):
            text = (message.message or "").strip()
            if text.startswith(STATE_MARKER):
                return _extract_state(text), message.id
    except Exception:
        pass

    try:
        messages = await client.get_messages("me", limit=200)
    except Exception:
        messages = []
    for message in messages:
        text = (getattr(message, "message", "") or "").strip()
        if text.startswith(STATE_MARKER):
            return _extract_state(text), getattr(message, "id", None)
    return {}, None


async def _save_state(client: Any, state: dict[str, Any], state_message_id: str | int | None) -> tuple[str | int | None, bool]:
    global DB_STATE_LAST_ERROR
    state_payload = _build_state_payload(state)
    if _is_db_state_enabled():
        database_id = _state_database_id()
        collection_id = _state_collection_id()
        last_db_exc: Exception | None = None
        state_payload_max_raw = os.getenv("DB_STATE_PAYLOAD_MAX_CHARS", "1800").strip()
        try:
            state_payload_max = int(state_payload_max_raw)
        except ValueError:
            state_payload_max = 1800
        state_payload_max = max(120, min(MAX_STATE_PAYLOAD_CHARS, state_payload_max))
        state_payload_db = _build_state_payload_with_limit(state=state, max_chars=state_payload_max)
        state_json = _compact_state_for_save(state)
        payload = {
            "state_key": "global",
            # Store as JSON text because the deployed Appwrite schema uses a longtext column.
            "payload_json": json.dumps(state_json, separators=(",", ":"), ensure_ascii=True),
            "payload": state_payload_db,
            "run_ts_ms": int(time.time() * 1000),
        }
        if not isinstance(state_message_id, str):
            state_message_id = STATE_ROW_ID
        for attempt in (1, 2, 3):
            try:
                if isinstance(state_message_id, str):
                    try:
                        _appwrite_request_json(
                            method="PATCH",
                            path=f"/tablesdb/{database_id}/tables/{collection_id}/rows/{state_message_id}",
                            payload={"data": payload},
                        )
                    except Exception as patch_exc:
                        patch_text = str(patch_exc).lower()
                        if "404" in patch_text or "not found" in patch_text:
                            try:
                                _appwrite_request_json(
                                    method="PATCH",
                                    path=f"/databases/{database_id}/collections/{collection_id}/documents/{state_message_id}",
                                    payload={"data": payload},
                                )
                            except Exception as legacy_patch_exc:
                                legacy_patch_text = str(legacy_patch_exc).lower()
                                if "404" in legacy_patch_text or "not found" in legacy_patch_text:
                                    try:
                                        _appwrite_request_json(
                                            method="POST",
                                            path=f"/tablesdb/{database_id}/tables/{collection_id}/rows",
                                            payload={"rowId": state_message_id, "data": payload},
                                        )
                                    except Exception as create_tables_exc:
                                        create_tables_text = str(create_tables_exc).lower()
                                        if "already exists" in create_tables_text:
                                            continue
                                        try:
                                            _appwrite_request_json(
                                                method="POST",
                                                path=f"/databases/{database_id}/collections/{collection_id}/documents",
                                                payload={"documentId": state_message_id, "data": payload},
                                            )
                                        except Exception as create_doc_exc:
                                            if "already exists" in str(create_doc_exc).lower():
                                                continue
                                            raise
                                else:
                                    raise
                        else:
                            raise
                    DB_STATE_LAST_ERROR = ""
                    return state_message_id, True
                created = _appwrite_create_document(database_id=database_id, collection_id=collection_id, data=payload)
                created_id = created.get("$id")
                DB_STATE_LAST_ERROR = ""
                return (str(created_id) if isinstance(created_id, str) else state_message_id), True
            except Exception as exc:
                last_db_exc = exc
                if "row_already_exists" in str(exc).lower() or "already exists" in str(exc).lower():
                    # Row already exists under deterministic state ID; treat as retryable update.
                    continue
                if _strip_unknown_attribute_from_error(payload, str(exc)):
                    continue
                if _adjust_json_field_by_error(payload, "payload_json", str(exc)):
                    continue
                if attempt == 1:
                    # Retry with ultra-compact payload for strict schemas.
                    payload["payload"] = _build_state_payload_with_limit(state=state, max_chars=240)
                    time.sleep(0.25)
                    continue
                DB_STATE_LAST_ERROR = f"{type(exc).__name__}: {exc}"
                return state_message_id, False
        if last_db_exc is not None:
            DB_STATE_LAST_ERROR = f"{type(last_db_exc).__name__}: {last_db_exc}"
            return state_message_id, False

    if not _is_telegram_state_fallback_enabled():
        DB_STATE_LAST_ERROR = _db_state_disable_reason() or "db_state_unavailable_and_telegram_fallback_disabled"
        return state_message_id, False

    # Legacy fallback (opt-in).
    try:
        from telethon.errors.rpcerrorlist import MessageNotModifiedError

        if isinstance(state_message_id, int) and state_message_id:
            try:
                await client.edit_message("me", state_message_id, state_payload)
                return state_message_id, True
            except MessageNotModifiedError:
                return state_message_id, False
            except Exception:
                sent = await client.send_message("me", state_payload)
                return getattr(sent, "id", None), True
        sent = await client.send_message("me", state_payload)
        return getattr(sent, "id", None), True
    except Exception:
        DB_STATE_LAST_ERROR = "telegram_fallback_failed"
        return state_message_id, False


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
                        _sanitize_message_text(getattr(msg, "message", None) or "")
                        != (getattr(msg, "message", "") or "")
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
        "state_backend": "appwrite_db" if isinstance(state_message_id, str) else "telegram_saved_messages",
        "db_logging_enabled": int(_is_db_logging_enabled()),
        "db_state_enabled": int(_is_db_state_enabled()),
        "db_auth_mode": _appwrite_auth_mode(),
        "db_log_last_error": DB_LOG_LAST_ERROR,
        "db_state_last_error": DB_STATE_LAST_ERROR,
        **_appwrite_env_diagnostics(),
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

    original_text = getattr(message, "message", None) or ""
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

        raw_text = getattr(msg, "message", None) or ""
        text = raw_text
        cleaned_text = _sanitize_message_text(text)
        entities = getattr(msg, "entities", None) if cleaned_text == text else None
        has_media = _has_sendable_media(msg)
        try:
            if has_media:
                send_file_kwargs = {
                    "entity": destination_channel_id,
                    "file": msg.media,
                    "caption": cleaned_text or None,
                }
                if entities:
                    send_file_kwargs["formatting_entities"] = entities
                await client.send_file(**send_file_kwargs)
                copied += 1
                processed.append(int(mid))
                continue

            if cleaned_text:
                send_message_kwargs = {
                    "entity": destination_channel_id,
                    "message": cleaned_text,
                    "link_preview": False,
                }
                if entities:
                    send_message_kwargs["formatting_entities"] = entities
                await client.send_message(**send_message_kwargs)
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
            f"build_version={result.get('build_version')}, "
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
            f"state_backend={result.get('state_backend')}, "
            f"db_logging_enabled={result.get('db_logging_enabled', 0)}, "
            f"db_state_enabled={result.get('db_state_enabled', 0)}, "
            f"db_auth_mode={result.get('db_auth_mode')}, "
            f"appwrite_endpoint_set={result.get('appwrite_endpoint_set', 0)}, "
            f"appwrite_auth_set={result.get('appwrite_auth_set', 0)}, "
            f"appwrite_database_id_set={result.get('appwrite_database_id_set', 0)}, "
            f"appwrite_state_collection_id={result.get('appwrite_state_collection_id')}, "
            f"run_snapshot_count={result.get('run_snapshot_count', 0)}, "
            f"updated_sources={len(result.get('updated_sources', []))}, "
            f"state_message_id={result.get('state_message_id')}",
        )
        if result.get("db_log_last_error"):
            _safe_log(context, f"DB log write issue: {result.get('db_log_last_error')}", is_error=True)
        if result.get("db_state_last_error"):
            _safe_log(context, f"DB state write issue: {result.get('db_state_last_error')}", is_error=True)
        if result.get("source_errors"):
            _safe_log(context, f"Source errors: {json.dumps(result.get('source_errors'), ensure_ascii=True)}", is_error=True)
        return _respond_json(context, {"ok": True, **result})
    except Exception as exc:
        _safe_log(context, f"Sync failed: {type(exc).__name__}: {exc}", is_error=True)
        _safe_log(context, traceback.format_exc(), is_error=True)
        return _respond_json(context, {"ok": False, "error": str(exc)}, 500)
