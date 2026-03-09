# AI Integration Guide (Telegram Copier)

## Can AI be added?

Yes. Your current bot can be extended with AI in the Appwrite function flow.

## Good AI Use Cases for This Project

1. Message classification
- Decide whether to copy/skip by intent (`news`, `ad`, `spam`, `promo`, `urgent`).

2. Content rewrite
- Clean or reformat text before sending to destination.

3. Translation
- Auto-translate source posts before forwarding.

4. Summarization
- Summarize long posts and send concise versions.

5. Safety moderation
- Block unsafe/abusive content beyond regex rules.

## Recommended Architecture

Current flow:
- Fetch messages -> apply rules -> forward/repost.

AI-enabled flow:
- Fetch messages -> base filters (dedupe/ad/gif) -> AI decision/transform -> send -> update state.

Why this order:
- Keep cheap deterministic filters first.
- Call AI only for eligible messages to reduce cost/latency.

## Where to Add in Code

Main function file:
- `appwrite/functions/webhook/src/main.py`

Best insertion points:
1. Before `_copy_single_with_optional_sanitize(...)`
- Ask AI if message should be copied.

2. Before `send_message/send_file`
- Ask AI to transform caption/text.

## Suggested AI Decision Contract

Return JSON from AI:

```json
{
  "action": "copy|skip|rewrite",
  "reason": "short reason",
  "rewritten_text": "optional"
}
```

Rules:
- `skip` -> mark as processed (avoid retries).
- `copy` -> send original.
- `rewrite` -> send rewritten text/caption.

## Config You Should Add

Function environment variables:
- `AI_ENABLED=0|1`
- `AI_MODE=classify|rewrite|translate|summary`
- `AI_MODEL=<model-name>`
- `AI_API_KEY=<secret>`
- `AI_TIMEOUT_MS=3000`
- `AI_MAX_CHARS=2000`

Fail-safe flags:
- `AI_FAIL_OPEN=1` (if AI fails, still copy)
- `AI_FAIL_OPEN=0` (if AI fails, skip)

## Production Safety Checklist

1. Start with shadow mode
- Run AI, log decisions, but do not affect sending.

2. Add metrics
- `ai_calls`, `ai_skips`, `ai_rewrites`, `ai_errors`, `ai_latency_ms`.

3. Set strict timeout
- If AI is slow, do not block whole run.

4. Budget control
- Only AI-check messages that pass base filters.

5. Prompt hardening
- Keep prompt short, deterministic, and JSON-only output.

## Example Rollout Plan

1. Phase 1: `AI_ENABLED=1`, shadow mode only.
2. Phase 2: enforce `skip` decisions for obvious promo/spam.
3. Phase 3: enable rewrite/translation for selected channels.
4. Phase 4: tune prompts and thresholds using logs.

## Important Notes

- AI can increase runtime and API cost.
- Keep Appwrite timeout high enough for peak loads.
- Do not send secrets in prompts.
- Keep deterministic fallback logic active even when AI is enabled.
