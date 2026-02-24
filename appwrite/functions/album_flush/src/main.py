import json
from typing import Any


def _respond_json(context: Any, payload: dict[str, Any], status_code: int = 200):
    res = context.res
    try:
        return res.json(payload, status_code)
    except TypeError:
        try:
            return res.json(payload)
        except TypeError:
            return res.send(json.dumps(payload), status_code)


def main(context: Any):
    return _respond_json(
        context,
        {
            "ok": True,
            "status": "noop",
            "message": "Album flush placeholder deployed successfully.",
        },
    )
