from __future__ import annotations

import importlib.util
from pathlib import Path
from types import SimpleNamespace


MODULE_PATH = (
    Path(__file__).resolve().parents[1] / "appwrite" / "functions" / "album_flush" / "src" / "main.py"
)


def load_module():
    spec = importlib.util.spec_from_file_location("appwrite_album_flush_main", MODULE_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class FakeRes:
    def __init__(self) -> None:
        self.payload = None
        self.status = None

    def json(self, payload, status=200):  # noqa: ANN001
        self.payload = payload
        self.status = status
        return payload


def test_album_flush_returns_ok() -> None:
    module = load_module()
    res = FakeRes()
    context = SimpleNamespace(req=SimpleNamespace(), res=res)

    module.main(context)

    assert res.status == 200
    assert res.payload["ok"] is True
    assert res.payload["status"] == "noop"
