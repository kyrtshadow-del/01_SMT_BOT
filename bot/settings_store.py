from __future__ import annotations

import json
from pathlib import Path
from threading import Lock
from typing import Any, Dict


class SettingsStore:
    """Thread-safe JSON-backed store for per-user bot settings."""

    def __init__(self, path: Path) -> None:
        self._path = Path(path)
        self._lock = Lock()
        self._path.parent.mkdir(parents=True, exist_ok=True)

    def load(self, chat_id: int) -> Dict[str, Any]:
        payload = self._read_all()
        entry = payload.get(str(chat_id))
        if isinstance(entry, dict):
            return dict(entry)
        return {}

    def save(self, chat_id: int, settings: Dict[str, Any]) -> None:
        clean = {key: value for key, value in settings.items() if not key.startswith("_")}
        with self._lock:
            payload = self._read_all()
            payload[str(chat_id)] = clean
            self._path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def _read_all(self) -> Dict[str, Dict[str, Any]]:
        if not self._path.exists():
            return {}
        try:
            data = json.loads(self._path.read_text(encoding="utf-8"))
        except Exception:
            return {}
        if not isinstance(data, dict):
            return {}
        return {key: value for key, value in data.items() if isinstance(value, dict)}


__all__ = ["SettingsStore"]
