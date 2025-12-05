"""Store for the latest telemetry snapshot per unit."""

from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Iterable, Iterator, Mapping, Tuple, Dict, Any

from pipeline.events import Event


class LatestTelemetryStore:
    """Tracks the latest telemetry payload for each unit."""

    def __init__(self, root: Path) -> None:
        self.path = Path(root) / "latest_metrics.json"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._payload: dict[str, dict] | None = None

    def _load(self) -> dict[str, dict]:
        if self._payload is not None:
            return self._payload
        if not self.path.exists():
            self._payload = {}
            return self._payload
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
        except Exception:
            data = {}
        if not isinstance(data, dict):
            data = {}
        self._payload = {str(k): v for k, v in data.items() if isinstance(v, dict)}
        return self._payload

    def _save(self) -> None:
        tmp_path = self.path.with_suffix(".tmp")
        payload = self._payload or {}
        tmp_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        tmp_path.replace(self.path)

    def update_from_events(self, events: Iterable[Event]) -> None:
        events = list(events)
        if not events:
            return
        with self._lock:
            data = self._load()
            updated = False
            for event in events:
                entry = {
                    "device_ts": event.device_ts,
                    "received_ts": event.received_ts,
                    "params": event.params or {},
                }
                if event.latitude is not None:
                    entry["lat"] = event.latitude
                if event.longitude is not None:
                    entry["lon"] = event.longitude
                if event.speed is not None:
                    entry["speed"] = event.speed
                if event.course is not None:
                    entry["course"] = event.course
                data[str(event.unit_id)] = entry
                updated = True
            if updated:
                self._save()

    def get_latest(self, unit_id: int) -> Mapping[str, object] | None:
        with self._lock:
            data = self._load()
            entry = data.get(str(unit_id))
            if not isinstance(entry, dict):
                return None
            return dict(entry)

    def iter_latest(self) -> Iterator[Tuple[int, Dict[str, Any]]]:
        with self._lock:
            data = self._load()
            for key, entry in data.items():
                if not isinstance(entry, dict):
                    continue
                try:
                    unit_id = int(key)
                except (TypeError, ValueError):
                    continue
                yield unit_id, dict(entry)
