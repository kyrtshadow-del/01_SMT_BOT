"""Persistence layer for normalised telemetry events."""

from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Iterable, List, Sequence

from pipeline.events import Event


class RawStorage:
    """Simple file-based event storage (JSON lines placeholder).

    Parquet/Arrow will replace this once the pipeline stabilises, but even эта
    реализация уже изолирует format от остального кода.
    """

    def __init__(self, root: Path) -> None:
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)
        self._write_lock = threading.Lock()

    def _day_path(self, day_key: str) -> Path:
        day_dir = self.root / day_key
        day_dir.mkdir(parents=True, exist_ok=True)
        return day_dir

    def append(self, day_key: str, events: Iterable[Event]) -> int:
        """Persist events for the given day. Returns count."""

        buffer: List[str] = []
        for event in events:
            buffer.append(json.dumps(event.as_dict(), ensure_ascii=False))
        if not buffer:
            return 0
        file_path = self._day_path(day_key) / "events.jsonl"
        payload = "\n".join(buffer) + "\n"
        with self._write_lock:
            with file_path.open("a", encoding="utf-8") as fh:
                fh.write(payload)
        return len(buffer)

    def fetch(self, day_key: str, unit_id: int | None = None) -> Sequence[Event]:
        """Read all events for the specified day."""

        file_path = self._day_path(day_key) / "events.jsonl"
        if not file_path.exists():
            return []
        events: List[Event] = []
        with file_path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                payload = json.loads(line)
                if unit_id is not None and payload.get("unit_id") != unit_id:
                    continue
                events.append(Event(**payload))
        return events
