"""Persistence layer for normalised telemetry events."""

from __future__ import annotations

import json
import threading
import time
from pathlib import Path
from typing import Iterable, List, Sequence, Tuple

from pipeline.events import Event
import logging


_log = logging.getLogger("pipeline.raw_storage")


class RawStorage:
    """Simple file-based event storage (JSON lines placeholder).

    Parquet/Arrow will replace this once the pipeline stabilises, but even эта
    реализация уже изолирует format от остального кода.
    """

    def __init__(self, root: Path) -> None:
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)
        self._write_lock = threading.Lock()
        # In-process dedup to avoid double-appends on retries (unit_id, device_ts)
        self._recent_keys: dict[Tuple[int, int], float] = {}
        self._recent_limit = 5000
        # size limit (bytes) for events.jsonl before rotating; default 50 MB, override via env PIPELINE_RAW_MAX_BYTES
        import os
        self._max_bytes = int(float(os.getenv("PIPELINE_RAW_MAX_BYTES", 50 * 1024 * 1024)))

    @staticmethod
    def day_key(ts: int) -> str:
        return time.strftime("%Y-%m-%d", time.gmtime(ts))

    def _day_path(self, day_key: str) -> Path:
        day_dir = self.root / day_key
        day_dir.mkdir(parents=True, exist_ok=True)
        return day_dir

    def append(self, day_key: str, events: Iterable[Event]) -> int:
        """Persist events for the given day. Returns count."""

        buffer: List[str] = []
        now = time.time()
        for event in events:
            key = (event.unit_id, event.device_ts)
            if key in self._recent_keys:
                # seen recently; skip duplicate
                continue
            self._recent_keys[key] = now
            buffer.append(json.dumps(event.as_dict(), ensure_ascii=False))
        if not buffer:
            return 0
        # trim dedup cache
        if len(self._recent_keys) > self._recent_limit:
            cutoff = now - 3600  # keep last hour of keys
            self._recent_keys = {k: ts for k, ts in self._recent_keys.items() if ts >= cutoff}
        file_path = self._day_path(day_key) / "events.jsonl"
        payload = "\n".join(buffer) + "\n"
        with self._write_lock:
            # rotate if exceeds max bytes
            try:
                if file_path.exists() and file_path.stat().st_size + len(payload.encode("utf-8")) > self._max_bytes:
                    ts_suffix = time.strftime("%H%M%S", time.gmtime(now))
                    rotated = file_path.with_name(f"events-{ts_suffix}.jsonl")
                    file_path.rename(rotated)
            except OSError:
                pass
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
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError as exc:
                    _log.warning("raw_storage: bad json line skipped day=%s path=%s err=%s", day_key, file_path, exc)
                    continue
                if unit_id is not None and payload.get("unit_id") != unit_id:
                    continue
                try:
                    events.append(Event(**payload))
                except Exception as exc:  # защитимся от битых схем
                    _log.warning("raw_storage: bad event payload skipped day=%s path=%s err=%s", day_key, file_path, exc)
                    continue
        return events
