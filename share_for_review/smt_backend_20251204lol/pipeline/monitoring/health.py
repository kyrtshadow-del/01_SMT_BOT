"""Simple JSON-based health reporter for pipeline components."""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Dict


class HealthReporter:
    """Writes health snapshots to a JSON file (atomic replace)."""

    def __init__(self, path: Path | str | None = None) -> None:
        self.path = Path(path or Path("logs") / "pipeline_health.json")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        # optional HTTP endpoint
        self._last_payload: Dict[str, Any] | None = None

    def report(self, payload: Dict[str, Any]) -> None:
        tmp_name = f"{self.path.name}.tmp-{os.getpid()}-{int(time.time() * 1000)}"
        tmp_path = self.path.with_name(tmp_name)
        with tmp_path.open("w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=2)
        tmp_path.replace(self.path)
        self._last_payload = payload

    def latest(self) -> Dict[str, Any] | None:
        return self._last_payload
