from __future__ import annotations

import copy
import gzip
import json
import logging
import os
import shutil
import threading
import time
import traceback
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

geo_log = logging.getLogger("geo")
app_log = logging.getLogger("wialon_cf_bot")
drain_cache_log = logging.getLogger("drain_cache_watch")

class ZoneFileStore:
    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._payload: Optional[Dict[str, Any]] = None
        self._payload_mtime: Optional[float] = None
        self.mtime: Optional[float] = None
        self.strategy: Optional[str] = None
        self.flags_used: Optional[int] = None

    def load_if_present(self) -> Optional[Dict[str, Any]]:
        try:
            stat = self.path.stat()
        except FileNotFoundError:
            with self._lock:
                self._payload = None
                self._payload_mtime = None
                self.mtime = None
                self.strategy = None
                self.flags_used = None
            return None
        with self._lock:
            if (
                isinstance(self._payload, dict)
                and self._payload_mtime == stat.st_mtime
            ):
                self.mtime = stat.st_mtime
                return copy.deepcopy(self._payload)
        try:
            with gzip.open(self.path, "rt", encoding="utf-8") as fh:
                payload = json.load(fh)
        except FileNotFoundError:
            return None
        except Exception as exc:
            geo_log.error("file_cache: failed to read %s: %s", self.path, exc)
            return None
        if not isinstance(payload, dict):
            geo_log.error(
                "file_cache: invalid payload type %s in %s",
                type(payload).__name__,
                self.path,
            )
            return None
        with self._lock:
            self._payload = payload
            self._payload_mtime = stat.st_mtime
            self.mtime = stat.st_mtime
            self.strategy = payload.get("strategy") if isinstance(payload.get("strategy"), str) else None
            flags_val = payload.get("flags_used")
            self.flags_used = int(flags_val) if isinstance(flags_val, int) else None
        return copy.deepcopy(payload)

    def save_atomic(self, payload: Dict[str, Any]) -> None:
        tmp_name = f"{self.path.name}.tmp-{os.getpid()}-{threading.get_ident()}"
        tmp_path = self.path.with_name(tmp_name)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        serializable = copy.deepcopy(payload)
        try:
            with gzip.open(tmp_path, "wt", encoding="utf-8") as fh:
                json.dump(serializable, fh, ensure_ascii=False)
        except Exception:
            try:
                tmp_path.unlink()
            except FileNotFoundError:
                pass
            except Exception as exc:
                geo_log.debug("file_cache: failed to remove tmp %s: %s", tmp_path, exc)
            raise
        os.replace(tmp_path, self.path)
        try:
            stat = self.path.stat()
        except FileNotFoundError:
            stat = None
        with self._lock:
            self._payload = serializable
            self._payload_mtime = stat.st_mtime if stat else None
            self.mtime = stat.st_mtime if stat else None
            self.strategy = serializable.get("strategy") if isinstance(serializable.get("strategy"), str) else None
            flags_val = serializable.get("flags_used")
            self.flags_used = int(flags_val) if isinstance(flags_val, int) else None

@dataclass
class MessageCacheDownloadResult:
    status: str
    day: str
    label: str
    start_ts: int
    end_ts: int
    completed: int
    total: int
    failed: int
    payload: Optional[Dict[str, Any]] = None

__all__ = ["ZoneFileStore", "MessageCacheDownloadResult"]
