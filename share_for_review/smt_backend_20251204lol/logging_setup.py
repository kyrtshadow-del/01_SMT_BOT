"""Logging helpers for detector JSON logs."""
from __future__ import annotations

import json
import logging
import logging.handlers
from pathlib import Path
from typing import Any, Dict

_MAX_BYTES = 10 * 1024 * 1024
_BACKUP_COUNT = 5


class _JsonFormatter(logging.Formatter):
    """Formatter that emits structured JSON lines."""

    def format(self, record: logging.LogRecord) -> str:  # type: ignore[override]
        payload: Dict[str, Any]
        extra_payload = getattr(record, "json_payload", None)
        if isinstance(extra_payload, dict):
            payload = dict(extra_payload)
        else:
            payload = {}
        payload.setdefault("message", record.getMessage())
        payload.setdefault("level", record.levelname)
        payload.setdefault("ts", int(record.created))
        if "name" not in payload:
            payload["logger"] = record.name
        return json.dumps(payload, ensure_ascii=False)


def setup_logging(base_dir: Path) -> logging.Logger:
    """Configure and return the detector logger with JSONL output."""

    base_dir = base_dir.resolve()
    base_dir.mkdir(parents=True, exist_ok=True)
    log_path = base_dir / "detector.log"

    logger = logging.getLogger("detector")
    logger.setLevel(logging.INFO)
    logger.propagate = False

    for handler in logger.handlers:
        if isinstance(handler, logging.handlers.RotatingFileHandler) and Path(handler.baseFilename) == log_path:
            return logger

    handler = logging.handlers.RotatingFileHandler(
        log_path, maxBytes=_MAX_BYTES, backupCount=_BACKUP_COUNT, encoding="utf-8"
    )
    handler.setFormatter(_JsonFormatter())
    logger.addHandler(handler)
    return logger
