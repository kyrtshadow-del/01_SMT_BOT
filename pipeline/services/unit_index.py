"""Local index over units table in Postgres to power search."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import Dict, List, Optional

try:
    import psycopg  # type: ignore
except Exception:  # pragma: no cover
    psycopg = None  # type: ignore


log = logging.getLogger(__name__)


@dataclass
class _UnitEntry:
    unit_id: int
    name: str
    searchable: str
    summary: Dict[str, object]


class UnitIndex:
    """Loads units from Postgres and performs simple substring search."""

    def __init__(self, dsn: Optional[str] = None) -> None:
        self.dsn = dsn or _dsn_from_env()
        self._lock = Lock()
        self._entries: List[_UnitEntry] | None = None
        self._by_id: Dict[int, _UnitEntry] | None = None

    def _build_entry(self, unit_id: int, name: str, reg_number: Optional[str]) -> _UnitEntry:
        nm = name or f"id {unit_id}"
        search_parts: List[str] = [nm.casefold(), str(unit_id)]
        summary: Dict[str, object] = {"id": unit_id, "nm": nm}

        if reg_number:
            reg = reg_number.strip()
            if reg:
                summary["reg_number"] = reg
                search_parts.append(reg.casefold())

        entry = _UnitEntry(
            unit_id=unit_id,
            name=name,
            searchable=" ".join(search_parts),
            summary=summary,
        )
        return entry

    def _ensure_loaded(self) -> None:
        if self._entries is not None and self._by_id is not None:
            return
        with self._lock:
            if self._entries is not None and self._by_id is not None:
                return
            entries: List[_UnitEntry] = []
            by_id: Dict[int, _UnitEntry] = {}
            if psycopg is None or not self.dsn:
                log.warning("UnitIndex: psycopg or DSN missing, index is empty")
                self._entries = entries
                self._by_id = by_id
                return
            try:
                with psycopg.connect(self.dsn) as conn:  # type: ignore[arg-type]
                    cur = conn.cursor()
                    try:
                        # New schema (schema_pg.sql) with reg_number
                        cur.execute("SELECT id, name, reg_number FROM units")
                        rows = cur.fetchall()
                        for row in rows:
                            unit_id, name, reg = row
                            entry = self._build_entry(int(unit_id), str(name), reg)
                            entries.append(entry)
                            by_id[int(unit_id)] = entry
                    except Exception:
                        # Fallback: legacy schema without reg_number
                        cur.execute("SELECT id, name FROM units")
                        rows = cur.fetchall()
                        for row in rows:
                            unit_id, name = row
                            entry = self._build_entry(int(unit_id), str(name), None)
                            entries.append(entry)
                            by_id[int(unit_id)] = entry
            except Exception as exc:
                log.warning("UnitIndex: failed to load units from DB: %s", exc)
            self._entries = entries
            self._by_id = by_id

    def search(self, query: str, limit: int = 50) -> List[Dict[str, object]]:
        normalized = query.strip().casefold()
        if not normalized:
            return []
        self._ensure_loaded()
        entries = self._entries or []
        matches: List[_UnitEntry] = []
        for entry in entries:
            if normalized in entry.searchable:
                matches.append(entry)
        matches.sort(
            key=lambda entry: (
                0 if entry.name.casefold().startswith(normalized) else 1,
                entry.name.casefold(),
            )
        )
        return [dict(entry.summary) for entry in matches[:limit]]

    def get(self, unit_id: int) -> Optional[Dict[str, object]]:
        self._ensure_loaded()
        by_id = self._by_id or {}
        entry = by_id.get(unit_id)
        if entry is None:
            return None
        return dict(entry.summary)


_INDEX: UnitIndex | None = None
_INDEX_LOCK = Lock()


def get_unit_index() -> UnitIndex:
    """Singleton accessor for the default unit index based on Postgres."""

    global _INDEX
    if _INDEX is not None:
        return _INDEX
    with _INDEX_LOCK:
        if _INDEX is None:
            _INDEX = UnitIndex()
    return _INDEX


def _dsn_from_env() -> Optional[str]:
    return os.getenv("DEVICE_REGISTRY_DSN") or os.getenv("DATABASE_URL")
