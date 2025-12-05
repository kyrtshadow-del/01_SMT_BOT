"""Local index over unit snapshot to power offline search."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import Dict, List, Optional, Union

from pipeline.services.unit_snapshot_service import (
    UnitSnapshotRecord,
    UnitSnapshotService,
    get_unit_snapshot_service,
)


@dataclass
class _UnitEntry:
    unit_id: int
    name: str
    searchable: str
    summary: Dict[str, object]


class UnitIndex:
    """Loads the unified unit snapshot and performs simple substring search."""

    def __init__(self, source: Union[UnitSnapshotService, Path, str, None] = None) -> None:
        if isinstance(source, UnitSnapshotService):
            self.service = source
        elif isinstance(source, (str, Path)):
            self.service = UnitSnapshotService(snapshot_path=source)
        else:
            self.service = get_unit_snapshot_service()
        self._lock = Lock()
        self._entries: List[_UnitEntry] | None = None
        self._by_id: Dict[int, _UnitEntry] | None = None
        self._bundle_marker: int | None = None

    def _build_entry(self, record: UnitSnapshotRecord) -> _UnitEntry:
        name = record.name or f"id {record.unit_id}"
        search_parts: List[str] = [name.casefold(), str(record.unit_id)]

        summary: Dict[str, object] = {"id": record.unit_id, "nm": name}

        if record.reg_number:
            reg = str(record.reg_number).strip()
            if reg:
                summary["reg_number"] = reg
                search_parts.append(reg.casefold())

        primary_contact: Optional[str] = None
        contacts = record.contacts or {}
        for value in contacts.values():
            text = str(value or "").strip()
            if not text:
                continue
            if primary_contact is None:
                primary_contact = text
            search_parts.append(text.casefold())
        if primary_contact:
            summary["ph"] = primary_contact

        device_uid = record.device.uid
        if device_uid:
            search_parts.append(device_uid.casefold())
        device_hw = record.device.hardware
        if device_hw:
            search_parts.append(str(device_hw).casefold())

        entry = _UnitEntry(
            unit_id=record.unit_id,
            name=name,
            searchable=" ".join(search_parts),
            summary=summary,
        )
        return entry

    def _ensure_loaded(self) -> None:
        bundle = self.service.load_bundle()
        marker = id(bundle)
        if self._entries is not None and self._bundle_marker == marker:
            return
        with self._lock:
            if self._entries is not None and self._bundle_marker == marker:
                return
            entries: List[_UnitEntry] = []
            by_id: Dict[int, _UnitEntry] = {}
            for record in bundle.units.values():
                entry = self._build_entry(record)
                entries.append(entry)
                by_id[record.unit_id] = entry
            self._entries = entries
            self._by_id = by_id
            self._bundle_marker = marker

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


def get_unit_index(source: Union[UnitSnapshotService, Path, str, None] = None) -> UnitIndex:
    """Singleton accessor for the default unit index."""

    global _INDEX
    if source is not None:
        return UnitIndex(source)
    if _INDEX is not None:
        return _INDEX
    with _INDEX_LOCK:
        if _INDEX is None:
            _INDEX = UnitIndex()
    return _INDEX
