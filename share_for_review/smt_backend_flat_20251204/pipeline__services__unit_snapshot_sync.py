"""Helpers to synchronize UnitSnapshotService with cached payloads."""

from __future__ import annotations

import logging
from typing import Dict, Iterable, List, Mapping, Optional, Tuple

from pipeline.services.unit_snapshot_service import (
    UnitSnapshotRecord,
    UnitSnapshotService,
)

log = logging.getLogger("unit_snapshot_sync")


def _safe_int(value) -> Optional[int]:
    try:
        if value is None:
            return None
        return int(value)
    except Exception:
        return None


def records_from_payload(payload: Mapping[str, object]) -> Tuple[List[UnitSnapshotRecord], Optional[int]]:
    """Convert UNIT_STORE-like payload into UnitSnapshotRecord list + dump timestamp."""

    items = payload.get("items_by_id")
    records: List[UnitSnapshotRecord] = []
    if isinstance(items, dict):
        for key, raw in items.items():
            try:
                unit_id = int(key)
            except Exception:
                continue
            if not isinstance(raw, dict):
                continue
            try:
                records.append(UnitSnapshotRecord.from_dict(unit_id, raw))
            except Exception as exc:  # pragma: no cover - defensive logging
                log.debug("unit_snapshot_sync: failed to parse unit_id=%s err=%s", unit_id, exc)
    dump_ts = _safe_int(payload.get("dump_ts"))
    return records, dump_ts


def sync_unit_snapshot_from_payload(
    payload: Mapping[str, object],
    service: UnitSnapshotService,
    *,
    source_kind: str = "unit_cache",
    force: bool = False,
) -> bool:
    """Update UnitSnapshotService from cached payload; returns True if refresh happened."""

    records, dump_ts = records_from_payload(payload)
    if not records:
        return False
    bundle = service.load_bundle()
    if (
        not force
        and bundle.dump_ts is not None
        and dump_ts is not None
        and dump_ts <= bundle.dump_ts
    ):
        return False
    service.refresh(records, source_kind=source_kind, dump_ts=dump_ts)
    log.info(
        "unit_snapshot_sync: refreshed units=%s dump_ts=%s source=%s force=%s",
        len(records),
        dump_ts,
        source_kind,
        force,
    )
    return True


__all__ = ["records_from_payload", "sync_unit_snapshot_from_payload"]
