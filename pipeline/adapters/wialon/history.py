"""Adapter that reuses existing loader to dump history into RawStorage."""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import DefaultDict, Iterable, List, Sequence, TYPE_CHECKING

from pipeline.adapters.base import HistoryAdapter
from pipeline.events import Event, UnitSnapshot
from pipeline.storage.raw_storage import RawStorage

if TYPE_CHECKING:
    from bot_new import WialonClient
log = logging.getLogger(__name__)


def _day_key(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).date().isoformat()


def _safe_float(value) -> float | None:
    try:
        return float(value)
    except Exception:
        return None


@dataclass
class WialonHistoryAdapter(HistoryAdapter):
    client: "WialonClient"
    raw_storage: RawStorage
    label: str = "wialon"
    flags: int = 0x0000
    flags_mask: int = 0xFF00
    load_count: int = 0xFFFFFFFF

    async def fetch_history(
        self,
        unit_ids: Sequence[int],
        start_ts: int,
        end_ts: int,
    ) -> Iterable[Event]:
        semaphore = asyncio.Semaphore(
            max(1, getattr(self.client, "max_parallel", 4))
        )
        events: List[Event] = []

        async def _fetch(unit_id: int) -> List[Event]:
            async with semaphore:
                return await asyncio.to_thread(
                    self._load_unit_with_token,
                    unit_id,
                    start_ts,
                    end_ts,
                )

        results = await asyncio.gather(
            *[_fetch(unit_id) for unit_id in unit_ids],
            return_exceptions=True,
        )
        for unit_id, result in zip(unit_ids, results):
            if isinstance(result, Exception):  # pragma: no cover - defensive
                log.warning("history: unit=%s failed err=%s", unit_id, result)
                continue
            if not result:
                continue
            self._persist(result)
            events.extend(result)
        return events

    def _load_unit_with_token(
        self,
        unit_id: int,
        start_ts: int,
        end_ts: int,
    ) -> List[Event]:
        with self.client.lease() as lease:
            log.info(
                "history: token=%s unit=%s fetching window=%s..%s",
                lease.label,
                unit_id,
                start_ts,
                end_ts,
            )
            resp = lease.client.load_messages_interval(
                unit_id,
                start_ts,
                end_ts,
                flags=self.flags,
                flags_mask=self.flags_mask,
                load_count=self.load_count,
            )
            events = self._parse_messages(unit_id, resp)
            if events:
                log.info(
                    "history: token=%s unit=%s events=%s",
                    lease.label,
                    unit_id,
                    len(events),
                )
            return events

    async def list_units(self) -> Iterable[UnitSnapshot]:
        # TODO: use search_items (оставим заглушку, чтобы интерфейс был полным)
        return []

    def _parse_messages(self, unit_id: int, resp: dict | list) -> List[Event]:
        if isinstance(resp, dict) and resp.get("error"):
            err = resp.get("error")
            if err == 6:  # no data
                log.debug("history: unit=%s empty window", unit_id)
                return []
            if err == 7:  # access denied
                log.warning("history: access denied unit=%s, skipping", unit_id)
                return []
            raise RuntimeError(f"Wialon error {err} in messages/load_interval")
        if isinstance(resp, dict):
            messages = resp.get("messages") or []
        elif isinstance(resp, list):
            messages = resp
        else:
            messages = []

        events: List[Event] = []
        for message in messages:
            event = self._build_event(unit_id, message)
            if event is not None:
                events.append(event)
        log.info(
            "history: fetched unit=%s events=%s window=%s..%s",
            unit_id,
            len(events),
            messages[0].get("t") if messages else None,
            messages[-1].get("t") if messages else None,
        )
        return events

    def _build_event(self, unit_id: int, payload: dict) -> Event | None:
        if not isinstance(payload, dict):
            return None
        ts = int(payload.get("t") or payload.get("time") or 0)
        if ts <= 0:
            return None
        received_ts = int(payload.get("rt") or payload.get("serverTime") or ts)
        lat = payload.get("y")
        if lat is None:
            lat = payload.get("lat")
        lon = payload.get("x")
        if lon is None:
            lon = payload.get("lon")
        speed = payload.get("s")
        if speed is None:
            speed = payload.get("speed")
        course = payload.get("c")
        if course is None:
            course = payload.get("course")
        params = payload.get("p") or payload.get("params") or {}
        return Event(
            unit_id=unit_id,
            device_ts=ts,
            received_ts=received_ts,
            latitude=_safe_float(lat),
            longitude=_safe_float(lon),
            speed=_safe_float(speed),
            course=_safe_float(course),
            params=params,
            source=self.label,
            raw_payload=payload,
        )

    def _persist(self, events: Iterable[Event]) -> None:
        grouped: DefaultDict[str, List[Event]] = defaultdict(list)
        for event in events:
            grouped[_day_key(event.device_ts)].append(event)
        for day_key, chunk in grouped.items():
            count = self.raw_storage.append(day_key, chunk)
            log.info("history: stored day=%s events=%s", day_key, count)
