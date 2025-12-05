"""Stream adapter that polls Wialon for latest messages."""

from __future__ import annotations

import asyncio
import heapq
import logging
import random
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
import math
from concurrent.futures import ThreadPoolExecutor
from typing import (
    AsyncIterator,
    Deque,
    DefaultDict,
    Dict,
    Iterable,
    List,
    Sequence,
    Set,
    Tuple,
    TYPE_CHECKING,
)

if TYPE_CHECKING:
    from bot_new import WialonClient
from pipeline.events import Event
from pipeline.services.location_enricher import LocationEnricher
from pipeline.monitoring.health import HealthReporter
from pipeline.storage.raw_storage import RawStorage
from pipeline.storage.latest_metrics import LatestTelemetryStore
from pipeline.sources.base import PollStrategy

log = logging.getLogger(__name__)


def _day_key(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).date().isoformat()


def _safe_float(value) -> float | None:
    try:
        return float(value)
    except Exception:
        return None


@dataclass(order=True)
class _ScheduledEntry:
    next_poll: float
    seq: int
    unit_id: int


@dataclass
class WialonStreamAdapter:
    client: "WialonClient"
    raw_storage: RawStorage
    unit_ids: Sequence[int]
    poll_interval: float = 5.0
    label: str = "wialon_stream"
    flags: int = 0
    flags_mask: int = 0
    load_count: int = 64
    metrics_interval: int = 60
    _last_ts: Dict[int, int] = field(default_factory=dict)
    _metrics_counter: int = 0
    _metrics_started: float = field(default_factory=time.perf_counter)
    _max_workers: int = 1
    _executor: ThreadPoolExecutor | None = field(default=None, init=False, repr=False)
    _retry_queue: Deque[int] = field(default_factory=deque, init=False, repr=False)
    _retry_attempts: Dict[int, int] = field(default_factory=dict, init=False, repr=False)
    _retry_pending: Set[int] = field(default_factory=set, init=False, repr=False)
    max_retry_attempts: int = 5
    health_report_path: Path | None = None
    _health_reporter: HealthReporter | None = field(default=None, init=False, repr=False)
    latest_store: LatestTelemetryStore | None = None
    location_enricher: LocationEnricher | None = None
    poll_strategy: PollStrategy | None = None
    _schedule_heap: list["_ScheduledEntry"] = field(default_factory=list, init=False, repr=False)
    _schedule_index: Dict[int, float] = field(default_factory=dict, init=False, repr=False)
    _schedule_seq: int = field(default=0, init=False, repr=False)
    _empty_streaks: Dict[int, int] = field(default_factory=dict, init=False, repr=False)

    def __post_init__(self) -> None:
        if self.health_report_path is None:
            self.health_report_path = Path("logs/pipeline_health.json")
        self._health_reporter = HealthReporter(self.health_report_path)
        if self.poll_strategy is None:
            self.poll_strategy = PollStrategy()
        self._init_schedule()

    def _init_schedule(self) -> None:
        now = time.monotonic()
        spacing = 0.001
        for idx, unit_id in enumerate(self.unit_ids):
            self._schedule_unit(unit_id, now + idx * spacing)

    def _schedule_unit(self, unit_id: int, next_poll: float) -> None:
        self._schedule_seq += 1
        entry = _ScheduledEntry(next_poll=next_poll, seq=self._schedule_seq, unit_id=unit_id)
        self._schedule_index[unit_id] = next_poll
        heapq.heappush(self._schedule_heap, entry)

    def _next_due_units(self) -> tuple[List[int], float]:
        strategy = self.poll_strategy or PollStrategy()
        max_batch = max(1, strategy.max_batch_units)
        pending = self._consume_retry_units(max_batch)
        now = time.monotonic()
        while len(pending) < max_batch and self._schedule_heap:
            head = self._schedule_heap[0]
            index_val = self._schedule_index.get(head.unit_id)
            if index_val is not None and head.next_poll != index_val:
                heapq.heappop(self._schedule_heap)
                continue
            if head.next_poll > now and not pending:
                wait = min(self.poll_interval, head.next_poll - now)
                return [], max(0.05, wait)
            if head.next_poll > now:
                break
            heapq.heappop(self._schedule_heap)
            self._schedule_index.pop(head.unit_id, None)
            pending.append(head.unit_id)
        if pending:
            return pending, 0.0
        if self._schedule_heap:
            wait = self._schedule_heap[0].next_poll - now
            return [], max(0.05, min(self.poll_interval, wait))
        return [], self.poll_interval

    def _plan_chunks(self, units: Sequence[int]) -> List[Tuple[int, Sequence[int]]]:
        if not units:
            return []
        if self._max_workers <= 1 or len(units) <= 1:
            return [(0, units)]
        base_chunk = math.ceil(len(units) / self._max_workers)
        rotate_limit = max(1, getattr(self.client, "rotate_after_units", base_chunk))
        chunk_size = max(1, min(base_chunk, rotate_limit))
        plan: List[Tuple[int, Sequence[int]]] = []
        for index, start in enumerate(range(0, len(units), chunk_size)):
            plan.append((index, units[start : start + chunk_size]))
        return plan

    def _schedule_after_poll(self, outcomes: Dict[int, bool]) -> None:
        now = time.monotonic()
        for unit_id, had_new in outcomes.items():
            if unit_id in self._retry_pending:
                continue
            interval = self._compute_interval(unit_id, had_new)
            self._schedule_unit(unit_id, now + interval)

    async def subscribe(self) -> AsyncIterator[Event]:
        log.info(
            "stream: starting poller units=%s interval=%ss",
            len(self.unit_ids),
            self.poll_interval,
        )
        self._max_workers = max(
            1,
            min(
                getattr(self.client, "max_parallel", 1),
                len(self.unit_ids) or 1,
            ),
        )
        self._ensure_executor()
        loop = asyncio.get_running_loop()
        try:
            self._metrics_started = time.perf_counter()
            while True:
                due_units, wait_hint = self._next_due_units()
                if not due_units:
                    await asyncio.sleep(max(0.05, wait_hint))
                    continue
                chunk_plan = self._plan_chunks(due_units)
                chunk_results = await asyncio.gather(
                    *[
                        loop.run_in_executor(
                            self._executor, self._poll_chunk, chunk_index, chunk_units
                        )
                        for chunk_index, chunk_units in chunk_plan
                    ],
                    return_exceptions=True,
                )
                new_events: List[Event] = []
                poll_outcomes: Dict[int, bool] = {}
                for result in chunk_results:
                    if isinstance(result, Exception):  # pragma: no cover - defensive
                        log.warning("stream: chunk failed err=%s", result)
                        continue
                    if not result:
                        continue
                    events, outcomes = result
                    if events:
                        self._persist(events)
                        new_events.extend(events)
                    poll_outcomes.update(outcomes)
                for event in new_events:
                    yield event
                if poll_outcomes:
                    self._schedule_after_poll(poll_outcomes)
                self._metrics_counter += len(new_events)
                now = time.perf_counter()
                if now - self._metrics_started >= self.metrics_interval:
                    eps = self._metrics_counter / max(1.0, now - self._metrics_started)
                    log.info(
                        "stream: health units=%s events=%s eps=%.2f last_ts=%s",
                        len(self.unit_ids),
                        self._metrics_counter,
                        eps,
                        {
                            unit_id: ts
                            for unit_id, ts in sorted(self._last_ts.items())
                        },
                    )
                    self._report_health(
                        chunk_count=len(chunk_plan),
                        new_events=len(new_events),
                    )
                    self._metrics_started = now
                    self._metrics_counter = 0
                await asyncio.sleep(self.poll_interval)
        except asyncio.CancelledError:  # graceful shutdown
            log.info("stream: poller cancelled")
            raise
        finally:
            self._shutdown_executor()

    def _ensure_executor(self) -> None:
        if self._executor is None:
            self._executor = ThreadPoolExecutor(max_workers=self._max_workers)

    def _shutdown_executor(self) -> None:
        if self._executor:
            self._executor.shutdown(wait=False)
            self._executor = None

    def _consume_retry_units(self, limit: int) -> List[int]:
        items: List[int] = []
        while self._retry_queue and len(items) < limit:
            unit_id = self._retry_queue.popleft()
            self._retry_pending.discard(unit_id)
            items.append(unit_id)
        return items

    def _poll_chunk(self, chunk_index: int, chunk: Sequence[int]) -> Tuple[List[Event], Dict[int, bool]]:
        collected: List[Event] = []
        outcomes: Dict[int, bool] = {}
        rotate_limit = max(
            1, getattr(self.client, "rotate_after_units", len(chunk))
        )
        index = 0
        part = 0
        while index < len(chunk):
            sub_chunk = chunk[index : index + rotate_limit]
            with self.client.lease() as lease:
                log.info(
                    "stream: token=%s chunk=%s part=%s units=%s",
                    lease.label,
                    chunk_index,
                    part,
                    len(sub_chunk),
                )
                before = len(collected)
                for unit_id in sub_chunk:
                    try:
                        events = self._poll_unit(unit_id, lease.client)
                    except Exception as exc:
                        log.warning(
                            "stream: token=%s unit=%s err=%s", lease.label, unit_id, exc
                        )
                        self._schedule_retry(unit_id)
                        outcomes[unit_id] = False
                        continue
                    if events:
                        collected.extend(events)
                    outcomes[unit_id] = bool(events)
                processed = len(collected) - before
                if processed:
                    log.info(
                        "stream: token=%s chunk=%s part=%s processed=%s",
                        lease.label,
                        chunk_index,
                        part,
                        processed,
                    )
            index += len(sub_chunk)
            part += 1
        return collected, outcomes

    def _poll_unit(self, unit_id: int, client: "WialonClient") -> List[Event]:
        params = {
            "itemId": int(unit_id),
            "lastTime": 0,
            "lastCount": 10,
            "flags": self.flags,
            "flagsMask": self.flags_mask,
            "loadCount": self.load_count,
        }
        resp = client.request("messages/load_last", params)
        self._clear_retry(unit_id)
        messages = resp.get("messages") if isinstance(resp, dict) else resp
        if not messages:
            return []
        events: List[Event] = []
        last_ts = self._last_ts.get(unit_id, 0)
        for payload in messages:
            event = self._build_event(unit_id, payload)
            if event is None or event.device_ts <= last_ts:
                continue
            events.append(event)
        if events:
            events.sort(key=lambda e: e.device_ts)
            self._last_ts[unit_id] = events[-1].device_ts
            log.debug("stream: unit=%s new_events=%s", unit_id, len(events))
        return events

    def _compute_interval(self, unit_id: int, had_new_data: bool) -> float:
        strategy = self.poll_strategy or PollStrategy()
        if had_new_data:
            self._empty_streaks.pop(unit_id, None)
        else:
            self._empty_streaks[unit_id] = self._empty_streaks.get(unit_id, 0) + 1
        last_ts = self._last_ts.get(unit_id)
        now = time.time()
        base = strategy.idle_interval_sec
        if last_ts:
            age = max(0.0, now - last_ts)
            if age <= strategy.active_age_sec:
                base = strategy.active_interval_sec
            elif age <= strategy.warm_age_sec:
                base = strategy.warm_interval_sec
            else:
                streak = self._empty_streaks.get(unit_id, 0)
                backoff = min(strategy.idle_interval_sec, streak * strategy.empty_backoff_sec)
                base = strategy.cold_interval_sec + backoff
        jitter = 1.0
        if strategy.jitter > 0:
            jitter += random.uniform(-strategy.jitter, strategy.jitter)
        interval = max(0.2, base * max(0.1, jitter))
        return interval

    def _schedule_retry(self, unit_id: int) -> None:
        attempts = self._retry_attempts.get(unit_id, 0) + 1
        if attempts > self.max_retry_attempts:
            log.error(
                "stream: unit=%s exceeded max retry attempts=%s, dropping",
                unit_id,
                self.max_retry_attempts,
            )
            self._retry_attempts.pop(unit_id, None)
            self._retry_pending.discard(unit_id)
            strategy = self.poll_strategy or PollStrategy()
            self._schedule_unit(unit_id, time.monotonic() + strategy.idle_interval_sec)
            return
        self._retry_attempts[unit_id] = attempts
        if unit_id not in self._retry_pending:
            self._retry_queue.append(unit_id)
            self._retry_pending.add(unit_id)
            log.info(
                "stream: unit=%s scheduled for retry attempt=%s",
                unit_id,
                attempts,
            )

    def _clear_retry(self, unit_id: int) -> None:
        if unit_id in self._retry_attempts:
            self._retry_attempts.pop(unit_id, None)

    def _report_health(self, chunk_count: int, new_events: int) -> None:
        reporter = self._health_reporter
        if not reporter:
            return
        token_snapshot = None
        describe = getattr(self.client, "describe_pool", None)
        if callable(describe):
            try:
                token_snapshot = describe()
            except Exception:
                token_snapshot = None
        payload = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "units_total": len(self.unit_ids),
            "chunk_count": chunk_count,
            "new_events": new_events,
            "retry_queue_len": len(self._retry_queue),
            "retry_queue_preview": list(self._retry_queue)[:20],
            "token_pool": token_snapshot,
        }
        reporter.report(payload)

    def _build_event(self, unit_id: int, payload: dict) -> Event | None:
        if not isinstance(payload, dict):
            return None
        ts = int(payload.get("t") or payload.get("time") or 0)
        if ts <= 0:
            return None
        received_ts = int(payload.get("rt") or payload.get("serverTime") or ts)
        pos = payload.get("pos") or {}
        lat = pos.get("y") or payload.get("y")
        lon = pos.get("x") or payload.get("x")
        speed = pos.get("s") or payload.get("s")
        course = pos.get("c") or payload.get("c")
        params_raw = payload.get("p") or payload.get("params") or {}
        params = dict(params_raw) if isinstance(params_raw, dict) else {}
        sat_count = pos.get("sc") or params.get("sats") or params.get("satellites")
        if sat_count is None:
            gps = params.get("sats_gps")
            glonass = params.get("sats_glonass")
            try:
                if gps is not None or glonass is not None:
                    sat_count = int(gps or 0) + int(glonass or 0)
            except Exception:
                sat_count = None
        if sat_count is not None:
            try:
                params.setdefault("sats", int(sat_count))
            except Exception:
                pass
        if self.location_enricher and lat is not None and lon is not None:
            enrichment = self.location_enricher.enrich(
                float(lat),
                float(lon),
                existing_address=params.get("address") or params.get("addr") or payload.get("addr"),
            )
            address = enrichment.get("address")
            if address:
                params["address"] = address
            geofences = enrichment.get("geofences")
            if geofences:
                params["geofences"] = geofences
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
        batch = list(events)
        if not batch:
            return
        grouped: DefaultDict[str, List[Event]] = defaultdict(list)
        for event in batch:
            grouped[_day_key(event.device_ts)].append(event)
        for day_key, chunk in grouped.items():
            count = self.raw_storage.append(day_key, chunk)
            log.info("stream: stored day=%s events=%s", day_key, count)
        if self.latest_store is not None:
            self.latest_store.update_from_events(batch)
