"""Service to estimate expected message interval per unit."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from pipeline.events import Event
from pipeline.services.storage_service import PipelineStorageService, get_pipeline_storage_service
from pipeline.services.unit_snapshot_service import get_unit_snapshot_service
from pipeline.engine.status import EXPECTED_MIN_SEC, EXPECTED_MAX_SEC


EXPECTED_STORE = Path("data/expected_interval.json")


@dataclass
class ExpectedIntervalRecord:
    unit_id: int
    expected_interval_sec: float
    last_recalc_ts: float


def _load_expected() -> Dict[int, ExpectedIntervalRecord]:
    if not EXPECTED_STORE.exists():
        return {}
    try:
        raw = json.loads(EXPECTED_STORE.read_text("utf-8"))
    except Exception:
        return {}
    result: Dict[int, ExpectedIntervalRecord] = {}
    for k, v in raw.items():
        try:
            uid = int(k)
            rec = ExpectedIntervalRecord(
                unit_id=uid,
                expected_interval_sec=float(v.get("expected_interval_sec", 300)),
                last_recalc_ts=float(v.get("last_recalc_ts", 0)),
            )
            result[uid] = rec
        except Exception:
            continue
    return result


def _save_expected(data: Dict[int, ExpectedIntervalRecord]) -> None:
    payload = {
        str(uid): {
            "expected_interval_sec": rec.expected_interval_sec,
            "last_recalc_ts": rec.last_recalc_ts,
        }
        for uid, rec in data.items()
    }
    EXPECTED_STORE.parent.mkdir(parents=True, exist_ok=True)
    EXPECTED_STORE.write_text(json.dumps(payload, ensure_ascii=False, indent=2), "utf-8")


def _safe_interval(dt: float, min_dt: float, max_dt: float) -> bool:
    return dt >= min_dt and dt <= max_dt


def _filter_online_events(evs: List[Event], online_threshold_sec: float) -> List[Event]:
    """Фильтруем интервалы только в рамках живых отрезков: пропускаем цепочки,
    где gap между событиями не превышает online_threshold_sec."""
    if len(evs) < 2:
        return evs
    filtered: List[Event] = []
    prev = evs[0]
    filtered.append(prev)
    for cur in evs[1:]:
        if cur.device_ts is None:
            continue
        prev_ts = prev.device_ts or prev.received_ts
        cur_ts = cur.device_ts or cur.received_ts
        if prev_ts is None or cur_ts is None:
            continue
        if cur_ts - prev_ts <= online_threshold_sec:
            filtered.append(cur)
        prev = cur
    return filtered if len(filtered) >= 2 else evs


def estimate_interval_from_events(
    events: Iterable[Event],
    *,
    base_expected: float,
    min_dt: float = 10.0,
    clamp_factor: float = 2.0,
    min_samples: int = 20,
) -> Optional[float]:
    """Estimate interval from a sequence of events (assumed ordered by device_ts).

    - Берём только интервалы в диапазоне [min_dt, clamp_factor * base_expected].
    - Если «хороших» интервалов меньше min_samples — возвращаем None.
    - Возвращаем медиану как оценку.
    """
    dts: List[float] = []
    prev_ts: Optional[float] = None
    max_dt = clamp_factor * base_expected
    for ev in events:
        ts = ev.device_ts or ev.received_ts
        try:
            ts = float(ts)
        except Exception:
            ts = None
        if ts is None:
            continue
        if prev_ts is not None:
            dt = ts - prev_ts
            if _safe_interval(dt, min_dt, max_dt):
                dts.append(dt)
        prev_ts = ts
    if len(dts) < min_samples:
        return None
    dts.sort()
    mid = len(dts) // 2
    if len(dts) % 2 == 0:
        return (dts[mid - 1] + dts[mid]) / 2.0
    return dts[mid]


def recalc_expected_intervals(
    *,
    storage: Optional[PipelineStorageService] = None,
    window_days: int = 2,
    recalc_period_hours: int = 12,
    base_expected_sec: float = 300.0,
    max_units: int = 200,
    min_samples: int = 20,
    alpha: float = 0.7,
    online_threshold_sec: Optional[float] = None,
) -> Dict[int, ExpectedIntervalRecord]:
    """Recalculate expected intervals for a limited number of units.

    - Читает последние `window_days` для каждого юнита.
    - Пересчитывает не чаще, чем раз в `recalc_period_hours`.
    - Обновляет локальный json `data/expected_interval.json`.
    """

    svc = storage or get_pipeline_storage_service()
    # auto-fill online_threshold_sec to clamp intervals внутри «живого» окна
    if online_threshold_sec is None:
        try:
            from pipeline.engine.status import get_online_threshold  # lazy import to avoid cycles

            online_threshold_sec = float(get_online_threshold())
        except Exception:
            online_threshold_sec = None

    existing = _load_expected()
    now = time.time()
    days = list(svc.list_days())[-window_days:]
    # берём unit_id из снапшота (полный список объектов)
    units = list(get_unit_snapshot_service().load_bundle().units.keys())
    updated = 0
    log_lines: List[str] = []
    for uid in units:
        if updated >= max_units:
            break
        rec = existing.get(uid)
        if rec and now - rec.last_recalc_ts < recalc_period_hours * 3600:
            continue
        # собираем события из окна
        evs: List[Event] = []
        for day in days:
            evs.extend(svc.fetch_day(day, unit_id=uid))
        if not evs:
            continue
        evs.sort(key=lambda e: e.device_ts or e.received_ts or 0)
        # если задан online_threshold_sec — оставляем только интервалы, которые вписываются в «нормальный» онлайн
        if online_threshold_sec is not None:
            evs = _filter_online_events(evs, online_threshold_sec)
        if len(evs) < min_samples:
            continue
        base_expected = rec.expected_interval_sec if rec else base_expected_sec
        est = estimate_interval_from_events(evs, base_expected=base_expected, min_samples=min_samples)
        if est is None:
            continue
        if rec:
            est = alpha * rec.expected_interval_sec + (1 - alpha) * est
        exp = max(EXPECTED_MIN_SEC, min(EXPECTED_MAX_SEC, est))
        existing[uid] = ExpectedIntervalRecord(unit_id=uid, expected_interval_sec=exp, last_recalc_ts=now)
        updated += 1
        log_lines.append(f"{uid}: {exp:.1f}s (est={est:.1f})")

    if updated:
        _save_expected(existing)
        try:
            log_path = Path("logs/expected_interval_recalc.log")
            log_path.parent.mkdir(parents=True, exist_ok=True)
            ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(now))
            payload = "\n".join([f"[{ts}] updated={updated}"] + log_lines) + "\n"
            log_path.write_text(payload, "utf-8")
        except Exception:
            pass
    return existing
