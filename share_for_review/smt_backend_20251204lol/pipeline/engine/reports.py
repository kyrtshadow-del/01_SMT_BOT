"""Simple report builders over stored events."""

from __future__ import annotations

import logging
from collections import defaultdict
from typing import Dict, Iterable, List

from pipeline.engine.sensors import SensorCalculator
from pipeline.events import Event

log = logging.getLogger(__name__)


def build_day_summary(events: Iterable[Event], calculator: SensorCalculator) -> Dict[str, object]:
    events_list: List[Event] = sorted(events, key=lambda e: e.device_ts)
    total_events = len(events_list)
    if total_events == 0:
        return {"total_events": 0, "units": 0, "span": 0, "fuel": {}}

    units = defaultdict(int)
    for event in events_list:
        units[event.unit_id] += 1
    times = [event.device_ts for event in events_list]
    span = times[-1] - times[0] if len(times) > 1 else 0
    fuel_stats = calculator.fuel_stats(events_list)
    summary = {
        "total_events": total_events,
        "units": len(units),
        "span": span,
        "first_ts": times[0],
        "last_ts": times[-1],
        "fuel": fuel_stats,
    }
    log.info(
        "report: events=%s units=%s span=%ss fuel_samples=%s",
        total_events,
        len(units),
        span,
        fuel_stats.get("samples", 0),
    )
    return summary

