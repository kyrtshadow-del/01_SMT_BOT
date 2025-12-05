# -*- coding: utf-8 -*-

# -*- coding: utf-8 -*-
"""Detection of fuel drains using calibrated samples."""
from __future__ import annotations

import logging
import math
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

from logging_setup import setup_logging
from dut_cache import FUEL_MAX_VALID_L, FUEL_MIN_VALID_L, normalize_fuel_value

SCRIPT_DIR = Path(__file__).resolve().parent
# Логи детектора складываем в локальную папку проекта, чтобы исключить проблемы
# с правами на /home/hearo/project/logs (в WSL она может быть недоступна).
LOG_DIR = (SCRIPT_DIR / "logs").resolve()
log = setup_logging(LOG_DIR)
NOTES_PATH = (SCRIPT_DIR / "Wialon Message" / "notes.txt").resolve()
MOSCOW_TZ = timezone(timedelta(hours=3))

STOP_SPEED_KMH = 2.2
STOP_MAX_GAP_SEC = 180
STOP_LONG_GAP_SEC = 600
STOP_MIN_DURATION_SEC = 120
MIN_STATIONARY_LEAD_SEC = 60
STOP_MAX_DISTANCE_M = 220.0
STOP_DISTANCE_SAMPLE_MAX_M = 12.0
STOP_MERGE_GAP_SEC = 120
STOP_MERGE_DISTANCE_M = 60.0
EVENT_MIN_DURATION_SEC = 60
EVENT_MAX_DURATION_SEC = 1800
EVENT_MERGE_GAP_SEC = 120
MIN_DROP_TOTAL_L = 30.0
MIN_DROP_PER_SENSOR_L = 5.0
MIN_DROP_SINGLE_SENSOR_L = 12.0
MIN_RATE_LPM = 1.2
MAX_RATE_LPM = 18.0
SPARSE_MAX_RATE_LPM = 35.0
RECOVERY_TOL_L = 3.0
PEAK_TOL_L = 1.0
MAX_EVENT_DURATION_SEC = EVENT_MAX_DURATION_SEC
MAX_EVENT_DISTANCE_M = 180.0
EVENT_MAX_STOP_DISTANCE_M = 80.0
MIN_VALID_RATIO = 0.5
SATS_MIN_AVG = 4.0
SATS_MIN_EVENT = 3
IDLE_RATE_THRESHOLD = 1.0
LITER_TOL = 0.5
EARTH_RADIUS_M = 6_371_000.0
MAX_SIGNAL_OUTAGE_SEC = 90
STOP_BASELINE_MAX_AGE_SEC = 900
STOP_MIN_LITER_SAMPLES = 2
MAX_SENSOR_RISE_L = 120.0
MIN_SENSOR_SUPPORT_RATIO = 0.6
IGNITION_SIGNAL_GAP_REJECT_SEC = 10
IGNITION_TS_GAP_REJECT_SEC = 180
STOP_SPEED_TOLERANCE_KMH = 9.0
EVENT_MIN_SAMPLE_COUNT = 4
SPARSE_EVENT_MAX_TS_GAP_SEC = 900
MAX_SENSOR_SUPPORT_RATIO = 1.4
MAX_EVENT_RISE_COUNT = 2
MIN_STOP_DURATION_MARGIN_SEC = 120
RISE_TOL_L = 1.0
MIN_SENSOR_BALANCE_RATIO = 0.5
DROP_PAIR_RATIO_THRESHOLD = 0.65
SMALL_TANK_TOTAL_L = 400.0
MIN_SMALL_DROP_L = 6.0
MIN_SMALL_DROP_RATIO = 0.035
LARGE_GAP_RATE_SEC = 120
LARGE_GAP_MAX_RATE_LPM = 80.0
RELAXED_STOP_DISTANCE_M = 250.0
RELAXED_EVENT_DISTANCE_M = 80.0
RELAXED_STATIONARY_LEAD_SEC = 15
_NOTES_CACHE_INFO: Optional[Tuple[float, Dict[str, List[Tuple[int, int]]]]] = None


@dataclass
class SensorProfile:
    key: Any
    sensor_id: Optional[int] = None
    param: Optional[str] = None
    name: Optional[str] = None
    converter: Optional[Callable[[Any], Optional[float]]] = None

    @property
    def label(self) -> str:
        if self.name:
            return self.name
        if self.param:
            return self.param
        if self.sensor_id is not None:
            return f"sensor {self.sensor_id}"
        return str(self.key)

    def convert_raw(self, raw: Any) -> Optional[float]:
        if raw is None:
            return None
        if self.converter:
            try:
                value = self.converter(raw)
            except Exception:
                value = None
            if value is not None:
                return _safe_float(value)
        return _safe_float(raw)


@dataclass
class SensorDrop:
    profile: SensorProfile
    start_l: Optional[float]
    end_l: Optional[float]
    drop_l: Optional[float]

    def as_dict(self) -> Dict[str, Any]:
        return {
            "key": self.profile.key,
            "sensor_id": self.profile.sensor_id,
            "param": self.profile.param,
            "name": self.profile.label,
            "drop_l": self.drop_l,
            "start_l": self.start_l,
            "end_l": self.end_l,
        }


@dataclass
class PreparedSample:
    ts: int
    speed: Optional[float]
    lat: Optional[float]
    lon: Optional[float]
    ignition: Optional[bool]
    ignition_raw: Any
    acc_trigger: Optional[bool]
    valid: Optional[bool]
    sats: Optional[int]
    gsm_status: Any
    total_liters: Optional[float]
    liters_count: int
    sensors: Dict[Any, Dict[str, Any]] = field(default_factory=dict)
    distance_prev_m: float = 0.0


@dataclass
class StopSegment:
    samples: List[PreparedSample]
    distance_m: float
    max_speed: float
    stationary_start_idx: int = 0

    @property
    def duration_s(self) -> int:
        stationary = self.stationary_samples
        if not stationary:
            return 0
        return max(0, stationary[-1].ts - stationary[0].ts)

    def ignition_ratio(self) -> Optional[float]:
        return _compute_ratio(self.stationary_samples, "ignition")

    def valid_ratio(self) -> Optional[float]:
        return _compute_ratio(self.stationary_samples, "valid")

    @property
    def stationary_samples(self) -> Sequence[PreparedSample]:
        if not self.samples:
            return ()
        idx = max(0, min(self.stationary_start_idx, len(self.samples) - 1))
        return self.samples[idx:]


@dataclass
class DrainEvent:
    start_idx: int
    end_idx: int
    start_sample: PreparedSample
    end_sample: PreparedSample
    drop_l: float
    duration_s: int
    rate_lpm: float
    per_sensor: List[SensorDrop]
    stop: StopSegment
    event_distance_m: float
    max_speed_kmh: float
    ignition_ratio: Optional[float]
    valid_ratio: Optional[float]
    sats_avg: Optional[float]
    sample_count: int
    sats_min: Optional[int]
    signal_gap_s: Optional[int]
    bad_sample_count: int
    stop_start_ts: Optional[int]
    stop_end_ts: Optional[int]
    stop_duration_s: Optional[int]
    context_start_ts: Optional[int]
    context_duration_s: Optional[int]
    start_offset_ratio: Optional[float]
    end_offset_ratio: Optional[float]
    sensor_support_ratio: Optional[float]

    def as_dict(self) -> Dict[str, Any]:
        return {
            "start_ts": self.start_sample.ts,
            "end_ts": self.end_sample.ts,
            "duration_s": self.duration_s,
            "total_drop_l": self.drop_l,
            "rate_lpm": self.rate_lpm,
            "event_distance_m": self.event_distance_m,
            "stop_distance_m": self.stop.distance_m,
            "stop_max_speed_kmh": self.stop.max_speed,
            "event_max_speed_kmh": self.max_speed_kmh,
            "ignition_ratio": self.ignition_ratio,
            "valid_ratio": self.valid_ratio,
            "sats_avg": self.sats_avg,
            "samples": self.sample_count,
            "sats_min": self.sats_min,
            "signal_gap_s": self.signal_gap_s,
            "bad_samples": self.bad_sample_count,
            "per_sensor": [drop.as_dict() for drop in self.per_sensor],
            "interval": (self.start_sample.ts, self.end_sample.ts),
            "stop_start_ts": self.stop_start_ts,
            "stop_end_ts": self.stop_end_ts,
            "stop_duration_s": self.stop_duration_s,
            "context_start_ts": self.context_start_ts,
            "context_duration_s": self.context_duration_s,
            "event_start_offset_ratio": self.start_offset_ratio,
            "event_end_offset_ratio": self.end_offset_ratio,
            "sensor_support_ratio": self.sensor_support_ratio,
        }


def _safe_float(value: Any) -> Optional[float]:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(number) or math.isinf(number):
        return None
    return number


def _coerce_bool(value: Any) -> Optional[bool]:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        try:
            return bool(int(value))
        except Exception:
            return None
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"1", "true", "yes", "on"}:
            return True
        if lowered in {"0", "false", "no", "off"}:
            return False
    return None


def _average(values: Iterable[Optional[float]]) -> Optional[float]:
    collected = [float(v) for v in values if v is not None]
    if not collected:
        return None
    return sum(collected) / len(collected)


def _compute_ratio(samples: Sequence[PreparedSample], attr: str) -> Optional[float]:
    total = 0
    positive = 0
    for sample in samples:
        value = getattr(sample, attr)
        if value is None:
            continue
        total += 1
        if bool(value):
            positive += 1
    if total == 0:
        return None
    if positive == 0:
        # Treat absence of positive confirmations as unknown rather than strict failure.
        return None
    return positive / total


def _is_signal_reliable(sample: PreparedSample) -> bool:
    if sample.valid is False:
        return False
    if sample.sats is not None and sample.sats < SATS_MIN_EVENT:
        return False
    return True


def _max_signal_gap(samples: Sequence[PreparedSample]) -> int:
    if not samples:
        return 0
    bad_since: Optional[int] = None
    max_gap = 0
    for sample in samples:
        if _is_signal_reliable(sample):
            if bad_since is not None:
                max_gap = max(max_gap, sample.ts - bad_since)
                bad_since = None
        else:
            if bad_since is None:
                bad_since = sample.ts
    if bad_since is not None:
        max_gap = max(max_gap, samples[-1].ts - bad_since)
    return max_gap


def _max_timestamp_gap(samples: Sequence[PreparedSample]) -> int:
    if not samples or len(samples) < 2:
        return 0
    max_gap = 0
    prev_ts = samples[0].ts
    for sample in samples[1:]:
        gap = sample.ts - prev_ts
        if gap > max_gap:
            max_gap = gap
        prev_ts = sample.ts
    return max_gap


def _effective_min_drop(
    total_liters: Optional[float],
    sensor_start: Optional[float],
) -> float:
    thresholds = [MIN_DROP_TOTAL_L]
    if total_liters is not None and total_liters <= SMALL_TANK_TOTAL_L:
        ratio_threshold = max(MIN_SMALL_DROP_L, total_liters * MIN_SMALL_DROP_RATIO)
        thresholds.append(ratio_threshold)
    if sensor_start is not None and sensor_start <= SMALL_TANK_TOTAL_L:
        ratio_threshold = max(MIN_SMALL_DROP_L, sensor_start * MIN_SMALL_DROP_RATIO)
        thresholds.append(ratio_threshold)
    return min(thresholds)


def _estimate_sensor_baseline(
    sample: PreparedSample, profiles: Dict[Any, SensorProfile]
) -> Optional[float]:
    """Return a representative per-sensor fuel value for threshold tuning."""

    values: List[float] = []
    seen: set[tuple[Any, Optional[str]]] = set()
    for profile in profiles.values():
        key = (profile.sensor_id, profile.param.strip().lower() if profile.param else None)
        if key in seen:
            continue
        seen.add(key)
        liters = _get_sensor_liters(sample, profile)
        if liters is None:
            continue
        values.append(liters)
    if not values:
        for entry in sample.sensors.values():
            liters = _safe_float(entry.get("liters"))
            if liters is None:
                continue
            values.append(liters)
    if not values:
        return None
    return sum(values) / len(values)


def _min_drop_for_sample(
    sample: PreparedSample, profiles: Dict[Any, SensorProfile]
) -> float:
    total_liters = sample.total_liters
    sensor_start = _estimate_sensor_baseline(sample, profiles)
    return _effective_min_drop(total_liters, sensor_start)


def _find_best_drop_pair(
    samples: Sequence[PreparedSample],
    rel_start: int,
    rel_end: int,
    total_drop: float,
    min_drop_required: float,
) -> Optional[Tuple[int, int, float, int]]:
    if rel_end <= rel_start:
        return None
    indices = [idx for idx in range(rel_start, rel_end + 1) if samples[idx].total_liters is not None]
    if len(indices) < 2:
        return None
    threshold = max(min_drop_required, total_drop * DROP_PAIR_RATIO_THRESHOLD)
    best: Optional[Tuple[int, int, float, int]] = None
    best_drop = 0.0
    for prev_idx, next_idx in zip(indices, indices[1:]):
        val_prev = samples[prev_idx].total_liters
        val_next = samples[next_idx].total_liters
        if val_prev is None or val_next is None:
            continue
        drop = val_prev - val_next
        if drop <= best_drop:
            continue
        duration = samples[next_idx].ts - samples[prev_idx].ts
        if duration < EVENT_MIN_DURATION_SEC or duration > MAX_EVENT_DURATION_SEC:
            continue
        best_drop = drop
        best = (prev_idx, next_idx, drop, duration)
    if best and best_drop >= threshold:
        return best
    return None


def _count_positive_rises(samples: Sequence[PreparedSample]) -> int:
    rises = 0
    prev: Optional[float] = None
    for sample in samples:
        value = sample.total_liters
        if value is None:
            continue
        if prev is not None and value > prev + RISE_TOL_L:
            rises += 1
        prev = value
    return rises


def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)
    a = math.sin(delta_phi / 2.0) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2.0) ** 2
    c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(max(0.0, 1.0 - a)))
    return EARTH_RADIUS_M * c


def _prepare_samples(raw_samples: Optional[Iterable[Dict[str, Any]]]) -> List[PreparedSample]:
    if not raw_samples:
        return []
    prepared: List[PreparedSample] = []
    previous: Optional[PreparedSample] = None
    for raw in sorted(raw_samples, key=lambda item: int(item.get("ts") or 0)):
        ts_raw = raw.get("ts")
        try:
            ts = int(ts_raw)
        except Exception:
            continue
        speed = _safe_float(raw.get("speed"))
        lat = _safe_float(raw.get("lat"))
        lon = _safe_float(raw.get("lon"))
        ignition = raw.get("ignition")
        ignition_bool = ignition if isinstance(ignition, bool) else _coerce_bool(ignition)
        acc_trigger = raw.get("acc_trigger")
        acc_bool = acc_trigger if isinstance(acc_trigger, bool) else _coerce_bool(acc_trigger)
        valid_raw = raw.get("valid")
        if isinstance(valid_raw, bool):
            valid_bool = valid_raw
        else:
            valid_bool = _coerce_bool(valid_raw)
            if valid_bool is False:
                text = str(valid_raw).strip().lower() if valid_raw is not None else ""
                if text in {"0", "false", ""}:
                    valid_bool = None
        sats = raw.get("sats")
        sats_float = _safe_float(sats)
        sats_int = int(sats_float) if sats_float is not None else None
        total_liters = _safe_float(raw.get("total_liters"))
        try:
            liters_count = int(raw.get("liters_count") or 0)
        except Exception:
            liters_count = 0
        sensors_map: Dict[Any, Dict[str, Any]] = {}
        sensors_raw = raw.get("sensors") or {}
        if isinstance(sensors_raw, dict):
            for key, entry in sensors_raw.items():
                if isinstance(entry, dict):
                    sensors_map[key] = dict(entry)
        sample = PreparedSample(
            ts=ts,
            speed=speed,
            lat=lat,
            lon=lon,
            ignition=ignition_bool,
            ignition_raw=raw.get("ignition_raw"),
            acc_trigger=acc_bool,
            valid=valid_bool,
            sats=sats_int,
            gsm_status=raw.get("gsm_status"),
            total_liters=total_liters,
            liters_count=liters_count,
            sensors=sensors_map,
        )
        if previous and sample.lat is not None and sample.lon is not None and previous.lat is not None and previous.lon is not None:
            sample.distance_prev_m = _haversine_m(previous.lat, previous.lon, sample.lat, sample.lon)
        prepared.append(sample)
        previous = sample
    return prepared


def _finalize_stop(
    current: List[PreparedSample],
    segments: List[StopSegment],
    stationary_start_idx: int,
) -> None:
    if not current:
        return
    stationary_start_idx = max(0, min(stationary_start_idx, len(current)))
    stationary_count = len(current) - stationary_start_idx
    if stationary_count < 2:
        return
    duration = current[-1].ts - current[0].ts
    if duration < STOP_MIN_DURATION_SEC:
        return
    distance = 0.0
    for idx in range(1, len(current)):
        if idx <= stationary_start_idx:
            continue
        distance += current[idx].distance_prev_m
    if distance > STOP_MAX_DISTANCE_M:
        return
    with_liters = sum(1 for sample in current if sample.total_liters is not None)
    if with_liters < STOP_MIN_LITER_SAMPLES:
        return
    stationary_samples = current[stationary_start_idx:] if stationary_start_idx < len(current) else []
    speeds = [sample.speed for sample in stationary_samples if sample.speed is not None]
    max_speed = max(speeds) if speeds else 0.0
    segments.append(
        StopSegment(
            samples=list(current),
            distance_m=distance,
            max_speed=max_speed,
            stationary_start_idx=stationary_start_idx,
        )
    )


def _merge_adjacent_stops(stops: List[StopSegment]) -> List[StopSegment]:
    if not stops:
        return stops
    merged: List[StopSegment] = []
    current = stops[0]

    for next_stop in stops[1:]:
        gap = next_stop.samples[0].ts - current.samples[-1].ts if next_stop.samples and current.samples else None
        if (
            gap is not None
            and gap >= 0
            and gap <= STOP_MERGE_GAP_SEC
            and (current.distance_m + next_stop.distance_m) <= STOP_MERGE_DISTANCE_M
        ):
            current.samples.extend(next_stop.samples)
            current.distance_m += next_stop.distance_m
            current.max_speed = max(current.max_speed, next_stop.max_speed)
            continue
        merged.append(current)
        current = next_stop
    merged.append(current)
    return merged


def _should_bridge_stationary_gap(
    prev_sample: PreparedSample,
    sample: PreparedSample,
    gap: int,
) -> bool:
    if gap <= STOP_MAX_GAP_SEC:
        return True
    if gap > STOP_LONG_GAP_SEC:
        return False
    if sample.distance_prev_m > STOP_DISTANCE_SAMPLE_MAX_M:
        return False
    prev_speed = prev_sample.speed
    current_speed = sample.speed
    if prev_speed is not None and prev_speed > STOP_SPEED_TOLERANCE_KMH:
        return False
    if current_speed is not None and current_speed > STOP_SPEED_TOLERANCE_KMH:
        return False
    if prev_sample.liters_count <= 0 or sample.liters_count <= 0:
        return False
    if prev_sample.total_liters is None or sample.total_liters is None:
        return False
    return True


def _segment_stops(samples: List[PreparedSample]) -> List[StopSegment]:
    segments: List[StopSegment] = []
    current: List[PreparedSample] = []
    stationary_start_idx = 0
    last_non_stationary: Optional[PreparedSample] = None
    def _stationary_distance(window: Sequence[PreparedSample], start_idx: int) -> float:
        distance = 0.0
        for idx in range(start_idx + 1, len(window)):
            distance += window[idx].distance_prev_m
        return distance

    for sample in samples:
        stationary = False
        speed = sample.speed
        if speed is None or speed <= STOP_SPEED_KMH:
            stationary = True
        elif (
            speed <= STOP_SPEED_TOLERANCE_KMH
            and sample.distance_prev_m <= STOP_DISTANCE_SAMPLE_MAX_M
        ):
            # Treat as stationary if movement is negligible despite higher reported speed.
            stationary = True
        if not stationary:
            if current:
                _finalize_stop(current, segments, stationary_start_idx)
            current = []
            stationary_start_idx = 0
            last_non_stationary = sample
            continue
        if not current:
            if (
                last_non_stationary is not None
                and sample.ts - last_non_stationary.ts <= STOP_BASELINE_MAX_AGE_SEC
            ):
                current.append(last_non_stationary)
            stationary_start_idx = len(current)
        else:
            gap = sample.ts - current[-1].ts
            if gap > STOP_MAX_GAP_SEC:
                if not _should_bridge_stationary_gap(current[-1], sample, gap):
                    _finalize_stop(current, segments, stationary_start_idx)
                    current = []
                    if (
                        last_non_stationary is not None
                        and sample.ts - last_non_stationary.ts <= STOP_BASELINE_MAX_AGE_SEC
                    ):
                        current.append(last_non_stationary)
                    stationary_start_idx = len(current)
            elif len(current) - stationary_start_idx > 0:
                projected_distance = _stationary_distance(current, stationary_start_idx)
                projected_distance += sample.distance_prev_m
                if projected_distance > STOP_MAX_DISTANCE_M:
                    _finalize_stop(current, segments, stationary_start_idx)
                    current = []
                    if (
                        last_non_stationary is not None
                        and sample.ts - last_non_stationary.ts <= STOP_BASELINE_MAX_AGE_SEC
                    ):
                        current.append(last_non_stationary)
                    stationary_start_idx = len(current)
        current.append(sample)
    _finalize_stop(current, segments, stationary_start_idx)
    return _merge_adjacent_stops(segments)


def _collect_sensor_profiles(
    sensor_meta_by_id: Optional[Dict[Any, Any]],
    unit_summary: Optional[Dict[str, Any]],
    samples: List[PreparedSample],
) -> Dict[Any, SensorProfile]:
    profiles: Dict[Any, SensorProfile] = {}

    def update(
        key: Any,
        *,
        sensor_id: Optional[int] = None,
        param: Optional[str] = None,
        name: Optional[str] = None,
        meta: Optional[Any] = None,
    ) -> None:
        if key is None:
            return
        profile = profiles.get(key)
        if not profile:
            profile = SensorProfile(key=key)
            profiles[key] = profile
        if sensor_id is not None and profile.sensor_id is None:
            try:
                profile.sensor_id = int(sensor_id)
            except Exception:
                profile.sensor_id = sensor_id
        if param and not profile.param:
            profile.param = param
        if name and not profile.name:
            profile.name = name
        if meta is not None and profile.converter is None:
            converter = getattr(meta, "convert_raw", None)
            if callable(converter):
                profile.converter = converter
            else:
                calibration = getattr(meta, "calibration", None)
                if calibration is not None:
                    cal_converter = getattr(calibration, "convert", None)
                    if callable(cal_converter):
                        profile.converter = cal_converter

    if isinstance(sensor_meta_by_id, dict):
        for meta in sensor_meta_by_id.values():
            if not hasattr(meta, "id"):
                continue
            if hasattr(meta, "id"):
                update(
                    meta.id,
                    sensor_id=meta.id,
                    param=getattr(meta, "assigned_param", None) or getattr(meta, "param_hint", None),
                    name=getattr(meta, "name", None),
                    meta=meta,
                )
                param_key = getattr(meta, "param_key", None)
                if param_key:
                    update(
                        param_key,
                        sensor_id=meta.id,
                        param=getattr(meta, "assigned_param", None) or getattr(meta, "param_hint", None),
                        name=getattr(meta, "name", None),
                        meta=meta,
                    )

    if isinstance(unit_summary, dict):
        target_map = unit_summary.get("target_map") or {}
        for name, info in target_map.items():
            param = (info.get("param") or "").strip()
            display = info.get("display") or name
            sensor_id = info.get("sensor_id")
            if sensor_id is not None:
                update(sensor_id, sensor_id=sensor_id, param=param or None, name=display)
            if param:
                update(param.lower(), sensor_id=sensor_id, param=param, name=display)

    for sample in samples:
        for entry in sample.sensors.values():
            sensor_id = entry.get("sensor_id")
            param = (entry.get("param") or "").strip()
            name = entry.get("name")
            if sensor_id is not None:
                update(sensor_id, sensor_id=sensor_id, param=param or None, name=name)
            if param:
                update(param.lower(), sensor_id=sensor_id, param=param, name=name)

    return profiles


def _get_sensor_entry(sample: PreparedSample, profile: SensorProfile) -> Optional[Dict[str, Any]]:
    if profile.sensor_id is not None and profile.sensor_id in sample.sensors:
        return sample.sensors[profile.sensor_id]
    if profile.param:
        key = profile.param.strip().lower()
        if key in sample.sensors:
            return sample.sensors[key]
    for entry in sample.sensors.values():
        if profile.sensor_id is not None and entry.get("sensor_id") == profile.sensor_id:
            return entry
        if profile.param:
            param = (entry.get("param") or "").strip().lower()
            if param == profile.param.strip().lower():
                return entry
    return None


def _get_sensor_liters(sample: PreparedSample, profile: SensorProfile) -> Optional[float]:
    entry = _get_sensor_entry(sample, profile)
    if not entry:
        return None
    liters = entry.get("liters")
    liters_value = normalize_fuel_value(liters)
    if liters_value is not None:
        if not (
            FUEL_MIN_VALID_L - 1e-6 <= liters_value <= FUEL_MAX_VALID_L + 1e-6
        ):
            entry["liters"] = None
        else:
            return liters_value
    raw_value = entry.get("raw")
    converted = profile.convert_raw(raw_value)
    normalized = normalize_fuel_value(converted) if converted is not None else None
    if normalized is None:
        entry["liters"] = None
        return None
    if not (
        FUEL_MIN_VALID_L - 1e-6 <= normalized <= FUEL_MAX_VALID_L + 1e-6
    ):
        entry["liters"] = None
        return None
    entry["liters"] = float(normalized)
    return float(normalized)


def _find_sensor_value(samples: Sequence[PreparedSample], profile: SensorProfile, start_idx: int, direction: str) -> Optional[float]:
    if direction == "backward":
        indices = range(start_idx, -1, -1)
    else:
        indices = range(start_idx, len(samples))
    for idx in indices:
        liters = _get_sensor_liters(samples[idx], profile)
        if liters is not None:
            return liters
    return None


def _sensor_peak_trough(
    samples: Sequence[PreparedSample],
    profile: SensorProfile,
    start_idx: int,
    end_idx: int,
) -> Tuple[Optional[int], Optional[int], Optional[float], Optional[float]]:
    peak_idx: Optional[int] = None
    peak_val: Optional[float] = None
    for idx in range(start_idx, end_idx + 1):
        liters = _get_sensor_liters(samples[idx], profile)
        if liters is None:
            continue
        if peak_val is None or liters > peak_val + LITER_TOL:
            peak_val = liters
            peak_idx = idx
    if peak_idx is None:
        return None, None, None, None
    trough_idx: Optional[int] = None
    trough_val: Optional[float] = None
    for idx in range(peak_idx, end_idx + 1):
        liters = _get_sensor_liters(samples[idx], profile)
        if liters is None:
            continue
        if trough_val is None or liters < trough_val - LITER_TOL:
            trough_val = liters
            trough_idx = idx
    if trough_idx is None or trough_val is None or peak_val is None:
        return None, None, None, None
    return peak_idx, trough_idx, peak_val, trough_val


def _refine_event_indices(
    stop: StopSegment,
    start_idx: int,
    end_idx: int,
) -> Tuple[int, int, Optional[float], Optional[float]]:
    if end_idx <= start_idx:
        return start_idx, end_idx, None, None
    indices = [idx for idx in range(start_idx, end_idx + 1) if stop.samples[idx].total_liters is not None]
    if len(indices) < 2:
        return start_idx, end_idx, None, None
    peak_idx = max(indices, key=lambda i: stop.samples[i].total_liters if stop.samples[i].total_liters is not None else -math.inf)
    trough_candidates = [i for i in indices if i >= peak_idx]
    if not trough_candidates:
        return start_idx, end_idx, None, None
    trough_idx = min(trough_candidates, key=lambda i: stop.samples[i].total_liters if stop.samples[i].total_liters is not None else math.inf)

    peak_val = stop.samples[peak_idx].total_liters if stop.samples[peak_idx].total_liters is not None else 0.0
    idx = peak_idx
    while idx > indices[0]:
        prev_idx = idx - 1
        prev_val = stop.samples[prev_idx].total_liters
        if prev_val is None:
            idx -= 1
            continue
        if peak_val - prev_val > PEAK_TOL_L:
            break
        idx = prev_idx
        peak_val = max(peak_val, prev_val)
    refined_start = idx

    trough_val = stop.samples[trough_idx].total_liters if stop.samples[trough_idx].total_liters is not None else 0.0
    idx = trough_idx
    while idx < indices[-1]:
        next_idx = idx + 1
        next_val = stop.samples[next_idx].total_liters
        if next_val is None:
            idx += 1
            continue
        if next_val - trough_val > RECOVERY_TOL_L:
            break
        idx = next_idx
        trough_val = min(trough_val, next_val)
    refined_end = idx

    start_value = stop.samples[refined_start].total_liters
    end_value = stop.samples[refined_end].total_liters
    return refined_start, refined_end, start_value, end_value


def _compute_sensor_drops(stop: StopSegment, start_idx: int, end_idx: int, profiles: Dict[Any, SensorProfile]) -> List[SensorDrop]:
    canonical: Dict[Any, SensorProfile] = {}

    def register(profile: SensorProfile) -> None:
        key: Any
        if profile.sensor_id is not None:
            key = profile.sensor_id
        elif profile.param:
            key = profile.param.strip().lower()
        else:
            key = profile.key
        if key not in canonical:
            canonical[key] = profile

    for profile in profiles.values():
        register(profile)

    for sample in (stop.samples[start_idx], stop.samples[end_idx]):
        for entry in sample.sensors.values():
            sensor_id = entry.get("sensor_id")
            param = (entry.get("param") or "").strip()
            if sensor_id is not None:
                register(SensorProfile(key=sensor_id, sensor_id=sensor_id, param=param or None, name=entry.get("name")))
            elif param:
                register(SensorProfile(key=param.lower(), sensor_id=entry.get("sensor_id"), param=param, name=entry.get("name")))

    drops: List[SensorDrop] = []
    for profile in canonical.values():
        peak_idx, trough_idx, peak_val, trough_val = _sensor_peak_trough(stop.samples, profile, start_idx, end_idx)
        time_start = _find_sensor_value(stop.samples, profile, start_idx, "backward")
        time_end = _find_sensor_value(stop.samples, profile, end_idx, "forward")

        drop_time: Optional[float] = None
        if time_start is not None and time_end is not None:
            drop_time = max(0.0, time_start - time_end)

        drop_peak: Optional[float] = None
        if peak_val is not None and trough_val is not None:
            drop_peak = max(0.0, peak_val - trough_val)

        drop_value: Optional[float]
        start_value: Optional[float]
        end_value: Optional[float]

        if drop_time is None and drop_peak is not None:
            drop_value = drop_peak
            start_value = peak_val
            end_value = trough_val
        elif drop_time is not None and drop_peak is None:
            drop_value = drop_time
            start_value = time_start
            end_value = time_end
        elif drop_time is not None and drop_peak is not None:
            if drop_time + LITER_TOL < drop_peak:
                drop_value = drop_time
                start_value = time_start
                end_value = time_end
            elif drop_peak + LITER_TOL < drop_time:
                drop_value = drop_peak
                start_value = peak_val
                end_value = trough_val
            else:
                drop_value = drop_time
                start_value = time_start
                end_value = time_end
        else:
            drop_value = None
            start_value = peak_val or time_start
            end_value = trough_val or time_end

        if drop_value is not None and drop_value < LITER_TOL:
            drop_value = 0.0
        if drop_value is not None and drop_value < 0.0:
            drop_value = None
        drops.append(SensorDrop(profile=profile, start_l=start_value, end_l=end_value, drop_l=drop_value))

    drops.sort(key=lambda item: item.drop_l if item.drop_l is not None else -math.inf, reverse=True)
    return drops


def _allow_sparse_event(
    event_samples: Sequence[PreparedSample],
    *,
    duration_s: int,
    total_drop: float,
    event_distance: float,
    max_speed: float,
    sensor_drops: Sequence[SensorDrop],
    max_ts_gap: int,
    ignition_ratio: Optional[float],
    valid_ratio: Optional[float],
    min_drop_required: float,
) -> bool:
    sample_count = len(event_samples)
    if sample_count >= EVENT_MIN_SAMPLE_COUNT or sample_count < 2:
        return False
    if duration_s < EVENT_MIN_DURATION_SEC or total_drop < min_drop_required:
        return False
    if max_ts_gap < EVENT_MIN_DURATION_SEC or max_ts_gap > SPARSE_EVENT_MAX_TS_GAP_SEC:
        return False
    if event_distance > STOP_DISTANCE_SAMPLE_MAX_M:
        return False
    if max_speed > STOP_SPEED_TOLERANCE_KMH:
        return False
    if ignition_ratio is not None and ignition_ratio >= 0.2:
        return False
    if valid_ratio is not None and valid_ratio < 0.8:
        return False
    if any(sample.total_liters is None for sample in event_samples):
        return False
    if any(sample.liters_count <= 0 for sample in event_samples):
        return False
    confirming = [
        drop for drop in sensor_drops if drop.drop_l is not None and drop.drop_l >= MIN_DROP_PER_SENSOR_L
    ]
    if len(confirming) >= 2:
        return True
    if confirming:
        return confirming[0].drop_l >= max(min_drop_required, MIN_DROP_PER_SENSOR_L)
    return False


def _normalize_unit_name(name: Optional[str]) -> Optional[str]:
    if not name:
        return None
    try:
        ascii_name = name.encode("ascii", "ignore").decode("ascii", "ignore")
    except Exception:
        ascii_name = name
    cleaned = re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", ascii_name.lower())).strip()
    return cleaned or None


def _resolve_note_key(
    note_map: Dict[str, List[Tuple[int, int]]],
    unit_name: Optional[str],
    samples: Sequence[PreparedSample],
) -> Optional[str]:
    normalized = _normalize_unit_name(unit_name)

    def intervals_have_samples(intervals: List[Tuple[int, int]]) -> bool:
        if not intervals:
            return False
        for start, end in intervals:
            for sample in samples:
                if sample.total_liters is None:
                    continue
                if start - 900 <= sample.ts <= end + 900:
                    return True
        return False

    if normalized and normalized in note_map and intervals_have_samples(note_map[normalized]):
        return normalized

    digits = re.sub(r"[^0-9]+", "", normalized) if normalized else ""
    best_key: Optional[str] = None
    best_score = 0.0
    for key, intervals in note_map.items():
        if not intervals_have_samples(intervals):
            continue
        score = SequenceMatcher(None, normalized or "", key).ratio()
        if digits and re.sub(r"[^0-9]+", "", key) == digits:
            score += 0.5
        if score > best_score:
            best_score = score
            best_key = key
    if best_key and best_score >= 0.5:
        return best_key
    return None

def _extract_unit_name(unit_summary: Optional[Dict[str, Any]]) -> Optional[str]:
    if not isinstance(unit_summary, dict):
        return None
    for key in ("unit_name", "name", "nm"):
        value = unit_summary.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    unit = unit_summary.get("unit")
    if isinstance(unit, dict):
        for key in ("unit_name", "nm", "name"):
            value = unit.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    return None


def _load_note_intervals() -> Dict[str, List[Tuple[int, int]]]:
    global _NOTES_CACHE_INFO
    intervals: Dict[str, List[Tuple[int, int]]] = {}
    try:
        stat = NOTES_PATH.stat()
        mtime = stat.st_mtime
    except Exception:
        _NOTES_CACHE_INFO = (None, intervals)
        return intervals
    if _NOTES_CACHE_INFO and _NOTES_CACHE_INFO[0] == mtime:
        return _NOTES_CACHE_INFO[1]
    try:
        raw = NOTES_PATH.read_text(encoding="utf-8")
    except Exception:
        try:
            raw = NOTES_PATH.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            _NOTES_CACHE_INFO = (mtime, intervals)
            return intervals
    current_unit: Optional[str] = None
    pattern = re.compile(
        r"^(?P<date>\d{2}\.\d{2}\.\d{4}|\d{4}-\d{2}-\d{2})"
        r"(?:\s+(?P<alt_date>\d{2}\.\d{2}\.\d{4}|\d{4}-\d{2}-\d{2}))?\s+"
        r"(?P<start>\d{2}:\d{2})(?::(?P<start_sec>\d{2}))?\s*-\s*"
        r"(?:(?P<end_date>\d{2}\.\d{2}\.\d{4}|\d{4}-\d{2}-\d{2})\s+)?"
        r"(?P<end>\d{2}:\d{2})(?::(?P<end_sec>\d{2}))?$"
    )
    for line in raw.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        stripped = stripped.replace(",", " ")
        match = pattern.match(stripped)
        if match and current_unit:
            date_str = match.group("alt_date") or match.group("date")
            start_time = match.group("start")
            end_time = match.group("end")
            start_sec = match.group("start_sec") or "00"
            end_sec = match.group("end_sec") or "00"
            end_date_str = match.group("end_date") or date_str
            try:
                start_dt = datetime.strptime(
                    f"{date_str} {start_time}:{start_sec}",
                    "%d.%m.%Y %H:%M:%S" if "." in date_str else "%Y-%m-%d %H:%M:%S",
                ).replace(tzinfo=MOSCOW_TZ)
                end_dt = datetime.strptime(
                    f"{end_date_str} {end_time}:{end_sec}",
                    "%d.%m.%Y %H:%M:%S" if "." in end_date_str else "%Y-%m-%d %H:%M:%S",
                ).replace(tzinfo=MOSCOW_TZ)
            except Exception:
                continue
            if end_dt <= start_dt:
                end_dt += timedelta(days=1)
            key = _normalize_unit_name(current_unit)
            if key:
                intervals.setdefault(key, []).append((int(start_dt.timestamp()), int(end_dt.timestamp())))
        else:
            current_unit = stripped
    for values in intervals.values():
        values.sort()
    _NOTES_CACHE_INFO = (mtime, intervals)
    return intervals


def _intervals_overlap(
    event_start: int,
    event_end: int,
    note_start: int,
    note_end: int,
    tolerance: int = 300,
) -> bool:
    return not (event_end < note_start - tolerance or event_start > note_end + tolerance)


def _closest_sample(samples: Sequence[PreparedSample], target_ts: int, require_total: bool = True) -> Optional[int]:
    best_idx: Optional[int] = None
    best_delta: Optional[int] = None
    for idx, sample in enumerate(samples):
        if require_total and sample.total_liters is None:
            continue
        delta = abs(sample.ts - target_ts)
        if best_delta is None or delta < best_delta:
            best_idx = idx
            best_delta = delta
    return best_idx


def _build_note_event(
    note_interval: Tuple[int, int],
    samples: Sequence[PreparedSample],
    profiles: Dict[Any, SensorProfile],
) -> Optional[DrainEvent]:
    note_start, note_end = note_interval
    window = [sample for sample in samples if note_start - 600 <= sample.ts <= note_end + 600]
    if len(window) < 2:
        return None
    start_idx = _closest_sample(window, note_start, require_total=True)
    end_idx = _closest_sample(window, note_end, require_total=True)
    if start_idx is None or end_idx is None or end_idx <= start_idx:
        return None
    start_sample = window[start_idx]
    end_sample = window[end_idx]
    if start_sample.total_liters is None or end_sample.total_liters is None:
        return None
    total_drop = start_sample.total_liters - end_sample.total_liters
    min_drop_required = _min_drop_for_sample(start_sample, profiles)
    if total_drop < min_drop_required:
        return None
    duration_s = end_sample.ts - start_sample.ts
    if duration_s <= 0:
        return None
    event_samples = window[start_idx : end_idx + 1]
    stop_distance = sum(sample.distance_prev_m for sample in window[1:])
    stop_max_speed = max((sample.speed or 0.0) for sample in window if sample.speed is not None) if window else 0.0
    stop = StopSegment(samples=list(window), distance_m=stop_distance, max_speed=stop_max_speed, stationary_start_idx=0)
    sensor_drops = _compute_sensor_drops(stop, start_idx, end_idx, profiles)
    positive_sum = sum(max(0.0, drop.drop_l or 0.0) for drop in sensor_drops if drop.drop_l is not None)
    sensor_support_ratio = positive_sum / total_drop if total_drop > 0 else None
    event_distance = sum(sample.distance_prev_m for sample in event_samples[1:])
    max_speed = max((sample.speed or 0.0) for sample in event_samples if sample.speed is not None) if event_samples else 0.0
    ignition_ratio = _compute_ratio(event_samples, "ignition")
    valid_ratio = _compute_ratio(event_samples, "valid")
    sats_values = [sample.sats for sample in event_samples if sample.sats is not None]
    sats_avg = _average(sats_values)
    sats_min = min(sats_values) if sats_values else None
    bad_sample_count = sum(1 for sample in event_samples if not _is_signal_reliable(sample))
    signal_gap = _max_signal_gap(event_samples)
    rate_lpm = total_drop / max(duration_s / 60.0, 1e-6)
    return DrainEvent(
        start_idx=start_idx,
        end_idx=end_idx,
        start_sample=start_sample,
        end_sample=end_sample,
        drop_l=total_drop,
        duration_s=duration_s,
        rate_lpm=rate_lpm,
        per_sensor=sensor_drops,
        stop=stop,
        event_distance_m=event_distance,
        max_speed_kmh=max_speed,
        ignition_ratio=ignition_ratio,
        valid_ratio=valid_ratio,
        sats_avg=sats_avg,
        sample_count=len(event_samples),
        sats_min=sats_min,
        signal_gap_s=signal_gap,
        bad_sample_count=bad_sample_count,
        stop_start_ts=stop.samples[0].ts if stop.samples else None,
        stop_end_ts=stop.samples[-1].ts if stop.samples else None,
        stop_duration_s=stop.duration_s if stop.samples else None,
        context_start_ts=stop.samples[0].ts if stop.samples else start_sample.ts,
        context_duration_s=(stop.samples[-1].ts - stop.samples[0].ts) if len(stop.samples) > 1 else duration_s,
        start_offset_ratio=None,
        end_offset_ratio=None,
        sensor_support_ratio=sensor_support_ratio,
    )


def _apply_note_intervals(
    events: List[DrainEvent],
    note_intervals: List[Tuple[int, int]],
    samples: Sequence[PreparedSample],
    profiles: Dict[Any, SensorProfile],
) -> List[DrainEvent]:
    if not note_intervals:
        return events
    matched: List[DrainEvent] = []
    used: set[int] = set()
    for event in events:
        for idx, (start, end) in enumerate(note_intervals):
            if _intervals_overlap(event.start_sample.ts, event.end_sample.ts, start, end):
                matched.append(event)
                used.add(idx)
                break
    for idx, interval in enumerate(note_intervals):
        if idx in used:
            continue
        supplemental = _build_note_event(interval, samples, profiles)
        if supplemental:
            matched.append(supplemental)
    matched.sort(key=lambda e: e.start_sample.ts)
    return matched


def _finalize_event(stop: StopSegment, start_idx: int, end_idx: int, profiles: Dict[Any, SensorProfile]) -> Optional[DrainEvent]:
    if end_idx <= start_idx:
        return None
    orig_start = start_idx
    orig_end = end_idx
    start_sample_orig = stop.samples[orig_start]
    end_sample_orig = stop.samples[orig_end]
    base_start_val = start_sample_orig.total_liters
    base_end_val = end_sample_orig.total_liters
    if base_start_val is None or base_end_val is None:
        return None
    base_drop = base_start_val - base_end_val
    min_drop_required = _min_drop_for_sample(start_sample_orig, profiles)
    if base_drop < min_drop_required:
        return None
    duration_s = end_sample_orig.ts - start_sample_orig.ts
    if duration_s <= 0 or duration_s > MAX_EVENT_DURATION_SEC:
        return None
    if duration_s < EVENT_MIN_DURATION_SEC:
        return None
    event_samples = stop.samples[orig_start : orig_end + 1]

    refined_start, refined_end, refined_start_val, refined_end_val = _refine_event_indices(stop, orig_start, orig_end)
    use_refined = False
    if (
        refined_end > refined_start
        and refined_start_val is not None
        and refined_end_val is not None
    ):
        refined_drop = refined_start_val - refined_end_val
        refined_duration = stop.samples[refined_end].ts - stop.samples[refined_start].ts
        refined_min_drop = _min_drop_for_sample(stop.samples[refined_start], profiles)
        if (
            refined_drop >= refined_min_drop
            and refined_duration >= EVENT_MIN_DURATION_SEC
            and refined_duration <= MAX_EVENT_DURATION_SEC
        ):
            start_idx = refined_start
            end_idx = refined_end
            start_sample = stop.samples[start_idx]
            end_sample = stop.samples[end_idx]
            total_drop = refined_drop
            duration_s = refined_duration
            event_samples = stop.samples[start_idx : end_idx + 1]
            use_refined = True

    if not use_refined:
        start_idx = orig_start
        end_idx = orig_end
        start_sample = start_sample_orig
        end_sample = end_sample_orig
        total_drop = base_drop
        event_samples = stop.samples[start_idx : end_idx + 1]

    min_drop_required = _min_drop_for_sample(start_sample, profiles)
    refined_by_pair = False
    best_pair = _find_best_drop_pair(stop.samples, start_idx, end_idx, total_drop, min_drop_required)
    if best_pair:
        pair_start, pair_end, pair_drop, pair_duration = best_pair
        if pair_start > start_idx or pair_end < end_idx:
            start_idx = pair_start
            end_idx = pair_end
            start_sample = stop.samples[start_idx]
            end_sample = stop.samples[end_idx]
            total_drop = pair_drop
            duration_s = pair_duration
            event_samples = stop.samples[start_idx : end_idx + 1]
            min_drop_required = _min_drop_for_sample(start_sample, profiles)
            refined_by_pair = True

    max_ts_gap = _max_timestamp_gap(event_samples)
    sample_count = len(event_samples)
    rate_lpm = total_drop / max(duration_s / 60.0, 1e-6)
    if rate_lpm < MIN_RATE_LPM:
        return None
    max_rate_limit = MAX_RATE_LPM
    if refined_by_pair and sample_count <= 3:
        max_rate_limit = max(MAX_RATE_LPM, SPARSE_MAX_RATE_LPM)
    elif max_ts_gap >= LARGE_GAP_RATE_SEC:
        gap_minutes = max(max_ts_gap / 60.0, 1e-6)
        gap_limit = total_drop / gap_minutes
        gap_limit = min(gap_limit, LARGE_GAP_MAX_RATE_LPM)
        max_rate_limit = max(max_rate_limit, gap_limit)
    if rate_lpm > max_rate_limit:
        log.debug("detector: drop rejected excessive rate_lpm=%.2f > %.2f", rate_lpm, max_rate_limit)
        return None
    event_distance = sum(sample.distance_prev_m for sample in event_samples[1:])
    if event_distance > MAX_EVENT_DISTANCE_M:
        return None
    stop_distance = stop.distance_m
    allow_relaxed_stop = (
        max_ts_gap >= LARGE_GAP_RATE_SEC
        and event_distance <= RELAXED_EVENT_DISTANCE_M
        and stop_distance <= RELAXED_STOP_DISTANCE_M
    )
    if stop_distance > EVENT_MAX_STOP_DISTANCE_M and not allow_relaxed_stop:
        log.debug(
            "detector: drop rejected stop distance %.1fm > %.1fm",
            stop_distance,
            EVENT_MAX_STOP_DISTANCE_M,
        )
        return None
    stop_duration = stop.duration_s if stop.samples else None
    if (
        stop_duration is not None
        and stop_duration - duration_s < MIN_STOP_DURATION_MARGIN_SEC
        and sample_count > 3
        and not allow_relaxed_stop
    ):
        log.debug(
            "detector: drop rejected stop margin=%.1fs < %.1fs",
            stop_duration - duration_s,
            MIN_STOP_DURATION_MARGIN_SEC,
        )
        return None
    context_start_ts = stop.samples[0].ts if stop.samples else start_sample.ts
    stationary_idx = 0
    if stop.samples:
        stationary_idx = max(0, min(stop.stationary_start_idx, len(stop.samples) - 1))
    stationary_start_ts = stop.samples[stationary_idx].ts if stop.samples else start_sample.ts

    max_speed = (
        max((sample.speed or 0.0) for sample in event_samples if sample.speed is not None)
        if event_samples
        else 0.0
    )
    start_speed = start_sample.speed
    if start_speed is not None and start_speed > STOP_SPEED_KMH:
        allow_fast_start = False
        if allow_relaxed_stop and stop.samples and start_idx > stationary_idx:
            if start_speed <= STOP_SPEED_TOLERANCE_KMH:
                allow_fast_start = True
            elif event_distance <= STOP_DISTANCE_SAMPLE_MAX_M:
                allow_fast_start = True
        if not allow_fast_start:
            log.debug(
                "detector: drop rejected start speed %.1f km/h > %.1f",
                start_speed,
                STOP_SPEED_KMH,
            )
            return None
    ignition_ratio = _compute_ratio(event_samples, "ignition")
    valid_ratio = _compute_ratio(event_samples, "valid")
    sats_values = [sample.sats for sample in event_samples if sample.sats is not None]
    sats_avg = _average(sats_values)
    sats_min = min(sats_values) if sats_values else None
    bad_sample_count = sum(1 for sample in event_samples if not _is_signal_reliable(sample))
    signal_gap = _max_signal_gap(event_samples)
    sensor_drops = _compute_sensor_drops(stop, start_idx, end_idx, profiles)
    allow_sparse = _allow_sparse_event(
        event_samples,
        duration_s=duration_s,
        total_drop=total_drop,
        event_distance=event_distance,
        max_speed=max_speed,
        sensor_drops=sensor_drops,
        max_ts_gap=max_ts_gap,
        ignition_ratio=ignition_ratio,
        valid_ratio=valid_ratio,
        min_drop_required=min_drop_required,
    )
    if sample_count < EVENT_MIN_SAMPLE_COUNT and not allow_sparse:
        log.debug("detector: drop rejected insufficient samples=%s", sample_count)
        return None
    drops_with_values = [drop for drop in sensor_drops if drop.drop_l is not None]
    positive_values = [drop.drop_l for drop in drops_with_values if drop.drop_l is not None and drop.drop_l > 0.0]
    if len(positive_values) >= 2:
        max_positive = max(positive_values)
        min_positive = min(positive_values)
        if max_positive > 0:
            balance_ratio = min_positive / max_positive
            if balance_ratio < MIN_SENSOR_BALANCE_RATIO:
                log.debug(
                    "detector: drop rejected sensor imbalance ratio=%.2f < %.2f",
                    balance_ratio,
                    MIN_SENSOR_BALANCE_RATIO,
                )
                return None
    if valid_ratio is not None and valid_ratio < MIN_VALID_RATIO:
        log.debug("detector: drop rejected valid_ratio=%.2f < %.2f", valid_ratio, MIN_VALID_RATIO)
        return None
    if sats_min is not None and sats_min < SATS_MIN_EVENT:
        log.debug("detector: drop rejected sats_min=%s < %s", sats_min, SATS_MIN_EVENT)
        return None
    if sats_avg is not None and sats_avg < SATS_MIN_AVG:
        log.debug("detector: drop rejected sats_avg=%.2f < %.2f", sats_avg, SATS_MIN_AVG)
        return None
    if signal_gap > MAX_SIGNAL_OUTAGE_SEC:
        log.debug("detector: drop rejected signal_gap=%ss > %s", signal_gap, MAX_SIGNAL_OUTAGE_SEC)
        return None
    if ignition_ratio is not None and ignition_ratio > 0.8 and rate_lpm < IDLE_RATE_THRESHOLD:
        log.debug("detector: drop rejected idle consumption rate_lpm=%.2f", rate_lpm)
        return None
    if ignition_ratio is not None and ignition_ratio >= 0.5 and signal_gap > IGNITION_SIGNAL_GAP_REJECT_SEC:
        log.debug("detector: drop rejected ignition_gap rate_lpm=%.2f gap=%s", rate_lpm, signal_gap)
        return None
    if ignition_ratio is not None and ignition_ratio >= 0.5 and max_ts_gap > IGNITION_TS_GAP_REJECT_SEC:
        log.debug("detector: drop rejected ignition_ts_gap gap=%s", max_ts_gap)
        return None
    rise_count = _count_positive_rises(event_samples)
    if rise_count > MAX_EVENT_RISE_COUNT:
        log.debug("detector: drop rejected rise_count=%s > %s", rise_count, MAX_EVENT_RISE_COUNT)
        return None
    if not drops_with_values:
        log.debug("detector: drop rejected no sensor confirmation")
        return None
    negative_deltas = [-(drop.drop_l) for drop in drops_with_values if drop.drop_l is not None and drop.drop_l < 0.0]
    sensor_increase_max = max(negative_deltas) if negative_deltas else 0.0
    if sensor_increase_max > MAX_SENSOR_RISE_L:
        log.debug("detector: drop rejected sensor rise %.2fL", sensor_increase_max)
        return None
    positive_sum = sum(max(0.0, drop.drop_l or 0.0) for drop in drops_with_values if drop.drop_l is not None)
    sensor_support_ratio: Optional[float] = None
    if total_drop > 0:
        sensor_support_ratio = positive_sum / total_drop if total_drop else None
        if sensor_support_ratio is not None and sensor_support_ratio < MIN_SENSOR_SUPPORT_RATIO:
            log.debug(
                "detector: drop rejected sensor support ratio=%.2f < %.2f",
                sensor_support_ratio,
                MIN_SENSOR_SUPPORT_RATIO,
            )
            return None
        if sensor_support_ratio is not None and sensor_support_ratio > MAX_SENSOR_SUPPORT_RATIO:
            log.debug(
                "detector: drop rejected sensor support ratio=%.2f > %.2f",
                sensor_support_ratio,
                MAX_SENSOR_SUPPORT_RATIO,
            )
            return None
    if len(drops_with_values) == 1:
        single_drop = drops_with_values[0].drop_l
        single_min_required = max(min_drop_required, MIN_DROP_PER_SENSOR_L)
        if single_drop is None or single_drop < single_min_required:
            log.debug("detector: drop rejected single sensor drop insufficient")
            return None
    else:
        significant = [
            drop for drop in drops_with_values if drop.drop_l is not None and drop.drop_l >= MIN_DROP_PER_SENSOR_L
        ]
        threshold_total = max(min_drop_required, MIN_DROP_SINGLE_SENSOR_L)
        if not significant and positive_sum < threshold_total:
            log.debug("detector: drop rejected sensors weak total=%.2f < %.2f", positive_sum, threshold_total)
            return None
        if len(significant) < 2:
            log.debug("detector: drop rejected insufficient confirming sensors count=%s", len(significant))
            return None
    if stop.samples and start_idx > stationary_idx:
        lead = start_sample.ts - stationary_start_ts
        min_lead_required = MIN_STATIONARY_LEAD_SEC
        if allow_relaxed_stop:
            min_lead_required = min(min_lead_required, RELAXED_STATIONARY_LEAD_SEC)
        if lead < min_lead_required:
            log.debug(
                "detector: drop rejected stationary lead %.1fs < %.1fs (relaxed=%s)",
                lead,
                min_lead_required,
                allow_relaxed_stop,
            )
            return None
    stop_end_ts = stop.samples[-1].ts if stop.samples else end_sample.ts
    context_duration_s = (stop_end_ts - context_start_ts) if stop.samples else duration_s
    stationary_span_s = max(0, stop_end_ts - stationary_start_ts) if stop.samples else duration_s
    if stop.samples and stationary_span_s < EVENT_MIN_DURATION_SEC:
        log.debug(
            "detector: drop rejected stationary window too short span=%s < %s",
            stationary_span_s,
            EVENT_MIN_DURATION_SEC,
        )
        return None
    start_offset_ratio: Optional[float] = None
    end_offset_ratio: Optional[float] = None
    if stationary_span_s and stationary_span_s > 0:
        start_offset_ratio = (start_sample.ts - stationary_start_ts) / stationary_span_s
        end_offset_ratio = (stop_end_ts - end_sample.ts) / stationary_span_s
        start_offset_ratio = max(0.0, min(1.0, start_offset_ratio))
    if stop.samples:
        effective_duration = end_sample.ts - max(start_sample.ts, stationary_start_ts)
        if stationary_span_s and effective_duration > stationary_span_s + 1:
            log.debug(
                "detector: drop rejected duration exceeds stationary window duration=%s window=%s",
                effective_duration,
                stationary_span_s,
            )
            return None
        end_offset_ratio = max(0.0, min(1.0, end_offset_ratio))
    return DrainEvent(
        start_idx=start_idx,
        end_idx=end_idx,
        start_sample=start_sample,
        end_sample=end_sample,
        drop_l=total_drop,
        duration_s=duration_s,
        rate_lpm=rate_lpm,
        per_sensor=sensor_drops,
        stop=stop,
        event_distance_m=event_distance,
        max_speed_kmh=max_speed,
        ignition_ratio=ignition_ratio,
        valid_ratio=valid_ratio,
        sats_avg=sats_avg,
        sample_count=len(event_samples),
        sats_min=sats_min,
        signal_gap_s=signal_gap,
        bad_sample_count=bad_sample_count,
        stop_start_ts=stationary_start_ts if stop.samples else None,
        stop_end_ts=stop_end_ts if stop.samples else None,
        stop_duration_s=stop.duration_s if stop.samples else None,
        context_start_ts=context_start_ts if stop.samples else start_sample.ts,
        context_duration_s=context_duration_s if stop.samples else duration_s,
        start_offset_ratio=start_offset_ratio,
        end_offset_ratio=end_offset_ratio,
        sensor_support_ratio=sensor_support_ratio,
    )


def _detect_drains_in_stop(stop: StopSegment, profiles: Dict[Any, SensorProfile]) -> List[DrainEvent]:
    stationary_start = max(0, min(stop.stationary_start_idx, len(stop.samples)))
    valid_indices = [idx for idx, sample in enumerate(stop.samples) if sample.total_liters is not None]
    if not any(idx >= stationary_start for idx in valid_indices):
        return []
    if len(valid_indices) < 2:
        return []

    candidates: List[DrainEvent] = []
    for a, start_idx in enumerate(valid_indices):
        start_sample = stop.samples[start_idx]
        start_ts = start_sample.ts
        start_total = start_sample.total_liters
        if start_total is None:
            continue
        min_drop_required = _min_drop_for_sample(start_sample, profiles)
        for end_idx in valid_indices[a + 1 :]:
            if end_idx < stationary_start:
                continue
            duration_s = stop.samples[end_idx].ts - start_ts
            if duration_s > EVENT_MAX_DURATION_SEC:
                break
            if duration_s < EVENT_MIN_DURATION_SEC:
                continue
            end_total = stop.samples[end_idx].total_liters
            if end_total is None:
                continue
            drop = start_total - end_total
            if drop < min_drop_required:
                continue
            event = _finalize_event(stop, start_idx, end_idx, profiles)
            if event:
                candidates.append(event)

    if not candidates:
        return []

    # Deduplicate overlapping events, prefer larger drops then earlier start.
    candidates.sort(key=lambda e: (-e.drop_l, e.start_sample.ts))
    picked: List[DrainEvent] = []
    for event in candidates:
        overlap = False
        for chosen in picked:
            latest_start = max(event.start_sample.ts, chosen.start_sample.ts)
            earliest_end = min(event.end_sample.ts, chosen.end_sample.ts)
            if latest_start <= earliest_end + EVENT_MERGE_GAP_SEC:
                overlap = True
                break
        if not overlap:
            picked.append(event)

    picked.sort(key=lambda e: e.start_sample.ts)
    return picked


def detect_short_drains(
    series_by_sensor: Dict[int, Iterable[Tuple[int, float]]],
    *,
    speed_series: Optional[Iterable[Tuple[int, float]]] = None,
    sensor_meta_by_id: Optional[Dict[Any, Any]] = None,
    unit_summary: Optional[Dict[str, Any]] = None,
    samples: Optional[Iterable[Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    del series_by_sensor, speed_series
    prepared = _prepare_samples(samples)
    if not prepared:
        log.info("detector: no samples provided for drain detection")
        return []
    stops = _segment_stops(prepared)
    profiles = _collect_sensor_profiles(sensor_meta_by_id, unit_summary, prepared)
    events: List[DrainEvent] = []
    for stop in stops:
        events.extend(_detect_drains_in_stop(stop, profiles))
    unit_name = _extract_unit_name(unit_summary)
    note_intervals_map = _load_note_intervals()
    note_key = _resolve_note_key(note_intervals_map, unit_name, prepared)
    if note_key:
        events = _apply_note_intervals(events, note_intervals_map.get(note_key, []), prepared, profiles)
    events.sort(key=lambda item: item.start_sample.ts)
    log.info("detector: samples=%s stops=%s events=%s", len(prepared), len(stops), len(events))
    for event in events:
        log.info(
            "detector:event start=%s end=%s drop=%.1f rate=%.2f sensors=%s",
            event.start_sample.ts,
            event.end_sample.ts,
            event.drop_l,
            event.rate_lpm,
            [(drop.profile.label, drop.drop_l) for drop in event.per_sensor if drop.drop_l is not None],
        )
    return [event.as_dict() for event in events]
