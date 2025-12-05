"""Utilities for identifying DUT (fuel level) sensors for the detector."""
from __future__ import annotations

import logging
import math
import re
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Tuple


from dut_cache import (
    CUSTOM_SENSOR_TYPE_KEYS,
    DUT_ZERO_EPS,
    EXACT_FUEL_TYPE,
    FUEL_MAX_VALID_L,
    FUEL_MIN_VALID_L,
    RE_ANY_DUT,
    RE_DUT_CUSTOM_PATTERN,
    RE_FUEL_ALTS,
    RE_MAIN_FUEL,
    RE_MAIN_FUEL_PREFIX,
    extract_assigned_param_name,
    cached_filtered_custom_sensors,
    extract_param_hint_from_sensor,
    extract_params_from_item,
    normalize_fuel_value,
)


log = logging.getLogger("detector.dut_filter")


@dataclass
class CalibrationSegment:
    """Single calibration segment (y = a*x + b starting from x0)."""

    x0: float
    a: float
    b: float

    def apply(self, raw: float) -> float:
        return self.a * raw + self.b


@dataclass
class CalibrationCurve:
    """Piecewise-linear calibration constructed from Wialon sensor table."""

    segments: List[CalibrationSegment] = field(default_factory=list)
    min_raw: Optional[float] = None
    max_raw: Optional[float] = None

    def is_valid(self) -> bool:
        return bool(self.segments)

    def convert(self, raw: Any) -> Optional[float]:
        if not self.segments:
            return None
        try:
            raw_value = float(raw)
        except (TypeError, ValueError):
            return None
        if math.isnan(raw_value) or math.isinf(raw_value):
            return None
        chosen = self.segments[0]
        for segment in self.segments:
            if raw_value >= segment.x0:
                chosen = segment
            else:
                break
        value = chosen.apply(raw_value)
        if math.isnan(value) or math.isinf(value):
            return None
        return value

    def as_dict(self) -> Dict[str, Any]:
        return {
            "segments": [{"x0": seg.x0, "a": seg.a, "b": seg.b} for seg in self.segments],
            "min_raw": self.min_raw,
            "max_raw": self.max_raw,
        }


@dataclass
class SensorMeta:
    """Metadata about a sensor that may be used for drain detection."""

    id: Optional[int]
    name: str
    sensor_type: str
    raw_sensor: Dict[str, Any]
    match_reason: str
    value: Optional[float]
    param_hint: Optional[str] = None
    assigned_param: Optional[str] = None
    is_primary: bool = False
    is_zero_value: bool = False
    zero_reason: Optional[str] = None
    calibration: Optional[CalibrationCurve] = None
    details: Dict[str, Any] = field(default_factory=dict)

    @property
    def param_key(self) -> Optional[str]:
        base = self.assigned_param or self.param_hint
        return base.strip().lower() if isinstance(base, str) and base.strip() else None

    def convert_raw(self, raw: Any) -> Optional[float]:
        if not self.calibration:
            return None
        return self.calibration.convert(raw)

    def as_dict(self) -> Dict[str, Any]:
        payload = {
            "id": self.id,
            "name": self.name,
            "type": self.sensor_type,
            "match": self.match_reason,
            "param_hint": self.param_hint,
            "assigned_param": self.assigned_param,
            "param_key": self.param_key,
            "value": self.value,
            "is_primary": self.is_primary,
            "is_zero_value": self.is_zero_value,
            "zero_reason": self.zero_reason,
        }
        if self.calibration and self.calibration.is_valid():
            payload["calibration"] = self.calibration.as_dict()
        if self.details:
            payload["details"] = dict(self.details)
        return payload


@dataclass
class RejectionReason:
    """Information describing why a sensor was rejected."""

    id: Optional[int]
    name: str
    reason: str
    details: Dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> Dict[str, Any]:
        payload = {"id": self.id, "name": self.name, "reason": self.reason}
        if self.details:
            payload["details"] = dict(self.details)
        return payload


def _sensor_id(sensor: Dict[str, Any]) -> Optional[int]:
    for key in ("id", "i", "sensor_id"):
        if key in sensor:
            try:
                return int(sensor[key])
            except Exception:
                continue
    return None


def _sensor_name(sensor: Dict[str, Any]) -> str:
    return (sensor.get("n") or sensor.get("name") or "").strip()


def _sensor_type(sensor: Dict[str, Any]) -> str:
    return (sensor.get("t") or sensor.get("type") or "").strip()


def _match_reason(name: str, type_key: str) -> Optional[str]:
    if type_key == EXACT_FUEL_TYPE:
        return "type=fuel level"
    if name:
        if RE_MAIN_FUEL.match(name):
            return "name=main"
        if RE_MAIN_FUEL_PREFIX.search(name):
            return "name=prefix"
        for pattern in RE_FUEL_ALTS:
            if pattern.match(name):
                return "name=alt"
        if RE_ANY_DUT.match(name) or RE_DUT_CUSTOM_PATTERN.match(name):
            return "name=dut"
    if type_key in CUSTOM_SENSOR_TYPE_KEYS:
        return "type=custom"
    return None


def _extract_value(
    sensor: Dict[str, Any], params_map: Dict[str, str]
) -> Tuple[Optional[float], Optional[str], Optional[str]]:
    value_candidate: Any = None
    for key in ("last_value", "v", "value"):
        if key in sensor:
            value_candidate = sensor.get(key)
            break
    assigned = extract_assigned_param_name(sensor)
    param_hint = extract_param_hint_from_sensor(sensor)
    if not assigned:
        assigned = param_hint
    if (value_candidate is None or str(value_candidate).strip() == "") and assigned:
        value_candidate = params_map.get(assigned)
    value_numeric = normalize_fuel_value(value_candidate)
    return value_numeric, assigned, param_hint


def _build_calibration_curve(sensor: Dict[str, Any]) -> Optional[CalibrationCurve]:
    tbl = sensor.get("tbl")
    if not isinstance(tbl, list):
        return None

    segments: List[CalibrationSegment] = []
    min_raw: Optional[float] = None
    max_raw: Optional[float] = None
    for entry in tbl:
        if not isinstance(entry, dict):
            continue
        try:
            x0 = float(entry.get("x"))
            a = float(entry.get("a"))
            b = float(entry.get("b"))
        except (TypeError, ValueError):
            continue
        if any(math.isnan(val) or math.isinf(val) for val in (x0, a, b)):
            continue
        segments.append(CalibrationSegment(x0=x0, a=a, b=b))
        min_raw = x0 if min_raw is None else min(min_raw, x0)
        max_raw = x0 if max_raw is None else max(max_raw, x0)

    if not segments:
        return None

    segments.sort(key=lambda seg: seg.x0)
    return CalibrationCurve(segments=segments, min_raw=min_raw, max_raw=max_raw)


def _evaluate_sensor(
    sensor: Dict[str, Any], params_map: Dict[str, str]
) -> Tuple[Optional[SensorMeta], Optional[RejectionReason]]:
    if not isinstance(sensor, dict):
        return None, RejectionReason(None, "", "invalid_payload")

    sid = _sensor_id(sensor)
    name = _sensor_name(sensor)
    if not name:
        return None, RejectionReason(sid, "", "missing_name")

    stype_raw = _sensor_type(sensor)
    type_key = stype_raw.lower()
    match_reason = _match_reason(name, type_key)
    if not match_reason:
        return None, RejectionReason(sid, name, "regex_mismatch", {"type": stype_raw})

    value_numeric, assigned_param, param_hint = _extract_value(sensor, params_map)
    details: Dict[str, Any] = {"match": match_reason}
    if assigned_param:
        details["assigned_param"] = assigned_param
    if param_hint and param_hint != assigned_param:
        details["param_hint"] = param_hint

    if value_numeric is not None:
        if not (FUEL_MIN_VALID_L - 1e-3 <= value_numeric <= FUEL_MAX_VALID_L + 1e-3):
            return None, RejectionReason(
                sid,
                name,
                "invalid_range",
                {"value": value_numeric, "bounds": (FUEL_MIN_VALID_L, FUEL_MAX_VALID_L)},
            )
        details["value"] = value_numeric
    else:
        details["value"] = None

    is_zero = False
    zero_reason: Optional[str] = None
    if value_numeric is not None and abs(value_numeric) <= DUT_ZERO_EPS:
        is_zero = True
        zero_reason = "value≈0"
        if assigned_param:
            zero_reason += f" via {assigned_param}"

    calibration = _build_calibration_curve(sensor)
    if calibration and calibration.is_valid():
        details["calibration_bounds"] = {
            "min_raw": calibration.min_raw,
            "max_raw": calibration.max_raw,
            "segments": len(calibration.segments),
        }
    else:
        calibration = None

    meta = SensorMeta(
        id=sid,
        name=name,
        sensor_type=stype_raw,
        raw_sensor=sensor,
        match_reason=match_reason,
        value=value_numeric,
        param_hint=param_hint,
        assigned_param=assigned_param,
        is_zero_value=is_zero,
        zero_reason=zero_reason,
        calibration=calibration,
        details=details,
    )
    return meta, None


def list_dut_candidates(unit_item: Dict[str, Any]) -> List[SensorMeta]:
    sens_map = (unit_item or {}).get("sens") or {}
    if not isinstance(sens_map, dict):
        return []

    params_map, _ = extract_params_from_item(unit_item or {})
    candidates: List[SensorMeta] = []
    for sensor in sens_map.values():
        meta, reason = _evaluate_sensor(sensor, params_map)
        if meta is not None:
            candidates.append(meta)
        else:
            if reason and log.isEnabledFor(logging.DEBUG):
                log.debug("dut reject: %s", reason.as_dict())
    return candidates


def select_primary_dut(candidates: List[SensorMeta]) -> Optional[SensorMeta]:
    if not candidates:
        return None

    for meta in candidates:
        meta.is_primary = False

    def _match(pattern: re.Pattern[str], meta: SensorMeta) -> bool:
        try:
            return bool(pattern.match(meta.name))
        except Exception:
            return False

    fuel = [m for m in candidates if m.sensor_type.strip().lower() == EXACT_FUEL_TYPE]
    search_space = fuel if fuel else candidates

    for pattern in (RE_MAIN_FUEL, RE_MAIN_FUEL_PREFIX):
        for meta in search_space:
            if _match(pattern, meta):
                meta.is_primary = True
                return meta

    for meta in search_space:
        if any(pattern.match(meta.name) for pattern in RE_FUEL_ALTS):
            meta.is_primary = True
            return meta

    chosen = search_space[0]
    chosen.is_primary = True
    return chosen


def explain_rejection(unit_or_sensors: Any) -> List[RejectionReason]:
    if isinstance(unit_or_sensors, dict) and isinstance(unit_or_sensors.get("sens"), dict):
        sensors = list(unit_or_sensors.get("sens", {}).values())
        params_map, _ = extract_params_from_item(unit_or_sensors)
    else:
        sensors = list(unit_or_sensors or [])
        params_map = {}

    reasons: List[RejectionReason] = []
    for sensor in sensors:
        meta, reason = _evaluate_sensor(sensor, params_map)
        if reason is not None:
            reasons.append(reason)
    return reasons


def resolve_target_duts(
    unit_meta: Dict[str, Any], raw_sensors: Optional[Dict[str, Any]] = None
) -> Dict[str, Dict[str, Any]]:
    """Return mapping of selected DUT sensors keyed by their business names."""

    _, filtered_entries = cached_filtered_custom_sensors(unit_meta or {})
    sens_map = raw_sensors
    if sens_map is None:
        sens_map = (unit_meta or {}).get("sens") or {}
    if not isinstance(sens_map, dict):
        sens_map = {}

    param_to_sensor: Dict[str, Dict[str, Any]] = {}
    for sensor in sens_map.values():
        if not isinstance(sensor, dict):
            continue
        param_name = extract_assigned_param_name(sensor) or extract_param_hint_from_sensor(sensor)
        if not param_name:
            continue
        key = param_name.strip().lower()
        if not key or key in param_to_sensor:
            continue
        param_to_sensor[key] = sensor

    result: Dict[str, Dict[str, Any]] = {}
    for entry in filtered_entries:
        name = (entry.name or '').strip()
        param = (entry.param or '').strip()
        if not name or not param or not entry.param_present:
            continue
        param_key = param.lower()
        sensor = param_to_sensor.get(param_key)

        sensor_id = None
        calibration_dict: Optional[Dict[str, Any]] = None
        raw_sensor_payload: Optional[Dict[str, Any]] = None
        if isinstance(sensor, dict):
            raw_sensor_payload = sensor
            sensor_id_raw = sensor.get('id') or sensor.get('i')
            try:
                sensor_id = int(sensor_id_raw) if sensor_id_raw is not None else None
            except Exception:
                sensor_id = None
            curve = _build_calibration_curve(sensor)
            if curve and curve.is_valid():
                calibration_dict = curve.as_dict()

        value_numeric = entry.value_numeric
        is_zero = value_numeric is not None and abs(value_numeric) <= DUT_ZERO_EPS
        upper = name.upper()
        if '\u0417' in upper:
            kind = 'rear'
        elif '\u041B' in upper:
            kind = 'left'
        else:
            kind = 'other'

        result[name] = {
            'param': param,
            'param_key': param_key,
            'display': entry.display or entry.name,
            'kind': kind,
            'sensor_id': sensor_id,
            'value_numeric': value_numeric,
            'is_zero': is_zero,
            'zero_reason': 'value?0' if is_zero else None,
            'strict_custom': entry.strict_custom,
            'legacy': entry.legacy,
            'calibration': calibration_dict,
            'raw_sensor': raw_sensor_payload,
        }
    return result


__all__ = [
    "CalibrationCurve",
    "CalibrationSegment",
    "SensorMeta",
    "RejectionReason",
    "resolve_target_duts",
    "list_dut_candidates",
    "select_primary_dut",
    "explain_rejection",
]
