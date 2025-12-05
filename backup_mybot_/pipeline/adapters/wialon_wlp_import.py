"""Utilities to convert Wialon .wlp exports into UnitConfig dataclasses."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from pipeline.config.unit_config import (
    CalibrationPoint,
    CalibrationSegment,
    CounterConfig,
    GeneralConfig,
    HWConfig,
    HWParam,
    IconConfig,
    IntervalConfig,
    ProfileField,
    SensorCalibration,
    SensorConfig,
    SensorValidation,
    UnitConfig,
)
from pipeline.config.unit_config_service import UnitConfigService


def load_wlp_unit_config(path: Path | str, *, unit_id: int, source_kind: str = "wialon") -> UnitConfig:
    """Parse a .wlp file into a UnitConfig."""

    resolved = Path(path)
    payload = json.loads(resolved.read_text(encoding="utf-8"))
    return unit_config_from_wlp_payload(payload, unit_id=unit_id, source_kind=source_kind)


def unit_config_from_wlp_payload(
    payload: Mapping[str, Any],
    *,
    unit_id: int,
    source_kind: str = "wialon",
) -> UnitConfig:
    general = _parse_general(payload.get("general") or {})
    name = general.name or str(unit_id)
    icon = _parse_icon(payload.get("icon") or {})
    hw_config = _parse_hw(payload.get("hwConfig") or {})
    counters = _parse_counters(payload.get("counters"))
    profile = _parse_profile(payload.get("profile"))
    intervals = _parse_intervals(payload.get("intervals"))
    sensors = _parse_sensors(payload.get("sensors"))

    advanced = _ensure_dict(payload.get("advProps"))
    report_props = _ensure_dict(payload.get("reportProps"))
    aliases = _ensure_list(payload.get("aliases"))
    driving = _ensure_dict(payload.get("driving"))
    trip = _ensure_dict(payload.get("trip"))
    extra = _collect_extra(payload, exclude_keys=_KNOWN_TOP_LEVEL)

    return UnitConfig(
        unit_id=unit_id,
        source_kind=source_kind,
        dump_ts=None,
        general=general if general.name else GeneralConfig(name=name),
        icon=icon,
        hw_config=hw_config,
        counters=counters,
        advanced=advanced,
        profile=profile,
        intervals=intervals,
        sensors=sensors,
        report_props=report_props,
        aliases=aliases,
        driving=driving,
        trip=trip,
        extra=extra,
    )


def import_wlp_to_service(
    input_path: Path | str,
    *,
    unit_id: int,
    service: UnitConfigService,
    source_kind: str = "wialon",
) -> Path:
    """Load a .wlp file and persist UnitConfig via UnitConfigService."""

    config = load_wlp_unit_config(input_path, unit_id=unit_id, source_kind=source_kind)
    return service.save(config)


def _parse_general(block: Mapping[str, Any]) -> GeneralConfig:
    extra = _collect_extra(block, exclude_keys={"n", "uid", "uid2", "ph", "ph2", "psw", "hw"})
    return GeneralConfig(
        name=str(block.get("n") or "").strip() or "",
        uid=_opt_str(block.get("uid")),
        uid2=_opt_str(block.get("uid2")),
        phone=_opt_str(block.get("ph")),
        phone2=_opt_str(block.get("ph2")),
        password=_opt_str(block.get("psw")),
        hardware=_opt_str(block.get("hw")),
        extra=extra,
    )


def _parse_icon(block: Mapping[str, Any]) -> IconConfig:
    if not block:
        return IconConfig()
    extra = _collect_extra(block, exclude_keys={"lib", "url", "imgRot"})
    rotation_mode = _opt_str(block.get("imgRot")) or _opt_str(block.get("rotation_mode"))
    return IconConfig(
        library=_opt_str(block.get("lib") or block.get("library")),
        url=_opt_str(block.get("url")),
        rotation_mode=rotation_mode,
        extra=extra,
    )


def _parse_hw(block: Mapping[str, Any]) -> Optional[HWConfig]:
    if not block:
        return None
    params = []
    for raw in _ensure_list(block.get("params")):
        if not isinstance(raw, Mapping):
            continue
        extra = _collect_extra(
            raw,
            exclude_keys={"name", "label", "type", "value", "default", "description", "readonly", "minval", "maxval"},
        )
        params.append(
            HWParam(
                name=str(raw.get("name") or "").strip(),
                label=_opt_str(raw.get("label")),
                type=_opt_str(raw.get("type")),
                value=raw.get("value"),
                default=raw.get("default"),
                description=_opt_str(raw.get("description")),
                readonly=_opt_bool(raw.get("readonly")),
                minval=_opt_float(raw.get("minval")),
                maxval=_opt_float(raw.get("maxval")),
                extra=extra,
            )
        )
    extra = _collect_extra(block, exclude_keys={"hw", "fullData", "params"})
    return HWConfig(
        hardware=_opt_str(block.get("hw")),
        full_data=_opt_bool(block.get("fullData")),
        params=params,
        extra=extra,
    )


def _parse_counters(block: Any) -> Optional[CounterConfig]:
    data = _ensure_dict(block)
    if not data:
        return None
    values: Dict[str, float] = {}
    for key, value in data.items():
        try:
            values[str(key)] = float(value)
        except (TypeError, ValueError):
            continue
    return CounterConfig(values=values)


def _parse_profile(block: Any) -> List[ProfileField]:
    fields: List[ProfileField] = []
    for entry in _iter_entries(block):
        if not isinstance(entry, Mapping):
            continue
        name = str(entry.get("name") or entry.get("n") or "").strip()
        if not name:
            continue
        extra = _collect_extra(entry, exclude_keys={"id", "field_id", "seq", "name", "n", "value", "v", "meta"})
        try:
            field_id = int(entry.get("field_id") or entry.get("id") or entry.get("seq") or 0)
        except Exception:
            field_id = 0
        value = entry.get("value", entry.get("v"))
        meta = _ensure_dict(entry.get("meta"))
        if extra:
            meta.update(extra)
        fields.append(ProfileField(field_id=field_id, name=name, value=value, meta=meta))
    return fields


def _parse_intervals(block: Any) -> List[IntervalConfig]:
    intervals: List[IntervalConfig] = []
    for entry in _iter_entries(block):
        if not isinstance(entry, Mapping):
            continue
        try:
            interval_id = int(entry.get("interval_id") or entry.get("id") or entry.get("seq") or 0)
        except Exception:
            continue
        name = str(entry.get("name") or entry.get("n") or "").strip()
        if not name:
            continue
        params = _ensure_dict(entry.get("params"))
        intervals.append(IntervalConfig(interval_id=interval_id, name=name, params=params))
    return intervals


def _parse_sensors(block: Any) -> List[SensorConfig]:
    sensors: List[SensorConfig] = []
    for entry in _ensure_list(block):
        config = sensor_config_from_entry(entry)
        if config is not None:
            sensors.append(config)
    return sensors


def sensor_config_from_entry(entry: Mapping[str, Any]) -> Optional[SensorConfig]:
    if not isinstance(entry, Mapping):
        return None
    try:
        sensor_id = int(entry.get("id"))
    except Exception:
        sensor_id = 0
    name = str(entry.get("n") or entry.get("name") or "").strip() or f"Sensor {sensor_id}" if sensor_id else None
    if not name:
        return None
    sensor_type = str(entry.get("t") or entry.get("type") or "").strip() or "custom"
    parameters = _extract_sensor_parameters(entry)
    validation = _extract_sensor_validation(entry)
    calibration = _extract_sensor_calibration(entry, sensor_id=sensor_id or int(hash(name) & 0xFFFFFFFF))
    meta = _collect_extra(entry, exclude_keys=_SENSOR_KNOWN_FIELDS)
    return SensorConfig(
        sensor_id=sensor_id,
        name=name,
        type=sensor_type,
        units=_opt_str(entry.get("u") or entry.get("units") or entry.get("m")),
        description=_opt_str(entry.get("d") or entry.get("description")),
        parameters=parameters,
        expression=_opt_str(entry.get("p") or entry.get("expression")),
        validation=validation,
        calibration=calibration,
        meta=meta,
    )


def _extract_sensor_parameters(entry: Mapping[str, Any]) -> Dict[str, Any]:
    parameters: Dict[str, Any] = {}
    for key in ("p", "params", "m", "f"):
        value = entry.get(key)
        if value is None:
            continue
        if isinstance(value, Mapping):
            parameters[key] = dict(value)
        else:
            parameters[key] = value
    return parameters


def _extract_sensor_validation(entry: Mapping[str, Any]) -> Optional[SensorValidation]:
    validation_block = entry.get("validation") or entry.get("val")
    if not isinstance(validation_block, Mapping):
        return None
    return SensorValidation(
        mode=_opt_str(validation_block.get("mode")),
        min_value=_opt_float(validation_block.get("min") or validation_block.get("min_value")),
        max_value=_opt_float(validation_block.get("max") or validation_block.get("max_value")),
        hysteresis=_opt_float(validation_block.get("hysteresis") or validation_block.get("hyst")),
        params=_ensure_dict(validation_block.get("params")),
    )


def _extract_sensor_calibration(entry: Mapping[str, Any], *, sensor_id: int) -> Optional[SensorCalibration]:
    table = entry.get("tbl") or entry.get("calibration")
    if not table:
        return None
    points: List[CalibrationPoint] = []
    segments: List[CalibrationSegment] = []
    for row in _ensure_list(table):
        if isinstance(row, Sequence) and len(row) >= 2:
            raw_value, out_value = row[0], row[1]
        elif isinstance(row, Mapping):
            raw_value = row.get("x") or row.get("raw")
            out_value = row.get("y") or row.get("value")
        else:
            continue
        raw_f = _opt_float(raw_value)
        val_f = _opt_float(out_value)
        if raw_f is None or val_f is None:
            continue
        points.append(CalibrationPoint(raw=raw_f, value=val_f))
    if not points:
        return None
    sorted_points = sorted(points, key=lambda pt: pt.raw)
    for a, b in zip(sorted_points, sorted_points[1:]):
        dx = b.raw - a.raw
        if dx == 0:
            continue
        slope = (b.value - a.value) / dx
        intercept = a.value - slope * a.raw
        segments.append(CalibrationSegment(start_x=a.raw, a=slope, b=intercept))
    return SensorCalibration(sensor_id=sensor_id, points=sorted_points, segments=segments)


def _ensure_dict(value: Any) -> Dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _ensure_list(value: Any) -> List[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, Mapping):
        return list(value.values())
    if value is None:
        return []
    return [value]


def _iter_entries(value: Any) -> Iterable[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, Mapping):
        return value.values()
    return []


def _opt_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _opt_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _opt_bool(value: Any) -> Optional[bool]:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if not text:
        return None
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return None


def _collect_extra(payload: Mapping[str, Any], *, exclude_keys: Iterable[str]) -> Dict[str, Any]:
    exclusions = {key.lower() for key in exclude_keys}
    extra: Dict[str, Any] = {}
    for key, value in payload.items():
        if key.lower() in exclusions:
            continue
        extra[key] = value
    return extra


_KNOWN_TOP_LEVEL = {
    "type",
    "version",
    "mu",
    "general",
    "icon",
    "hwconfig",
    "counters",
    "advprops",
    "profile",
    "intervals",
    "sensors",
    "reportprops",
    "aliases",
    "driving",
    "trip",
}


_SENSOR_KNOWN_FIELDS = {
    "id",
    "n",
    "name",
    "t",
    "type",
    "u",
    "units",
    "d",
    "description",
    "p",
    "params",
    "m",
    "f",
    "vt",
    "vs",
    "tbl",
    "calibration",
    "validation",
    "val",
}


__all__ = [
    "load_wlp_unit_config",
    "unit_config_from_wlp_payload",
    "import_wlp_to_service",
    "sensor_config_from_entry",
]
