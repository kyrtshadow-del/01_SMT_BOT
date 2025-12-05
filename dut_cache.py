from __future__ import annotations

import csv
import io
import logging
import math
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from typing import Any, Dict, Iterable, List, Optional, Tuple

try:  # pragma: no cover - optional dependency
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError  # type: ignore
except Exception:  # pragma: no cover - fallback for Python<3.9 on Windows etc.
    ZoneInfo = None  # type: ignore
    ZoneInfoNotFoundError = Exception  # type: ignore[misc,assignment]


log = logging.getLogger(__name__)

if ZoneInfo:
    try:  # pragma: no cover - depends on OS tzdata availability
        MOSCOW_TZ = ZoneInfo("Europe/Moscow")
    except ZoneInfoNotFoundError:  # pragma: no cover - Windows without tzdata
        MOSCOW_TZ = timezone(timedelta(hours=3))
else:  # pragma: no cover - Python <3.9 fallback
    MOSCOW_TZ = timezone(timedelta(hours=3))

INVALID_SENSOR_VALUE_SENTINEL = -348201.3876
INVALID_SENSOR_TOLERANCE = 1e-3
FUEL_MIN_VALID_L = 0.0
FUEL_MAX_VALID_L = 100000.0

EXACT_FUEL_TYPE = "fuel level"
CUSTOM_SENSOR_TYPE_DISPLAY = "Custom"
CUSTOM_SENSOR_TYPE_KEYS = {"custom", "произвольный датчик"}
SENSOR_TYPE_DISPLAY_OVERRIDES = {
    EXACT_FUEL_TYPE: "fuel level",
    "custom": CUSTOM_SENSOR_TYPE_DISPLAY,
    "произвольный датчик": CUSTOM_SENSOR_TYPE_DISPLAY,
}

RE_ANY_DUT = re.compile(r"^\s*(?:ДУТ|DUT)\b", re.IGNORECASE)
RE_DUT_CUSTOM_PATTERN = re.compile(r"^\s*(?:ДУТ|DUT)\s*(\d+)(.*)$", re.IGNORECASE)
RE_MAIN_FUEL = re.compile(r"^\s*ДУТ\s*$", re.IGNORECASE)
RE_MAIN_FUEL_PREFIX = re.compile(r"^\s*ДУТ(?!\d)", re.IGNORECASE)
RE_FUEL_ALTS = tuple(
    re.compile(pattern, re.IGNORECASE)
    for pattern in (
        r"^\s*fuel\s+level\s*$",
        r"^\s*fuel\s+remain\s*$",
        r"^\s*liquid\s+level\s*$",
    )
)

DUT_ZERO_EPS = 1e-6

__all__ = [
    "RE_ANY_DUT",
    "RE_DUT_CUSTOM_PATTERN",
    "RE_MAIN_FUEL",
    "RE_MAIN_FUEL_PREFIX",
    "RE_FUEL_ALTS",
    "CUSTOM_SENSOR_TYPE_KEYS",
    "CUSTOM_SENSOR_TYPE_DISPLAY",
    "SENSOR_TYPE_DISPLAY_OVERRIDES",
    "EXACT_FUEL_TYPE",
    "INVALID_SENSOR_VALUE_SENTINEL",
    "INVALID_SENSOR_TOLERANCE",
    "FUEL_MIN_VALID_L",
    "FUEL_MAX_VALID_L",
    "format_liters",
    "normalize_fuel_value",
    "select_fuel_sensor",
    "extract_params_from_item",
    "extract_cached_dut_rows",
    "build_unit_cache_preview_rows",
    "build_unit_cache_preview_file",
    "build_dut_zero_report_file",
    "cached_message_summary",
    "cached_raw_message_body",
    "cached_dut_sensor_summaries",
    "cached_custom_sensor_value_summaries",
    "cached_filtered_custom_sensors",
    "summarize_zero_custom_sensors",
    "cached_offline_status",
    "cached_voltage_text",
    "extract_param_hint_from_sensor",
    "extract_assigned_param_name",
]


def _normalize_param_value(val: Any) -> str:
    if isinstance(val, str):
        return val
    if isinstance(val, (int, float)):
        return str(val)
    if isinstance(val, bool):
        return "1" if val else "0"
    if isinstance(val, dict):
        try:
            import json

            return json.dumps(val, ensure_ascii=False)
        except Exception:
            return str(val)
    if isinstance(val, (list, tuple)):
        return ", ".join(_normalize_param_value(v) for v in val)
    if val is None:
        return ""
    return str(val)


def extract_params_from_item(item: Dict[str, Any]) -> Tuple[Dict[str, str], List[str]]:
    params: Dict[str, str] = {}
    order: List[str] = []

    def _add_param(name: Any, value: Any) -> None:
        key = str(name).strip()
        if not key:
            return
        if key not in params:
            order.append(key)
        params[key] = _normalize_param_value(value).strip()

    lmsg = (item or {}).get("lmsg") or {}
    candidates = [
        lmsg.get("p"),
        lmsg.get("prms"),
        lmsg.get("params"),
        lmsg.get("last_message"),
        lmsg.get("d"),
        item.get("last_message"),
    ]

    for candidate in candidates:
        if isinstance(candidate, dict):
            for name, value in candidate.items():
                _add_param(name, value)
        elif isinstance(candidate, (list, tuple)):
            for row in candidate:
                if isinstance(row, dict):
                    name = row.get("n") or row.get("name")
                    value = row.get("v") or row.get("value")
                    if name is not None:
                        _add_param(name, value)
        elif isinstance(candidate, str):
            pieces = [p.strip() for p in candidate.split(",")]
            for piece in pieces:
                if not piece:
                    continue
                if "=" in piece:
                    name, value = piece.split("=", 1)
                else:
                    name, value = piece, ""
                _add_param(name, value)
        if params:
            break

    if not params:
        raw = lmsg.get("t")
        if isinstance(raw, dict):
            for name, value in raw.items():
                _add_param(name, value)

    return params, order


def format_liters(val: Optional[Any]) -> Optional[str]:
    if val is None:
        return None
    try:
        number = float(val)
    except (TypeError, ValueError):
        return None
    if math.isnan(number) or math.isinf(number):
        return None
    q = Decimal(str(number)).quantize(Decimal("0.1"), rounding=ROUND_HALF_UP).normalize()
    s = format(q, "f").rstrip("0").rstrip(".")
    return s or "0"


def normalize_fuel_value(value: Any) -> Optional[float]:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(number) or math.isinf(number):
        return None
    if abs(number - INVALID_SENSOR_VALUE_SENTINEL) <= INVALID_SENSOR_TOLERANCE:
        return None
    return number


def _sensor_name(sensor: Dict[str, Any]) -> str:
    return (sensor.get("n") or "").strip()


def select_fuel_sensor(unit_item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    sens = (unit_item or {}).get("sens") or {}
    if not isinstance(sens, dict) or not sens:
        return None

    sensors: List[Dict[str, Any]] = [v for v in sens.values() if isinstance(v, dict)]
    fuel = [
        s
        for s in sensors
        if isinstance(s.get("t"), str)
        and (s.get("t") or "").strip().lower() == EXACT_FUEL_TYPE
    ]

    def _log_choice(sensor: Dict[str, Any]) -> Dict[str, Any]:
        log.debug(
            "select_fuel_sensor: chosen sensor id=%s name=%r type=%r",
            sensor.get("id"),
            _sensor_name(sensor) or None,
            sensor.get("t"),
        )
        return sensor

    if fuel:
        for sensor in fuel:
            if RE_MAIN_FUEL.match(_sensor_name(sensor)):
                return _log_choice(sensor)
        for sensor in fuel:
            name = _sensor_name(sensor)
            if RE_MAIN_FUEL_PREFIX.search(name):
                return _log_choice(sensor)
            if any(pattern.match(name) for pattern in RE_FUEL_ALTS):
                return _log_choice(sensor)
        return _log_choice(fuel[0])

    named_fallback = [s for s in sensors if RE_MAIN_FUEL.match(_sensor_name(s))]
    if named_fallback:
        return _log_choice(named_fallback[0])

    return None


def extract_param_hint_from_sensor(s: Dict[str, Any]) -> Optional[str]:
    pattern = re.compile(r"\b(?:adc|ai|ain)\d+\b", re.IGNORECASE)
    seen: set[int] = set()

    def walk(obj: Any) -> Optional[str]:
        oid = id(obj)
        if oid in seen:
            return None
        seen.add(oid)
        if isinstance(obj, str):
            m = pattern.search(obj)
            return m.group(0) if m else None
        if isinstance(obj, dict):
            for k in obj.keys():
                if isinstance(k, str):
                    m = pattern.search(k)
                    if m:
                        return m.group(0)
            for v in obj.values():
                res = walk(v)
                if res:
                    return res
        if isinstance(obj, list):
            for v in obj:
                res = walk(v)
                if res:
                    return res
        return None

    return walk(s)


def extract_assigned_param_name(s: Dict[str, Any]) -> Optional[str]:
    preferred_keys = [
        "p",
        "param",
        "src",
        "source",
        "channel",
        "ch",
        "inp",
        "input",
        "uid",
    ]

    def simple(v: Any) -> Optional[str]:
        if isinstance(v, str) and v.strip():
            return v.strip()
        return None

    for key in preferred_keys:
        val = s.get(key)
        if isinstance(val, dict):
            cand = simple(val.get("name") or val.get("n"))
            if cand:
                return cand
        cand = simple(val)
        if cand:
            return cand

    seen: set[int] = set()

    def walk(obj: Any) -> Optional[str]:
        oid = id(obj)
        if oid in seen:
            return None
        seen.add(oid)
        if isinstance(obj, str):
            stripped = obj.strip()
            if stripped:
                return stripped
            return None
        if isinstance(obj, dict):
            for v in obj.values():
                res = walk(v)
                if res:
                    return res
        if isinstance(obj, list):
            for v in obj:
                res = walk(v)
                if res:
                    return res
        return None

    result = walk(s)
    if result:
        return result

    return extract_param_hint_from_sensor(s)


@dataclass
class CachedSensorEntry:
    name: str
    display: str
    legacy: bool
    strict_custom: bool
    value_text: str
    value_numeric: Optional[float]
    param: Optional[str]
    param_present: bool
    sensor: Dict[str, Any]


def _display_type(raw_type: str) -> str:
    key = raw_type.strip().lower()
    if key in SENSOR_TYPE_DISPLAY_OVERRIDES:
        return SENSOR_TYPE_DISPLAY_OVERRIDES[key]
    return raw_type or ""


def _collect_dut_sensor_entries(item: Dict[str, Any]) -> Tuple[
    List[str],
    List[str],
    List[CachedSensorEntry],
    List[CachedSensorEntry],
    List[CachedSensorEntry],
]:
    sens_map = (item or {}).get("sens") or {}
    if not isinstance(sens_map, dict):
        return [], [], [], [], []

    params_map, _ = extract_params_from_item(item)

    dut_summary: List[str] = []
    legacy_summary: List[str] = []
    strict_entries: List[CachedSensorEntry] = []
    legacy_entries: List[CachedSensorEntry] = []
    fuel_entries: List[CachedSensorEntry] = []

    for sensor in sens_map.values():
        if not isinstance(sensor, dict):
            continue
        name = (sensor.get("n") or "").strip()
        if not name:
            continue
        if not RE_ANY_DUT.match(name) and not RE_DUT_CUSTOM_PATTERN.match(name):
            continue
        raw_type = str(sensor.get("t") or "").strip()
        type_key = raw_type.lower()
        display_type = _display_type(type_key or raw_type)

        match = RE_DUT_CUSTOM_PATTERN.match(name)
        suffix = (match.group(2) or "").strip() if match else ""
        strict_custom = bool(match and not suffix)
        legacy_custom = bool(match and suffix)
        is_fuel_type = type_key == EXACT_FUEL_TYPE
        if is_fuel_type:
            dut_summary.append(f"{name}/{display_type or 'fuel level'}")
        elif strict_custom and (type_key in CUSTOM_SENSOR_TYPE_KEYS or not type_key):
            dut_summary.append(f"{name}/{display_type or CUSTOM_SENSOR_TYPE_DISPLAY}")
        else:
            legacy_summary.append(f"{name}/{display_type or CUSTOM_SENSOR_TYPE_DISPLAY}")

        if not (type_key in CUSTOM_SENSOR_TYPE_KEYS or not type_key):
            if is_fuel_type:
                pass
            else:
                # keep as legacy for completeness
                legacy_custom = True

        value_candidate: Any = None
        for key in ("last_value", "v", "value"):
            if key in sensor:
                value_candidate = sensor.get(key)
                break
        param_name: Optional[str] = extract_assigned_param_name(sensor)
        if not param_name:
            param_name = extract_param_hint_from_sensor(sensor)
        if param_name:
            param_name = param_name.strip()
        if (value_candidate is None or str(value_candidate).strip() == "") and param_name:
            value_candidate = params_map.get(param_name)

        value_text = str(value_candidate).strip() if value_candidate not in (None, "") else "_"
        try:
            value_numeric = float(value_candidate)
        except (TypeError, ValueError):
            value_numeric = None

        entry = CachedSensorEntry(
            name=name,
            display=f"{name}/{display_type or CUSTOM_SENSOR_TYPE_DISPLAY}".strip("/"),
            legacy=legacy_custom and not is_fuel_type,
            strict_custom=strict_custom and not is_fuel_type,
            value_text=value_text,
            value_numeric=value_numeric,
            param=param_name,
            param_present=bool(param_name and params_map.get(param_name) not in (None, "")),
            sensor=sensor,
        )

        if entry.strict_custom:
            strict_entries.append(entry)
        elif entry.legacy:
            legacy_entries.append(entry)
        elif is_fuel_type:
            fuel_entries.append(entry)
        else:
            # treat as legacy fallback
            legacy_entries.append(entry)

    dut_summary.sort(key=str.casefold)
    legacy_summary.sort(key=str.casefold)
    strict_entries.sort(key=lambda e: e.name.casefold())
    legacy_entries.sort(key=lambda e: e.name.casefold())

    return dut_summary, legacy_summary, strict_entries, legacy_entries, fuel_entries


def cached_dut_sensor_summaries(item: Dict[str, Any]) -> Tuple[str, str]:
    dut_summary, legacy_summary, _, _, _ = _collect_dut_sensor_entries(item)
    main_text = ", ".join(dut_summary) if dut_summary else "—"
    legacy_text = ", ".join(legacy_summary) if legacy_summary else "—"
    return main_text, legacy_text


def cached_custom_sensor_value_summaries(item: Dict[str, Any]) -> Tuple[str, str]:
    _, _, strict_entries, legacy_entries, _ = _collect_dut_sensor_entries(item)

    def _format(entries: Iterable[CachedSensorEntry]) -> str:
        parts: List[str] = []
        for entry in entries:
            parts.append(f"{entry.display} = {entry.value_text}")
        return ", ".join(parts) if parts else "—"

    return _format(strict_entries), _format(legacy_entries)


def cached_filtered_custom_sensors(
    item: Dict[str, Any]
) -> Tuple[str, List[CachedSensorEntry]]:
    _, _, strict_entries, legacy_entries, _ = _collect_dut_sensor_entries(item)

    def _eligible(entry: CachedSensorEntry) -> bool:
        return bool(entry.param and entry.param_present)

    strict = [e for e in strict_entries if _eligible(e)]
    legacy = [e for e in legacy_entries if _eligible(e)]

    chosen: List[CachedSensorEntry] = []
    used_params: set[str] = set()

    for entry in strict:
        key = entry.param.lower() if entry.param else ""
        if key and key not in used_params:
            chosen.append(entry)
            used_params.add(key)

    for entry in legacy:
        key = entry.param.lower() if entry.param else ""
        if not key or key in used_params:
            continue
        chosen.append(entry)
        used_params.add(key)

    chosen.sort(key=lambda e: e.name.casefold())
    names = ", ".join(entry.name for entry in chosen) if chosen else "—"
    return names, chosen


def summarize_zero_custom_sensors(entries: Iterable[CachedSensorEntry]) -> str:
    problems: List[str] = []
    for entry in entries:
        if entry.value_numeric is None:
            continue
        if abs(entry.value_numeric) <= DUT_ZERO_EPS:
            if entry.param:
                problems.append(f"{entry.name} не работает по {entry.param}")
            else:
                problems.append(f"{entry.name} не работает")
    return ", ".join(problems) if problems else "—"


def cached_message_summary(item: Dict[str, Any]) -> str:
    lmsg = (item or {}).get("lmsg") or {}
    pos = (item or {}).get("pos") or {}

    ts = None
    for candidate in (lmsg.get("t"), pos.get("t"), item.get("last_msg_ts")):
        if candidate:
            try:
                ts = int(candidate)
            except Exception:
                continue
            else:
                break

    if ts:
        dt_utc = datetime.fromtimestamp(ts, tz=timezone.utc)
        dt_local = dt_utc.astimezone(MOSCOW_TZ) if MOSCOW_TZ else dt_utc
        ts_text = dt_local.strftime("%d.%m.%Y %H:%M:%S")
        ago = datetime.now(timezone.utc) - dt_utc
        if ago < timedelta(minutes=1):
            ago_text = "<1 мин назад"
        elif ago < timedelta(hours=1):
            mins = int(ago.total_seconds() // 60)
            ago_text = f"{mins} мин назад"
        elif ago < timedelta(days=1):
            hours = int(ago.total_seconds() // 3600)
            ago_text = f"{hours} ч назад"
        else:
            days = int(ago.total_seconds() // 86400)
            ago_text = f"{days} дн назад"
    else:
        ts_text = "—"
        ago_text = "нет данных"

    sats = None
    if isinstance(pos, dict):
        for key in ("sc", "sats", "sat_count"):
            val = pos.get(key)
            if isinstance(val, (int, float)):
                sats = int(val)
                break

    sats_text = f"спутн.: {sats}" if sats is not None else "спутн.: —"
    return f"{ts_text} ({ago_text}); {sats_text}"


def cached_raw_message_body(item: Dict[str, Any]) -> str:
    params_map, order = extract_params_from_item(item)
    if not params_map:
        return "—"

    pieces: List[str] = []
    for key in order:
        value = params_map.get(key, "")
        if value:
            pieces.append(f"{key}={value}")
        else:
            pieces.append(key)

    if not pieces:
        return "—"

    return ", ".join(pieces)


def _format_duration(delta: timedelta) -> str:
    total_minutes = int(delta.total_seconds() // 60)
    if total_minutes < 60:
        return f"{total_minutes} мин" if total_minutes >= 0 else "—"
    total_hours = total_minutes // 60
    if total_hours < 24:
        return f"{total_hours} ч"
    total_days = total_hours // 24
    return f"{total_days} дн"


def cached_offline_status(item: Dict[str, Any]) -> str:
    lmsg = (item or {}).get("lmsg") or {}
    pos = (item or {}).get("pos") or {}
    ts_candidate = None
    for candidate in (pos.get("t"), lmsg.get("t"), item.get("last_msg_ts")):
        if candidate:
            try:
                ts_candidate = int(candidate)
            except Exception:
                continue
            else:
                break
    if not ts_candidate:
        return "—"
    dt = datetime.fromtimestamp(ts_candidate, tz=timezone.utc)
    delta = datetime.now(timezone.utc) - dt
    if delta < timedelta(minutes=0):
        delta = timedelta(minutes=0)
    return _format_duration(delta)


def cached_voltage_text(item: Dict[str, Any]) -> str:
    params_map, _ = extract_params_from_item(item)
    for key in ("pwr_ext", "power_ext", "external_power"):
        raw = params_map.get(key)
        if raw in (None, ""):
            continue
        try:
            val = float(raw)
        except (TypeError, ValueError):
            continue
        return f"{val:.1f} В"
    return "—"


def _cached_custom_sensor_value_rows(
    item: Dict[str, Any]
) -> Tuple[str, str, str, List[CachedSensorEntry]]:
    dut_summary, legacy_summary, strict_entries, legacy_entries, _ = _collect_dut_sensor_entries(item)
    strict_values = ", ".join(f"{e.display} = {e.value_text}" for e in strict_entries) or "—"
    legacy_values = ", ".join(f"{e.display} = {e.value_text}" for e in legacy_entries) or "—"
    filtered_names, filtered_entries = cached_filtered_custom_sensors(item)
    zero_summary = summarize_zero_custom_sensors(filtered_entries)
    return (
        ", ".join(dut_summary) if dut_summary else "—",
        ", ".join(legacy_summary) if legacy_summary else "—",
        strict_values,
        legacy_values,
        filtered_names,
        zero_summary,
        filtered_entries,
    )


def build_unit_cache_preview_rows(
    cached_map: Dict[int, Dict[str, Any]]
) -> List[Tuple[str, str, str, str, str, str, str, str, str, str, str]]:
    rows: List[Tuple[str, str, str, str, str, str, str, str, str, str, str]] = []
    items = list(cached_map.items())
    items.sort(key=lambda kv: (str(kv[1].get("nm") or "").casefold(), kv[0]))
    for uid, item in items:
        name = str(item.get("nm") or "").strip() or f"id {uid}"
        last_seen = cached_message_summary(item)
        raw_message = cached_raw_message_body(item)
        sensors_summary, legacy_summary = cached_dut_sensor_summaries(item)
        custom_values, legacy_values = cached_custom_sensor_value_summaries(item)
        filtered_names, filtered_entries = cached_filtered_custom_sensors(item)
        zero_summary = summarize_zero_custom_sensors(filtered_entries)
        offline_status = cached_offline_status(item)
        voltage_text = cached_voltage_text(item)
        rows.append(
            (
                last_seen,
                name,
                raw_message,
                sensors_summary,
                legacy_summary,
                custom_values,
                legacy_values,
                filtered_names,
                zero_summary,
                offline_status,
                voltage_text,
            )
        )
    return rows


def build_unit_cache_preview_file(
    rows: List[Tuple[str, str, str, str, str, str, str, str, str, str, str]]
) -> Tuple[str, bytes, int]:
    output = io.StringIO()
    writer = csv.writer(output, delimiter=";")
    writer.writerow(
        [
            "Последняя связь",
            "Имя объекта",
            "Последнее сообщение",
            "Сенсоры ДУТ",
            "Сенсоры ДУТ Legacy",
            "Произвольные датчики (значения)",
            "Legacy произвольные датчики (значения)",
            "Отфильтрованные датчики",
            "Датчики с нулевым значением",
            "Нет связи",
            "Напряжение",
        ]
    )
    for row in rows:
        writer.writerow(list(row))
    content = output.getvalue().encode("utf-8-sig")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"unit_cache_preview_{timestamp}.csv"
    return filename, content, len(rows)


def build_dut_zero_report_file(rows: List[Tuple[str, str, str, str]]) -> Tuple[str, bytes, int]:
    output = io.StringIO()
    writer = csv.writer(output, delimiter=";")
    writer.writerow([
        "Нет связи",
        "Имя объекта",
        "Датчики с нулевым значением",
        "Напряжение",
    ])
    for offline_text, name, sensor_summary, voltage_text in rows:
        writer.writerow([offline_text, name, sensor_summary, voltage_text])
    content = output.getvalue().encode("utf-8-sig")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"dut_report_cache_zero_{timestamp}.csv"
    return filename, content, len(rows)


def extract_cached_dut_rows(
    cached_map: Dict[int, Dict[str, Any]]
) -> Tuple[
    List[Tuple[str, str, str, str, str, str, str, str, str, str, str]],
    List[Tuple[str, str, str, str]],
]:
    preview_rows = build_unit_cache_preview_rows(cached_map)
    zero_rows: List[Tuple[str, str, str, str]] = []
    for row in preview_rows:
        if len(row) < 11:
            continue
        name = row[1]
        zero_summary = row[8]
        offline_status = row[9]
        voltage_text = row[10]
        if isinstance(zero_summary, str) and zero_summary.strip() and zero_summary.strip() != "—":
            zero_rows.append(
                (
                    offline_status if isinstance(offline_status, str) else "—",
                    str(name),
                    zero_summary,
                    voltage_text if isinstance(voltage_text, str) else "—",
                )
            )
    return preview_rows, zero_rows
