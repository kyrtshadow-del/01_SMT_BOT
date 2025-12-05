from __future__ import annotations

import html
import json
import re
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple

from bot.constants import FUEL_MAX_VALID_L, FUEL_MIN_VALID_L
from pipeline.config.unit_config import SensorConfig, UnitConfig
from pipeline.services.unit_snapshot_service import UnitSnapshotRecord

__all__ = [
    "format_line_with_link",
    "format_lines_with_links",
    "display_or_dash",
    "append_group",
    "build_unit_config_general_entries",
    "build_unit_config_sensor_entries",
    "compute_unit_config_sensor_values",
    "pick_unit_config_fuel_display",
    "format_sensor_settings_blocks",
    "chunk_sensor_settings_messages",
    "CardLayout",
    "build_card_layout",
]


@dataclass
class CardLayout:
    lines: List[str]
    links: Dict[int, Tuple[str, str]]
    location_indices: List[int]


def format_line_with_link(line: str, link_text: str, url: str) -> str:
    safe_url = html.escape(url, quote=True)
    safe_text = html.escape(link_text)
    prefix, sep, _ = line.partition(":")
    if sep:
        prefix_html = html.escape(prefix + sep)
        return f"{prefix_html} <a href=\"{safe_url}\">{safe_text}</a>"
    return f"{html.escape(line)} <a href=\"{safe_url}\">{safe_text}</a>"


def format_lines_with_links(lines: List[str], links: Dict[int, Tuple[str, str]]) -> str:
    formatted: List[str] = []

    def _format_value(value_text: str, link: Optional[Tuple[str, str]]) -> str:
        clean_value = value_text.lstrip() or "—"
        safe_value = html.escape(clean_value)
        value_core = f"<i>{safe_value}</i>"
        if link and all(link):
            safe_url = html.escape(link[1], quote=True)
            return f"<a href=\"{safe_url}\">{value_core}</a>"
        return value_core

    for idx, line in enumerate(lines):
        link = links.get(idx)
        prefix, sep, suffix = line.partition(":")
        if sep:
            label = prefix.strip()
            label_html = html.escape(label)
            value_html = _format_value(suffix, link)
            formatted.append(f"<b>{label_html}{sep}</b> {value_html}")
        else:
            if link and all(link):
                formatted.append(format_line_with_link(line, link[0], link[1]))
            else:
                formatted.append(html.escape(line))

    return "\n".join(formatted)


def display_or_dash(value: Optional[str]) -> str:
    return value if value else "—"


def append_group(lines: List[str], heading: str, entries: List[str]) -> List[int]:
    if not entries:
        return []
    if lines:
        lines.append("")
    lines.append(heading)
    start_idx = len(lines)
    lines.extend(entries)
    return list(range(start_idx, start_idx + len(entries)))


def build_card_layout(
    *,
    unit_name: str,
    reg_number: Optional[str],
    online_text: str,
    satellites_text: str,
    last_message_text: str,
    fuel_text: Optional[str],
    driver_text: Optional[str],
    coords_text: Optional[str],
    address_hint: Optional[str],
    snapshot_record: Optional[UnitSnapshotRecord],
    device_dut_lines: Sequence[str],
    info_wialon_id: Optional[str],
    info_device_type: Optional[str],
    info_terminal_id: Optional[str],
    unit_config: Optional[UnitConfig],
    params_map: Mapping[str, Any],
    sensor_values: Optional[Mapping[str, Tuple[Optional[float], Optional[float]]]] = None,
    location_display: Optional[str] = None,
    location_map_url: Optional[str] = None,
    geozone_display: Optional[str] = None,
    geozone_map_url: Optional[str] = None,
) -> CardLayout:
    params_map = params_map or {}
    if sensor_values is None and unit_config is not None:
        sensor_values = compute_unit_config_sensor_values(unit_config, params_map)
    sensor_values = sensor_values or {}
    lines: List[str] = []

    general_entries = [f"• Имя: {unit_name}"]
    if reg_number:
        general_entries.append(f"• Госномер: {reg_number}")
    general_entries.extend(
        [
            f"• Связь: {online_text}",
            f"• Спутники: {satellites_text}",
            f"• Последнее сообщение: {last_message_text}",
            f"• ДУТ (уровень топлива): {display_or_dash(fuel_text)}",
            f"• Назначенный водитель: {display_or_dash(driver_text)}",
        ]
    )
    append_group(lines, "🧾 Общая информация", general_entries)

    location_entries = [f"• Координаты: {display_or_dash(coords_text)}"]
    derived_address = address_hint
    if not derived_address and snapshot_record:
        derived_address = snapshot_record.meta.get("address") or snapshot_record.meta.get("addr")
    if derived_address:
        location_entries.append(f"• Адрес: {derived_address}")
    location_indices = append_group(lines, "🗺️ Местоположение", location_entries)

    device_entries = [line for line in device_dut_lines if line.strip()]
    device_entries.extend(
        [
            f"• ID объекта Wialon: {display_or_dash(info_wialon_id)}",
            f"• Тип устройства: {display_or_dash(info_device_type)}",
            f"• ID терминала: {display_or_dash(info_terminal_id)}",
        ]
    )
    append_group(lines, "🆔 Параметры устройства", device_entries)

    if snapshot_record and snapshot_record.contacts:
        contact_entries: List[str] = []
        for key, value in snapshot_record.contacts.items():
            if value:
                contact_entries.append(f"• {key}: {value}")
            if len(contact_entries) >= 5:
                break
        if contact_entries:
            append_group(lines, "📞 Контакты", contact_entries)

    if snapshot_record and snapshot_record.sensors:
        sensor_entries: List[str] = []
        max_sensors = 6
        for sensor in snapshot_record.sensors[:max_sensors]:
            sensor_name = sensor.name or sensor.type or f"Сенсор {sensor.sensor_id or ''}".strip()
            details: List[str] = []
            if sensor.type and sensor.type != sensor_name:
                details.append(sensor.type)
            if sensor.units:
                details.append(sensor.units)
            if sensor.meta.get("last_value") is not None:
                details.append(str(sensor.meta["last_value"]))
            if details:
                sensor_entries.append(f"• {sensor_name} ({', '.join(details)})")
            else:
                sensor_entries.append(f"• {sensor_name}")
        remaining = len(snapshot_record.sensors) - max_sensors
        if remaining > 0:
            sensor_entries.append(f"… и ещё {remaining}")
        append_group(lines, "🧪 Датчики", sensor_entries)

    config_general_entries = build_unit_config_general_entries(unit_config)
    if config_general_entries:
        append_group(lines, "⚙️ Свойства (конфиг)", config_general_entries)

    config_sensor_entries = build_unit_config_sensor_entries(
        unit_config,
        params_map,
        sensor_values=sensor_values,
    )
    if config_sensor_entries:
        append_group(lines, "🧷 Датчики (UnitConfig)", config_sensor_entries)

    links_map: Dict[int, Tuple[str, str]] = {}
    if location_map_url and location_indices:
        link_text = location_display or coords_text or "Открыть карту"
        links_map[location_indices[0]] = (link_text, location_map_url)
    if (
        geozone_map_url
        and geozone_display
        and geozone_display not in {"—", "вне зон"}
        and len(location_indices) > 1
    ):
        links_map[location_indices[1]] = (geozone_display, geozone_map_url)

    return CardLayout(lines=lines, links=links_map, location_indices=location_indices)


def build_unit_config_general_entries(unit_config: Optional[UnitConfig]) -> List[str]:
    if unit_config is None:
        return []
    entries: List[str] = []
    general = unit_config.general
    if general:
        if general.uid:
            entries.append(f"• UID конфигурации: {general.uid}")
        if general.phone:
            entries.append(f"• Телефон SIM: {general.phone}")
        if general.phone2:
            entries.append(f"• Телефон SIM 2: {general.phone2}")
    hw_name = None
    if unit_config.hw_config and unit_config.hw_config.hardware:
        hw_name = unit_config.hw_config.hardware
    elif general and general.hardware:
        hw_name = general.hardware
    if hw_name:
        entries.append(f"• Тип устройства (конфиг): {hw_name}")
    if unit_config.hw_config and unit_config.hw_config.params:
        labels: List[str] = []
        for param in unit_config.hw_config.params[:3]:
            title = param.label or param.name
            if title:
                labels.append(title)
        if labels:
            entries.append(f"• Параметры HW: {', '.join(labels)}")
    if unit_config.counters and unit_config.counters.values:
        counter_pairs = []
        for key, value in list(unit_config.counters.values.items())[:4]:
            counter_pairs.append(f"{key}={value}")
        if counter_pairs:
            entries.append(f"• Счётчики: {', '.join(counter_pairs)}")
    return entries


def build_unit_config_sensor_entries(
    unit_config: Optional[UnitConfig],
    params_map: Mapping[str, Any],
    limit: int = 5,
    *,
    sensor_values: Optional[Mapping[str, Tuple[Optional[float], Optional[float]]]] = None,
) -> List[str]:
    if unit_config is None or not unit_config.sensors:
        return []
    if sensor_values is None:
        sensor_values = compute_unit_config_sensor_values(unit_config, params_map)
    entries: List[str] = []
    for sensor in unit_config.sensors[:limit]:
        info = sensor_values.get(sensor.name)
        display = _format_sensor_display(info, sensor)
        details: List[str] = []
        if sensor.type and sensor.type != sensor.name:
            details.append(sensor.type)
        units_text = _extract_sensor_units(sensor)
        if units_text:
            details.append(units_text)
        source = _guess_sensor_source_key(sensor.parameters)
        if source:
            details.append(source)
        suffix = f" ({', '.join(details)})" if details else ""
        entries.append(f"• {sensor.name}: {display}{suffix}")
    remaining = len(unit_config.sensors) - limit
    if remaining > 0:
        entries.append(f"… и ещё {remaining}")
    return entries


def compute_unit_config_sensor_values(
    config: UnitConfig,
    params_map: Mapping[str, Any],
) -> Dict[str, Tuple[Optional[float], Optional[float]]]:
    sensor_by_name = {sensor.name: sensor for sensor in config.sensors if sensor.name}
    memo: Dict[str, Tuple[Optional[float], Optional[float]]] = {}
    visiting: set[str] = set()

    def compute(name: str) -> Tuple[Optional[float], Optional[float]]:
        if name in memo:
            return memo[name]
        if name in visiting:
            return (None, None)
        visiting.add(name)
        sensor = sensor_by_name.get(name)
        if sensor is None:
            raw = _get_numeric_param(params_map, name)
            memo[name] = (raw, raw)
            visiting.discard(name)
            return memo[name]
        raw_value = _evaluate_sensor_raw(sensor, params_map, compute, sensor_by_name)
        value = _apply_calibration_value(sensor, raw_value)
        memo[name] = (value if value is not None else raw_value, raw_value)
        visiting.discard(name)
        return memo[name]

    for sensor in config.sensors:
        compute(sensor.name)
    return memo


def pick_unit_config_fuel_display(
    unit_config: Optional[UnitConfig],
    sensor_values: Mapping[str, Tuple[Optional[float], Optional[float]]],
) -> Optional[str]:
    if unit_config is None or not unit_config.sensors:
        return None
    primary: List[SensorConfig] = []
    secondary: List[SensorConfig] = []
    for sensor in unit_config.sensors:
        if _is_fuel_sensor_type(sensor.type or ""):
            primary.append(sensor)
        elif _is_fuel_sensor(sensor):
            secondary.append(sensor)

    def _pick_from(group: List[SensorConfig]) -> Optional[str]:
        for sensor in group:
            info = sensor_values.get(sensor.name)
            if not info:
                continue
            display = _format_sensor_display(info, sensor)
            if not display or display == "—":
                continue
            units = (sensor.units or "").strip()
            if units and not display.endswith(units):
                display = f"{display} {units}"
            return display
        return None

    result = _pick_from(primary)
    if result is not None:
        return result
    return _pick_from(secondary)


def format_sensor_settings_blocks(
    config: UnitConfig,
    sensor_values: Mapping[str, Tuple[Optional[float], Optional[float]]],
) -> List[str]:
    blocks: List[str] = []
    for sensor in config.sensors:
        lines: List[str] = [f"<b>{html.escape(sensor.name)}</b>"]
        display = _format_sensor_display(sensor_values.get(sensor.name), sensor)
        units_text = _extract_sensor_units(sensor)
        if display and display != "—" and units_text and not display.endswith(units_text):
            display = f"{display} {units_text}"
        lines.append(f"Значение: {html.escape(display or '—')}")
        if sensor.type:
            lines.append(f"Тип: {html.escape(sensor.type)}")
        if units_text:
            lines.append(f"Единицы: {html.escape(units_text)}")
        source_key = _guess_sensor_source_key(sensor.parameters)
        if source_key:
            lines.append(f"Источник: <code>{html.escape(source_key)}</code>")
        if sensor.expression:
            lines.append(f"Формула: <code>{html.escape(sensor.expression)}</code>")
        if sensor.parameters:
            param_lines: List[str] = []
            for key, value in sensor.parameters.items():
                value_repr: Any = value
                if isinstance(value, (dict, list, tuple)):
                    try:
                        value_repr = json.dumps(value, ensure_ascii=False)
                    except Exception:
                        value_repr = str(value)
                param_lines.append(
                    f"• {html.escape(str(key))}: <code>{html.escape(str(value_repr))}</code>"
                )
            if param_lines:
                lines.append("Параметры:\n" + "\n".join(param_lines))
        if sensor.validation:
            validation = sensor.validation
            parts: List[str] = []
            if validation.mode:
                parts.append(f"mode={validation.mode}")
            if validation.min_value is not None:
                parts.append(f"min={validation.min_value}")
            if validation.max_value is not None:
                parts.append(f"max={validation.max_value}")
            if validation.hysteresis is not None:
                parts.append(f"hyst={validation.hysteresis}")
            if parts:
                lines.append("Валидация: " + ", ".join(html.escape(part) for part in parts))
        if sensor.calibration and sensor.calibration.points:
            lines.append(f"Тарировка: {len(sensor.calibration.points)} точек (CSV во вложении)")
        blocks.append("\n".join(lines))
    return blocks


def chunk_sensor_settings_messages(header: str, blocks: Sequence[str], max_len: int = 3500) -> List[str]:
    messages: List[str] = []
    current = header
    for block in blocks:
        addition = ("\n\n" if current else "") + block
        if current and len(current) + len(addition) > max_len:
            messages.append(current)
            current = block
        else:
            current = (current + addition) if current else block
    if current:
        messages.append(current)
    return messages


def _extract_sensor_units(sensor: SensorConfig) -> Optional[str]:
    units = (sensor.units or "").strip()
    if units:
        return units
    if isinstance(sensor.parameters, Mapping):
        value = sensor.parameters.get("m") or sensor.parameters.get("units")
        if isinstance(value, str):
            units = value.strip()
            if units:
                return units
    return None


def _is_simple_param_expression(expr: str) -> bool:
    expr = expr.strip()
    if not expr:
        return False
    if any(ch in expr for ch in "[]{}()+-*/ \t"):
        return False
    return bool(re.fullmatch(r"[A-Za-z0-9_:.+-]+", expr))


def _evaluate_sensor_raw(
    sensor: SensorConfig,
    params_map: Mapping[str, Any],
    compute_fn: Callable[[str], Tuple[Optional[float], Optional[float]]],
    sensor_by_name: Mapping[str, SensorConfig],
) -> Optional[float]:
    expr = (sensor.expression or "").strip()
    if expr:
        if _is_simple_param_expression(expr):
            value = _get_numeric_param(params_map, expr)
            if value is not None:
                return value
        else:
            evaluated = _evaluate_sensor_expression(expr, params_map, compute_fn, sensor_by_name)
            if evaluated is not None:
                return evaluated
    param_key = _guess_sensor_source_key(sensor.parameters)
    if param_key:
        return _get_numeric_param(params_map, param_key)
    return _get_numeric_param(params_map, sensor.name)


def _evaluate_sensor_expression(
    expression: str,
    params_map: Mapping[str, Any],
    compute_fn: Callable[[str], Tuple[Optional[float], Optional[float]]],
    sensor_by_name: Mapping[str, SensorConfig],
) -> Optional[float]:
    refs = _extract_sensor_refs(expression)
    value_map: Dict[str, float] = {}
    for ref in refs:
        if ref in sensor_by_name:
            computed = compute_fn(ref)[0]
        else:
            computed = _get_numeric_param(params_map, ref)
        if computed is None:
            return None
        value_map[ref] = computed

    def replace(match: re.Match[str]) -> str:
        key = match.group(1).strip()
        return str(value_map.get(key, 0))

    python_expr = re.sub(r"\[([^\]]+)\]", replace, expression)
    if not re.fullmatch(r"[0-9+\-*/(). \t]+", python_expr):
        return None
    try:
        result = eval(python_expr, {"__builtins__": None}, {})
    except Exception:
        return None
    try:
        return float(result)
    except (TypeError, ValueError):
        return None



def _guess_sensor_source_key(parameters: Mapping[str, Any]) -> Optional[str]:
    if not isinstance(parameters, Mapping):
        return None

    def _is_param_name(value: str) -> bool:
        if not value:
            return False
        if any(ch in value for ch in "[]{}()+-*/ "):
            return False
        return True

    p_value = parameters.get("p")
    if isinstance(p_value, str):
        candidate = p_value.strip()
        if _is_param_name(candidate):
            return candidate

    candidates = ["src", "source", "param", "p1", "user_param", "signal"]
    for key in candidates:
        value = parameters.get(key)
        if isinstance(value, str):
            candidate = value.strip()
            if candidate:
                return candidate
    skip_keys = {"m", "units", "unit", "f", "format", "min", "max"}
    for key, value in parameters.items():
        if key in skip_keys:
            continue
        if isinstance(value, str):
            candidate = value.strip()
            if candidate and _is_param_name(candidate):
                return candidate
    return None


def _get_numeric_param(params_map: Mapping[str, Any], key: str) -> Optional[float]:
    if not key:
        return None
    value = params_map.get(key)
    if value is None:
        value = params_map.get(key.lower())
    if value is None:
        value = params_map.get(key.upper())
    if value is None:
        return None
    try:
        return float(str(value).replace(",", "."))
    except (TypeError, ValueError):
        return None


def _apply_calibration_value(sensor: SensorConfig, raw_value: Optional[float]) -> Optional[float]:
    if raw_value is None:
        return None
    calibration = sensor.calibration
    if not calibration:
        return raw_value
    if calibration.segments:
        segments = sorted(calibration.segments, key=lambda seg: seg.start_x)
        candidate = None
        for seg in segments:
            if raw_value >= seg.start_x:
                candidate = seg
            else:
                break
        if candidate is None:
            candidate = segments[0]
        return candidate.a * raw_value + candidate.b
    if calibration.points:
        points = sorted(calibration.points, key=lambda pt: pt.raw)
        if raw_value <= points[0].raw:
            return points[0].value
        if raw_value >= points[-1].raw:
            return points[-1].value
        for left, right in zip(points, points[1:]):
            if left.raw <= raw_value <= right.raw:
                span = right.raw - left.raw
                if span == 0:
                    return left.value
                ratio = (raw_value - left.raw) / span
                return left.value + ratio * (right.value - left.value)
    return raw_value


def _format_sensor_display(
    value_info: Optional[Tuple[Optional[float], Optional[float]]],
    sensor: SensorConfig,
) -> str:
    if not value_info:
        return "—"
    value, raw = value_info
    sensor_type = (sensor.type or "").strip().lower()
    if sensor_type in {"custom", "произвольный датчик"}:
        raw_check = raw if raw is not None else value
        if raw_check is not None and not (1 <= raw_check <= 4096):
            raw_display = _format_number(raw_check) or str(raw_check)
            return f"не работает ({raw_display})"
    formatted_value = _format_number(value)
    formatted_raw = _format_number(raw)
    if sensor.type and sensor.type.lower() in {"custom", "произвольный датчик"}:
        if formatted_value is not None and formatted_raw is not None and formatted_value != formatted_raw:
            return f"{formatted_value} (raw: {formatted_raw})"
    if formatted_value is not None:
        if formatted_raw is not None and formatted_raw != formatted_value:
            return f"{formatted_value} (raw: {formatted_raw})"
        return formatted_value
    if formatted_raw is not None:
        return formatted_raw
    return "—"


def _format_number(value: Optional[float]) -> Optional[str]:
    if value is None:
        return None
    try:
        return f"{value:.2f}".rstrip("0").rstrip(".")
    except Exception:
        return str(value)


FUEL_SENSOR_TYPE_TOKENS = {
    "fuel level",
    "fuel_level",
    "fuel",
    "fuel level sensor",
    "fuel level impulse",
    "fuel level impulse sensor",
}


def _is_fuel_sensor_type(sensor_type: str) -> bool:
    normalized = sensor_type.strip().lower()
    if not normalized:
        return False
    if normalized in FUEL_SENSOR_TYPE_TOKENS:
        return True
    normalized = normalized.replace("_", " ")
    return "fuel" in normalized and "level" in normalized


def _is_fuel_sensor(sensor: SensorConfig) -> bool:
    name = (sensor.name or "").strip().lower()
    sensor_type = (sensor.type or "").strip().lower()
    if not name and not sensor_type:
        return False
    if _is_fuel_sensor_type(sensor_type) or sensor_type in {"dut", "дут"}:
        return True
    keywords = ("fuel", "дут", "уров", "level")
    return any(keyword in sensor_type for keyword in keywords) or any(keyword in name for keyword in keywords)


def _extract_sensor_refs(expression: str) -> List[str]:
    return re.findall(r"\[([^\]]+)\]", expression)
