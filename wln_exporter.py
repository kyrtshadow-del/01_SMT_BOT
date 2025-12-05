import io
import math
import re
import zipfile
from typing import Any, Dict, Iterable, List, Optional, Tuple


STATUS_PARAM_KEYS = {
    "sats",
    "SATS",
    "gsm_status",
    "acc_trigger",
    "dev_status",
    "valid",
    "soft",
    "iridium_msg",
    "ignition",
    "ignition_raw",
}

DRIVER_PARAM_KEYS = {
    "driver",
    "DRIVER",
    "avl_driver",
    "ibutton_code",
}

SKIP_PARAM_KEYS = {
    "course",
    "heading",
    "dir",
    "bearing",
}


def sanitize_filename(name: str) -> str:
    if not name:
        return "unit"
    sanitized = re.sub(r"[<>:\"/\\|?*\n\r\t]+", "_", name)
    sanitized = sanitized.strip().strip(".")
    return sanitized or "unit"


def _format_float(value: float, decimals: int = 6) -> str:
    formatted = f"{value:.{decimals}f}"
    formatted = formatted.rstrip("0").rstrip(".")
    return formatted if formatted else "0"


def _format_number(value: float) -> str:
    if math.isfinite(value):
        formatted = f"{value:.3f}"
        formatted = formatted.rstrip("0").rstrip(".")
        return formatted if formatted else "0"
    return "0"


def _format_value(value: Any) -> str:
    if value is None:
        return "0"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (int, float)):
        return _format_number(float(value))
    return str(value)


def _collect_numeric_pairs(items: Iterable[Tuple[str, Any]]) -> List[Tuple[str, Any]]:
    pairs: List[Tuple[str, Any]] = []
    for key, value in items:
        if value is None:
            continue
        if isinstance(value, (int, float)) and math.isnan(float(value)):
            continue
        pairs.append((key, value))
    return pairs


def build_wln_line(sample: Dict[str, Any]) -> Optional[str]:
    try:
        timestamp = int(sample.get("ts"))
    except (TypeError, ValueError):
        return None
    if timestamp <= 0:
        return None

    lon = sample.get("lon")
    lat = sample.get("lat")
    if lon is None or lat is None:
        return None

    try:
        lon_value = float(lon)
        lat_value = float(lat)
    except (TypeError, ValueError):
        return None

    try:
        speed_raw = sample.get("speed")
        speed = int(round(float(speed_raw))) if speed_raw is not None else 0
    except (TypeError, ValueError):
        speed = 0

    course_sources = [
        sample.get("course"),
        sample.get("heading"),
        sample.get("dir"),
        sample.get("bearing"),
    ]
    params = sample.get("params") or {}
    course_sources.append(params.get("course"))
    course = 0
    for value in course_sources:
        if value is None:
            continue
        try:
            course = int(round(float(value)))
            break
        except (TypeError, ValueError):
            continue

    analog_pairs: List[Tuple[str, Any]] = []
    alt = sample.get("alt")
    if alt is not None:
        analog_pairs.append(("ALT", alt))

    sample_numeric_items: List[Tuple[str, Any]] = []
    for key, value in sample.items():
        if key in {"ts", "lat", "lon", "speed", "alt", "params", "sensors"}:
            continue
        if key in SKIP_PARAM_KEYS:
            continue
        if isinstance(value, (int, float)):
            sample_numeric_items.append((key, value))
    analog_pairs.extend(_collect_numeric_pairs(sample_numeric_items))

    param_pairs = list(params.items())
    driver_pairs: List[Tuple[str, Any]] = []
    status_pairs: List[Tuple[str, Any]] = []
    generic_param_pairs: List[Tuple[str, Any]] = []

    for key, value in param_pairs:
        if key in SKIP_PARAM_KEYS:
            continue
        if key in DRIVER_PARAM_KEYS:
            driver_pairs.append((key.upper(), value))
        elif key in STATUS_PARAM_KEYS:
            normalized_key = key.upper()
            status_pairs.append((normalized_key, value))
        else:
            generic_param_pairs.append((key, value))

    analog_pairs.extend(_collect_numeric_pairs(generic_param_pairs))

    sensors = sample.get("sensors")
    if isinstance(sensors, dict):
        for sensor_id, sensor_payload in sensors.items():
            if not isinstance(sensor_payload, dict):
                continue
            liters = sensor_payload.get("liters")
            if liters is not None:
                analog_pairs.append((f"sensor{sensor_id}_liters", liters))
            raw_value = sensor_payload.get("raw")
            if raw_value is not None:
                analog_pairs.append((f"sensor{sensor_id}_raw", raw_value))

    if "sats" not in params and "SATS" not in params:
        sats = sample.get("sats")
        if sats is not None:
            status_pairs.append(("SATS", sats))

    ignition_value = sample.get("ignition")
    if ignition_value is not None:
        status_pairs.append(("ignition", ignition_value))
    ignition_raw_value = sample.get("ignition_raw")
    if ignition_raw_value is not None:
        status_pairs.append(("ignition_raw", ignition_raw_value))

    analog_string = ",".join(f"{key}:{_format_value(val)}" for key, val in analog_pairs) if analog_pairs else ""
    status_string = ",".join(f"{key}:{_format_value(val)}" for key, val in status_pairs) if status_pairs else ""
    driver_string_parts: List[str] = []
    for key, value in driver_pairs:
        if value is None:
            continue
        if isinstance(value, str):
            driver_string_parts.append(f'{key}:"{value}"')
        else:
            driver_string_parts.append(f"{key}:{_format_value(value)}")
    driver_string = ",".join(driver_string_parts)

    wln_timestamp_ns = timestamp * 1_000_000_000
    tail_pairs: List[str] = [f"WLNTMNS:{wln_timestamp_ns}"]
    ibutton = params.get("ibutton_code") or sample.get("ibutton_code")
    if ibutton is not None and "ibutton_code" not in DRIVER_PARAM_KEYS:
        tail_pairs.append(f"ibutton_code:{_format_value(ibutton)}")
    tail_string = ",".join(tail_pairs)

    line_parts: List[str] = [
        "REG",
        str(timestamp),
        _format_float(lon_value, decimals=6),
        _format_float(lat_value, decimals=6),
        str(int(speed)),
        str(int(course)),
        analog_string,
        status_string,
        driver_string,
        tail_string,
        "",
    ]
    return ";".join(line_parts)


def convert_unit_to_wln(unit_payload: Dict[str, Any]) -> Tuple[str, Dict[str, int]]:
    samples = unit_payload.get("samples") or []
    lines: List[str] = []
    for sample in samples:
        if not isinstance(sample, dict):
            continue
        line = build_wln_line(sample)
        if line:
            lines.append(line)
    content = "\n".join(lines)
    if content:
        content += "\n"
    stats = {
        "messages": len(lines),
    }
    return content, stats


def export_units_to_zip(
    day_label: str,
    units_payload: Dict[str, Any],
    unit_names: Dict[int, str],
) -> Tuple[str, bytes, Dict[str, int]]:
    zip_buffer = io.BytesIO()
    summary = {
        "units": 0,
        "messages": 0,
        "empty_units": 0,
    }
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
        for unit_id_str, payload in sorted(units_payload.items(), key=lambda item: int(item[0])):
            try:
                unit_id = int(unit_id_str)
            except (TypeError, ValueError):
                unit_id = None
            name = unit_names.get(unit_id) if unit_id is not None else None
            if not name and isinstance(payload, dict):
                name = payload.get("name")
            if not name:
                name = f"unit_{unit_id_str}"
            safe_name = sanitize_filename(name)
            content, stats = convert_unit_to_wln(payload or {})
            if not content:
                summary["empty_units"] += 1
                continue
            zip_file.writestr(f"{safe_name}.wln", content)
            summary["units"] += 1
            summary["messages"] += stats.get("messages", 0)

        if summary["units"] == 0:
            zip_file.writestr("README.txt", "Нет сообщений для выбранной даты.\n")

    zip_filename = f"{day_label}.zip"
    return zip_filename, zip_buffer.getvalue(), summary
