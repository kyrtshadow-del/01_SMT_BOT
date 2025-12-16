"""Status helpers for Monitoring (shared between web and bot layers).

Pure logic only: no file I/O, no JSON caches.
Calculates: Online/Offline, Status (Moving/Stop), Ignition.
"""

from __future__ import annotations

import os
import time
from typing import Any, Dict, Optional

DEFAULT_ONLINE_THRESHOLD_SEC = 600  # 10 minutes
DEFAULT_EXPECTED_INTERVAL_SEC = float(os.getenv("EXPECTED_DEFAULT_SEC", 300))  # регламент 5 минут
EXPECTED_MIN_SEC = float(os.getenv("EXPECTED_MIN_SEC", 60))
EXPECTED_MAX_SEC = float(os.getenv("EXPECTED_MAX_SEC", 900))
ONLINE_MIN_SEC = float(os.getenv("ONLINE_MIN_SEC", 300))   # 5 мин
ONLINE_MAX_SEC = float(os.getenv("ONLINE_MAX_SEC", 1200))  # 20 мин
K_ONLINE = float(os.getenv("STATUS_K_ONLINE", 3))
K_WARN = float(os.getenv("STATUS_K_WARN", 3))
K_CRIT = float(os.getenv("STATUS_K_CRIT", 24))
WARN_MIN_SEC = float(os.getenv("STATUS_WARN_MIN_SEC", 600))  # 10 мин
CRIT_MIN_SEC = float(os.getenv("STATUS_CRIT_MIN_SEC", 10800))  # 3 часа
DEFAULT_STOP_TO_PARK_SEC = 300  # 5 минут: всё, что короче, считаем стоянкой


def _online_threshold() -> int:
    try:
        return int(os.getenv("MONITORING_ONLINE_SEC", DEFAULT_ONLINE_THRESHOLD_SEC))
    except (TypeError, ValueError):
        return DEFAULT_ONLINE_THRESHOLD_SEC


def get_online_threshold() -> int:
    """Expose the effective online threshold for diagnostics/UI."""

    return _online_threshold()


def is_online(last_ts: Optional[int], *, now: Optional[int] = None) -> bool:
    if last_ts is None:
        return False
    try:
        last_ts = int(last_ts)
    except (TypeError, ValueError):
        return False
    if now is None:
        now = int(time.time())
    return now - last_ts <= _online_threshold()


def infer_has_fuel(snapshot: Dict[str, Any], unit_config: Optional[Dict[str, Any]] = None) -> bool:
    # Prefer UnitConfig sensors if supplied; else fall back to snapshot sensors
    sensors = []
    if unit_config and isinstance(unit_config.get("sensors"), list):
        sensors = unit_config["sensors"]
    elif isinstance(snapshot.get("sensors"), list):
        sensors = snapshot["sensors"]
    elif isinstance(snapshot.get("sens"), list):
        sensors = snapshot["sens"]
    for s in sensors:
        s_type = str(s.get("type") or s.get("t") or "").lower()
        name = str(s.get("name") or s.get("n") or "").lower()
        if "fuel" in s_type or "дут" in s_type or "fuel" in name or "дут" in name:
            return True
    return False


def derive_ignition(params: Dict[str, Any], advanced: Optional[Dict[str, Any]] = None) -> Optional[bool]:
    """Heuristic ignition detection similar to Wialon."""

    def _to_int(val: Any) -> Optional[int]:
        if isinstance(val, bool):
            return int(val)
        if isinstance(val, (int, float)):
            return int(val)
        if isinstance(val, str) and val.strip().isdigit():
            return int(val.strip())
        return None

    adv = advanced or {}

    acc = _to_int(params.get("acc_trigger") or params.get("ignition") or params.get("ign"))
    if acc is not None:
        return bool(acc)

    dev_status = _to_int(params.get("dev_status"))
    if dev_status is not None:
        # Wialon часто использует младший бит dev_status как "ignition"
        return bool(dev_status & 0x1)

    # inputs_status: allow choosing bit via env IGNITION_INPUT_BIT (0-based)
    inputs = _to_int(params.get("inputs_status"))
    if inputs is not None:
        bit_idx = adv.get("ignition_input_bit")
        if bit_idx is None:
            try:
                bit_idx = int(os.getenv("IGNITION_INPUT_BIT", "-1"))
            except (TypeError, ValueError):
                bit_idx = -1
        if isinstance(bit_idx, (int, float)) and bit_idx >= 0:
            return bool(inputs & (1 << int(bit_idx)))

    # voltage heuristic (ручные пороги, если заданы)
    pwr_ext = params.get("pwr_ext")
    try:
        pwr_ext = float(pwr_ext) if pwr_ext is not None else None
    except Exception:
        pwr_ext = None

    v_on = adv.get("ignition_threshold_v_on")
    v_off = adv.get("ignition_threshold_v_off")
    if v_on is not None and v_off is not None and pwr_ext is not None:
        try:
            v_on = float(v_on)
            v_off = float(v_off)
            if pwr_ext >= v_on:
                return True
            if pwr_ext <= v_off:
                return False
        except Exception:
            pass

    # авто-порог по напряжению вычисляется вне (в веб-слое) — здесь только None
    return None


def _extract_params(latest: Dict[str, Any]) -> Dict[str, Any]:
    params = latest.get("params") or latest.get("prms")
    if isinstance(params, dict):
        return params
    return {}


def compute_status(
    snapshot: Dict[str, Any],
    latest: Dict[str, Any],
    unit_config: Optional[Dict[str, Any]] = None,
    unit_id: Optional[int] = None,  # noqa: ARG001
    *,
    now: Optional[int] = None,
    health_ok: bool = True,
) -> Dict[str, Any]:
    """
    Returns a compact status dict used by web Monitoring.
    Keys: online, status, status_label, speed, last_ts, has_fuel, age_sec, reason, expected_interval_sec.
    """

    # ---------- timestamps / age ----------
    last_ts = latest.get("device_ts") or latest.get("received_ts") or snapshot.get("t")
    if now is None:
        now = int(time.time())
    age_sec: Optional[float] = None
    try:
        if last_ts is not None:
            age_sec = max(0, now - int(last_ts))
    except Exception:
        age_sec = None

    # ---------- expected interval & thresholds ----------
    # ожидаемый интервал: либо из UnitConfig, либо дефолт
    expected_interval = DEFAULT_EXPECTED_INTERVAL_SEC
    try:
        cfg_adv = unit_config.get("advanced") if isinstance(unit_config, dict) else None
        cfg_val = cfg_adv.get("expected_interval_sec") if isinstance(cfg_adv, dict) else None
        if cfg_val is not None:
            expected_interval = float(cfg_val)
    except Exception:
        pass
    if expected_interval <= 0:
        expected_interval = DEFAULT_EXPECTED_INTERVAL_SEC
    expected_interval = max(EXPECTED_MIN_SEC, min(EXPECTED_MAX_SEC, expected_interval))

    online_dynamic = max(ONLINE_MIN_SEC, min(ONLINE_MAX_SEC, K_ONLINE * expected_interval))
    stale_warn = max(int(K_WARN * expected_interval), int(WARN_MIN_SEC))
    stale_crit = max(int(K_CRIT * expected_interval), int(max(stale_warn, CRIT_MIN_SEC)))

    # ---------- helpers ----------
    def _fmt_age(sec: float) -> str:
        if sec < 60:
            return f"{int(sec)} с"
        if sec < 3600:
            mins = int(sec // 60)
            rem_s = int(sec % 60)
            if rem_s == 0:
                return f"{mins} мин"
            return f"{mins} мин {rem_s} с"
        hours = sec / 3600.0
        if hours < 48:
            return f"{hours:.1f} ч"
        days = hours / 24.0
        return f"{days:.1f} дн"

    params = _extract_params(latest)
    advanced = unit_config.get("advanced") if isinstance(unit_config, dict) else None

    # ---------- health ingestion ----------
    if not health_ok:
        return {
            "online": False,
            "status": "offline",
            "status_label": "Нет связи с источником"
            + (f" · {_fmt_age(age_sec)}" if age_sec is not None else ""),
            "speed": latest.get("speed"),
            "last_ts": last_ts,
            "has_fuel": infer_has_fuel(snapshot, unit_config),
            "ignition": derive_ignition({**latest, **params}, advanced=advanced),
            "age_sec": age_sec,
            "expected_interval_sec": expected_interval,
            "reason": "no_source",
        }

    online = False
    if age_sec is not None:
        online = age_sec <= online_dynamic
    speed = latest.get("speed")
    try:
        spd = float(speed) if speed is not None else 0.0
    except (TypeError, ValueError):
        spd = 0.0
    ignition = derive_ignition({**latest, **params}, advanced=advanced)
    stop_duration = (
        params.get("stop_duration_s")
        or params.get("stop_dur")
        or latest.get("stop_duration_s")
        or latest.get("stop_dur")
        or latest.get("stop")
    )
    try:
        if stop_duration is not None:
            stop_duration = float(stop_duration)
    except Exception:
        stop_duration = None
    try:
        stop_threshold = int(os.getenv("STOP_TO_PARK_SEC", DEFAULT_STOP_TO_PARK_SEC))
    except Exception:
        stop_threshold = DEFAULT_STOP_TO_PARK_SEC
    if stop_threshold < DEFAULT_STOP_TO_PARK_SEC:
        stop_threshold = DEFAULT_STOP_TO_PARK_SEC

    # fallback: если стоим и нет стоп-таймера, используем возраст пакета как нижнюю оценку
    if stop_duration is None and online and spd < 1.0 and age_sec is not None:
        stop_duration = age_sec

    def _fmt_duration(sec: Optional[float]) -> Optional[str]:
        if sec is None:
            return None
        try:
            s = float(sec)
        except Exception:
            return None
        if s < 0:
            return None
        if s < 60:
            return f"{int(s)} сек"
        mins = s / 60
        if mins < 60:
            return f"{int(round(mins))} мин"
        hours = mins / 60
        if hours < 24:
            whole_h = int(hours)
            mins_rest = int(round((hours - whole_h) * 60))
            return f"{whole_h} ч {mins_rest} мин".strip()
        days = hours / 24
        whole_d = int(days)
        hours_rest = int(round((days - whole_d) * 24))
        return f"{whole_d} д {hours_rest} ч".strip()

    status = "offline"
    if online:
        if spd >= 1.0:
            status = "moving"
        else:
            # до порога → «Стоянка», после порога → «Остановка» (длинная)
            if stop_duration is None or stop_duration < stop_threshold:
                status = "stopped"
            else:
                if ignition is True:
                    status = "park_ign_on"
                elif ignition is False:
                    status = "park_ign_off"
                else:
                    status = "stop"
    status_label = {
        "moving": "Движение",
        "stop": "Остановка",
        "park_ign_on": "Остановка, зажиг. вкл",
        "park_ign_off": "Остановка, зажиг. выкл",
        "stopped": "Стоянка",
        "offline": "Нет связи",
    }.get(status, status)
    duration_text = _fmt_duration(stop_duration)
    if duration_text and status in ("stop", "park_ign_on", "park_ign_off", "stopped"):
        status_label = f"{status_label} · {duration_text}"
    has_fuel = infer_has_fuel(snapshot, unit_config)
    result = {
        "online": online,
        "status": status,
        "speed": speed,
        "last_ts": last_ts,
        "has_fuel": has_fuel,
        "ignition": ignition,
        "age_sec": age_sec,
        "expected_interval_sec": expected_interval,
        "reason": "ok" if online else "no_connection",
        "status_label": status_label,
        "stop_duration_s": stop_duration,
    }

    # age unavailable → нет данных
    if age_sec is None:
        result["status_label"] = "Нет данных"
        result["reason"] = "no_data" if health_ok else "no_source"
        result["online"] = False
        result["status"] = "offline"
        return result

    # offline ветка
    if not online:
        result["status_label"] = f"Нет связи {_fmt_age(age_sec)}"
        result["reason"] = "no_connection"
        return result

    # online: устаревшие данные
    if age_sec > stale_warn:
        result["status_label"] = f"Онлайн, нет данных {_fmt_age(age_sec)}"
        result["reason"] = "no_data"
    else:
        # для остановок/стоянок уже добавили длительность, лишний возраст не показываем
        if status in ("stop", "park_ign_on", "park_ign_off", "stopped") and duration_text:
            result["status_label"] = status_label
        else:
            result["status_label"] = f"{status_label} · {_fmt_age(age_sec)}"
        result["reason"] = "ok"

    return result


__all__ = ["compute_status", "is_online", "infer_has_fuel", "get_online_threshold"]

