"""Adapter that converts Wialon .wlp exports into UnitConfig snapshots."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional

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


class WialonUnitConfigAdapter:
    """Transforms raw Wialon unit card exports (.wlp) into UnitConfig dataclasses."""

    def __init__(self, *, source_kind: str = "wialon") -> None:
        self.source_kind = source_kind

    # Public API -------------------------------------------------

    def from_wlp_file(self, path: Path, *, unit_id: int, dump_ts: Optional[int] = None) -> UnitConfig:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
        return self.from_wlp_dict(payload, unit_id=unit_id, dump_ts=dump_ts, source_path=path)

    def from_wlp_dict(
        self,
        payload: Mapping[str, Any],
        *,
        unit_id: int,
        dump_ts: Optional[int] = None,
        source_path: Optional[Path] = None,
    ) -> UnitConfig:
        dump_ts = dump_ts or int(time.time())

        general = self._build_general(payload.get("general", {}))
        icon = self._build_icon(payload.get("icon", {}))
        hw_config = self._build_hw(payload.get("hwConfig"))
        counters = self._build_counters(payload.get("counters"))
        profile = [self._build_profile(entry) for entry in payload.get("profile", [])]
        intervals = [self._build_interval(entry) for entry in payload.get("intervals", [])]
        sensors = [self._build_sensor(entry) for entry in payload.get("sensors", [])]

        extra: Dict[str, Any] = {}
        for key in ("type", "version", "mu"):
            value = payload.get(key)
            if value not in (None, ""):
                extra[f"wialon_{key}"] = value
        if source_path:
            extra["source_file"] = str(source_path)

        return UnitConfig(
            unit_id=unit_id,
            source_kind=self.source_kind,
            dump_ts=dump_ts,
            general=general,
            icon=icon,
            hw_config=hw_config,
            counters=counters,
            advanced=payload.get("advProps", {}) or {},
            profile=profile,
            intervals=intervals,
            sensors=sensors,
            report_props=payload.get("reportProps", {}) or {},
            aliases=payload.get("aliases", []) or [],
            driving=payload.get("driving", {}) or {},
            trip=payload.get("trip", {}) or {},
            extra=extra,
        )

    # Builders ---------------------------------------------------

    def _build_general(self, raw: Mapping[str, Any]) -> Optional[GeneralConfig]:
        if not raw:
            return None
        return GeneralConfig(
            name=raw.get("n", f"unit_{int(time.time())}"),
            uid=_optional_str(raw.get("uid")),
            uid2=_optional_str(raw.get("uid2")),
            phone=_optional_str(raw.get("ph")),
            phone2=_optional_str(raw.get("ph2")),
            password=_optional_str(raw.get("psw")),
            hardware=_optional_str(raw.get("hw")),
            extra={},
        )

    def _build_icon(self, raw: Mapping[str, Any]) -> Optional[IconConfig]:
        if not raw:
            return None
        return IconConfig(
            library=_optional_str(raw.get("lib")),
            url=_optional_str(raw.get("url")),
            rotation_mode=_optional_str(raw.get("imgRot")),
            extra={},
        )

    def _build_hw(self, raw: Optional[Mapping[str, Any]]) -> Optional[HWConfig]:
        if not raw:
            return None
        params = [self._build_hw_param(item) for item in raw.get("params", [])]
        full_data = raw.get("fullData")
        if isinstance(full_data, str):
            full_data = full_data == "1"
        return HWConfig(
            hardware=_optional_str(raw.get("hw")),
            full_data=bool(full_data) if full_data is not None else None,
            params=params,
            extra={},
        )

    def _build_hw_param(self, raw: Mapping[str, Any]) -> HWParam:
        return HWParam(
            name=str(raw.get("name") or ""),
            label=_optional_str(raw.get("label")),
            type=_optional_str(raw.get("type")),
            value=raw.get("value"),
            default=raw.get("default"),
            description=_optional_str(raw.get("description")),
            readonly=_maybe_bool(raw.get("readonly")),
            minval=_maybe_float(raw.get("minval")),
            maxval=_maybe_float(raw.get("maxval")),
            extra={},
        )

    def _build_counters(self, raw: Optional[Mapping[str, Any]]) -> Optional[CounterConfig]:
        if not raw:
            return None
        values = {key: _maybe_float(value) for key, value in raw.items()}
        return CounterConfig(values=values)

    def _build_profile(self, raw: Mapping[str, Any]) -> ProfileField:
        meta = {}
        if "ct" in raw:
            meta["created_ts"] = raw.get("ct")
        if "mt" in raw:
            meta["updated_ts"] = raw.get("mt")
        return ProfileField(
            field_id=int(raw.get("id", 0)),
            name=str(raw.get("n", "")),
            value=raw.get("v"),
            meta=meta,
        )

    def _build_interval(self, raw: Mapping[str, Any]) -> IntervalConfig:
        params = {k: v for k, v in raw.items() if k not in ("id", "n")}
        return IntervalConfig(
            interval_id=int(raw.get("id", 0)),
            name=str(raw.get("n", "")),
            params=params,
        )

    def _build_sensor(self, raw: Mapping[str, Any]) -> SensorConfig:
        parameters: Dict[str, Any] = {}
        if raw.get("p") not in (None, ""):
            parameters["param"] = raw.get("p")
        if raw.get("f") not in (None, ""):
            parameters["flags"] = raw.get("f")
        config_data = _parse_json_maybe(raw.get("c"))
        if config_data is not None:
            parameters["config"] = config_data
        elif raw.get("c"):
            parameters["config_raw"] = raw.get("c")

        validation = self._build_validation(raw)
        calibration = self._build_calibration(raw)

        meta = {}
        if raw.get("ct") is not None:
            meta["created_ts"] = raw["ct"]
        if raw.get("mt") is not None:
            meta["updated_ts"] = raw["mt"]

        return SensorConfig(
            sensor_id=int(raw.get("id", 0)),
            name=str(raw.get("n", "")),
            type=str(raw.get("t", "")),
            units=_optional_str(raw.get("m")),
            description=_optional_str(raw.get("d")),
            parameters=parameters,
            expression=None,
            validation=validation,
            calibration=calibration,
            meta=meta,
        )

    def _build_validation(self, raw: Mapping[str, Any]) -> Optional[SensorValidation]:
        vt = raw.get("vt")
        vs = raw.get("vs")
        if vt is None and vs is None:
            return None
        params = {}
        if vs is not None:
            params["vs"] = vs
        return SensorValidation(
            mode=str(vt) if vt is not None else None,
            min_value=None,
            max_value=None,
            hysteresis=None,
            params=params,
        )

    def _build_calibration(self, raw: Mapping[str, Any]) -> Optional[SensorCalibration]:
        rows = raw.get("tbl") or []
        if not rows:
            return None
        points: List[CalibrationPoint] = []
        segments: List[CalibrationSegment] = []
        for entry in rows:
            start_x = _maybe_float(entry.get("x"))
            a = _maybe_float(entry.get("a"))
            b = _maybe_float(entry.get("b"))
            if start_x is None or a is None or b is None:
                continue
            segments.append(CalibrationSegment(start_x=start_x, a=a, b=b))
            if entry.get("y") is not None:
                value = _maybe_float(entry.get("y"))
                if value is not None:
                    points.append(CalibrationPoint(raw=start_x, value=value))
        meta = {}
        if raw.get("ct") is not None:
            meta["created_ts"] = raw["ct"]
        if raw.get("mt") is not None:
            meta["updated_ts"] = raw["mt"]
        return SensorCalibration(
            sensor_id=int(raw.get("id", 0)),
            tank_id=None,
            points=points,
            segments=segments,
            smoothing=None,
            source="wlp",
            updated_at=raw.get("mt"),
            meta=meta,
        )


# Helper functions -----------------------------------------------


def _optional_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    value = str(value)
    if not value:
        return None
    return value


def _maybe_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _maybe_bool(value: Any) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        if value in {"1", "true", "True"}:
            return True
        if value in {"0", "false", "False"}:
            return False
    return None


def _parse_json_maybe(value: Any) -> Optional[Dict[str, Any]]:
    if not value:
        return None
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return None
    return None


__all__ = ["WialonUnitConfigAdapter"]
