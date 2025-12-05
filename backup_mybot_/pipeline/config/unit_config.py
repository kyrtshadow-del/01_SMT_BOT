"""Unified UnitConfig dataclasses for card/settings snapshot."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class GeneralConfig:
    """Basic unit identity."""

    name: str
    uid: Optional[str] = None
    uid2: Optional[str] = None
    phone: Optional[str] = None
    phone2: Optional[str] = None
    password: Optional[str] = None
    hardware: Optional[str] = None
    extra: Dict[str, Any] = field(default_factory=dict)


@dataclass
class IconConfig:
    library: Optional[str] = None
    url: Optional[str] = None
    rotation_mode: Optional[str] = None
    extra: Dict[str, Any] = field(default_factory=dict)


@dataclass
class HWParam:
    name: str
    label: Optional[str] = None
    type: Optional[str] = None
    value: Optional[Any] = None
    default: Optional[Any] = None
    description: Optional[str] = None
    readonly: Optional[bool] = None
    minval: Optional[float] = None
    maxval: Optional[float] = None
    extra: Dict[str, Any] = field(default_factory=dict)


@dataclass
class HWConfig:
    hardware: Optional[str] = None
    full_data: Optional[bool] = None
    params: List[HWParam] = field(default_factory=list)
    extra: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CalibrationPoint:
    raw: float
    value: float


@dataclass
class CalibrationSegment:
    start_x: float
    a: float
    b: float


@dataclass
class SensorCalibration:
    sensor_id: int
    tank_id: Optional[str] = None
    points: List[CalibrationPoint] = field(default_factory=list)
    segments: List[CalibrationSegment] = field(default_factory=list)
    smoothing: Optional[Dict[str, Any]] = None
    source: Optional[str] = None
    updated_at: Optional[int] = None
    meta: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SensorValidation:
    mode: Optional[str] = None
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    hysteresis: Optional[float] = None
    params: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SensorConfig:
    sensor_id: int
    name: str
    type: str
    units: Optional[str] = None
    description: Optional[str] = None
    parameters: Dict[str, Any] = field(default_factory=dict)
    expression: Optional[str] = None
    validation: Optional[SensorValidation] = None
    calibration: Optional[SensorCalibration] = None
    meta: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CounterConfig:
    values: Dict[str, float] = field(default_factory=dict)


@dataclass
class IntervalConfig:
    interval_id: int
    name: str
    params: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ProfileField:
    field_id: int
    name: str
    value: Any
    meta: Dict[str, Any] = field(default_factory=dict)


@dataclass
class UnitConfig:
    """Top-level snapshot for a unit."""

    unit_id: int
    source_kind: str
    dump_ts: Optional[int] = None

    general: Optional[GeneralConfig] = None
    icon: Optional[IconConfig] = None
    hw_config: Optional[HWConfig] = None
    counters: Optional[CounterConfig] = None
    advanced: Dict[str, Any] = field(default_factory=dict)
    profile: List[ProfileField] = field(default_factory=list)
    intervals: List[IntervalConfig] = field(default_factory=list)
    sensors: List[SensorConfig] = field(default_factory=list)
    report_props: Dict[str, Any] = field(default_factory=dict)
    aliases: List[Dict[str, Any]] = field(default_factory=list)
    driving: Dict[str, Any] = field(default_factory=dict)
    trip: Dict[str, Any] = field(default_factory=dict)
    extra: Dict[str, Any] = field(default_factory=dict)


__all__ = [
    "GeneralConfig",
    "IconConfig",
    "HWParam",
    "HWConfig",
    "CalibrationPoint",
    "CalibrationSegment",
    "SensorCalibration",
    "SensorValidation",
    "SensorConfig",
    "CounterConfig",
    "IntervalConfig",
    "ProfileField",
    "UnitConfig",
]
