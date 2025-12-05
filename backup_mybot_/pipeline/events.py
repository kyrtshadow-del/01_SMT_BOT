"""Core event and unit data structures used across the pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Mapping, Optional


@dataclass(frozen=True)
class UnitSnapshot:
    """Static information about a unit/device that helps interpret events."""

    unit_id: int
    name: str
    sensors: Mapping[str, Any] = field(default_factory=dict)
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class Event:
    """Normalised telemetry event shared between adapters and the engine."""

    unit_id: int
    device_ts: int
    received_ts: int
    latitude: Optional[float]
    longitude: Optional[float]
    speed: Optional[float]
    course: Optional[float]
    params: Mapping[str, Any] = field(default_factory=dict)
    source: str = "unknown"
    raw_payload: Mapping[str, Any] = field(default_factory=dict)

    def as_dict(self) -> Dict[str, Any]:
        """Return a serialisable representation."""

        return {
            "unit_id": self.unit_id,
            "device_ts": self.device_ts,
            "received_ts": self.received_ts,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "speed": self.speed,
            "course": self.course,
            "params": dict(self.params),
            "source": self.source,
            "raw_payload": dict(self.raw_payload),
        }

