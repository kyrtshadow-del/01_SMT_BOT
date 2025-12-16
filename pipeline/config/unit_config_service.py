"""Persistence/validation helpers for UnitConfig snapshots (PostgreSQL-backed)."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import json
import logging
import os
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence

import psycopg
from psycopg.rows import dict_row

try:
    import jsonschema  # type: ignore
except ImportError:  # pragma: no cover
    jsonschema = None  # type: ignore

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

LOGGER = logging.getLogger(__name__)


class UnitConfigValidationError(Exception):
    """Raised when a config fails schema validation."""


@dataclass(frozen=True)
class UnitConfigSummary:
    unit_id: int
    path: Path
    updated_ts: float
    source_kind: str


class UnitConfigService:
    """Read/write UnitConfig documents in PostgreSQL `unit_configs` table."""

    def __init__(
        self,
        dsn: str,
        *,
        schema_path: Optional[Path] = None,
        extra_validators: Optional[Sequence[Callable[[Dict[str, Any]], None]]] = None,
    ) -> None:
        self.dsn = dsn
        self.schema_path = schema_path or Path(__file__).with_name("unit_config.schema.json")
        self.schema = self._load_schema(self.schema_path)
        self.extra_validators = list(extra_validators or [])
        self._warned_about_schema = False

    def _get_conn(self):
        return psycopg.connect(self.dsn, row_factory=dict_row, autocommit=True)

    # Public API -----------------------------------------------------

    def save(self, config: UnitConfig) -> None:
        payload = self.to_dict(config)
        self._validate(payload)
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO unit_configs (unit_id, config)
                    VALUES (%s, %s)
                    ON CONFLICT (unit_id) DO UPDATE SET config = EXCLUDED.config
                    """,
                    (config.unit_id, json.dumps(payload, ensure_ascii=False)),
                )

    def load(self, unit_id: int) -> UnitConfig:
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT config FROM unit_configs WHERE unit_id = %s", (unit_id,))
                row = cur.fetchone()
        if not row:
            # Fallback: return minimal default config instead of raising
            return UnitConfig(unit_id=unit_id, source_kind="unknown")
        raw_config = row["config"]
        if isinstance(raw_config, str):
            payload = json.loads(raw_config)
        else:
            payload = dict(raw_config)
        self._validate(payload)
        return self.from_dict(payload)

    def list_configs(self) -> List[UnitConfigSummary]:
        summaries: List[UnitConfigSummary] = []
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT unit_id, config, updated_at FROM unit_configs ORDER BY unit_id")
                for row in cur.fetchall():
                    unit_id = int(row["unit_id"])
                    config = row["config"]
                    if isinstance(config, str):
                        try:
                            payload = json.loads(config)
                        except json.JSONDecodeError:
                            payload = {}
                    else:
                        payload = dict(config or {})
                    source = payload.get("source_kind", "unknown")
                    updated_at = row.get("updated_at")
                    updated_ts = float(updated_at.timestamp()) if updated_at is not None else 0.0
                    summaries.append(
                        UnitConfigSummary(
                            unit_id=unit_id,
                            path=Path(str(unit_id)),
                            updated_ts=updated_ts,
                            source_kind=source,
                        )
                    )
        return summaries

    def delete(self, unit_id: int) -> None:
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM unit_configs WHERE unit_id = %s", (unit_id,))

    # Serialization helpers -----------------------------------------

    @staticmethod
    def to_dict(config: UnitConfig) -> Dict[str, Any]:
        return asdict(config)

    @staticmethod
    def from_dict(payload: Dict[str, Any]) -> UnitConfig:
        general = _build_general(payload.get("general"))
        icon = _build_icon(payload.get("icon"))
        hw_config = _build_hw(payload.get("hw_config"))
        counters = _build_counters(payload.get("counters"))
        profile = [_build_profile_field(item) for item in payload.get("profile", [])]
        intervals = [_build_interval(item) for item in payload.get("intervals", [])]
        sensors = [_build_sensor(item) for item in payload.get("sensors", [])]

        return UnitConfig(
            unit_id=int(payload["unit_id"]),
            source_kind=str(payload["source_kind"]),
            dump_ts=payload.get("dump_ts"),
            general=general,
            icon=icon,
            hw_config=hw_config,
            counters=counters,
            advanced=payload.get("advanced", {}) or {},
            profile=profile,
            intervals=intervals,
            sensors=sensors,
            report_props=payload.get("report_props", {}) or {},
            aliases=payload.get("aliases", []) or [],
            driving=payload.get("driving", {}) or {},
            trip=payload.get("trip", {}) or {},
            extra=payload.get("extra", {}) or {},
        )

    # Internal utilities --------------------------------------------

    def _validate(self, payload: Dict[str, Any]) -> None:
        if "unit_id" not in payload or "source_kind" not in payload:
            raise UnitConfigValidationError("unit_id and source_kind are required")
        if not isinstance(payload["unit_id"], int):
            raise UnitConfigValidationError("unit_id must be int")
        if not isinstance(payload["source_kind"], str):
            raise UnitConfigValidationError("source_kind must be str")

        if self.schema:
            if jsonschema is None:
                if not self._warned_about_schema:
                    LOGGER.warning("jsonschema is not installed; UnitConfig validation is minimal")
                    self._warned_about_schema = True
            else:
                try:
                    jsonschema.validate(payload, self.schema)  # type: ignore[attr-defined]
                except jsonschema.ValidationError as exc:  # type: ignore[attr-defined]
                    raise UnitConfigValidationError(str(exc)) from exc

        for validator in self.extra_validators:
            validator(payload)

    def _load_schema(self, path: Optional[Path]) -> Optional[Dict[str, Any]]:
        if not path:
            return None
        if not path.exists():
            LOGGER.debug("unit_config: schema file %s missing, skipping strict validation", path)
            return None
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            LOGGER.warning("unit_config: failed to load schema from %s: %s", path, exc)
            return None


# Builders for nested dataclasses ---------------------------------


def _build_general(raw: Optional[Dict[str, Any]]) -> Optional[GeneralConfig]:
    if not raw:
        return None
    return GeneralConfig(
        name=raw["name"],
        uid=raw.get("uid"),
        uid2=raw.get("uid2"),
        phone=raw.get("phone"),
        phone2=raw.get("phone2"),
        password=raw.get("password"),
        hardware=raw.get("hardware"),
        extra=raw.get("extra", {}) or {},
    )


def _build_icon(raw: Optional[Dict[str, Any]]) -> Optional[IconConfig]:
    if not raw:
        return None
    return IconConfig(
        library=raw.get("library"),
        url=raw.get("url"),
        rotation_mode=raw.get("rotation_mode"),
        extra=raw.get("extra", {}) or {},
    )


def _build_hw(raw: Optional[Dict[str, Any]]) -> Optional[HWConfig]:
    if not raw:
        return None
    params = [_build_hw_param(item) for item in raw.get("params", [])]
    return HWConfig(
        hardware=raw.get("hardware"),
        full_data=raw.get("full_data"),
        params=params,
        extra=raw.get("extra", {}) or {},
    )


def _build_hw_param(raw: Dict[str, Any]) -> HWParam:
    return HWParam(
        name=raw["name"],
        label=raw.get("label"),
        type=raw.get("type"),
        value=raw.get("value"),
        default=raw.get("default"),
        description=raw.get("description"),
        readonly=raw.get("readonly"),
        minval=raw.get("minval"),
        maxval=raw.get("maxval"),
        extra=raw.get("extra", {}) or {},
    )


def _build_counters(raw: Optional[Dict[str, Any]]) -> Optional[CounterConfig]:
    if not raw:
        return None
    return CounterConfig(values=raw.get("values", {}) or {})


def _build_profile_field(raw: Dict[str, Any]) -> ProfileField:
    return ProfileField(
        field_id=int(raw["field_id"]),
        name=raw["name"],
        value=raw.get("value"),
        meta=raw.get("meta", {}) or {},
    )


def _build_interval(raw: Dict[str, Any]) -> IntervalConfig:
    return IntervalConfig(
        interval_id=int(raw["interval_id"]),
        name=raw["name"],
        params=raw.get("params", {}) or {},
    )


def _build_sensor(raw: Dict[str, Any]) -> SensorConfig:
    validation = _build_validation(raw.get("validation"))
    calibration = _build_calibration(raw.get("calibration"))
    return SensorConfig(
        sensor_id=int(raw["sensor_id"]),
        name=raw["name"],
        type=raw["type"],
        units=raw.get("units"),
        description=raw.get("description"),
        parameters=raw.get("parameters", {}) or {},
        expression=raw.get("expression"),
        validation=validation,
        calibration=calibration,
        meta=raw.get("meta", {}) or {},
    )


def _build_validation(raw: Optional[Dict[str, Any]]) -> Optional[SensorValidation]:
    if not raw:
        return None
    return SensorValidation(
        mode=raw.get("mode"),
        min_value=raw.get("min_value"),
        max_value=raw.get("max_value"),
        hysteresis=raw.get("hysteresis"),
        params=raw.get("params", {}) or {},
    )


def _build_calibration(raw: Optional[Dict[str, Any]]) -> Optional[SensorCalibration]:
    if not raw:
        return None
    points = [_build_calibration_point(item) for item in raw.get("points", [])]
    segments = [_build_calibration_segment(item) for item in raw.get("segments", [])]
    return SensorCalibration(
        sensor_id=int(raw["sensor_id"]),
        tank_id=raw.get("tank_id"),
        points=points,
        segments=segments,
        smoothing=raw.get("smoothing"),
        source=raw.get("source"),
        updated_at=raw.get("updated_at"),
        meta=raw.get("meta", {}) or {},
    )


def _build_calibration_point(raw: Dict[str, Any]) -> CalibrationPoint:
    return CalibrationPoint(raw=float(raw["raw"]), value=float(raw["value"]))


def _build_calibration_segment(raw: Dict[str, Any]) -> CalibrationSegment:
    return CalibrationSegment(
        start_x=float(raw["start_x"]),
        a=float(raw["a"]),
        b=float(raw["b"]),
    )


def load_default_service(storage_root: Optional[Path] = None) -> UnitConfigService:
    """Helper used by consumers that previously passed pipeline storage root.

    Now uses PostgreSQL DSN from env and ignores storage_root.
    """

    dsn = os.getenv("DEVICE_REGISTRY_DSN") or os.getenv("DATABASE_URL")
    if not dsn:
        raise ValueError("DEVICE_REGISTRY_DSN or DATABASE_URL env var is required for UnitConfigService")
    return UnitConfigService(dsn)
