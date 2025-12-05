from __future__ import annotations

import csv
import html
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Awaitable, Callable, Mapping, Optional

from telegram import CallbackQuery, InputFile
from telegram.ext import ContextTypes

from bot.features import card as card_features
from pipeline.config.unit_config import SensorConfig, UnitConfig

log = logging.getLogger(__name__)

__all__ = [
    "SensorsFeatureDeps",
    "ClearSensorsDeps",
    "build_show_sensor_settings_handler",
    "build_clear_sensors_handler",
    "export_sensor_calibration_csv",
]


@dataclass
class SensorsFeatureDeps:
    safe_answer_callback: Callable[[CallbackQuery, Optional[str], bool], Awaitable[None]]
    load_unit_config: Callable[[int], Optional[UnitConfig]]
    exports_dir: Path
    state_wait_unit: int


def export_sensor_calibration_csv(exports_dir: Path, unit_id: int, sensor: SensorConfig) -> Optional[Path]:
    calibration = sensor.calibration
    if not calibration or not calibration.points:
        return None
    exports_dir.mkdir(parents=True, exist_ok=True)
    slug = re.sub(r"[^0-9A-Za-z_-]+", "_", sensor.name or "").strip("_") or "sensor"
    path = exports_dir / f"{unit_id}_{slug}.csv"
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["raw", "value"])
        for point in calibration.points:
            writer.writerow([point.raw, point.value])
    return path


def build_show_sensor_settings_handler(
    deps: SensorsFeatureDeps,
) -> Callable[[CallbackQuery, ContextTypes.DEFAULT_TYPE], Awaitable[int]]:
    async def handler(q: CallbackQuery, context: ContextTypes.DEFAULT_TYPE) -> int:
        unit = context.user_data.get("chosen_unit")
        if not unit:
            await deps.safe_answer_callback(q, "Сначала выберите объект", show_alert=True)
            return deps.state_wait_unit
        try:
            unit_id = int(unit.get("id"))
        except (TypeError, ValueError):
            await deps.safe_answer_callback(q, "Некорректный ID объекта", show_alert=True)
            return deps.state_wait_unit
        unit_name = unit.get("nm") or f"id {unit_id}"
        await deps.safe_answer_callback(q)
        config = deps.load_unit_config(unit_id)
        if not config or not config.sensors:
            if q.message:
                await q.message.reply_text(
                    "Для выбранного объекта нет локальных датчиков. Импортируйте конфигурацию или добавьте ДУТ."
                )
            return deps.state_wait_unit
        params_map = context.user_data.get("params_map")
        if not isinstance(params_map, Mapping):
            params_map = {}
        try:
            sensor_values = card_features.compute_unit_config_sensor_values(config, params_map)
        except Exception as exc:
            log.warning("sensor_settings: compute failed unit=%s err=%s", unit_id, exc)
            sensor_values = {}
        header = f"🔧 Настройки датчиков — <b>{html.escape(unit_name)}</b>\nВсего: {len(config.sensors)}"
        blocks = card_features.format_sensor_settings_blocks(config, sensor_values)
        if not blocks:
            if q.message:
                await q.message.reply_text(header + "\nНет датчиков в конфиге.", parse_mode="HTML")
            return deps.state_wait_unit
        messages = card_features.chunk_sensor_settings_messages(header, blocks)
        log.info("sensor_settings: start unit=%s chunks=%s", unit_id, len(messages))
        for text in messages:
            if q.message:
                await q.message.reply_text(text, parse_mode="HTML", disable_web_page_preview=True)
        sent_csv = 0
        for sensor in config.sensors:
            path = export_sensor_calibration_csv(deps.exports_dir, unit_id, sensor)
            if not path:
                continue
            try:
                with path.open("rb") as fh:
                    if q.message:
                        await q.message.reply_document(
                            document=InputFile(fh, filename=path.name),
                            caption=f"Тарировка: {sensor.name}",
                        )
                sent_csv += 1
            except Exception as exc:  # pragma: no cover - defensive upload
                log.warning("sensor_settings: failed to send calibration %s: %s", path, exc)
        log.info("sensor_settings: done unit=%s csv=%s", unit_id, sent_csv)
        return deps.state_wait_unit

    return handler


@dataclass
class ClearSensorsDeps:
    safe_answer_callback: Callable[[CallbackQuery, Optional[str], bool], Awaitable[None]]
    pipeline_card_enabled: Callable[[], bool]
    ensure_unit_config_object: Callable[[int], UnitConfig]
    save_unit_config: Callable[[UnitConfig], None]
    clear_unit_config_cache: Callable[[], None]
    exports_dir: Path
    show_stats_then_actions: Callable[
        [CallbackQuery, ContextTypes.DEFAULT_TYPE, int, str, Optional[Any], bool, bool], Awaitable[int]
    ]
    state_wait_unit: int


def build_clear_sensors_handler(
    deps: ClearSensorsDeps,
) -> Callable[[CallbackQuery, ContextTypes.DEFAULT_TYPE], Awaitable[int]]:
    async def handler(q: CallbackQuery, context: ContextTypes.DEFAULT_TYPE) -> int:
        if not deps.pipeline_card_enabled():
            await deps.safe_answer_callback(q, "Функция доступна только при включённом локальном pipeline.", show_alert=True)
            return deps.state_wait_unit
        unit = context.user_data.get("chosen_unit")
        if not unit:
            await deps.safe_answer_callback(q, "Сначала выберите объект", show_alert=True)
            return deps.state_wait_unit
        try:
            unit_id = int(unit.get("id"))
        except (TypeError, ValueError):
            await deps.safe_answer_callback(q, "Некорректный ID объекта", show_alert=True)
            return deps.state_wait_unit
        config = deps.ensure_unit_config_object(unit_id)
        sensors = list(config.sensors or [])
        if not sensors:
            await deps.safe_answer_callback(q)
            if q.message:
                await q.message.reply_text("Локальный конфиг уже пуст.")
            return deps.state_wait_unit
        removed = len(sensors)
        config.sensors = []
        deps.save_unit_config(config)
        deps.clear_unit_config_cache()
        removed_exports = 0
        exports_dir = deps.exports_dir
        if exports_dir.exists():
            for path in exports_dir.glob(f"{unit_id}_*.csv"):
                try:
                    path.unlink()
                    removed_exports += 1
                except Exception as exc:
                    log.debug("clear_sensors: failed to delete %s err=%s", path, exc)
        log.info(
            "clear_sensors: unit=%s sensors_removed=%s exports_removed=%s",
            unit_id,
            removed,
            removed_exports,
        )
        await deps.safe_answer_callback(q)
        if q.message:
            await q.message.reply_text(f"🗑 Удалены все датчики ({removed}) из локального конфига.")
        unit_name = unit.get("nm") or f"id {unit_id}"
        await deps.show_stats_then_actions(
            q,
            context,
            unit_id,
            unit_name,
            client=None,
            via_callback=True,
            preserve_previous=True,
        )
        return deps.state_wait_unit

    return handler
