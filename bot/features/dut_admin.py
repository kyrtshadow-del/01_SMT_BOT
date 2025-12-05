from __future__ import annotations

import copy
import logging
import re
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, List, Mapping, Optional, Sequence, Tuple

from telegram import CallbackQuery, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

from bot.features import sensors as sensor_features
from pipeline.adapters.wialon_wlp_import import sensor_config_from_entry
from pipeline.config.unit_config import SensorConfig, UnitConfig

log = logging.getLogger(__name__)

__all__ = [
    "normalize_unit_sensors",
    "collect_wialon_sensor_map",
    "merge_unit_item_sensors",
    "build_add_dut_start_handler",
    "build_add_dut_name_handler",
    "persist_dut_from_unit_item",
]


def normalize_unit_sensors(sens_raw: Any) -> Dict[str, Dict[str, Any]]:
    sensors: Dict[str, Dict[str, Any]] = {}
    if isinstance(sens_raw, dict):
        iterable = sens_raw.values()
    elif isinstance(sens_raw, list):
        iterable = sens_raw
    else:
        return sensors

    for idx, sensor in enumerate(iterable, start=1):
        if not isinstance(sensor, dict):
            continue
        sensor_copy = copy.deepcopy(sensor)
        sid = sensor_copy.get("id")
        key: Optional[str] = None
        if isinstance(sid, bool):
            sid = int(sid)
        if isinstance(sid, (int, float)):
            try:
                numeric_id = int(sid)
            except Exception:
                numeric_id = None
            if numeric_id is not None:
                sensor_copy["id"] = numeric_id
                key = str(numeric_id)
        elif isinstance(sid, str) and sid.strip():
            key = sid.strip()
            try:
                sensor_copy["id"] = int(key)
            except Exception:
                pass
        if not key:
            key = f"idx:{idx}"
        sensors[key] = sensor_copy
    return sensors


def collect_wialon_sensor_map(unit_item: Mapping[str, Any]) -> Dict[str, Dict[str, Any]]:
    sensors = normalize_unit_sensors(unit_item.get("sens"))
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for entry in sensors.values():
        name = str(entry.get("n") or entry.get("name") or "").strip()
        if not name:
            continue
        key = name.casefold()
        grouped.setdefault(key, []).append(dict(entry))

    result: Dict[str, Dict[str, Any]] = {}
    for key, entries in grouped.items():
        best_entry = entries[0]
        best_score = _sensor_entry_score(best_entry)
        for candidate in entries[1:]:
            score = _sensor_entry_score(candidate)
            if score > best_score:
                best_entry = candidate
                best_score = score
        result[key] = best_entry
    return result


def merge_unit_item_sensors(unit_item: Dict[str, Any], extra_sensors: Sequence[Mapping[str, Any]]) -> None:
    if not extra_sensors:
        return
    base_map = normalize_unit_sensors(unit_item.get("sens"))
    extra_map = normalize_unit_sensors(extra_sensors)
    if not extra_map:
        return
    for key, extra_entry in extra_map.items():
        target = base_map.get(key)
        if target is None:
            base_map[key] = dict(extra_entry)
            continue
        for field, value in extra_entry.items():
            if value in (None, "", [], {}, ()):
                continue
            if not target.get(field):
                target[field] = value
            elif isinstance(value, dict):
                base_field = target.setdefault(field, {})
                if isinstance(base_field, dict):
                    for sub_key, sub_value in value.items():
                        if sub_value not in (None, "", [], {}, ()):
                            base_field.setdefault(sub_key, sub_value)
        base_map[key] = target
    unit_item["sens"] = list(base_map.values())


def _sensor_entry_score(entry: Mapping[str, Any]) -> Tuple[int, int]:
    sensor_type = str(entry.get("t") or "").strip().lower()
    is_fuel = sensor_type in {"fuel level", "fuel_level", "fuel", "liquid level"}
    has_table = bool(entry.get("tbl"))
    return (1 if is_fuel else 0, 1 if has_table else 0)


def _resolve_sensor_by_name(sensors_map: Dict[str, Dict[str, Any]], query: str) -> Tuple[str, Optional[Dict[str, Any]]]:
    normalized = query.strip().casefold()
    if not normalized:
        return query.strip(), None
    for key, entry in sensors_map.items():
        if key == normalized:
            return entry.get("n") or entry.get("name") or query, dict(entry)
    return query.strip(), None


def _extract_sensor_refs(expression: str) -> List[str]:
    pattern = re.compile(r"\[([^\]]+)\]")
    return [match.strip() for match in pattern.findall(expression)]


def _prepare_dut_sensor_entries(
    sensor_entry: Dict[str, Any],
    sensors_map: Dict[str, Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    expression = str(sensor_entry.get("p") or sensor_entry.get("m") or sensor_entry.get("param") or sensor_entry.get("src") or "").strip()
    refs = _extract_sensor_refs(expression)
    created_entries: List[Dict[str, Any]] = []
    # если ссылок нет, но есть исходный param (adc11 и т.п.) — выделяем сырой кастомный сенсор
    if not refs and expression:
        raw_name = _generate_custom_dut_ref_name(sensor_entry, sensors_map)
        raw_entry = dict(sensor_entry)
        raw_entry["n"] = raw_name
        raw_entry["name"] = raw_name
        raw_entry["t"] = "custom"
        raw_entry["type"] = "custom"
        raw_entry["kind"] = raw_entry.get("kind") or "custom"
        raw_entry["p"] = expression
        sensors_map[raw_name.casefold()] = raw_entry
        created_entries.append(raw_entry)
        sensor_entry = dict(sensor_entry)
        sensor_entry["tbl"] = []
        sensor_entry["p"] = f"[{raw_name}]"
        refs = [raw_name]

    raw_name = sensor_entry.get("n") or sensor_entry.get("name")
    if not raw_name and refs:
        raw_name = refs[0]
    if not raw_name:
        raw_name = _generate_unique_sensor_name("ДУТ", sensors_map)
    sensor_entry = dict(sensor_entry)
    sensor_entry["n"] = raw_name

    referenced: List[Dict[str, Any]] = []
    for ref in refs:
        key, target = _resolve_sensor_by_name(sensors_map, ref)
        if target is None:
            raise ValueError(f"Ссылка на датчик '{ref}' не найдена")
        referenced.append(dict(target))
    payload: List[Dict[str, Any]] = []
    seen_names: Set[str] = set()
    for entry in created_entries + referenced:
        name = str(entry.get("n") or entry.get("name") or "").strip()
        key = name.casefold() if name else ""
        if key and key in seen_names:
            continue
        if key:
            seen_names.add(key)
        payload.append(entry)
    return payload, dict(sensor_entry)


def _generate_unique_sensor_name(base_name: str, sensors_map: Dict[str, Dict[str, Any]]) -> str:
    slug = base_name.strip() or "sensor"
    candidate = slug
    index = 1
    names_cf = {name.casefold() for name in sensors_map.keys()}
    while candidate.casefold() in names_cf:
        index += 1
        candidate = f"{slug}_{index}"
    return candidate


def _generate_custom_dut_ref_name(sensor_entry: Mapping[str, Any], sensors_map: Dict[str, Dict[str, Any]]) -> str:
    base_name = str(sensor_entry.get("n") or sensor_entry.get("name") or "ДУТ").strip() or "ДУТ"
    prefix = re.sub(r"\d+$", "", base_name).strip()
    if not prefix:
        prefix = "ДУТ"
    names_cf = {name.casefold() for name in sensors_map.keys()}
    index = 1
    while True:
        candidate = f"{prefix}{index}"
        if candidate.casefold() not in names_cf:
            return candidate
        index += 1


def _text_is_cancel(value: str) -> bool:
    return value.strip().lower() in {"отмена", "cancel", "/cancel"}


@dataclass
class AddDutStartDeps:
    safe_answer_callback: Callable[[CallbackQuery, Optional[str], bool], Awaitable[None]]
    kb_cancel: Callable[[], InlineKeyboardMarkup]
    state_wait_unit: int
    state_add_dut_wait_name: int
    log: logging.Logger


def build_add_dut_start_handler(
    deps: AddDutStartDeps,
) -> Callable[[CallbackQuery, ContextTypes.DEFAULT_TYPE], Awaitable[int]]:
    async def handler(q: CallbackQuery, context: ContextTypes.DEFAULT_TYPE) -> int:
        unit = context.user_data.get("chosen_unit")
        if not unit:
            await deps.safe_answer_callback(q, "Сначала выберите объект", show_alert=True)
            return deps.state_wait_unit
        try:
            unit_id = int(unit["id"])
        except (TypeError, ValueError):
            await deps.safe_answer_callback(q, "Некорректный ID объекта", show_alert=True)
            return deps.state_wait_unit
        unit_name = unit.get("nm") or f"id {unit_id}"
        context.user_data["add_dut_ctx"] = {"unit_id": unit_id, "unit_name": unit_name}
        deps.log.info("add_dut: start unit=%s name=%s", unit_id, unit_name)
        await deps.safe_answer_callback(q)
        context.user_data["last_card_callback_chat_id"] = q.message.chat_id if q.message else None
        context.user_data["last_card_message_id"] = q.message.message_id if q.message else None
        if q.message:
            await q.message.reply_text(
                "Введите имя датчика уровня топлива (как в системе Wialon).\nОтправьте 'Отмена', чтобы прервать добавление.",
                reply_markup=deps.kb_cancel(),
            )
        return deps.state_add_dut_wait_name

    return handler


@dataclass
class AddDutNameDeps:
    ensure_authorized: Callable[[Update, ContextTypes.DEFAULT_TYPE], Awaitable[bool]]
    require_wialon_client: Callable[[Update, ContextTypes.DEFAULT_TYPE, str], Awaitable[Any]]
    run_blocking: Callable[..., Awaitable[Any]]
    pipeline_card_enabled: Callable[[], bool]
    show_stats_then_actions: Callable[..., Awaitable[int]]
    clear_unit_config_cache: Callable[[], None]
    ensure_unit_config_object: Callable[[int], UnitConfig]
    save_unit_config: Callable[[UnitConfig], None]
    export_calibration_csv: Callable[[int, SensorConfig], Optional[Path]]
    state_wait_unit: int
    state_auth_wait_token: int
    state_add_dut_wait_name: int
    log: logging.Logger


def build_add_dut_name_handler(
    deps: AddDutNameDeps,
) -> Callable[[Update, ContextTypes.DEFAULT_TYPE], Awaitable[int]]:
    async def handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        if not await deps.ensure_authorized(update, context):
            return deps.state_auth_wait_token
        ctx = context.user_data.get("add_dut_ctx") or {}
        unit_id = ctx.get("unit_id")
        unit_name = ctx.get("unit_name") or (context.user_data.get("chosen_unit") or {}).get("nm")
        if not unit_id:
            if update.message:
                await update.message.reply_text("Нет активной операции добавления ДУТ.")
            return deps.state_wait_unit
        dut_name = (update.message.text or "").strip() if update.message else ""
        if not dut_name:
            if update.message:
                await update.message.reply_text("Имя не может быть пустым. Попробуйте ещё раз или отправьте 'Отмена'.")
            return deps.state_add_dut_wait_name
        if _text_is_cancel(dut_name):
            if update.message:
                await update.message.reply_text("Добавление ДУТ отменено.")
            context.user_data.pop("add_dut_ctx", None)
            return deps.state_wait_unit

        client = await deps.require_wialon_client(update, context, "Чтобы получить параметры датчика, авторизуйтесь.")
        if not client:
            return deps.state_auth_wait_token
        try:
            unit_item = await deps.run_blocking(client.get_unit_full_for_stats, unit_id)
        except Exception as exc:
            if update.message:
                await update.message.reply_text(f"❌ Не удалось загрузить объект: {exc}")
            return deps.state_wait_unit

        try:
            detailed_sensors = await deps.run_blocking(client.get_unit_sensors_detailed, unit_id)
        except Exception as exc:
            detailed_sensors = []
            deps.log.debug("add_dut: extra sensors fetch failed unit=%s err=%s", unit_id, exc)
        else:
            merge_unit_item_sensors(unit_item, detailed_sensors)

        try:
            result = await deps.run_blocking(
                persist_dut_from_unit_item,
                unit_id,
                dut_name,
                unit_item,
                deps.ensure_unit_config_object,
                deps.save_unit_config,
                deps.export_calibration_csv,
            )
        except ValueError as exc:
            if update.message:
                await update.message.reply_text(f"❌ {exc}\nПопробуйте другое имя или отправьте 'Отмена'.")
            return deps.state_add_dut_wait_name
        except Exception as exc:
            deps.log.exception("add_dut failed: %s", exc)
            if update.message:
                await update.message.reply_text(f"❌ Не удалось сохранить датчик: {exc}")
            return deps.state_wait_unit
        finally:
            deps.clear_unit_config_cache()

        created_names, csv_paths = result
        deps.log.info("add_dut: saved unit=%s sensors=%s", unit_id, created_names)
        lines = ["✅ ДУТ добавлен в локальный конфиг."]
        if created_names:
            lines.append("Созданы/обновлены датчики: " + ", ".join(created_names))
        if csv_paths:
            lines.append("Тарировки сохранены в файлах:")
            lines.extend(f"• {path}" for path in csv_paths)
        if update.message:
            await update.message.reply_text("\n".join(lines))

        context.user_data.pop("add_dut_ctx", None)
        unit = context.user_data.get("chosen_unit") or {"id": unit_id, "nm": unit_name or f"id {unit_id}"}
        client_for_refresh = None  # карточка должна обновиться из локальных данных, без Wialon
        await deps.show_stats_then_actions(
            update,
            context,
            int(unit["id"]),
            unit.get("nm") or f"id {unit_id}",
            client_for_refresh,
            preserve_previous=True,
        )
        return deps.state_wait_unit

    return handler


def persist_dut_from_unit_item(
    unit_id: int,
    dut_name: str,
    unit_item: Mapping[str, Any],
    ensure_unit_config_object: Callable[[int], UnitConfig],
    save_unit_config: Callable[[UnitConfig], None],
    export_calibration_csv: Callable[[int, SensorConfig], Optional[Path]],
) -> Tuple[List[str], List[Path]]:
    log.info("add_dut: persist unit=%s dut=%s", unit_id, dut_name)
    sensors_map = collect_wialon_sensor_map(unit_item)
    if not sensors_map:
        raise ValueError("В объекте нет датчиков")
    target_name, target_entry = _resolve_sensor_by_name(sensors_map, dut_name)
    if target_entry is None:
        raise ValueError(f"Датчик '{dut_name}' не найден")
    sensor_type = str(target_entry.get("t") or "").strip().lower()
    if sensor_type not in {"fuel level", "fuel_level", "fuel"}:
        raise ValueError(f"Датчик '{target_name}' не является датчиком уровня топлива")

    working_map = {name: dict(entry) for name, entry in sensors_map.items()}
    sensor_copy = dict(target_entry)
    linked_entries, main_entry = _prepare_dut_sensor_entries(sensor_copy, working_map)
    all_entries: List[Mapping[str, Any]] = linked_entries + [main_entry]
    configs: List[SensorConfig] = []
    for entry in all_entries:
        config = sensor_config_from_entry(entry)
        if config:
            configs.append(config)
    if not configs:
        raise ValueError("Не удалось преобразовать датчики")

    config_obj = ensure_unit_config_object(unit_id)
    existing_by_name = {sensor.name: sensor for sensor in config_obj.sensors}
    new_names: List[str] = []
    for cfg in configs:
        if cfg.name in existing_by_name:
            config_obj.sensors = [s for s in config_obj.sensors if s.name != cfg.name]
        config_obj.sensors.append(cfg)
        new_names.append(cfg.name)
    save_unit_config(config_obj)
    csv_paths = []
    for cfg in configs:
        path = export_calibration_csv(unit_id, cfg)
        if path:
            csv_paths.append(path)
    return new_names, csv_paths
