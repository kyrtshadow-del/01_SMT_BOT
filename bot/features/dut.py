from __future__ import annotations

import html
import logging
import math
import re
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, List, Mapping, Optional, Sequence, Set, Tuple

from telegram import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes

from pipeline.config.unit_config import CalibrationPoint, SensorCalibration, SensorConfig, UnitConfig

__all__ = [
    "DutCalibrationViewDeps",
    "DutCalibrationRefreshDeps",
    "build_show_calibration_handler",
    "build_refresh_calibration_handler",
]


def _pick_custom_sensor_for_calibration(config: UnitConfig) -> Optional[SensorConfig]:
    customs = [
        sensor
        for sensor in config.sensors
        if (sensor.type or "").strip().lower() in {"custom", "произвольный датчик"}
    ]
    customs.sort(key=lambda s: (s.name or "").casefold())
    return customs[0] if customs else None


def _fmt_cal_number(value: float) -> str:
    try:
        return f"{value:.3f}".rstrip("0").rstrip(".")
    except Exception:
        return str(value)


@dataclass
class DutCalibrationViewDeps:
    safe_answer_callback: Callable[[CallbackQuery, Optional[str], bool], Awaitable[None]]
    load_unit_config: Callable[[int], Optional[UnitConfig]]
    state_wait_unit: int
    log: logging.Logger
    dut_log: logging.Logger


def build_show_calibration_handler(
    deps: DutCalibrationViewDeps,
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
        context.user_data["last_dut_unit"] = unit_id
        context.user_data["last_dut_name"] = unit.get("nm")
        config = deps.load_unit_config(unit_id)
        sensor = _pick_custom_sensor_for_calibration(config) if config else None
        if not sensor:
            if q.message:
                await q.message.reply_text("Тарировка недоступна: произвольные датчики не найдены.")
            return deps.state_wait_unit
        context.user_data["last_dut_sensor"] = sensor.name
        deps.log.info("dut_cal_local: unit=%s sensor=%s", unit_id, sensor.name)
        deps.dut_log.info(
            "local_view: unit=%s sensor=%s type=%s sensor_id=%s points=%s",
            unit_id,
            sensor.name,
            sensor.type,
            getattr(sensor, "sensor_id", None),
            len(sensor.calibration.points) if sensor.calibration and sensor.calibration.points else 0,
        )
        calibration = sensor.calibration
        header = [
            f"📈 Тарировка — <b>{html.escape(sensor.name)}</b>",
            f"Объект: {html.escape(unit_name)}",
        ]
        if sensor.type:
            header.append(f"Тип: {html.escape(sensor.type)}")
        if sensor.units:
            header.append(f"Единицы: {html.escape(sensor.units)}")
        source = None
        if isinstance(sensor.parameters, Mapping):
            source = sensor.parameters.get("p") or sensor.parameters.get("param") or sensor.parameters.get("src")
        if isinstance(source, str) and source.strip():
            header.append(f"Источник: <code>{html.escape(source.strip())}</code>")

        lines = header + [""]
        if calibration and calibration.points:
            lines.append("Точки тарировки:")
            for idx, point in enumerate(calibration.points, start=1):
                lines.append(f"{idx:>3}. {_fmt_cal_number(point.raw)} → {_fmt_cal_number(point.value)}")
            lines.append("")
            lines.append(f"Всего точек: {len(calibration.points)}")
        else:
            lines.append("Нет сохранённой тарировки для этого датчика.")
        markup = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("🔄 Обновить из СМТ", callback_data="stats:dutcal_refresh"),
                    InlineKeyboardButton("⬅️ К карточке", callback_data="back:unit"),
                    InlineKeyboardButton("🔍 Другой объект", callback_data="back:find"),
                ]
            ]
        )
        if q.message:
            await q.message.reply_text("\n".join(lines), parse_mode="HTML", reply_markup=markup)
        deps.log.info(
            "dut_cal_local: response sent unit=%s points=%s",
            unit_id,
            len(calibration.points) if calibration and calibration.points else 0,
        )
        return deps.state_wait_unit

    return handler


def _extract_sensor_refs(expression: str) -> List[str]:
    pattern = re.compile(r"\[([^\]]+)\]")
    return [match.strip() for match in pattern.findall(expression)]


@dataclass
class DutCalibrationRefreshDeps:
    safe_answer_callback: Callable[[CallbackQuery, Optional[str], bool], Awaitable[None]]
    ensure_unit_config_object: Callable[[int], UnitConfig]
    save_unit_config: Callable[[UnitConfig], None]
    clear_unit_config_cache: Callable[[], None]
    wialon_for_chat: Callable[[Optional[int]], Any]
    not_authorized_exc: type
    user_blocked_exc: type
    run_blocking: Callable[..., Awaitable[Any]]
    log: logging.Logger
    dut_log: logging.Logger
    state_wait_unit: int
    show_calibration_handler: Callable[[CallbackQuery, ContextTypes.DEFAULT_TYPE], Awaitable[int]]


def build_refresh_calibration_handler(
    deps: DutCalibrationRefreshDeps,
) -> Callable[[CallbackQuery, ContextTypes.DEFAULT_TYPE], Awaitable[int]]:
    async def handler(q: CallbackQuery, context: ContextTypes.DEFAULT_TYPE) -> int:
        unit_id = context.user_data.get("last_dut_unit")
        sensor_name = context.user_data.get("last_dut_sensor")
        if not unit_id or not sensor_name:
            await deps.safe_answer_callback(q, "Сначала откройте тарировку через кнопку выше.", show_alert=True)
            return deps.state_wait_unit
        unit_name = context.user_data.get("last_dut_name") or f"id {unit_id}"
        config = deps.ensure_unit_config_object(int(unit_id))
        local_sensor = None
        for sensor in config.sensors:
            if sensor.name == sensor_name:
                local_sensor = sensor
                break
        if local_sensor is None:
            if q.message:
                await q.message.reply_text("Локальный конфиг не содержит этот датчик. Добавьте его заново.")
            return deps.state_wait_unit
        await deps.safe_answer_callback(q)
        deps.log.info("dut_cal_refresh: start unit=%s sensor=%s", unit_id, sensor_name)
        deps.dut_log.info(
            "refresh_start: unit=%s sensor=%s sensor_id=%s params=%s expr=%s",
            unit_id,
            sensor_name,
            getattr(local_sensor, "sensor_id", None),
            dict(local_sensor.parameters) if isinstance(local_sensor.parameters, Mapping) else local_sensor.parameters,
            local_sensor.expression,
        )
        chat = q.message.chat if q.message else None
        chat_id = chat.id if chat else None
        try:
            client = deps.wialon_for_chat(chat_id)
        except deps.not_authorized_exc:
            if q.message:
                await q.message.reply_text("Для обновления тарировки авторизуйтесь и отправьте токен Wialon.")
            return deps.state_wait_unit
        except deps.user_blocked_exc:
            if q.message:
                await q.message.reply_text("Доступ заблокирован. Обратитесь к администратору.")
            return deps.state_wait_unit

        try:
            sensors_api = await deps.run_blocking(client.get_unit_sensors_detailed, int(unit_id))
            deps.log.info("dut_cal_refresh: api sensors fetched unit=%s count=%s", unit_id, len(sensors_api))
            deps.dut_log.info("api_fetch: unit=%s count=%s", unit_id, len(sensors_api))
        except Exception as exc:
            deps.log.warning("dut_cal_refresh: api fetch failed unit=%s err=%s", unit_id, exc)
            if q.message:
                await q.message.reply_text(f"Не удалось запросить датчики: {exc}")
            deps.dut_log.info("api_error: unit=%s err=%s", unit_id, exc)
            return deps.state_wait_unit

        def _normalize_key(value: Any) -> Optional[str]:
            if value is None:
                return None
            text = str(value).strip()
            if not text:
                return None
            return text.casefold()

        sensors_by_id: Dict[int, Dict[str, Any]] = {}
        sensors_by_name: Dict[str, Dict[str, Any]] = {}
        sensors_by_param: Dict[str, List[Dict[str, Any]]] = {}
        for entry in sensors_api:
            entry_copy = dict(entry)
            try:
                sensor_id_val = int(entry_copy.get("id"))
            except Exception:
                sensor_id_val = None
            if sensor_id_val is not None:
                sensors_by_id[sensor_id_val] = entry_copy
            name_key = _normalize_key(entry_copy.get("n") or entry_copy.get("name"))
            if name_key:
                sensors_by_name[name_key] = entry_copy
            param_key = _normalize_key(entry_copy.get("p") or entry_copy.get("param") or entry_copy.get("src"))
            if param_key:
                sensors_by_param.setdefault(param_key, []).append(entry_copy)

        def _match_sensor_by_id() -> Optional[Dict[str, Any]]:
            try:
                sid = int(getattr(local_sensor, "sensor_id", None) or 0)
            except Exception:
                sid = 0
            if sid and sid in sensors_by_id:
                deps.log.info("dut_cal_refresh: matched by id unit=%s sensor_id=%s", unit_id, sid)
                deps.dut_log.info("match_by_id: unit=%s sensor_id=%s", unit_id, sid)
                return sensors_by_id[sid]
            return None

        def _collect_local_param_keys() -> List[str]:
            keys: List[str] = []
            if isinstance(local_sensor.parameters, Mapping):
                for field in ("p", "param", "src"):
                    value = local_sensor.parameters.get(field)
                    if isinstance(value, str):
                        cleaned = value.strip()
                        if cleaned:
                            keys.append(cleaned)
            expr = local_sensor.expression
            if isinstance(expr, str) and expr.strip():
                keys.append(expr.strip())
            return keys

        def _match_sensor(target_name: str) -> Optional[Dict[str, Any]]:
            key = _normalize_key(target_name)
            if not key:
                return None
            return sensors_by_name.get(key)

        def _extract_rows(entry: Mapping[str, Any], visited_names: Set[str], visited_params: Set[str]) -> List[Dict[str, Any]]:
            rows = entry.get("tbl") or entry.get("calibration")
            if isinstance(rows, dict):
                rows_iter = rows.values()
            elif isinstance(rows, list):
                rows_iter = rows
            else:
                rows_iter = []
            collected = [dict(row) for row in rows_iter if isinstance(row, Mapping)]
            if collected:
                return collected

            expr = entry.get("p") or entry.get("param") or entry.get("expression")
            expr_str = str(expr or "").strip()
            ref_names = _extract_sensor_refs(expr_str) if expr_str else []
            for ref in ref_names:
                key = _normalize_key(ref)
                if key and key not in visited_names:
                    visited_names.add(key)
                    linked = sensors_by_name.get(key)
                    if linked:
                        candidate = _extract_rows(linked, visited_names, visited_params)
                        if candidate:
                            return candidate

            param_key = _normalize_key(expr_str or entry.get("src"))
            if param_key and param_key not in visited_params:
                visited_params.add(param_key)
                for linked in sensors_by_param.get(param_key, []):
                    if linked is entry:
                        continue
                    candidate = _extract_rows(linked, visited_names, visited_params)
                    if candidate:
                        return candidate
            return []

        api_sensor = _match_sensor_by_id()
        if not api_sensor:
            api_sensor = _match_sensor(sensor_name)
        if not api_sensor:
            for candidate_expr in _collect_local_param_keys():
                key = _normalize_key(candidate_expr)
                if not key:
                    continue
                candidates = sensors_by_param.get(key)
                if candidates:
                    api_sensor = candidates[0]
                    deps.log.info(
                        "dut_cal_refresh: fallback sensor by param unit=%s sensor=%s expr=%s",
                        unit_id,
                        sensor_name,
                        candidate_expr,
                    )
                    deps.dut_log.info(
                        "match_by_param: unit=%s sensor=%s expr=%s candidate=%s",
                        unit_id,
                        sensor_name,
                        candidate_expr,
                        api_sensor.get("n"),
                    )
                    break
        if not api_sensor:
            if q.message:
                await q.message.reply_text("СМТ не вернула датчик с таким именем. Проверьте настройки.")
            deps.dut_log.info("match_failed: unit=%s sensor=%s reason=no_sensor", unit_id, sensor_name)
            return deps.state_wait_unit

        rows = _extract_rows(
            api_sensor,
            {sensor_name.strip().casefold()},
            set(),
        )
        deps.dut_log.info(
            "rows_extracted: unit=%s sensor=%s rows=%s sample=%s",
            unit_id,
            sensor_name,
            len(rows),
            rows[:3],
        )
        points: List[CalibrationPoint] = []
        for row in rows:
            if not isinstance(row, Mapping):
                continue
            x = row.get("x")
            y = row.get("y")
            if y is None:
                a = row.get("a")
                b = row.get("b")
                try:
                    if x is not None and a is not None and b is not None:
                        y = float(a) * float(x) + float(b)
                except (TypeError, ValueError):
                    y = None
            if x is None or y is None:
                deps.dut_log.info(
                    "row_skipped: unit=%s sensor=%s row=%s x=%s y=%s",
                    unit_id,
                    sensor_name,
                    row,
                    x,
                    y,
                )
                continue
            try:
                raw_val = float(x)
                out_val = float(y)
                if math.isnan(raw_val) or math.isinf(raw_val) or math.isnan(out_val) or math.isinf(out_val):
                    deps.dut_log.info(
                        "row_invalid: unit=%s sensor=%s row=%s raw=%s val=%s",
                        unit_id,
                        sensor_name,
                        row,
                        raw_val,
                        out_val,
                    )
                    continue
                points.append(CalibrationPoint(raw=raw_val, value=out_val))
            except Exception as exc:
                deps.dut_log.info(
                    "row_error: unit=%s sensor=%s row=%s err=%s",
                    unit_id,
                    sensor_name,
                    row,
                    exc,
                )
                continue

        deps.dut_log.info("points_parsed: unit=%s sensor=%s count=%s", unit_id, sensor_name, len(points))
        if not points:
            if q.message:
                await q.message.reply_text("СМТ вернула датчик без таблицы тарировки.")
            deps.log.info("dut_cal_refresh: no calibration rows unit=%s sensor=%s", unit_id, sensor_name)
            deps.dut_log.info("no_points: unit=%s sensor=%s", unit_id, sensor_name)
            return deps.state_wait_unit

        local_sensor.calibration = local_sensor.calibration or SensorCalibration(sensor_id=local_sensor.sensor_id or 0)
        local_sensor.calibration.points = points
        deps.save_unit_config(config)
        deps.clear_unit_config_cache()
        deps.log.info("dut_cal_refresh: saved unit=%s sensor=%s points=%s", unit_id, sensor_name, len(points))
        deps.dut_log.info("saved: unit=%s sensor=%s points=%s", unit_id, sensor_name, len(points))
        if q.message:
            await q.message.reply_text(f"Тарировка обновлена. Точек: {len(points)}")
        return await deps.show_calibration_handler(q, context)

    return handler
