from __future__ import annotations

import html
import logging
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

from telegram import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message, Update
from telegram.ext import ContextTypes, ConversationHandler

PARAM_PROMPT_TEXT = (
    "🔎 Выберите параметр.\n"
    "Введите минимум 2 символа или воспользуйтесь списком ниже."
)

__all__ = [
    "build_param_prompt_keyboard",
    "kb_param_results",
    "delete_param_result_message",
    "store_param_result_payload",
    "clear_param_runtime",
    "ordered_param_names",
    "filter_params",
    "build_params_text",
    "send_prompt",
    "update_prompt",
    "restore_after_cmd_return",
    "ParamsHandlerDeps",
    "build_params_query_handler",
    "build_params_buttons_handler",
]

log = logging.getLogger(__name__)


def build_param_prompt_keyboard(matches: List[str]) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    if matches:
        chunk_size = 10
        columns: List[Tuple[int, List[str]]] = []
        for start in range(0, len(matches), chunk_size):
            columns.append((start, matches[start:start + chunk_size]))

        max_rows = max((len(col) for _, col in columns), default=0)
        for row_idx in range(max_rows):
            row_buttons: List[InlineKeyboardButton] = []
            for start_idx, col in columns:
                if row_idx >= len(col):
                    continue
                name = col[row_idx]
                global_idx = start_idx + row_idx
                row_buttons.append(InlineKeyboardButton(name, callback_data=f"param:show:{global_idx}"))
            if row_buttons:
                rows.append(row_buttons)

        rows.append([
            InlineKeyboardButton("📥 Выгрузить найденные параметры", callback_data="param:download")
        ])
    rows.append([InlineKeyboardButton("📋 Показать все параметры", callback_data="param:show_all")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="param:back_stats")])
    return InlineKeyboardMarkup(rows)


def kb_param_results() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("🔍 Объект", callback_data="param:find_other")],
            [InlineKeyboardButton("📡 Команда", callback_data="param:cmd")],
            [
                InlineKeyboardButton("⬅️ Назад", callback_data="param:back_query"),
                InlineKeyboardButton("✅ Готово", callback_data="param:finish"),
            ],
        ]
    )


async def delete_param_result_message(context: ContextTypes.DEFAULT_TYPE) -> None:
    meta = context.user_data.pop("param_result_msg", None)
    if not meta:
        return
    try:
        await context.bot.delete_message(chat_id=meta["chat_id"], message_id=meta["message_id"])
    except Exception as exc:  # pragma: no cover - defensive logging
        log.debug("delete_param_result_message: %s", exc)


def store_param_result_payload(
    context: ContextTypes.DEFAULT_TYPE, text: str, parse_mode: Optional[str]
) -> None:
    context.user_data["param_result_payload"] = {"text": text, "parse_mode": parse_mode}


def clear_param_runtime(context: ContextTypes.DEFAULT_TYPE) -> None:
    for key in ("param_current_matches", "param_last_query", "param_last_note"):
        context.user_data.pop(key, None)


def ordered_param_names(context: ContextTypes.DEFAULT_TYPE) -> List[str]:
    params_map: Dict[str, str] = context.user_data.get("params_map") or {}
    order: List[str] = context.user_data.get("params_order") or []
    seen = set()
    result: List[str] = []
    for name in order:
        if name in params_map and name not in seen:
            result.append(name)
            seen.add(name)
    for name in params_map.keys():
        if name not in seen:
            result.append(name)
            seen.add(name)
    return result


def filter_params(context: ContextTypes.DEFAULT_TYPE, query: str) -> List[str]:
    params_map: Dict[str, str] = context.user_data.get("params_map") or {}
    if not params_map:
        return []
    query_cf = query.casefold()
    names = ordered_param_names(context)
    return [name for name in names if query_cf in name.casefold()]


def build_params_text(context: ContextTypes.DEFAULT_TYPE, names: List[str]) -> Tuple[str, Optional[str]]:
    params_map: Dict[str, str] = context.user_data.get("params_map") or {}
    if not names:
        return "❌ Параметр не найден", None

    limited_names = names[:40]
    entries_raw = [f"{name} = {params_map.get(name, '')}" for name in limited_names]
    truncated = len(names) > len(limited_names)

    lines = [html.escape(entry) for entry in entries_raw]
    pre_block = "<pre>" + "\n".join(lines) + "</pre>"
    if truncated:
        pre_block = f"{pre_block}\n\n… Показаны первые 40 параметров из {len(names)}."

    return pre_block, "HTML"


async def send_prompt(
    update_or_q,
    context: ContextTypes.DEFAULT_TYPE,
    matches: List[str],
    note: Optional[str],
    *,
    delete_anchor_cb: Callable[[ContextTypes.DEFAULT_TYPE], Awaitable[None]],
    set_anchor_cb: Callable[[Any, ContextTypes.DEFAULT_TYPE], Awaitable[None]],
    mode_value: str,
) -> None:
    text = PARAM_PROMPT_TEXT
    if note:
        text = f"{text}\n\n{note}"
    markup = build_param_prompt_keyboard(matches)

    await delete_anchor_cb(context)
    message = getattr(update_or_q, "message", None)
    if not message:
        return
    msg = await context.bot.send_message(
        chat_id=message.chat_id,
        text=text,
        reply_markup=markup,
        parse_mode=None,
    )
    await set_anchor_cb(msg, context)
    context.user_data["param_current_matches"] = list(matches)
    context.user_data["param_last_note"] = note or ""
    context.user_data["mode"] = mode_value


async def update_prompt(
    context: ContextTypes.DEFAULT_TYPE,
    matches: List[str],
    note: Optional[str],
    update_obj,
    *,
    edit_anchor_cb: Callable[[ContextTypes.DEFAULT_TYPE, str, Optional[InlineKeyboardMarkup], Optional[str]], Awaitable[None]],
    send_prompt_cb: Callable[[Any, ContextTypes.DEFAULT_TYPE, List[str], Optional[str]], Awaitable[None]],
    mode_value: str,
) -> None:
    text = PARAM_PROMPT_TEXT
    if note:
        text = f"{text}\n\n{note}"
    markup = build_param_prompt_keyboard(matches)
    if "anchor" not in context.user_data:
        if update_obj is not None:
            await send_prompt_cb(update_obj, context, matches, note)
        return
    await edit_anchor_cb(context, text, markup, parse_mode=None)
    context.user_data["param_current_matches"] = list(matches)
    context.user_data["param_last_note"] = note or ""
    context.user_data["mode"] = mode_value


async def restore_after_cmd_return(
    update_or_q,
    context: ContextTypes.DEFAULT_TYPE,
    *,
    mode_value: str,
    state_param_query: int,
    send_prompt_cb: Callable[[Any, ContextTypes.DEFAULT_TYPE, List[str], Optional[str]], Awaitable[None]],
) -> int:
    snapshot = context.user_data.pop("cmd_return_runtime", None) or {}

    matches_snapshot = snapshot.get("matches")
    if matches_snapshot is not None:
        context.user_data["param_current_matches"] = list(matches_snapshot)
    else:
        context.user_data.pop("param_current_matches", None)

    last_query = snapshot.get("last_query")
    if last_query is not None:
        context.user_data["param_last_query"] = last_query
    else:
        context.user_data.pop("param_last_query", None)

    last_note = snapshot.get("last_note")
    if last_note is not None:
        context.user_data["param_last_note"] = last_note
    else:
        context.user_data.pop("param_last_note", None)

    payload = context.user_data.pop("cmd_return_payload", None)
    context.user_data.pop("cmd_return", None)

    if isinstance(payload, dict) and payload.get("text") is not None:
        text = payload.get("text", "")
        parse_mode = payload.get("parse_mode")
        if update_or_q and getattr(update_or_q, "message", None):
            msg = await update_or_q.message.reply_text(
                text,
                reply_markup=kb_param_results(),
                parse_mode=parse_mode,
            )
            context.user_data["param_result_msg"] = {"chat_id": msg.chat_id, "message_id": msg.message_id}
            context.user_data["param_result_payload"] = dict(payload)
        context.user_data["mode"] = mode_value
        return state_param_query

    matches = context.user_data.get("param_current_matches") or ordered_param_names(context)
    note = context.user_data.get("param_last_note") or None
    await send_prompt_cb(update_or_q, context, matches, note)
    return state_param_query


@dataclass
class ParamsHandlerDeps:
    ensure_authorized: Callable[[Update, ContextTypes.DEFAULT_TYPE], Awaitable[bool]]
    safe_answer_callback: Callable[[CallbackQuery], Awaitable[None]]
    send_prompt_cb: Callable[[Any, ContextTypes.DEFAULT_TYPE, List[str], Optional[str]], Awaitable[None]]
    update_prompt_cb: Callable[[ContextTypes.DEFAULT_TYPE, List[str], Optional[str], Any], Awaitable[None]]
    delete_anchor_cb: Callable[[ContextTypes.DEFAULT_TYPE], Awaitable[None]]
    set_anchor_cb: Callable[[Message, ContextTypes.DEFAULT_TYPE], Awaitable[None]]
    delete_stats_message: Callable[[ContextTypes.DEFAULT_TYPE], Awaitable[None]]
    clear_stats_buttons: Callable[[ContextTypes.DEFAULT_TYPE], Awaitable[None]]
    remove_reply_markup_safe: Callable[[Message], Awaitable[None]]
    remove_reply_markup_by_meta: Callable[[ContextTypes.DEFAULT_TYPE, Optional[Dict[str, Any]]], Awaitable[None]]
    kb_cancel: Callable[[], Any]
    kb_cmd_entry_controls: Callable[[], Any]
    reply_menu: Callable[..., Any]
    require_wialon_client: Callable[[Update, ContextTypes.DEFAULT_TYPE, str], Awaitable[Any]]
    pipeline_card_enabled: Callable[[], bool]
    show_stats_then_actions: Callable[[Any, ContextTypes.DEFAULT_TYPE, int, str, Any, bool], Awaitable[int]]
    clear_export_job: Callable[[ContextTypes.DEFAULT_TYPE], None]
    state_menu: int
    state_param_query: int
    state_auth_wait_token: int
    state_find_query: int
    state_cmd_value: int
    mode_params: str
    mode_none: str
    mode_cmd: str


def build_params_query_handler(deps: ParamsHandlerDeps) -> Callable[[Update, ContextTypes.DEFAULT_TYPE], Awaitable[int]]:
    async def handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        if context.user_data.get("mode") != deps.mode_params:
            return deps.state_menu
        if not await deps.ensure_authorized(update, context):
            return deps.state_auth_wait_token

        if not update.message:
            return deps.state_param_query

        query = (update.message.text or "").strip()
        params_map: Dict[str, str] = context.user_data.get("params_map") or {}
        if not params_map:
            await deps.update_prompt_cb(context, [], "Нет данных о параметрах", update)
            return deps.state_param_query

        if len(query) < 2:
            current_matches = context.user_data.get("param_current_matches") or []
            await deps.update_prompt_cb(context, current_matches, "Введите минимум 2 символа для поиска", update)
            return deps.state_param_query

        matches = filter_params(context, query)
        context.user_data["param_last_query"] = query
        note = f"Найдено {len(matches)} параметров." if matches else "❌ Параметр не найден"

        exact_matches = [name for name in matches if name.casefold() == query.casefold()]
        if exact_matches:
            await deps.update_prompt_cb(context, matches, note, update)
            params_map = context.user_data.get("params_map") or {}
            name = exact_matches[0]
            value = params_map.get(name, "")
            await delete_param_result_message(context)
            text = f"{name} = {value}"
            msg = await update.message.reply_text(text, reply_markup=kb_param_results(), parse_mode=None)
            context.user_data["param_result_msg"] = {"chat_id": msg.chat_id, "message_id": msg.message_id}
            store_param_result_payload(context, text, None)
            return deps.state_param_query

        if matches:
            await deps.update_prompt_cb(context, matches, note, update)
        else:
            await deps.update_prompt_cb(context, [], note, update)
        return deps.state_param_query

    return handler


def build_params_buttons_handler(
    deps: ParamsHandlerDeps,
) -> Callable[[Update, ContextTypes.DEFAULT_TYPE], Awaitable[int]]:
    async def handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        q = update.callback_query
        if not q:
            return deps.state_param_query
        if not await deps.ensure_authorized(update, context):
            return deps.state_auth_wait_token
        await deps.safe_answer_callback(q)

        data = q.data or ""
        if not data.startswith("param:"):
            return deps.state_param_query

        command = data.split(":", 1)[1]
        parts = command.split(":")
        action = parts[0]

        if action == "show":
            try:
                idx = int(parts[1])
            except (IndexError, ValueError):
                return deps.state_param_query
            matches: List[str] = context.user_data.get("param_current_matches") or []
            if idx < 0 or idx >= len(matches):
                return deps.state_param_query
            name = matches[idx]
            params_map: Dict[str, str] = context.user_data.get("params_map") or {}
            value = params_map.get(name, "")
            await delete_param_result_message(context)
            await deps.delete_anchor_cb(context)
            text = f"{name} = {value}"
            if not q.message:
                return deps.state_param_query
            msg = await q.message.reply_text(text, reply_markup=kb_param_results(), parse_mode=None)
            context.user_data["param_result_msg"] = {"chat_id": msg.chat_id, "message_id": msg.message_id}
            store_param_result_payload(context, text, None)
            context.user_data["mode"] = deps.mode_params
            return deps.state_param_query

        if action == "download":
            matches = context.user_data.get("param_current_matches") or []
            if not matches:
                await deps.update_prompt_cb(context, [], "❌ Параметр не найден", q)
                return deps.state_param_query
            text, parse_mode = build_params_text(context, matches)
            await delete_param_result_message(context)
            await deps.delete_anchor_cb(context)
            if not q.message:
                return deps.state_param_query
            msg = await q.message.reply_text(text, reply_markup=kb_param_results(), parse_mode=parse_mode)
            context.user_data["param_result_msg"] = {"chat_id": msg.chat_id, "message_id": msg.message_id}
            store_param_result_payload(context, text, parse_mode)
            context.user_data["mode"] = deps.mode_params
            return deps.state_param_query

        if action == "show_all":
            names = ordered_param_names(context)
            if names:
                text, parse_mode = build_params_text(context, names)
            else:
                text, parse_mode = "Нет данных о параметрах", None
            await delete_param_result_message(context)
            await deps.delete_anchor_cb(context)
            if not q.message:
                return deps.state_param_query
            msg = await q.message.reply_text(text, reply_markup=kb_param_results(), parse_mode=parse_mode)
            context.user_data["param_result_msg"] = {"chat_id": msg.chat_id, "message_id": msg.message_id}
            store_param_result_payload(context, text, parse_mode)
            context.user_data["mode"] = deps.mode_params
            return deps.state_param_query

        if action == "back_query":
            await delete_param_result_message(context)
            context.user_data.pop("param_result_payload", None)
            matches = context.user_data.get("param_current_matches") or ordered_param_names(context)
            note = context.user_data.get("param_last_note") or None
            await deps.send_prompt_cb(q, context, matches, note)
            return deps.state_param_query

        if action == "back_stats":
            await delete_param_result_message(context)
            context.user_data.pop("param_result_payload", None)
            await deps.delete_anchor_cb(context)
            clear_param_runtime(context)
            context.user_data["mode"] = deps.mode_none
            unit = context.user_data.get("chosen_unit")
            if not unit:
                return ConversationHandler.END
            try:
                unit_id = int(unit["id"])
            except (TypeError, ValueError):
                return ConversationHandler.END
            unit_name = unit.get("nm") or f"id {unit_id}"
            client = None
            if not deps.pipeline_card_enabled():
                client = await deps.require_wialon_client(update, context, "Чтобы просматривать объект, авторизуйтесь.")
                if not client:
                    return deps.state_auth_wait_token
            return await deps.show_stats_then_actions(q, context, unit_id, unit_name, client, True)

        if action == "finish":
            if q.message:
                await deps.remove_reply_markup_safe(q.message)
            await deps.remove_reply_markup_by_meta(context, context.user_data.pop("param_result_msg", None))
            context.user_data.pop("param_result_payload", None)
            context.user_data.pop("params_map", None)
            context.user_data.pop("params_order", None)
            context.user_data.pop("param_current_matches", None)
            context.user_data.pop("param_last_note", None)
            context.user_data.pop("param_last_query", None)
            context.user_data.pop("cmd_return", None)
            context.user_data.pop("cmd_return_payload", None)
            context.user_data.pop("cmd_return_runtime", None)
            clear_param_runtime(context)
            context.user_data["mode"] = deps.mode_none
            await deps.delete_anchor_cb(context)

            unit = context.user_data.get("chosen_unit")
            if not unit:
                if q.message:
                    await q.message.reply_text(
                        "Диалог завершён.",
                        reply_markup=deps.reply_menu(chat_id=q.message.chat_id),
                    )
                return ConversationHandler.END

            try:
                unit_id = int(unit.get("id"))
            except (TypeError, ValueError):
                if q.message:
                    await q.message.reply_text(
                        "Диалог завершён.",
                        reply_markup=deps.reply_menu(chat_id=q.message.chat_id),
                    )
                return ConversationHandler.END

            unit_name = unit.get("nm") or f"id {unit_id}"
            client = None
            if not deps.pipeline_card_enabled():
                client = await deps.require_wialon_client(
                    update,
                    context,
                    "Чтобы просматривать объект, авторизуйтесь.",
                )
                if not client:
                    return deps.state_auth_wait_token

            await deps.delete_stats_message(context)
            return await deps.show_stats_then_actions(
                q,
                context,
                unit_id,
                unit_name,
                client,
                True,
            )

        if action == "find_other":
            await delete_param_result_message(context)
            await deps.delete_stats_message(context)
            deps.clear_export_job(context, cancel=True)
            context.user_data.clear()
            if not q.message:
                return deps.state_find_query
            msg = await q.message.reply_text(
                "Введите минимум 3 символа для поиска объекта (например 676 или 676мт).",
                reply_markup=deps.kb_cancel(),
            )
            await deps.set_anchor_cb(msg, context)
            return deps.state_find_query

        if action == "cmd":
            context.user_data["mode"] = deps.mode_cmd
            context.user_data["cmd_return"] = "params"
            payload_copy = context.user_data.get("param_result_payload")
            if isinstance(payload_copy, dict):
                context.user_data["cmd_return_payload"] = dict(payload_copy)
            else:
                context.user_data["cmd_return_payload"] = payload_copy
            context.user_data.pop("param_result_payload", None)
            runtime_snapshot = {
                "matches": list(context.user_data.get("param_current_matches") or []),
                "last_query": context.user_data.get("param_last_query"),
                "last_note": context.user_data.get("param_last_note"),
            }
            context.user_data["cmd_return_runtime"] = runtime_snapshot
            await delete_param_result_message(context)
            clear_param_runtime(context)
            await deps.clear_stats_buttons(context)
            unit = context.user_data.get("chosen_unit")
            if not unit:
                return ConversationHandler.END
            await deps.delete_anchor_cb(context)
            if not q.message:
                return deps.state_cmd_value
            msg = await q.message.reply_text(
                f"Выбран объект: {unit['nm']} — id {unit['id']}\\n\\n"
                f"Напишите TCP-команду для устройства.\\n"
                f"_Команда отправляется только при активном TCP-соединении._",
                reply_markup=deps.kb_cmd_entry_controls(),
            )
            await deps.set_anchor_cb(msg, context)
            return deps.state_cmd_value

        return deps.state_param_query

    return handler
