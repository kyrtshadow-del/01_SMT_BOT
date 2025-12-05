from __future__ import annotations

import uuid
import time
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes

SEARCH_GENERATION_KEY = "search_generation_token"
SEARCH_UNITS_PAGE_SIZE = 20
SEARCH_UNITS_LIMIT = 50

__all__ = [
    "SEARCH_GENERATION_KEY",
    "SEARCH_UNITS_PAGE_SIZE",
    "SEARCH_UNITS_LIMIT",
    "normalize_units_offset",
    "get_units",
    "set_units",
    "set_quick_mask",
    "quick_mask",
    "apply_quick_filter",
    "filtered_units",
    "list_header",
    "kb_units_page",
    "reset_search_context",
    "bump_search_generation",
    "current_search_generation",
]


def normalize_units_offset(total: int, offset: int) -> int:
    if total <= 0:
        return 0
    if offset < 0:
        return 0
    if offset >= total:
        last_page = ((total - 1) // SEARCH_UNITS_PAGE_SIZE) * SEARCH_UNITS_PAGE_SIZE
        return max(last_page, 0)
    return offset


def get_units(context: ContextTypes.DEFAULT_TYPE) -> List[Dict[str, Any]]:
    units = context.user_data.get("search_units")
    if isinstance(units, list):
        return [dict(u) for u in units]
    legacy = context.user_data.get("search_units_all")
    if isinstance(legacy, list):
        return [dict(u) for u in legacy]
    return []


def set_units(context: ContextTypes.DEFAULT_TYPE, units: Sequence[Dict[str, Any]]) -> None:
    stored = [dict(u) for u in units]
    context.user_data["search_units"] = stored
    context.user_data["search_units_all"] = list(stored)
    context.user_data["search_results"] = {str(u.get("id")): u for u in stored}
    context.user_data["search_units_offset"] = 0
    context.user_data["search_quick_mask"] = ""


def set_quick_mask(context: ContextTypes.DEFAULT_TYPE, mask: str) -> None:
    context.user_data["search_quick_mask"] = mask


def quick_mask(context: ContextTypes.DEFAULT_TYPE) -> str:
    return str(context.user_data.get("search_quick_mask") or "")


def apply_quick_filter(units: Iterable[Dict[str, Any]], mask: str) -> List[Dict[str, Any]]:
    if not mask:
        return [dict(u) for u in units]
    trimmed = mask.strip()
    if not trimmed:
        return [dict(u) for u in units]
    if len(trimmed) >= 3:
        return [dict(u) for u in units]
    needle = trimmed.casefold()
    filtered: List[Dict[str, Any]] = []
    for unit in units:
        name = str(unit.get("nm") or "")
        if needle in name.casefold():
            filtered.append(dict(unit))
    return filtered


def filtered_units(context: ContextTypes.DEFAULT_TYPE) -> List[Dict[str, Any]]:
    return apply_quick_filter(get_units(context), quick_mask(context))


def list_header(total: int, empty_hint: Optional[str] = None) -> str:
    lines = [f"Найдено: {total}"]
    if total:
        lines.append("Выберите объект:")
    else:
        lines.append(empty_hint or "Список пуст. Введите запрос для поиска.")
    return "\n\n".join(lines)


def kb_units_page(
    units: Sequence[Dict[str, Any]],
    offset: int = 0,
    *,
    status_marker: Optional[Callable[[Dict[str, Any]], Tuple[str, str]]] = None,
) -> InlineKeyboardMarkup:
    total = len(units)
    normalized_offset = normalize_units_offset(total, offset)
    slice_units = list(units)[normalized_offset : normalized_offset + SEARCH_UNITS_PAGE_SIZE]
    rows: List[List[InlineKeyboardButton]] = []
    for unit in slice_units:
        online_dot, sat_dot = ("", "")
        if status_marker is not None:
            try:
                online_dot, sat_dot = status_marker(unit)
            except Exception:
                online_dot, sat_dot = ("", "")
        name = str(unit.get("nm") or "")
        unit_id = unit.get("id")
        unit_id_str = str(unit_id) if unit_id is not None else ""
        label_id = unit_id_str or "—"
        button_text = f"{online_dot}{sat_dot} {name} — id {label_id}".strip()
        rows.append(
            [
                InlineKeyboardButton(
                    button_text,
                    callback_data=f"unit:{unit_id_str}",
                )
            ]
        )
    total_pages = max((total + SEARCH_UNITS_PAGE_SIZE - 1) // SEARCH_UNITS_PAGE_SIZE, 1)
    current_page = min(total_pages, (normalized_offset // SEARCH_UNITS_PAGE_SIZE) + 1)
    nav_row: List[InlineKeyboardButton] = []
    if total_pages > 1 and normalized_offset > 0:
        prev_offset = max(normalized_offset - SEARCH_UNITS_PAGE_SIZE, 0)
        nav_row.append(InlineKeyboardButton("◀️ Назад", callback_data=f"units:more:{prev_offset}"))
    nav_row.append(
        InlineKeyboardButton(f"стр. {current_page}/{total_pages}", callback_data="units:page")
    )
    if total_pages > 1 and normalized_offset + SEARCH_UNITS_PAGE_SIZE < total:
        nav_row.append(
            InlineKeyboardButton(
                "▶️ Далее", callback_data=f"units:more:{normalized_offset + SEARCH_UNITS_PAGE_SIZE}"
            )
        )
    rows.append(nav_row)
    rows.append([InlineKeyboardButton("❌ Отменить", callback_data="action:cancel")])
    return InlineKeyboardMarkup(rows)


def reset_search_context(
    context: ContextTypes.DEFAULT_TYPE,
    *,
    extra_keys: Optional[Iterable[str]] = None,
    after_reset: Optional[Callable[[ContextTypes.DEFAULT_TYPE], None]] = None,
) -> None:
    keys = [
        "search_units",
        "search_units_all",
        "search_units_offset",
        "search_quick_mask",
        "search_results",
    ]
    for key in keys:
        context.user_data.pop(key, None)
    if extra_keys:
        for key in extra_keys:
            context.user_data.pop(key, None)
    context.user_data.pop(SEARCH_GENERATION_KEY, None)
    if after_reset:
        after_reset(context)


def bump_search_generation(context: ContextTypes.DEFAULT_TYPE) -> str:
    token = uuid.uuid4().hex
    context.user_data[SEARCH_GENERATION_KEY] = token
    return token


def current_search_generation(context: ContextTypes.DEFAULT_TYPE) -> Optional[str]:
    token = context.user_data.get(SEARCH_GENERATION_KEY)
    return str(token) if token is not None else None
