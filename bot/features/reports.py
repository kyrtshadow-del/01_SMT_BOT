from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence, Tuple

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes

REPORT_PAGE_SIZE = 10
REPORT_START_LIST_SIZE = 5
REPORT_START_PROMPT = "Введите название отчёта (мин. 5 символов) или выберите из списка ниже."
REPORT_START_EMPTY_PROMPT = (
    "По текущей маске шаблоны не найдены. Введите название отчёта (мин. 5 символов)."
)

__all__ = [
    "REPORT_PAGE_SIZE",
    "REPORT_START_LIST_SIZE",
    "REPORT_START_PROMPT",
    "REPORT_START_EMPTY_PROMPT",
    "kb_report_candidates",
    "kb_report_periods",
    "kb_report_formats",
    "kb_report_wait",
    "kb_report_finish",
    "format_report_label",
    "match_saved_report_template",
    "build_report_start_prompt",
    "build_report_candidates_markup",
    "store_report_candidates",
    "describe_report_format",
]


def kb_report_candidates(
    candidates: Sequence[Tuple[int, Dict[str, Any]]],
    has_more: bool,
    next_offset: Optional[int],
) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    for idx, tpl in candidates:
        label = format_report_label(tpl)
        rows.append([InlineKeyboardButton(label, callback_data=f"report:choose:{idx}")])
    if has_more and next_offset is not None:
        rows.append([InlineKeyboardButton("▶ Далее 10", callback_data=f"report:more:{next_offset}")])
    rows.append([InlineKeyboardButton("↩️ К карточке", callback_data="back:unit")])
    return InlineKeyboardMarkup(rows)


def kb_report_periods() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🟢 Сегодня", callback_data="report:period:today"),
                InlineKeyboardButton("🟡 Вчера", callback_data="report:period:yesterday"),
            ],
            [
                InlineKeyboardButton("🔵 Последние 7 дней", callback_data="report:period:7days"),
            ],
            [
                InlineKeyboardButton("🟣 Последний месяц", callback_data="report:period:month"),
            ],
            [
                InlineKeyboardButton("🔁 Другой шаблон", callback_data="report:change"),
                InlineKeyboardButton("↩️ К карточке", callback_data="back:unit"),
            ],
        ]
    )


def kb_report_formats() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("📄 PDF", callback_data="report:format:pdf"),
                InlineKeyboardButton("📊 Excel (XLSX)", callback_data="report:format:excel"),
            ],
            [
                InlineKeyboardButton("⬅️ Назад", callback_data="report:back:period"),
                InlineKeyboardButton("↩️ К карточке", callback_data="back:unit"),
            ],
        ]
    )


def kb_report_wait() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="report:cancel")]])


def kb_report_finish(has_unit: bool) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    if has_unit:
        rows.append([InlineKeyboardButton("↩️ К карточке", callback_data="back:unit")])
    rows.append([InlineKeyboardButton("🔍 Найти новый объект", callback_data="back:find")])
    return InlineKeyboardMarkup(rows)


def format_report_label(tpl: Dict[str, Any]) -> str:
    name = (tpl.get("name") or "").strip() or f"ID {tpl.get('template_id')}"
    resource = (tpl.get("resource_name") or "").strip()
    if resource:
        return f"{name} ({resource})"
    return name


def describe_report_format(fmt: str) -> str:
    fmt_cf = (fmt or "").strip().lower()
    if fmt_cf == "pdf":
        return "PDF"
    if fmt_cf in ("excel", "xlsx"):
        return "Excel (XLSX)"
    if fmt_cf == "xls":
        return "Excel (XLS)"
    return fmt.upper() if fmt else "Неизвестный формат"


def _normalize_report_offset(total: int, offset: int) -> int:
    if total <= 0:
        return 0
    if offset < 0:
        return 0
    if offset >= total:
        last_page = ((total - 1) // REPORT_PAGE_SIZE) * REPORT_PAGE_SIZE
        return max(last_page, 0)
    return offset


def _report_candidates_visible(
    candidates: Sequence[Dict[str, Any]], offset: int
) -> Tuple[List[Tuple[int, Dict[str, Any]]], bool, Optional[int], int]:
    normalized_offset = _normalize_report_offset(len(candidates), offset)
    slice_items = candidates[normalized_offset : normalized_offset + REPORT_PAGE_SIZE]
    visible = list(enumerate(slice_items, start=normalized_offset))
    has_more = normalized_offset + REPORT_PAGE_SIZE < len(candidates)
    next_offset = normalized_offset + REPORT_PAGE_SIZE if has_more else None
    return visible, has_more, next_offset, normalized_offset


def store_report_candidates(
    context: ContextTypes.DEFAULT_TYPE,
    candidates: Sequence[Dict[str, Any]],
    text: str,
    offset: int = 0,
) -> InlineKeyboardMarkup:
    stored = [dict(tpl) for tpl in candidates]
    visible, has_more, next_offset, normalized_offset = _report_candidates_visible(stored, offset)
    context.user_data["report_matches"] = stored
    context.user_data["report_offset"] = normalized_offset
    context.user_data["report_candidates_text"] = text
    return kb_report_candidates(visible, has_more, next_offset)


def build_report_candidates_markup(
    context: ContextTypes.DEFAULT_TYPE, offset: Optional[int] = None
) -> InlineKeyboardMarkup:
    candidates = context.user_data.get("report_matches") or []
    try:
        current_offset = int(offset if offset is not None else context.user_data.get("report_offset", 0))
    except (TypeError, ValueError):
        current_offset = 0
    visible, has_more, next_offset, normalized_offset = _report_candidates_visible(candidates, current_offset)
    context.user_data["report_offset"] = normalized_offset
    return kb_report_candidates(visible, has_more, next_offset)


def _report_template_sort_key(tpl: Dict[str, Any]) -> Tuple[str, str, int]:
    name = (tpl.get("name") or "").casefold()
    resource_name = (tpl.get("resource_name") or "").casefold()
    try:
        template_id = int(tpl.get("template_id") or 0)
    except Exception:
        template_id = 0
    return name, resource_name, template_id


def _filter_report_templates(
    templates: Sequence[Dict[str, Any]], mask: str
) -> List[Dict[str, Any]]:
    q = (mask or "").strip()
    normalized = sorted(templates, key=_report_template_sort_key)
    if not q:
        return normalized
    q_cf = q.casefold()
    filtered = [tpl for tpl in normalized if q_cf in (tpl.get("name") or "").casefold()]
    return filtered


def build_report_start_prompt(
    context: ContextTypes.DEFAULT_TYPE,
    templates: Sequence[Dict[str, Any]],
    mask: str,
) -> Tuple[str, InlineKeyboardMarkup]:
    mask_val = (mask or "").strip()
    context.user_data["report_search_mask"] = mask_val
    filtered = _filter_report_templates(templates, mask_val)
    limited = filtered[:REPORT_START_LIST_SIZE]
    if limited:
        text = REPORT_START_PROMPT
    else:
        text = REPORT_START_EMPTY_PROMPT if mask_val else REPORT_START_PROMPT
    markup = store_report_candidates(context, limited, text, offset=0)
    return text, markup


def match_saved_report_template(
    saved_template: Optional[Dict[str, Any]],
    templates: Sequence[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    if not saved_template:
        return None
    try:
        saved_res = int(saved_template.get("resource_id"))
        saved_tpl = int(saved_template.get("template_id"))
    except (TypeError, ValueError):
        return None
    for tpl in templates:
        if not isinstance(tpl, dict):
            continue
        try:
            tpl_res = int(tpl.get("resource_id"))
            tpl_id = int(tpl.get("template_id"))
        except (TypeError, ValueError):
            continue
        if tpl_res == saved_res and tpl_id == saved_tpl:
            merged = dict(tpl)
            if not merged.get("name"):
                merged["name"] = saved_template.get("name")
            if not merged.get("resource_name"):
                merged["resource_name"] = saved_template.get("resource_name")
            return merged
    return None
