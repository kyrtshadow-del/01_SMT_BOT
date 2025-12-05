import asyncio
import csv
import functools
import html
import json
import logging
import os
import sqlite3
import tempfile
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple, TYPE_CHECKING

from telegram import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.error import RetryAfter, TelegramError
from telegram.ext import ContextTypes

from concurrent.futures import ThreadPoolExecutor

if TYPE_CHECKING:
    from bot_new import TokenStore
else:  # pragma: no cover - used only for type hints
    TokenStore = Any  # type: ignore


HTTP_POOL_MAX = int(os.getenv("HTTP_POOL_MAX", "64"))
HTTP_TIMEOUT_CONN = float(os.getenv("HTTP_TIMEOUT_CONN", "5.0"))
HTTP_TIMEOUT_READ = float(os.getenv("HTTP_TIMEOUT_READ", "20.0"))

EXECUTOR_WORKERS = int(os.getenv("EXECUTOR_WORKERS", "8"))

# Кэш геокодинга
GEOCODE_CACHE_MAX = int(os.getenv("GEOCODE_CACHE_MAX", "512"))
GEOCODE_TTL_SEC_BUCKET = int(os.getenv("GEOCODE_TTL_SEC_BUCKET", "300"))

# Кэш шаблонов отчётов
REPORT_TEMPLATES_TTL = int(os.getenv("REPORT_TEMPLATES_TTL", "900"))

ADMIN_RUN_MIGRATIONS = os.getenv("ADMIN_RUN_MIGRATIONS", "1") == "1"

try:
    from bot_new import EXECUTOR
except Exception:  # pragma: no cover - fallback for standalone usage
    EXECUTOR = ThreadPoolExecutor(max_workers=EXECUTOR_WORKERS)


log = logging.getLogger(__name__)


async def _db_run(fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    loop = asyncio.get_running_loop()
    bound = functools.partial(fn, *args, **kwargs)
    return await loop.run_in_executor(EXECUTOR, bound)


def _run_admin_migrations(store: "TokenStore") -> None:
    if not ADMIN_RUN_MIGRATIONS:
        return
    try:
        conn = sqlite3.connect(str(store.db_path))
        cur = conn.cursor()
        cur.execute("CREATE INDEX IF NOT EXISTS ix_known_users_updated ON known_users(updated_at)")
        cur.execute("CREATE INDEX IF NOT EXISTS ix_user_tokens_chat ON user_tokens(chat_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS ix_user_tokens_updated ON user_tokens(updated_at)")
        cur.execute("CREATE INDEX IF NOT EXISTS ix_blocked_users_chat ON blocked_users(chat_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS ix_known_users_username_lc ON known_users(lower(username))")
        try:
            cur.execute("ALTER TABLE user_tokens ADD COLUMN token_tail TEXT")
        except sqlite3.OperationalError as exc:
            if "duplicate column" not in str(exc).lower():
                raise
        cur.execute("CREATE INDEX IF NOT EXISTS ix_user_tokens_tail ON user_tokens(token_tail)")
        cur.execute(
            """
            CREATE TRIGGER IF NOT EXISTS trg_user_tokens_tail_ins AFTER INSERT ON user_tokens
            BEGIN
              UPDATE user_tokens SET token_tail = substr(NEW.token, length(NEW.token)-3, 4) WHERE rowid = NEW.rowid;
            END
            """
        )
        cur.execute(
            """
            CREATE TRIGGER IF NOT EXISTS trg_user_tokens_tail_upd AFTER UPDATE OF token ON user_tokens
            BEGIN
              UPDATE user_tokens SET token_tail = substr(NEW.token, length(NEW.token)-3, 4) WHERE rowid = NEW.rowid;
            END
            """
        )
        cur.execute(
            """
            UPDATE user_tokens
            SET token_tail = substr(token, length(token)-3, 4)
            WHERE token IS NOT NULL
            """
        )
        conn.commit()
    finally:
        try:
            conn.close()
        except Exception:
            pass


FILTER_MAP = {
    "fA": "all",
    "fT": "has_token",
    "fN": "no_token",
    "fB": "blocked",
}


@dataclass
class AdminUser:
    chat_id: int
    username: Optional[str]
    full_name: str
    user_id: Optional[int]
    has_token: bool
    token_mask: str
    token_updated_at: Optional[str]
    token_created_at: Optional[str]
    blocked: bool
    note: Optional[str]
    note_updated_at: Optional[str]
    profile_updated_at: Optional[str]


class AdminPanel:
    PAGE_SIZE = 6
    AUDIT_PAGE_SIZE = 12

    def __init__(
        self,
        token_store: TokenStore,
        verify_token_func: Callable[[str], Tuple[bool, Optional[str]]],
        prompt_sender: Callable[[int], Awaitable[None]],
        admin_whitelist: Optional[List[int]] = None,
    ) -> None:
        self.store = token_store
        self.verify_token_func = verify_token_func
        self.prompt_sender = prompt_sender
        self.whitelist = set(int(x) for x in (admin_whitelist or []))
        _run_admin_migrations(self.store)

    # -------------------------- helpers --------------------------
    def _panel_data(self, context: ContextTypes.DEFAULT_TYPE) -> Dict[str, Any]:
        return context.chat_data.setdefault("admin_panel", {})

    async def _log_action(
        self,
        admin_chat_id: int,
        target_chat_id: Optional[int],
        action: str,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        await _db_run(self.store.log_admin_action, admin_chat_id, target_chat_id, action, details or {})

    def _mask_token(self, token: Optional[str]) -> str:
        if not token:
            return "—"
        tail = token[-4:]
        return f"{'*' * 4}…{tail}"

    def _format_dt(self, value: Optional[str]) -> str:
        if not value:
            return "—"
        try:
            dt = datetime.fromisoformat(value)
            return dt.strftime("%Y-%m-%d %H:%M")
        except Exception:
            return value

    def _format_user_line(self, user: AdminUser, index: int) -> str:
        username = html.escape(f"@{user.username}") if user.username else "—"
        name = html.escape(user.full_name or "—")
        blocked = " • 🚫" if user.blocked else ""
        token = f"token: {user.token_mask}" if user.has_token else "token: —"
        updated = self._format_dt(user.token_updated_at or user.profile_updated_at)
        return (
            f"{index}. {username} / {name} • <code>{user.chat_id}</code>"
            f"\n   {token} • изм: {updated}{blocked}"
        )

    def _button_label_for_user(self, user: AdminUser, index: int) -> str:
        username = user.username or str(user.chat_id)
        suffix = "🚫" if user.blocked else ("✅" if user.has_token else "❌")
        return f"{index}. {username} {suffix}"

    async def _is_admin(self, chat_id: Optional[int]) -> bool:
        if chat_id is None:
            return False
        if chat_id in self.whitelist:
            return True
        role = await _db_run(self.store.get_admin_role, chat_id)
        return role in {"admin", "superadmin"}

    async def _is_superadmin(self, chat_id: int) -> bool:
        if chat_id in self.whitelist:
            return True
        role = await _db_run(self.store.get_admin_role, chat_id)
        return role == "superadmin"

    async def _ensure_anchor(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        text: str,
        reply_markup: InlineKeyboardMarkup,
        parse_mode: str = ParseMode.HTML,
    ) -> None:
        panel = self._panel_data(context)
        anchor = panel.get("anchor")
        chat = update.effective_chat
        if anchor:
            try:
                await context.bot.edit_message_text(
                    text=text,
                    chat_id=anchor["chat_id"],
                    message_id=anchor["message_id"],
                    reply_markup=reply_markup,
                    parse_mode=parse_mode,
                )
                return
            except TelegramError:
                panel.pop("anchor", None)
        if not chat:
            return
        msg = await context.bot.send_message(
            chat_id=chat.id,
            text=text,
            reply_markup=reply_markup,
            parse_mode=parse_mode,
        )
        panel["anchor"] = {"chat_id": msg.chat_id, "message_id": msg.message_id}

    async def _edit_anchor(
        self,
        context: ContextTypes.DEFAULT_TYPE,
        text: str,
        reply_markup: InlineKeyboardMarkup,
        parse_mode: str = ParseMode.HTML,
    ) -> None:
        panel = self._panel_data(context)
        anchor = panel.get("anchor")
        if not anchor:
            return
        try:
            await context.bot.edit_message_text(
                text=text,
                chat_id=anchor["chat_id"],
                message_id=anchor["message_id"],
                reply_markup=reply_markup,
                parse_mode=parse_mode,
            )
        except TelegramError:
            panel.pop("anchor", None)

    async def _reanchor(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        text: str,
        reply_markup: InlineKeyboardMarkup,
        parse_mode: str = ParseMode.HTML,
    ) -> None:
        panel = self._panel_data(context)
        anchor = panel.get("anchor") or {}
        chat_id = anchor.get("chat_id") if isinstance(anchor, dict) else None
        message_id = anchor.get("message_id") if isinstance(anchor, dict) else None
        if chat_id is not None and message_id is not None:
            try:
                await context.bot.edit_message_reply_markup(
                    chat_id=chat_id,
                    message_id=message_id,
                    reply_markup=None,
                )
            except TelegramError:
                pass
        if chat_id is None:
            chat = update.effective_chat
            if not chat:
                return
            chat_id = chat.id
        msg = await context.bot.send_message(
            chat_id=chat_id,
            text=text,
            reply_markup=reply_markup,
            parse_mode=parse_mode,
        )
        panel["anchor"] = {"chat_id": msg.chat_id, "message_id": msg.message_id}

    def _reset_wait(self, context: ContextTypes.DEFAULT_TYPE) -> None:
        panel = self._panel_data(context)
        panel.pop("awaiting", None)
        panel.pop("confirm", None)
        panel.pop("search_query", None)

    def _set_view(self, context: ContextTypes.DEFAULT_TYPE, view: str) -> None:
        panel = self._panel_data(context)
        panel["view"] = view

    def _schedule_message_deletion(
        self,
        context: ContextTypes.DEFAULT_TYPE,
        chat_id: int,
        message_id: int,
        delay: int = 60,
    ) -> None:
        async def _delete_later() -> None:
            try:
                await asyncio.sleep(delay)
                await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
            except TelegramError:
                pass
            except Exception:
                pass

        asyncio.create_task(_delete_later())

    # -------------------------- stats --------------------------
    def _load_stats(self) -> Dict[str, int]:
        cutoff = (datetime.utcnow() - timedelta(hours=24)).isoformat()
        with self.store.connect() as conn:
            total = conn.execute("SELECT COUNT(*) FROM known_users").fetchone()[0]
            with_token = conn.execute("SELECT COUNT(*) FROM user_tokens").fetchone()[0]
            without_token = total - with_token
            blocked = conn.execute("SELECT COUNT(*) FROM blocked_users").fetchone()[0]
            active = (
                conn.execute(
                    "SELECT COUNT(*) FROM known_users WHERE updated_at >= ?",
                    (cutoff,),
                ).fetchone()[0]
            )
        return {
            "total": total,
            "with_token": with_token,
            "without_token": without_token,
            "blocked": blocked,
            "active": active,
        }

    async def show_home(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        stats = await _db_run(self._load_stats)
        text = (
            "<b>Админ-панель</b>\n"
            f"Всего: {stats['total']} • С токеном: {stats['with_token']} • Без токена: {stats['without_token']} • "
            f"Заблокированы: {stats['blocked']} • Активны за 24ч: {stats['active']}"
        )
        kb = InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("👥 Пользователи", callback_data="adm:ul:p1:fA")],
                [InlineKeyboardButton("📦 Экспорт", callback_data="adm:export")],
                [InlineKeyboardButton("📣 Рассылка", callback_data="adm:broadcast")],
                [InlineKeyboardButton("⚙️ Настройки", callback_data="adm:settings")],
            ]
        )
        if update.message:
            await self._reanchor(update, context, text, kb)
        else:
            await self._ensure_anchor(update, context, text, kb)
        self._reset_wait(context)
        self._set_view(context, "home")

    # -------------------------- list --------------------------
    def _filter_clause(self, code: str) -> Tuple[str, Tuple[Any, ...]]:
        if code == "fT":
            return "WHERE ut.token IS NOT NULL", tuple()
        if code == "fN":
            return "WHERE ut.token IS NULL", tuple()
        if code == "fB":
            return "WHERE bu.chat_id IS NOT NULL", tuple()
        return "", tuple()

    def _load_users(self, filter_code: str, page: int) -> Tuple[List[AdminUser], int]:
        offset = (page - 1) * self.PAGE_SIZE
        clause, params = self._filter_clause(filter_code)
        base_query = (
            " FROM known_users ku "
            "LEFT JOIN user_tokens ut ON ut.chat_id = ku.chat_id "
            "LEFT JOIN blocked_users bu ON bu.chat_id = ku.chat_id "
            "LEFT JOIN user_admin_notes n ON n.chat_id = ku.chat_id "
        )
        with self.store.connect() as conn:
            total_row = conn.execute(
                f"SELECT COUNT(*){base_query} {clause}",
                params,
            ).fetchone()
            total = total_row[0] if total_row else 0
            rows = conn.execute(
                "SELECT ku.chat_id, ku.username, ku.first_name, ku.last_name, ku.user_id, ku.updated_at as profile_updated, "
                "ut.token, ut.updated_at as token_updated, ut.created_at as token_created, "
                "bu.chat_id IS NOT NULL AS blocked, n.note, n.updated_at as note_updated "
                + base_query
                + f" {clause} ORDER BY COALESCE(ut.updated_at, ku.updated_at) DESC LIMIT ? OFFSET ?",
                params + (self.PAGE_SIZE, offset),
            ).fetchall()
        users: List[AdminUser] = []
        for row in rows:
            full_name = " ".join(filter(None, [row["first_name"], row["last_name"]])).strip()
            users.append(
                AdminUser(
                    chat_id=row["chat_id"],
                    username=row["username"],
                    full_name=full_name,
                    user_id=row["user_id"],
                    has_token=bool(row["token"]),
                    token_mask=self._mask_token(row["token"]),
                    token_updated_at=row["token_updated"],
                    token_created_at=row["token_created"],
                    blocked=bool(row["blocked"]),
                    note=row["note"],
                    note_updated_at=row["note_updated"],
                    profile_updated_at=row["profile_updated"],
                )
            )
        return users, total

    async def show_user_list(
        self,
        context: ContextTypes.DEFAULT_TYPE,
        filter_code: str,
        page: int,
    ) -> None:
        if filter_code not in FILTER_MAP:
            filter_code = "fA"
        users, total = await _db_run(self._load_users, filter_code, page)
        total_pages = max(1, (total + self.PAGE_SIZE - 1) // self.PAGE_SIZE)
        page = max(1, min(page, total_pages))
        display_filter = {
            "fA": "Все",
            "fT": "С токеном",
            "fN": "Без токена",
            "fB": "Заблокированные",
        }.get(filter_code, "Все")
        lines = [f"<b>Пользователи — {display_filter} (стр. {page}/{total_pages})</b>"]
        lines.append(
            "Введите ниже, чтобы искать по username/chat_id/последним 4 символам токена."
        )
        buttons: List[List[InlineKeyboardButton]] = []
        for idx, user in enumerate(users, start=1 + (page - 1) * self.PAGE_SIZE):
            lines.append(self._format_user_line(user, idx))
            buttons.append(
                [
                    InlineKeyboardButton(
                        text=self._button_label_for_user(user, idx),
                        callback_data=f"adm:u:{user.chat_id}",
                    )
                ]
            )
        if not users:
            lines.append("Нет данных")
        nav_row: List[InlineKeyboardButton] = []
        if page > 1:
            nav_row.append(
                InlineKeyboardButton(
                    "◀️",
                    callback_data=f"adm:ul:p{page-1}:{filter_code}",
                )
            )
        nav_row.append(
            InlineKeyboardButton(
                f"Стр. {page}",
                callback_data="adm:noop",
            )
        )
        if page < total_pages:
            nav_row.append(
                InlineKeyboardButton(
                    "▶️",
                    callback_data=f"adm:ul:p{page+1}:{filter_code}",
                )
            )
        buttons.append(nav_row)
        buttons.append([InlineKeyboardButton("Назад", callback_data="adm:back:home")])
        kb = InlineKeyboardMarkup(buttons)
        await self._edit_anchor(context, "\n".join(lines), kb)
        panel = self._panel_data(context)
        panel["current_filter"] = filter_code
        panel["current_page"] = page
        panel.pop("search_query", None)
        panel["awaiting"] = {"type": "search"}
        self._set_view(context, "list")

    # -------------------------- user card --------------------------
    def _load_user_card(self, chat_id: int) -> Optional[AdminUser]:
        with self.store.connect() as conn:
            row = conn.execute(
                "SELECT ku.chat_id, ku.username, ku.first_name, ku.last_name, ku.user_id, ku.updated_at as profile_updated, "
                "ut.token, ut.updated_at as token_updated, ut.created_at as token_created, "
                "bu.chat_id IS NOT NULL AS blocked, n.note, n.updated_at as note_updated "
                "FROM known_users ku "
                "LEFT JOIN user_tokens ut ON ut.chat_id = ku.chat_id "
                "LEFT JOIN blocked_users bu ON bu.chat_id = ku.chat_id "
                "LEFT JOIN user_admin_notes n ON n.chat_id = ku.chat_id "
                "WHERE ku.chat_id = ?",
                (int(chat_id),),
            ).fetchone()
        if not row:
            return None
        full_name = " ".join(filter(None, [row["first_name"], row["last_name"]])).strip()
        return AdminUser(
            chat_id=row["chat_id"],
            username=row["username"],
            full_name=full_name,
            user_id=row["user_id"],
            has_token=bool(row["token"]),
            token_mask=self._mask_token(row["token"]),
            token_updated_at=row["token_updated"],
            token_created_at=row["token_created"],
            blocked=bool(row["blocked"]),
            note=row["note"],
            note_updated_at=row["note_updated"],
            profile_updated_at=row["profile_updated"],
        )

    async def show_user_card(
        self,
        context: ContextTypes.DEFAULT_TYPE,
        admin_chat_id: int,
        target_chat_id: int,
        info: Optional[str] = None,
        *,
        update: Optional[Update] = None,
    ) -> None:
        user = await _db_run(self._load_user_card, target_chat_id)
        if not user:
            markup = InlineKeyboardMarkup(
                [[InlineKeyboardButton("Назад", callback_data="adm:back:list")]]
            )
            if update:
                await self._reanchor(
                    update,
                    context,
                    f"Пользователь {target_chat_id} не найден",
                    markup,
                )
            else:
                await self._edit_anchor(
                    context,
                    f"Пользователь {target_chat_id} не найден",
                    markup,
                )
            return
        username = f"@{user.username}" if user.username else "—"
        note_block = ""
        if user.note:
            note_text = html.escape(user.note)
            note_updated = self._format_dt(user.note_updated_at)
            note_block = f"\n<b>Заметка:</b> {note_text} (обн. {note_updated})"
        status = "🚫 заблокирован" if user.blocked else ("✅ авторизован" if user.has_token else "❌ нет токена")
        token_line = "Токен: —"
        if user.has_token:
            token_line = (
                f"Токен: {user.token_mask}\nсоздан: {self._format_dt(user.token_created_at)}"
                f" • изм: {self._format_dt(user.token_updated_at)}"
            )
        text = (
            f"<b>{html.escape(user.full_name or '—')}</b> ({username})\n"
            f"chat_id: <code>{user.chat_id}</code>"
            + (f"\nuser_id: <code>{user.user_id}</code>" if user.user_id else "")
            + f"\nСтатус: {status}\n{token_line}{note_block}"
        )
        if info:
            text = f"{info}\n\n{text}"
        buttons: List[List[InlineKeyboardButton]] = []
        buttons.append([
            InlineKeyboardButton("🔁 Заменить токен", callback_data=f"adm:u:{user.chat_id}:rt"),
        ])
        if user.has_token:
            buttons.append([
                InlineKeyboardButton("🗑 Удалить токен", callback_data=f"adm:u:{user.chat_id}:dt"),
                InlineKeyboardButton("🧪 Проверить", callback_data=f"adm:u:{user.chat_id}:v"),
                InlineKeyboardButton("📋 Скопировать токен", callback_data=f"adm:u:{user.chat_id}:copy"),
            ])
        else:
            buttons.append([
                InlineKeyboardButton("🧪 Проверить", callback_data="adm:noop"),
            ])
        if user.blocked:
            buttons.append([
                InlineKeyboardButton("✅ Разблокировать", callback_data=f"adm:u:{user.chat_id}:unb"),
            ])
        else:
            buttons.append([
                InlineKeyboardButton("🚫 Заблокировать", callback_data=f"adm:u:{user.chat_id}:blk"),
            ])
        buttons.append([
            InlineKeyboardButton("✏️ Примечание", callback_data=f"adm:u:{user.chat_id}:note"),
            InlineKeyboardButton("↩️ Сбросить авторизацию", callback_data=f"adm:u:{user.chat_id}:reset"),
        ])
        if user.note:
            buttons.append([
                InlineKeyboardButton("🗑 Удалить примечание", callback_data=f"adm:u:{user.chat_id}:note_clear"),
            ])
        buttons.append([
            InlineKeyboardButton("Назад к списку", callback_data="adm:back:list"),
            InlineKeyboardButton("В главное меню", callback_data="adm:home"),
        ])
        kb = InlineKeyboardMarkup(buttons)
        if update:
            await self._reanchor(update, context, text, kb)
        else:
            await self._edit_anchor(context, text, kb)
        panel = self._panel_data(context)
        panel["view"] = "user"
        panel["current_user"] = user.chat_id
        snapshots = panel.setdefault("user_snapshots", {})
        snapshots[user.chat_id] = {
            "token_updated_at": user.token_updated_at,
            "token_mask": user.token_mask,
        }

    # -------------------------- search --------------------------
    def _search_users(self, query: str) -> List[AdminUser]:
        like = f"%{query.lower()}%"
        with self.store.connect() as conn:
            rows = conn.execute(
                "SELECT ku.chat_id, ku.username, ku.first_name, ku.last_name, ku.user_id, ku.updated_at as profile_updated, "
                "ut.token, ut.updated_at as token_updated, ut.created_at as token_created, "
                "bu.chat_id IS NOT NULL AS blocked, n.note, n.updated_at as note_updated "
                "FROM known_users ku "
                "LEFT JOIN user_tokens ut ON ut.chat_id = ku.chat_id "
                "LEFT JOIN blocked_users bu ON bu.chat_id = ku.chat_id "
                "LEFT JOIN user_admin_notes n ON n.chat_id = ku.chat_id "
                "WHERE LOWER(ku.username) LIKE ? OR CAST(ku.chat_id AS TEXT) LIKE ? OR (ut.token IS NOT NULL AND ut.token_tail = ?)",
                (like, like, query[-4:] if len(query) >= 4 else query)
            ).fetchall()
        users: List[AdminUser] = []
        for row in rows:
            token = row["token"]
            full_name = " ".join(filter(None, [row["first_name"], row["last_name"]])).strip()
            users.append(
                AdminUser(
                    chat_id=row["chat_id"],
                    username=row["username"],
                    full_name=full_name,
                    user_id=row["user_id"],
                    has_token=bool(token),
                    token_mask=self._mask_token(token),
                    token_updated_at=row["token_updated"],
                    token_created_at=row["token_created"],
                    blocked=bool(row["blocked"]),
                    note=row["note"],
                    note_updated_at=row["note_updated"],
                    profile_updated_at=row["profile_updated"],
                )
            )
        return users

    def _export_csv_file(self) -> str:
        with self.store.connect() as conn:
            with tempfile.NamedTemporaryFile("w", newline="", encoding="utf-8-sig", delete=False) as tmp:
                writer = csv.writer(
                    tmp,
                    delimiter=";",
                    lineterminator="\r\n",
                    quoting=csv.QUOTE_MINIMAL,
                )
                writer.writerow(["chat_id", "username", "has_token", "updated_at", "blocked"])
                cursor = conn.execute(
                    "SELECT ku.chat_id, ku.username, ut.token IS NOT NULL AS has_token, ut.updated_at,"
                    " bu.chat_id IS NOT NULL AS blocked "
                    "FROM known_users ku "
                    "LEFT JOIN user_tokens ut ON ut.chat_id = ku.chat_id "
                    "LEFT JOIN blocked_users bu ON bu.chat_id = ku.chat_id"
                )
                for row in cursor:
                    writer.writerow(
                        [
                            row["chat_id"],
                            row["username"] or "",
                            1 if row["has_token"] else 0,
                            row["updated_at"] or "",
                            1 if row["blocked"] else 0,
                        ]
                    )
                return tmp.name

    async def start_search(self, context: ContextTypes.DEFAULT_TYPE) -> None:
        panel = self._panel_data(context)
        panel["awaiting"] = {"type": "search"}
        text = "<b>Поиск пользователей</b>\nВведите username, chat_id или последние 4 символа токена."
        kb = InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("Очистить поиск", callback_data="adm:search:clear")],
                [InlineKeyboardButton("Назад", callback_data="adm:back:home")],
            ]
        )
        await self._edit_anchor(context, text, kb)
        self._set_view(context, "search")

    async def show_search_results(
        self,
        context: ContextTypes.DEFAULT_TYPE,
        query: str,
        update: Optional[Update] = None,
    ) -> None:
        users = await _db_run(self._search_users, query)
        panel = self._panel_data(context)
        panel["search_query"] = query
        panel["awaiting"] = {"type": "search"}
        lines = [f"<b>Результаты поиска:</b> {html.escape(query)}"]
        buttons: List[List[InlineKeyboardButton]] = []
        if not users:
            lines.append("Ничего не найдено")
        for idx, user in enumerate(users, start=1):
            lines.append(self._format_user_line(user, idx))
            buttons.append([
                InlineKeyboardButton(
                    text=self._button_label_for_user(user, idx),
                    callback_data=f"adm:u:{user.chat_id}",
                )
            ])
        buttons.append([InlineKeyboardButton("Очистить поиск", callback_data="adm:search:clear")])
        buttons.append([InlineKeyboardButton("Назад к списку", callback_data="adm:back:list")])
        kb = InlineKeyboardMarkup(buttons)
        text = "\n".join(lines)
        if update:
            await self._reanchor(update, context, text, kb)
        else:
            await self._edit_anchor(context, text, kb)

    # -------------------------- export --------------------------
    async def export_csv(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        admin_chat_id: int,
    ) -> None:
        if not await self._is_superadmin(admin_chat_id):
            await update.effective_chat.send_message("Недостаточно прав для экспорта.")
            return
        file_path = await _db_run(self._export_csv_file)
        try:
            with open(file_path, "rb") as fh:
                await context.bot.send_document(
                    chat_id=admin_chat_id,
                    document=fh,
                    filename="users.csv",
                    caption="CSV выгрузка готова",
                )
        finally:
            try:
                os.remove(file_path)
            except OSError:
                pass
        await self._log_action(admin_chat_id, None, "export_csv", {})

    # -------------------------- audit --------------------------
    def _load_audit(self, page: int) -> Tuple[List[sqlite3.Row], int]:
        offset = (page - 1) * self.AUDIT_PAGE_SIZE
        with self.store.connect() as conn:
            total = conn.execute("SELECT COUNT(*) FROM admin_audit").fetchone()[0]
            rows = conn.execute(
                """
                SELECT aa.*, ku.username
                FROM admin_audit aa
                LEFT JOIN known_users ku ON ku.chat_id = aa.admin_chat_id
                ORDER BY aa.ts DESC
                LIMIT ? OFFSET ?
                """,
                (self.AUDIT_PAGE_SIZE, offset),
            ).fetchall()
        return rows, total

    async def show_audit(self, context: ContextTypes.DEFAULT_TYPE, page: int) -> None:
        rows, total = await _db_run(self._load_audit, page)
        total_pages = max(1, (total + self.AUDIT_PAGE_SIZE - 1) // self.AUDIT_PAGE_SIZE)
        page = max(1, min(page, total_pages))
        lines = [f"<b>Аудит-лог</b> (стр. {page}/{total_pages})"]
        for row in rows:
            ts = self._format_dt(row["ts"])
            action = html.escape(row["action"])
            details = row["details_json"] or "{}"
            try:
                details_obj = json.loads(details)
                details_text = ", ".join(
                    f"{html.escape(str(k))}: {html.escape(str(v))}" for k, v in details_obj.items()
                )
            except Exception:
                details_text = html.escape(details)
            target = row["target_chat_id"]
            target_text = f" → <code>{target}</code>" if target else ""
            username = row["username"]
            username_text = html.escape(username) if username else "-"
            lines.append(
                f"{ts} • <code>{row['admin_chat_id']}</code>({username_text}){target_text} • {action} ({details_text})"
            )
        if not rows:
            lines.append("Пока пусто")
        buttons: List[List[InlineKeyboardButton]] = []
        nav_row: List[InlineKeyboardButton] = []
        if page > 1:
            nav_row.append(InlineKeyboardButton("◀️", callback_data=f"adm:audit:p{page-1}"))
        nav_row.append(InlineKeyboardButton(f"Стр. {page}", callback_data="adm:noop"))
        if page < total_pages:
            nav_row.append(InlineKeyboardButton("▶️", callback_data=f"adm:audit:p{page+1}"))
        buttons.append(nav_row)
        buttons.append([InlineKeyboardButton("Назад", callback_data="adm:back:settings")])
        kb = InlineKeyboardMarkup(buttons)
        await self._edit_anchor(context, "\n".join(lines), kb)
        self._set_view(context, "audit")

    # -------------------------- callbacks --------------------------
    async def handle_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        chat = update.effective_chat
        if not chat:
            return
        if not await self._is_admin(chat.id):
            await update.effective_message.reply_text("Нет прав")
            return
        await self.show_home(update, context)
        await self._log_action(chat.id, None, "open_home", {})

    async def handle_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        query = update.callback_query
        if not query:
            return
        chat_id = query.message.chat_id if query.message else None
        if not await self._is_admin(chat_id):
            await query.answer("Нет прав", show_alert=True)
            return
        data = query.data or ""
        await query.answer()
        if data == "adm:home":
            await self.show_home(update, context)
            await self._log_action(chat_id, None, "open_home", {})
            return
        if data == "adm:broadcast":
            await self.start_broadcast(update, context, chat_id)
            await self._log_action(chat_id, None, "broadcast_open", {})
            return
        if data.startswith("adm:ul:"):
            parts = data.split(":")
            page_part = parts[2]
            filter_part = parts[3] if len(parts) > 3 else "fA"
            page = int(page_part[1:]) if page_part.startswith("p") else 1
            await self.show_user_list(context, filter_part, page)
            await self._log_action(chat_id, None, "open_list", {"filter": filter_part, "page": page})
            return
        if data.startswith("adm:u:"):
            parts = data.split(":")
            target = int(parts[2])
            if len(parts) == 3:
                await self.show_user_card(context, chat_id, target)
                await self._log_action(chat_id, target, "open_user", {})
                return
            action = parts[3]
            if action == "copy":
                await self._handle_copy_token(query, context, chat_id, target)
                return
            await self._handle_user_action(context, chat_id, target, action)
            return
        if data == "adm:search":
            await self.start_search(context)
            await self._log_action(chat_id, None, "open_search", {})
            return
        if data.startswith("adm:confirm:"):
            await self.handle_confirmation(update, context, data)
            return
        if data == "adm:noop":
            return
        if data == "adm:search:clear":
            await self._log_action(chat_id, None, "search_clear", {})
            await self.show_user_list(context, "fA", 1)
            return
        if data == "adm:export":
            await self.export_csv(update, context, chat_id)
            return
        if data == "adm:settings":
            await self._show_settings(context, chat_id)
            await self._log_action(chat_id, None, "open_settings", {})
            return
        if data.startswith("adm:audit:" ):
            page = 1
            try:
                page = int(data.split(":")[2][1:])
            except Exception:
                pass
            await self.show_audit(context, page)
            await self._log_action(chat_id, None, "open_audit", {"page": page})
            return
        if data.startswith("adm:back:"):
            where = data.split(":")[2]
            await self._handle_back(update, context, chat_id, where)
            return

    async def handle_broadcast_callback(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        query = update.callback_query
        if not query:
            return
        chat_id = query.message.chat_id if query.message else None
        if chat_id is None or not await self._is_admin(chat_id):
            await query.answer("Нет прав", show_alert=True)
            return
        data = query.data or ""
        await query.answer()
        if data == "admin:broadcast:cancel":
            await self._cancel_broadcast(update, context, chat_id)
            return
        if data == "admin:broadcast:confirm":
            await self._confirm_broadcast(update, context, chat_id)
            return

    async def _handle_back(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        chat_id: int,
        where: str,
    ) -> None:
        if where == "home":
            await self.show_home(update, context)
            return
        if where == "list":
            panel = self._panel_data(context)
            filter_code = panel.get("current_filter", "fA")
            page = panel.get("current_page", 1)
            await self.show_user_list(context, filter_code, page)
            return
        if where == "settings":
            await self._show_settings(context, chat_id)
            return
        await self.show_home(update, context)

    async def _show_settings(self, context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> None:
        role = await _db_run(self.store.get_admin_role, chat_id)
        role_text = "Суперадмин" if role == "superadmin" or chat_id in self.whitelist else "Админ"
        text = (
            f"<b>Настройки</b>\nВаша роль: {role_text}\n"
            "Доступно: обновление статистики, аудит-лог."
        )
        buttons = [
            [InlineKeyboardButton("Обновить статистику", callback_data="adm:home")],
            [InlineKeyboardButton("Открыть аудит-лог", callback_data="adm:audit:p1")],
            [InlineKeyboardButton("Назад", callback_data="adm:back:home")],
        ]
        await self._edit_anchor(context, text, InlineKeyboardMarkup(buttons))
        self._set_view(context, "settings")

    # -------------------------- broadcast --------------------------
    async def start_broadcast(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        admin_chat_id: int,
    ) -> None:
        panel = self._panel_data(context)
        panel["awaiting"] = {"type": "broadcast_text"}
        panel["broadcast"] = {}
        prompt = (
            "<b>Рассылка</b>\n"
            "Отправьте текст сообщения для рассылки. Markdown поддерживается."
        )
        kb = InlineKeyboardMarkup(
            [[InlineKeyboardButton("❌ Отмена", callback_data="admin:broadcast:cancel")]]
        )
        await self._edit_anchor(context, prompt, kb)
        self._set_view(context, "broadcast")

    async def _handle_broadcast_text(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        admin_chat_id: int,
        text: str,
    ) -> None:
        if not text.strip():
            await update.message.reply_text("Текст рассылки не может быть пустым")
            return
        panel = self._panel_data(context)
        broadcast_state = panel.setdefault("broadcast", {})
        broadcast_state["text"] = text
        recipients = await _db_run(self.store.list_recipients)
        count = len(recipients)
        broadcast_state["count"] = count
        preview = text
        if len(preview) > 2000:
            preview = preview[:2000] + "…"
        preview_lines = [
            "<b>📣 Рассылка</b>",
            f"Получателей: {count}",
            "",
            "<b>Текст сообщения:</b>",
            f"<pre>{html.escape(preview)}</pre>",
        ]
        kb = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("✅ Отправить", callback_data="admin:broadcast:confirm"),
                    InlineKeyboardButton("❌ Отмена", callback_data="admin:broadcast:cancel"),
                ]
            ]
        )
        await self._reanchor(update, context, "\n".join(preview_lines), kb)
        panel["awaiting"] = {"type": "broadcast_confirm"}
        await self._log_action(
            admin_chat_id,
            None,
            "broadcast_preview",
            {"count": count, "text_len": len(text)},
        )

    async def _cancel_broadcast(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        admin_chat_id: int,
    ) -> None:
        panel = self._panel_data(context)
        panel.pop("awaiting", None)
        panel.pop("broadcast", None)
        await self._log_action(admin_chat_id, None, "broadcast_cancel", {})
        await self.show_home(update, context)

    async def _confirm_broadcast(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        admin_chat_id: int,
    ) -> None:
        panel = self._panel_data(context)
        broadcast_state = panel.get("broadcast") or {}
        text = broadcast_state.get("text")
        if not text:
            await self.start_broadcast(update, context, admin_chat_id)
            return
        panel.pop("awaiting", None)
        await self._edit_anchor(
            context,
            "📣 Отправляю сообщения…",
            reply_markup=None,
            parse_mode=ParseMode.HTML,
        )
        total, success, errors = await self._perform_broadcast(context, admin_chat_id, text)
        failures = total - success
        lines = [
            "<b>📣 Рассылка завершена</b>",
            f"Всего получателей: {total}",
            f"Успешно: {success}",
            f"Ошибок: {failures}",
        ]
        if errors:
            lines.append("\nПервые ошибки:")
            for chat_id, err in errors[:10]:
                lines.append(f"<code>{chat_id}</code> — {html.escape(err)}")
        summary = "\n".join(lines)
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("В меню", callback_data="adm:home")]])
        await self._reanchor(update, context, summary, kb)
        panel.pop("broadcast", None)
        self._set_view(context, "broadcast_result")

    async def _perform_broadcast(
        self,
        context: ContextTypes.DEFAULT_TYPE,
        admin_chat_id: int,
        text: str,
    ) -> Tuple[int, int, List[Tuple[int, str]]]:
        recipients = await _db_run(self.store.list_recipients)
        total = len(recipients)
        log.info("Админ %s запустил рассылку на %s получателей", admin_chat_id, total)
        await self._log_action(
            admin_chat_id,
            None,
            "broadcast_send",
            {"total": total, "text_len": len(text)},
        )
        success = 0
        errors: List[Tuple[int, str]] = []
        for chat_id in recipients:
            try:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=text,
                    parse_mode=ParseMode.MARKDOWN,
                    disable_web_page_preview=False,
                )
                success += 1
            except RetryAfter as exc:
                log.warning("Лимит Telegram при рассылке %s: %s", chat_id, exc)
                await asyncio.sleep(exc.retry_after + 1)
                try:
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text=text,
                        parse_mode=ParseMode.MARKDOWN,
                        disable_web_page_preview=False,
                    )
                    success += 1
                except Exception as err:
                    message = str(err)
                    errors.append((chat_id, message))
                    log.warning(
                        "Ошибка повторной отправки рассылки %s: %s",
                        chat_id,
                        err,
                        exc_info=isinstance(err, TelegramError),
                    )
            except Exception as err:
                message = str(err)
                errors.append((chat_id, message))
                log.warning(
                    "Ошибка рассылки %s: %s",
                    chat_id,
                    err,
                    exc_info=isinstance(err, TelegramError),
                )
            await asyncio.sleep(0.05)
        failures = total - success
        log.info(
            "Рассылка завершена админом %s: всего=%s успешно=%s ошибки=%s",
            admin_chat_id,
            total,
            success,
            failures,
        )
        await self._log_action(
            admin_chat_id,
            None,
            "broadcast_complete",
            {"total": total, "success": success, "errors": failures},
        )
        return total, success, errors

    async def _handle_user_action(
        self,
        context: ContextTypes.DEFAULT_TYPE,
        admin_chat_id: int,
        target_chat_id: int,
        action: str,
    ) -> None:
        panel = self._panel_data(context)
        if action == "v":
            await self._verify_token(context, admin_chat_id, target_chat_id)
            return
        if action == "dt":
            panel["confirm"] = {"type": "delete_token", "chat_id": target_chat_id}
            await self._edit_anchor(
                context,
                "Подтвердите удаление токена",
                InlineKeyboardMarkup(
                    [
                        [
                            InlineKeyboardButton("✅ Да", callback_data=f"adm:confirm:delete:{target_chat_id}"),
                            InlineKeyboardButton("❌ Нет", callback_data=f"adm:u:{target_chat_id}"),
                        ]
                    ]
                ),
            )
            return
        if action == "rt":
            panel["awaiting"] = {"type": "replace_token", "chat_id": target_chat_id}
            await self.show_user_card(context, admin_chat_id, target_chat_id, info="Вставьте новый токен:")
            return
        if action == "blk":
            await _db_run(self.store.set_blocked, target_chat_id, admin_chat_id, True)
            await self._log_action(admin_chat_id, target_chat_id, "block", {})
            await self.show_user_card(context, admin_chat_id, target_chat_id)
            return
        if action == "unb":
            await _db_run(self.store.set_blocked, target_chat_id, admin_chat_id, False)
            await self._log_action(admin_chat_id, target_chat_id, "unblock", {})
            await self.show_user_card(context, admin_chat_id, target_chat_id)
            return
        if action == "note":
            panel["awaiting"] = {"type": "note", "chat_id": target_chat_id}
            await self.show_user_card(
                context,
                admin_chat_id,
                target_chat_id,
                info="Введите текст примечания (отправьте сообщением).",
            )
            return
        if action == "note_clear":
            await _db_run(self.store.delete_admin_note, target_chat_id)
            await self._log_action(admin_chat_id, target_chat_id, "note_clear", {})
            await self.show_user_card(context, admin_chat_id, target_chat_id)
            return
        if action == "reset":
            snapshot = self._panel_data(context).get("user_snapshots", {}).get(target_chat_id)
            await _db_run(self.store.remove_token, target_chat_id)
            await self.prompt_sender(target_chat_id)
            details = {}
            if snapshot:
                details["token_mask"] = snapshot.get("token_mask")
            await self._log_action(admin_chat_id, target_chat_id, "reset_auth", details)
            await self.show_user_card(context, admin_chat_id, target_chat_id)
            return
        if action.startswith("confirm"):
            # handled separately
            return

    async def _handle_copy_token(
        self,
        query: CallbackQuery,
        context: ContextTypes.DEFAULT_TYPE,
        admin_chat_id: int,
        target_chat_id: int,
    ) -> None:
        token = await _db_run(self.store.get_token, target_chat_id)
        if not token:
            await query.answer("Токен отсутствует", show_alert=True)
            await self.show_user_card(context, admin_chat_id, target_chat_id)
            return
        message = await context.bot.send_message(
            chat_id=admin_chat_id,
            text=f"<code>{html.escape(token)}</code>",
            parse_mode=ParseMode.HTML,
        )
        self._schedule_message_deletion(context, message.chat_id, message.message_id, delay=60)
        await query.answer("Токен отправлен отдельным сообщением (самоудаление через 60 сек)")
        tail = token[-4:] if len(token) >= 4 else token
        await self._log_action(admin_chat_id, target_chat_id, "token_copy", {"token_tail": tail})

    async def _verify_token(
        self,
        context: ContextTypes.DEFAULT_TYPE,
        admin_chat_id: int,
        target_chat_id: int,
    ) -> None:
        token = await _db_run(self.store.get_token, target_chat_id)
        if not token:
            await self.show_user_card(context, admin_chat_id, target_chat_id, info="❌ Токен отсутствует")
            return
        panel = self._panel_data(context)
        panel.pop("awaiting", None)
        await self._edit_anchor(
            context,
            "🧪 Проверяю токен…",
            InlineKeyboardMarkup([[InlineKeyboardButton("Назад", callback_data=f"adm:u:{target_chat_id}")]]),
        )
        loop = asyncio.get_running_loop()
        ok, err = await loop.run_in_executor(EXECUTOR, self.verify_token_func, token)
        if ok:
            result = "✅ Токен валиден"
        else:
            result = f"❌ Ошибка проверки: {html.escape(err or 'неизвестно')}"
        await self._log_action(
            admin_chat_id,
            target_chat_id,
            "verify_token",
            {"token_mask": self._mask_token(token), "ok": ok, "error": err or ""},
        )
        await self.show_user_card(context, admin_chat_id, target_chat_id, info=result)

    async def handle_text(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
        chat = update.effective_chat
        if not chat or not await self._is_admin(chat.id):
            return False
        panel = self._panel_data(context)
        awaiting = panel.get("awaiting")
        if not awaiting:
            return False
        raw_text = update.message.text or ""
        if awaiting["type"] in {"broadcast_text", "broadcast_confirm"}:
            await self._handle_broadcast_text(update, context, chat.id, raw_text)
            return True
        text = raw_text.strip()
        if not text:
            await update.message.reply_text("Текст не распознан")
            return True
        target_chat_id = awaiting.get("chat_id")
        if awaiting["type"] == "replace_token":
            await self._process_token_replacement(update, context, chat.id, target_chat_id, text)
            return True
        if awaiting["type"] == "note":
            await _db_run(self.store.save_admin_note, target_chat_id, text, chat.id)
            await self._log_action(
                chat.id,
                target_chat_id,
                "note",
                {"preview": text[:120]},
            )
            await self.show_user_card(context, chat.id, target_chat_id, update=update)
            panel.pop("awaiting", None)
            return True
        if awaiting["type"] == "search":
            await self.show_search_results(context, text, update)
            await self._log_action(chat.id, None, "search", {"query": text})
            return True
        return False

    async def _process_token_replacement(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        admin_chat_id: int,
        target_chat_id: int,
        token: str,
    ) -> None:
        panel = self._panel_data(context)
        snapshot = panel.get("user_snapshots", {}).get(target_chat_id)
        current = await _db_run(self._load_user_card, target_chat_id)
        if snapshot and current and snapshot.get("token_updated_at") != current.token_updated_at:
            await update.message.reply_text("Данные обновлены другим администратором, повторите действие")
            panel.pop("awaiting", None)
            await self.show_user_card(context, admin_chat_id, target_chat_id, update=update)
            return
        await self._reanchor(
            update,
            context,
            "Проверяю токен…",
            InlineKeyboardMarkup([[InlineKeyboardButton("Отмена", callback_data=f"adm:u:{target_chat_id}")]]),
        )
        loop = asyncio.get_running_loop()
        ok, err = await loop.run_in_executor(EXECUTOR, self.verify_token_func, token)
        if not ok:
            await self._log_action(
                admin_chat_id,
                target_chat_id,
                "replace_token_failed",
                {"token_mask": self._mask_token(token), "error": err or ""},
            )
            await self.show_user_card(
                context,
                admin_chat_id,
                target_chat_id,
                info=f"❌ Ошибка: {html.escape(err or 'неизвестно')}",
                update=update,
            )
            panel.pop("awaiting", None)
            return
        profile = await _db_run(self.store.get_user_profile, target_chat_id)
        user_id = profile["user_id"] if profile else None
        await _db_run(self.store.save_token, target_chat_id, user_id, token)
        await self._log_action(
            admin_chat_id,
            target_chat_id,
            "replace_token",
            {"token_mask": self._mask_token(token)},
        )
        panel.pop("awaiting", None)
        await self.show_user_card(
            context,
            admin_chat_id,
            target_chat_id,
            info="✅ Токен обновлён",
            update=update,
        )

    async def handle_confirmation(self, update: Update, context: ContextTypes.DEFAULT_TYPE, data: str) -> None:
        parts = data.split(":")
        if len(parts) != 4:
            return
        _, _, action, chat_id_str = parts
        chat_id = int(chat_id_str)
        admin_chat_id = update.effective_chat.id if update.effective_chat else None
        if action == "delete":
            panel = self._panel_data(context)
            snapshot = panel.get("user_snapshots", {}).get(chat_id)
            current = await _db_run(self._load_user_card, chat_id)
            if snapshot and current and snapshot.get("token_updated_at") != current.token_updated_at:
                if update.callback_query:
                    await update.callback_query.answer("Данные обновлены, откройте карточку заново", show_alert=True)
                await self.show_user_card(context, admin_chat_id or chat_id, chat_id)
                return
            await _db_run(self.store.remove_token, chat_id)
            if admin_chat_id:
                details = {}
                if snapshot:
                    details["token_mask"] = snapshot.get("token_mask")
                await self._log_action(admin_chat_id, chat_id, "delete_token", details)
            await self.show_user_card(context, admin_chat_id or chat_id, chat_id)
            panel.pop("confirm", None)
