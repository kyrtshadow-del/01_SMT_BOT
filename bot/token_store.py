from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional


class TokenStore:
    """SQLite-backed storage for user tokens, admins, and audit trail."""

    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path)

    def connect(self) -> sqlite3.Connection:
        conn = self._connect()
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS user_tokens (
                    chat_id INTEGER PRIMARY KEY,
                    user_id INTEGER,
                    token TEXT NOT NULL,
                    token_tail TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS known_users (
                    chat_id INTEGER PRIMARY KEY,
                    user_id INTEGER,
                    username TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    language_code TEXT,
                    is_bot INTEGER DEFAULT 0,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS admins (
                    chat_id INTEGER PRIMARY KEY,
                    role TEXT NOT NULL CHECK(role IN ('admin','superadmin')),
                    created_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS blocked_users (
                    chat_id INTEGER PRIMARY KEY,
                    reason TEXT,
                    updated_at TEXT NOT NULL,
                    by_admin INTEGER
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS user_admin_notes (
                    chat_id INTEGER PRIMARY KEY,
                    note TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    by_admin INTEGER
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS admin_audit (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    admin_chat_id INTEGER NOT NULL,
                    target_chat_id INTEGER,
                    action TEXT NOT NULL,
                    details_json TEXT,
                    ts TEXT NOT NULL
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_user_tokens_updated_at ON user_tokens(updated_at)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_blocked_users_updated_at ON blocked_users(updated_at)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_admin_audit_ts ON admin_audit(ts)"
            )
            conn.commit()

    def get_token(self, chat_id: int) -> Optional[str]:
        with self._connect() as conn:
            cur = conn.execute(
                "SELECT token FROM user_tokens WHERE chat_id = ?",
                (int(chat_id),),
            )
            row = cur.fetchone()
            return row[0] if row else None

    def save_token(self, chat_id: int, user_id: Optional[int], token: str) -> None:
        ts = datetime.utcnow().isoformat()
        with self._connect() as conn:
            tail = token[-4:] if token else None
            conn.execute(
                """
                INSERT INTO user_tokens (chat_id, user_id, token, token_tail, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(chat_id) DO UPDATE SET
                    user_id=excluded.user_id,
                    token=excluded.token,
                    token_tail=excluded.token_tail,
                    updated_at=excluded.updated_at
                """,
                (
                    int(chat_id),
                    int(user_id) if user_id is not None else None,
                    token,
                    tail,
                    ts,
                    ts,
                ),
            )
            conn.commit()

    def remove_token(self, chat_id: int) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM user_tokens WHERE chat_id = ?", (int(chat_id),))
            conn.commit()

    def has_token(self, chat_id: int) -> bool:
        return self.get_token(chat_id) is not None

    def list_recipients(self) -> List[int]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT ku.chat_id
                FROM known_users ku
                LEFT JOIN blocked_users bu ON bu.chat_id = ku.chat_id
                WHERE bu.chat_id IS NULL
                """
            ).fetchall()
        recipients: List[int] = []
        for row in rows:
            try:
                if row and row[0] is not None:
                    recipients.append(int(row[0]))
            except (TypeError, ValueError):
                continue
        return recipients

    def upsert_user_profile(
        self,
        chat_id: int,
        user_id: Optional[int],
        username: Optional[str],
        first_name: Optional[str],
        last_name: Optional[str],
        language_code: Optional[str],
        is_bot: bool,
    ) -> None:
        ts = datetime.utcnow().isoformat()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO known_users (chat_id, user_id, username, first_name, last_name, language_code, is_bot, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(chat_id) DO UPDATE SET
                    user_id=excluded.user_id,
                    username=excluded.username,
                    first_name=excluded.first_name,
                    last_name=excluded.last_name,
                    language_code=excluded.language_code,
                    is_bot=excluded.is_bot,
                    updated_at=excluded.updated_at
                """,
                (
                    int(chat_id),
                    int(user_id) if user_id is not None else None,
                    username,
                    first_name,
                    last_name,
                    language_code,
                    1 if is_bot else 0,
                    ts,
                ),
            )
            conn.commit()

    def get_user_profile(self, chat_id: int) -> Optional[sqlite3.Row]:
        with self.connect() as conn:
            cur = conn.execute(
                "SELECT * FROM known_users WHERE chat_id = ?",
                (int(chat_id),),
            )
            return cur.fetchone()

    def log_login(self, chat_id: int, user_id: Optional[int]) -> None:
        ts = datetime.utcnow().isoformat()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO admin_audit (admin_chat_id, target_chat_id, action, details_json, ts)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    int(chat_id),
                    int(chat_id),
                    "login",
                    json.dumps({"user_id": user_id}, ensure_ascii=False),
                    ts,
                ),
            )
            conn.commit()

    def set_admin(self, chat_id: int, role: str) -> None:
        ts = datetime.utcnow().isoformat()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO admins (chat_id, role, created_at)
                VALUES (?, ?, ?)
                ON CONFLICT(chat_id) DO UPDATE SET
                    role = excluded.role
                """,
                (int(chat_id), role, ts),
            )
            conn.commit()

    def remove_admin(self, chat_id: int) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM admins WHERE chat_id = ?", (int(chat_id),))
            conn.commit()

    def is_blocked(self, chat_id: int) -> bool:
        with self._connect() as conn:
            cur = conn.execute(
                "SELECT 1 FROM blocked_users WHERE chat_id = ? LIMIT 1",
                (int(chat_id),),
            )
            return cur.fetchone() is not None

    def set_blocked(
        self,
        chat_id: int,
        by_admin: int,
        blocked: bool,
        reason: Optional[str] = None,
    ) -> None:
        ts = datetime.utcnow().isoformat()
        with self._connect() as conn:
            if blocked:
                conn.execute(
                    """
                    INSERT INTO blocked_users (chat_id, reason, updated_at, by_admin)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(chat_id) DO UPDATE SET
                        reason = excluded.reason,
                        updated_at = excluded.updated_at,
                        by_admin = excluded.by_admin
                    """,
                    (int(chat_id), reason, ts, int(by_admin)),
                )
            else:
                conn.execute(
                    "DELETE FROM blocked_users WHERE chat_id = ?",
                    (int(chat_id),),
                )
            conn.commit()

    def save_admin_note(self, chat_id: int, note: str, by_admin: int) -> None:
        ts = datetime.utcnow().isoformat()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO user_admin_notes (chat_id, note, updated_at, by_admin)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(chat_id) DO UPDATE SET
                    note = excluded.note,
                    updated_at = excluded.updated_at,
                    by_admin = excluded.by_admin
                """,
                (int(chat_id), note, ts, int(by_admin)),
            )
            conn.commit()

    def delete_admin_note(self, chat_id: int) -> None:
        with self._connect() as conn:
            conn.execute(
                "DELETE FROM user_admin_notes WHERE chat_id = ?",
                (int(chat_id),),
            )
            conn.commit()

    def get_admin_note(self, chat_id: int) -> Optional[sqlite3.Row]:
        with self.connect() as conn:
            cur = conn.execute(
                "SELECT * FROM user_admin_notes WHERE chat_id = ?",
                (int(chat_id),),
            )
            return cur.fetchone()

    def log_admin_action(
        self,
        admin_chat_id: int,
        target_chat_id: Optional[int],
        action: str,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        ts = datetime.utcnow().isoformat()
        payload = json.dumps(details or {}, ensure_ascii=False)
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO admin_audit (admin_chat_id, target_chat_id, action, details_json, ts)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    int(admin_chat_id),
                    int(target_chat_id) if target_chat_id is not None else None,
                    action,
                    payload,
                    ts,
                ),
            )
            conn.commit()

    def get_admin_role(self, chat_id: int) -> Optional[str]:
        with self._connect() as conn:
            cur = conn.execute(
                "SELECT role FROM admins WHERE chat_id = ?",
                (int(chat_id),),
            )
            row = cur.fetchone()
            return row[0] if row else None

    def ensure_admin(self, chat_id: int, role: str = "admin") -> None:
        ts = datetime.utcnow().isoformat()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO admins (chat_id, role, created_at) VALUES (?, ?, ?)
                """,
                (int(chat_id), role, ts),
            )
            conn.commit()


__all__ = ["TokenStore"]
