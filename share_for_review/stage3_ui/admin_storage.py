"""SQLite-backed storage for hierarchy, users and unit visibility.

This module is the single source of truth for:
- nodes (подразделения)
- web users and their roles
- unit visibility/archival metadata
- groups (фундамент под «Избранное» и произвольные группы ТС)

The goal is to keep all access/rights-related data in one place, with
simple, predictable migrations and a typed API for the rest of the code.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import Any, Iterable, List, Optional, Sequence, Tuple

import sqlite3
import time

BASE_DIR = Path(__file__).resolve().parents[2]
DB_PATH = BASE_DIR / "data" / "web_admin.sqlite3"


@dataclass(frozen=True)
class Node:
    id: int
    parent_id: Optional[int]
    name: str
    order: int


@dataclass(frozen=True)
class User:
    id: int
    login: str
    display_name: str
    node_id: Optional[int]
    manager_id: Optional[int]
    is_admin: bool
    can_manage_users: bool
    can_manage_units: bool


@dataclass(frozen=True)
class UnitMeta:
    unit_id: int
    owner_node_id: Optional[int]
    is_deleted: bool
    deleted_at: Optional[int]
    deleted_by: Optional[int]
    delete_reason: Optional[str]


class AdminStorage:
    """Thin wrapper over SQLite for hierarchy, users and unit metadata."""

    def __init__(self, db_path: Path | str = DB_PATH) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn: Optional[sqlite3.Connection] = None
        self._lock = Lock()
        self._ensure_connection()
        self._run_migrations()

    def _ensure_connection(self) -> sqlite3.Connection:
        if self._conn is not None:
            return self._conn
        with self._lock:
            if self._conn is None:
                conn = sqlite3.connect(str(self.db_path))
                conn.row_factory = sqlite3.Row
                self._conn = conn
        return self._conn

    # --------------------------- migrations ---------------------------

    def _run_migrations(self) -> None:
        conn = self._ensure_connection()
        cur = conn.cursor()
        # Nodes (подразделения)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS nodes (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              parent_id INTEGER REFERENCES nodes(id) ON DELETE SET NULL,
              name TEXT NOT NULL,
              ord INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS ix_nodes_parent ON nodes(parent_id)")

        # Users (WEB-пользователи)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              login TEXT NOT NULL UNIQUE,
              password_hash TEXT NOT NULL,
              display_name TEXT NOT NULL,
              node_id INTEGER REFERENCES nodes(id) ON DELETE SET NULL,
              manager_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
              is_admin INTEGER NOT NULL DEFAULT 0,
              can_manage_users INTEGER NOT NULL DEFAULT 0,
              can_manage_units INTEGER NOT NULL DEFAULT 0,
              created_at INTEGER NOT NULL,
              updated_at INTEGER NOT NULL
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS ix_users_node ON users(node_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS ix_users_login ON users(login)")

        # Unit metadata (владелец/архив)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS units_meta (
              unit_id INTEGER PRIMARY KEY,
              owner_node_id INTEGER REFERENCES nodes(id) ON DELETE SET NULL,
              is_deleted INTEGER NOT NULL DEFAULT 0,
              deleted_at INTEGER,
              deleted_by INTEGER REFERENCES users(id) ON DELETE SET NULL,
              delete_reason TEXT
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS ix_units_meta_owner ON units_meta(owner_node_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS ix_units_meta_deleted ON units_meta(is_deleted)")

        # Groups and relations (fundament for этап 4)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS groups (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              name TEXT NOT NULL,
              description TEXT,
              is_favorites INTEGER NOT NULL DEFAULT 0,
              owner_user_id INTEGER REFERENCES users(id) ON DELETE SET NULL
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS ix_groups_owner ON groups(owner_user_id)")
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS ix_groups_favorites ON groups(owner_user_id, is_favorites) WHERE is_favorites = 1")

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS unit_groups (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              unit_id INTEGER NOT NULL,
              group_id INTEGER NOT NULL REFERENCES groups(id) ON DELETE CASCADE
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS ix_unit_groups_unit ON unit_groups(unit_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS ix_unit_groups_group ON unit_groups(group_id)")

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS user_groups (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
              group_id INTEGER NOT NULL REFERENCES groups(id) ON DELETE CASCADE
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS ix_user_groups_user ON user_groups(user_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS ix_user_groups_group ON user_groups(group_id)")

        conn.commit()

    # ------------------------------- nodes -------------------------------

    def create_node(self, name: str, parent_id: Optional[int] = None, order: int = 0) -> Node:
        conn = self._ensure_connection()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO nodes (parent_id, name, ord) VALUES (?, ?, ?)",
            (parent_id, name, order),
        )
        node_id = int(cur.lastrowid)
        conn.commit()
        return Node(id=node_id, parent_id=parent_id, name=name, order=order)

    def list_nodes(self) -> List[Node]:
        conn = self._ensure_connection()
        cur = conn.cursor()
        cur.execute("SELECT id, parent_id, name, ord FROM nodes ORDER BY parent_id, ord, id")
        rows = cur.fetchall()
        return [
            Node(
                id=int(r["id"]),
                parent_id=int(r["parent_id"]) if r["parent_id"] is not None else None,
                name=str(r["name"]),
                order=int(r["ord"]),
            )
            for r in rows
        ]

    def rename_node(self, node_id: int, name: str) -> Node:
        """Rename a node and return the updated record."""

        conn = self._ensure_connection()
        cur = conn.cursor()
        cur.execute("UPDATE nodes SET name = ? WHERE id = ?", (name, int(node_id)))
        if cur.rowcount == 0:
            raise ValueError(f"node {node_id} not found")
        conn.commit()
        cur.execute("SELECT id, parent_id, name, ord FROM nodes WHERE id = ?", (int(node_id),))
        row = cur.fetchone()
        if row is None:
            raise ValueError(f"node {node_id} not found after update")
        return Node(
            id=int(row["id"]),
            parent_id=int(row["parent_id"]) if row["parent_id"] is not None else None,
            name=str(row["name"]),
            order=int(row["ord"]),
        )

    # -------------------------- units metadata --------------------------

    def upsert_unit_meta(
        self,
        unit_id: int,
        *,
        owner_node_id: Optional[int],
        is_deleted: bool = False,
        deleted_at: Optional[int] = None,
        deleted_by: Optional[int] = None,
        delete_reason: Optional[str] = None,
    ) -> UnitMeta:
        conn = self._ensure_connection()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO units_meta (unit_id, owner_node_id, is_deleted, deleted_at, deleted_by, delete_reason)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(unit_id) DO UPDATE SET
              owner_node_id=excluded.owner_node_id,
              is_deleted=excluded.is_deleted,
              deleted_at=excluded.deleted_at,
              deleted_by=excluded.deleted_by,
              delete_reason=excluded.delete_reason
            """,
            (
                int(unit_id),
                owner_node_id,
                1 if is_deleted else 0,
                deleted_at,
                deleted_by,
                delete_reason,
            ),
        )
        conn.commit()
        return UnitMeta(
            unit_id=int(unit_id),
            owner_node_id=owner_node_id,
            is_deleted=is_deleted,
            deleted_at=deleted_at,
            deleted_by=deleted_by,
            delete_reason=delete_reason,
        )

    def get_unit_meta_many(self, unit_ids: Sequence[int]) -> List[UnitMeta]:
        if not unit_ids:
            return []
        conn = self._ensure_connection()
        cur = conn.cursor()
        placeholders = ",".join("?" for _ in unit_ids)
        cur.execute(
            f"SELECT unit_id, owner_node_id, is_deleted, deleted_at, deleted_by, delete_reason FROM units_meta WHERE unit_id IN ({placeholders})",
            [int(uid) for uid in unit_ids],
        )
        rows = cur.fetchall()
        metas: List[UnitMeta] = []
        for r in rows:
            metas.append(
                UnitMeta(
                    unit_id=int(r["unit_id"]),
                    owner_node_id=int(r["owner_node_id"]) if r["owner_node_id"] is not None else None,
                    is_deleted=bool(r["is_deleted"]),
                    deleted_at=int(r["deleted_at"]) if r["deleted_at"] is not None else None,
                    deleted_by=int(r["deleted_by"]) if r["deleted_by"] is not None else None,
                    delete_reason=str(r["delete_reason"]) if r["delete_reason"] is not None else None,
                )
            )
        return metas

    # ------------------------------- users -------------------------------

    def create_user(
        self,
        *,
        login: str,
        password_hash: str,
        display_name: str,
        node_id: Optional[int],
        manager_id: Optional[int] = None,
        is_admin: bool = False,
        can_manage_users: bool = False,
        can_manage_units: bool = False,
        ) -> User:
        now = int(time.time())
        conn = self._ensure_connection()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO users (
              login, password_hash, display_name, node_id, manager_id,
              is_admin, can_manage_users, can_manage_units,
              created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                login,
                password_hash,
                display_name,
                node_id,
                manager_id,
                1 if is_admin else 0,
                1 if can_manage_users else 0,
                1 if can_manage_units else 0,
                now,
                now,
            ),
        )
        user_id = int(cur.lastrowid)
        conn.commit()
        return User(
            id=user_id,
            login=login,
            display_name=display_name,
            node_id=node_id,
            manager_id=manager_id,
            is_admin=is_admin,
            can_manage_users=can_manage_users,
            can_manage_units=can_manage_units,
        )

    def list_users(self) -> List[User]:
        conn = self._ensure_connection()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, login, display_name, node_id, manager_id,
                   is_admin, can_manage_users, can_manage_units
            FROM users
            ORDER BY id
            """
        )
        rows = cur.fetchall()
        result: List[User] = []
        for row in rows:
            result.append(
                User(
                    id=int(row["id"]),
                    login=str(row["login"]),
                    display_name=str(row["display_name"]),
                    node_id=int(row["node_id"]) if row["node_id"] is not None else None,
                    manager_id=int(row["manager_id"]) if row["manager_id"] is not None else None,
                    is_admin=bool(row["is_admin"]),
                    can_manage_users=bool(row["can_manage_users"]),
                    can_manage_units=bool(row["can_manage_units"]),
                )
            )
        return result

    def get_user_by_login(self, login: str) -> Optional[User]:
        conn = self._ensure_connection()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, login, display_name, node_id, manager_id,
                   is_admin, can_manage_users, can_manage_units
            FROM users
            WHERE login = ?
            """,
            (login,),
        )
        row = cur.fetchone()
        if row is None:
            return None
        return User(
            id=int(row["id"]),
            login=str(row["login"]),
            display_name=str(row["display_name"]),
            node_id=int(row["node_id"]) if row["node_id"] is not None else None,
            manager_id=int(row["manager_id"]) if row["manager_id"] is not None else None,
            is_admin=bool(row["is_admin"]),
            can_manage_users=bool(row["can_manage_users"]),
            can_manage_units=bool(row["can_manage_units"]),
        )



_STORAGE: Optional[AdminStorage] = None
_STORAGE_LOCK = Lock()


def get_admin_storage() -> AdminStorage:
    """Singleton-like accessor used by web and admin tooling."""

    global _STORAGE
    if _STORAGE is not None:
        return _STORAGE
    with _STORAGE_LOCK:
        if _STORAGE is None:
            _STORAGE = AdminStorage()
    return _STORAGE
