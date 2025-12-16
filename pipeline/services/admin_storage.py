"""PostgreSQL-backed storage for hierarchy, users and unit visibility.

Single source of truth for:
- nodes (подразделения)
- web users and their roles
- unit visibility/archival metadata

SQLite `web_admin.sqlite3` is deprecated; data can be migrated via
`migrate_admin_to_pg.py`. All new code should use this module, which
talks directly to the main PostgreSQL database.
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from threading import Lock
from typing import Any, List, Optional, Sequence

import psycopg
from psycopg.rows import dict_row

log = logging.getLogger("pipeline.admin_storage")


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
    """PostgreSQL wrapper for hierarchy, users and unit metadata."""

    def __init__(self, dsn: str) -> None:
        self.dsn = dsn
        self._init_schema()

    # --------------------------- connections ---------------------------

    def _get_conn(self) -> psycopg.Connection:
        """Create a new connection with dict_row factory."""

        return psycopg.connect(self.dsn, row_factory=dict_row, autocommit=True)

    def _init_schema(self) -> None:
        """Ensure required tables exist.

        DDL is intentionally simple; in production this should be
        replaced by a proper migrations tool, but this keeps dev
        environments self-contained.
        """

        with self._get_conn() as conn:
            cur = conn.cursor()

            # Nodes (подразделения)
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS nodes (
                  id SERIAL PRIMARY KEY,
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
                  id SERIAL PRIMARY KEY,
                  login TEXT NOT NULL UNIQUE,
                  password_hash TEXT NOT NULL,
                  display_name TEXT NOT NULL,
                  node_id INTEGER REFERENCES nodes(id) ON DELETE SET NULL,
                  manager_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
                  is_admin BOOLEAN NOT NULL DEFAULT FALSE,
                  can_manage_users BOOLEAN NOT NULL DEFAULT FALSE,
                  can_manage_units BOOLEAN NOT NULL DEFAULT FALSE,
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
                  is_deleted BOOLEAN NOT NULL DEFAULT FALSE,
                  deleted_at INTEGER,
                  deleted_by INTEGER REFERENCES users(id) ON DELETE SET NULL,
                  delete_reason TEXT
                )
                """
            )
            cur.execute("CREATE INDEX IF NOT EXISTS ix_units_meta_owner ON units_meta(owner_node_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS ix_units_meta_deleted ON units_meta(is_deleted)")

    # ------------------------------- nodes -------------------------------

    def create_node(self, name: str, parent_id: Optional[int] = None, order: int = 0) -> Node:
        with self._get_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO nodes (parent_id, name, ord) VALUES (%s, %s, %s) RETURNING id",
                (parent_id, name, order),
            )
            row = cur.fetchone()
        node_id = int(row["id"])
        return Node(id=node_id, parent_id=parent_id, name=name, order=order)

    def list_nodes(self) -> List[Node]:
        with self._get_conn() as conn:
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

        with self._get_conn() as conn:
            cur = conn.cursor()
            cur.execute("UPDATE nodes SET name = %s WHERE id = %s RETURNING id, parent_id, name, ord", (name, node_id))
            row = cur.fetchone()
            if row is None:
                raise ValueError(f"node {node_id} not found")
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
        with self._get_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO units_meta (unit_id, owner_node_id, is_deleted, deleted_at, deleted_by, delete_reason)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT(unit_id) DO UPDATE SET
                  owner_node_id=EXCLUDED.owner_node_id,
                  is_deleted=EXCLUDED.is_deleted,
                  deleted_at=EXCLUDED.deleted_at,
                  deleted_by=EXCLUDED.deleted_by,
                  delete_reason=EXCLUDED.delete_reason
                """,
                (int(unit_id), owner_node_id, is_deleted, deleted_at, deleted_by, delete_reason),
            )
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
        with self._get_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT unit_id, owner_node_id, is_deleted, deleted_at, deleted_by, delete_reason
                  FROM units_meta
                 WHERE unit_id = ANY(%s)
                """,
                (list(unit_ids),),
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
        with self._get_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO users (
                  login,
                  password_hash,
                  display_name,
                  node_id,
                  manager_id,
                  is_admin,
                  can_manage_users,
                  can_manage_units,
                  created_at,
                  updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    login,
                    password_hash,
                    display_name,
                    node_id,
                    manager_id,
                    is_admin,
                    can_manage_users,
                    can_manage_units,
                    now,
                    now,
                ),
            )
            row = cur.fetchone()
        user_id = int(row["id"])
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
        with self._get_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT id,
                       login,
                       display_name,
                       node_id,
                       manager_id,
                       is_admin,
                       can_manage_users,
                       can_manage_units
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
        with self._get_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT id,
                       login,
                       display_name,
                       node_id,
                       manager_id,
                       is_admin,
                       can_manage_users,
                       can_manage_units
                  FROM users
                 WHERE login = %s
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

    def get_user_auth(self, login: str) -> Optional[tuple[User, str]]:
        """Return (User, password_hash) pair for auth logic."""

        with self._get_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT id,
                       login,
                       display_name,
                       node_id,
                       manager_id,
                       is_admin,
                       can_manage_users,
                       can_manage_units,
                       password_hash
                  FROM users
                 WHERE login = %s
                """,
                (login,),
            )
            row = cur.fetchone()
        if row is None:
            return None
        user = User(
            id=int(row["id"]),
            login=str(row["login"]),
            display_name=str(row["display_name"]),
            node_id=int(row["node_id"]) if row["node_id"] is not None else None,
            manager_id=int(row["manager_id"]) if row["manager_id"] is not None else None,
            is_admin=bool(row["is_admin"]),
            can_manage_users=bool(row["can_manage_users"]),
            can_manage_units=bool(row["can_manage_units"]),
        )
        return user, str(row["password_hash"])


_STORAGE: Optional[AdminStorage] = None
_STORAGE_LOCK = Lock()


def _dsn_from_env() -> str:
    dsn = os.getenv("DEVICE_REGISTRY_DSN") or os.getenv("DATABASE_URL")
    if dsn:
        return dsn
    host = os.getenv("PGHOST", "127.0.0.1")
    user = os.getenv("PGUSER", "smt_user")
    password = os.getenv("PGPASSWORD", "smt_password")
    dbname = os.getenv("PGDATABASE", "smt_telematics")
    port = os.getenv("PGPORT", "5432")
    return f"postgresql://{user}:{password or ''}@{host}:{port}/{dbname}"


def get_admin_storage() -> AdminStorage:
    """Singleton-like accessor used by web and admin tooling."""

    global _STORAGE
    if _STORAGE is not None:
        return _STORAGE
    with _STORAGE_LOCK:
        if _STORAGE is None:
            dsn = _dsn_from_env()
            _STORAGE = AdminStorage(dsn=dsn)
    return _STORAGE


__all__ = ["AdminStorage", "Node", "User", "UnitMeta", "get_admin_storage"]

