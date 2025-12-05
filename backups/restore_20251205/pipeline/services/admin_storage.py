"""Postgres-backed storage for hierarchy, users and unit visibility."""

from __future__ import annotations

from dataclasses import dataclass
from threading import Lock
from typing import Any, List, Optional, Sequence

import os
import time

try:
    import psycopg
    from psycopg.rows import dict_row
except Exception as exc:  # pragma: no cover - defensive
    psycopg = None
    dict_row = None

DB_DSN = os.getenv("DEVICE_REGISTRY_DSN") or os.getenv("DATABASE_URL")


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
    """Thin wrapper over Postgres for hierarchy, users and unit metadata."""

    def __init__(self, dsn: Optional[str] = None) -> None:
        if psycopg is None:
            raise RuntimeError("psycopg is required for AdminStorage")
        self.dsn = dsn or DB_DSN or "postgresql://smt_user:smt_password@127.0.0.1:5432/smt_telematics"
        self._lock = Lock()
        self._ensure_schema()

    # --------------------------- connections ---------------------------
    def _conn(self):
        return psycopg.connect(self.dsn, row_factory=dict_row)

    # --------------------------- migrations ---------------------------
    def _ensure_schema(self) -> None:
        with self._conn() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS nodes (
                    id SERIAL PRIMARY KEY,
                    parent_id INTEGER REFERENCES nodes(id) ON DELETE SET NULL,
                    name VARCHAR(255) NOT NULL,
                    ord INTEGER NOT NULL DEFAULT 0
                )
                """
            )
            cur.execute("CREATE INDEX IF NOT EXISTS ix_nodes_parent ON nodes(parent_id)")

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY,
                    login VARCHAR(255) NOT NULL UNIQUE,
                    password_hash VARCHAR(255) NOT NULL,
                    display_name VARCHAR(255) NOT NULL,
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

            # groups, unit_groups, user_groups оставляем для совместимости структуры
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS groups (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    description TEXT,
                    is_favorites BOOLEAN NOT NULL DEFAULT FALSE,
                    owner_user_id INTEGER REFERENCES users(id) ON DELETE SET NULL
                )
                """
            )
            cur.execute("CREATE INDEX IF NOT EXISTS ix_groups_owner ON groups(owner_user_id)")
            cur.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS ix_groups_favorites
                ON groups(owner_user_id, is_favorites) WHERE is_favorites = TRUE
                """
            )

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS unit_groups (
                    id SERIAL PRIMARY KEY,
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
                    id SERIAL PRIMARY KEY,
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
        with self._conn() as conn:
            cur = conn.execute(
                "INSERT INTO nodes (parent_id, name, ord) VALUES (%s, %s, %s) RETURNING id",
                (parent_id, name, order),
            )
            node_id = int(cur.fetchone()["id"])
            return Node(id=node_id, parent_id=parent_id, name=name, order=order)

    def list_nodes(self) -> List[Node]:
        with self._conn() as conn:
            rows = conn.execute("SELECT id, parent_id, name, ord FROM nodes ORDER BY parent_id, ord, id").fetchall()
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
        with self._conn() as conn:
            cur = conn.execute("UPDATE nodes SET name=%s WHERE id=%s", (name, int(node_id)))
            if cur.rowcount == 0:
                raise ValueError(f"node {node_id} not found")
            row = conn.execute("SELECT id, parent_id, name, ord FROM nodes WHERE id=%s", (int(node_id),)).fetchone()
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
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO units_meta (unit_id, owner_node_id, is_deleted, deleted_at, deleted_by, delete_reason)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (unit_id) DO UPDATE SET
                  owner_node_id = EXCLUDED.owner_node_id,
                  is_deleted = EXCLUDED.is_deleted,
                  deleted_at = EXCLUDED.deleted_at,
                  deleted_by = EXCLUDED.deleted_by,
                  delete_reason = EXCLUDED.delete_reason
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
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT unit_id, owner_node_id, is_deleted, deleted_at, deleted_by, delete_reason FROM units_meta WHERE unit_id = ANY(%s)",
                (list(int(uid) for uid in unit_ids),),
            ).fetchall()
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
        with self._conn() as conn:
            cur = conn.execute(
                """
                INSERT INTO users (
                  login, password_hash, display_name, node_id, manager_id,
                  is_admin, can_manage_users, can_manage_units,
                  created_at, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id, login, display_name, node_id, manager_id, is_admin, can_manage_users, can_manage_units
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
            return User(
                id=int(row["id"]),
                login=row["login"],
                display_name=row["display_name"],
                node_id=row["node_id"],
                manager_id=row["manager_id"],
                is_admin=row["is_admin"],
                can_manage_users=row["can_manage_users"],
                can_manage_units=row["can_manage_units"],
            )

    def list_users(self) -> List[User]:
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT id, login, display_name, node_id, manager_id,
                       is_admin, can_manage_users, can_manage_units
                FROM users
                ORDER BY id
                """
            ).fetchall()
            return [
                User(
                    id=int(r["id"]),
                    login=str(r["login"]),
                    display_name=str(r["display_name"]),
                    node_id=int(r["node_id"]) if r["node_id"] is not None else None,
                    manager_id=int(r["manager_id"]) if r["manager_id"] is not None else None,
                    is_admin=bool(r["is_admin"]),
                    can_manage_users=bool(r["can_manage_users"]),
                    can_manage_units=bool(r["can_manage_units"]),
                )
                for r in rows
            ]

    def get_user_by_login(self, login: str) -> Optional[User]:
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT id, login, display_name, node_id, manager_id,
                       is_admin, can_manage_users, can_manage_units
                FROM users
                WHERE login = %s
                """,
                (login,),
            ).fetchone()
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
