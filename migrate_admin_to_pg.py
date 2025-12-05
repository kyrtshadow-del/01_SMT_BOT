"""
One-time migration of admin data (users/nodes/units_meta) from SQLite to Postgres.

Usage:
    PYTHONPATH=. .venv/bin/python migrate_admin_to_pg.py
"""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

SQLITE_PATH = Path("data/web_admin.sqlite3")
PG_DSN = os.getenv("DEVICE_REGISTRY_DSN") or os.getenv("DATABASE_URL") or "postgresql://smt_user:smt_password@127.0.0.1:5432/smt_telematics"


def migrate() -> None:
    if not SQLITE_PATH.exists():
        print("SQLite base not found, skipping migration.")
        return

    print(f"Migrating from {SQLITE_PATH} to Postgres...")

    sq = sqlite3.connect(str(SQLITE_PATH))
    sq.row_factory = sqlite3.Row
    pg = psycopg.connect(PG_DSN, row_factory=dict_row)

    try:
        users = sq.execute("SELECT * FROM users").fetchall()
        nodes = sq.execute("SELECT * FROM nodes").fetchall()
        metas = sq.execute("SELECT * FROM units_meta").fetchall()

        with pg.cursor() as cur:
            # Nodes
            for n in nodes:
                cur.execute(
                    """
                    INSERT INTO nodes (id, parent_id, name, ord)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (id) DO NOTHING
                    """,
                    (n["id"], n["parent_id"], n["name"], n["ord"]),
                )
            cur.execute("SELECT setval('nodes_id_seq', (SELECT COALESCE(MAX(id),1) FROM nodes))")

            # Users
            for u in users:
                cur.execute(
                    """
                    INSERT INTO users (id, login, password_hash, display_name, node_id, manager_id,
                                       is_admin, can_manage_users, can_manage_units, created_at, updated_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (id) DO NOTHING
                    """,
                    (
                        u["id"],
                        u["login"],
                        u["password_hash"],
                        u["display_name"],
                        u["node_id"],
                        u["manager_id"],
                        bool(u["is_admin"]),
                        bool(u["can_manage_users"]),
                        bool(u["can_manage_units"]),
                        u["created_at"],
                        u["updated_at"],
                    ),
                )
            cur.execute("SELECT setval('users_id_seq', (SELECT COALESCE(MAX(id),1) FROM users))")

            # Units meta
            for m in metas:
                cur.execute(
                    """
                    INSERT INTO units_meta (unit_id, owner_node_id, is_deleted, deleted_at, deleted_by, delete_reason)
                    VALUES (%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (unit_id) DO NOTHING
                    """,
                    (
                        m["unit_id"],
                        m["owner_node_id"],
                        bool(m["is_deleted"]),
                        m["deleted_at"],
                        m["deleted_by"],
                        m["delete_reason"],
                    ),
                )

        pg.commit()
        print("Migration complete.")
    except Exception as exc:  # pragma: no cover - defensive
        pg.rollback()
        print(f"Migration error: {exc}")
    finally:
        sq.close()
        pg.close()


if __name__ == "__main__":
    migrate()
