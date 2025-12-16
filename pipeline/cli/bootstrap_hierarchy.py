"""CLI helpers to bootstrap hierarchy and unit ownership.

Usage examples (from project root):

    source .venv/bin/activate
    PYTHONPATH=. python3 -m pipeline.cli.bootstrap_hierarchy --ensure-root "Компания"

    PYTHONPATH=. python3 -m pipeline.cli.bootstrap_hierarchy --set-owner 1001,1002 --node-id 1

Эти команды не используются рантаймом напрямую, но помогают быстро
заполнить таблицы иерархии/прав в Postgres без ручного редактирования БД.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pipeline.services.admin_storage import get_admin_storage
from pipeline.services.passwords import hash_password


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bootstrap and manage hierarchy, users and unit ownership.")
    parser.add_argument(
        "--ensure-root",
        help='Create root node if there are no nodes yet (default name: "Компания").',
        nargs="?",
        const="Компания",
    )
    parser.add_argument(
        "--set-owner",
        help="Comma-separated unit IDs to assign to a node.",
    )
    parser.add_argument(
        "--node-id",
        type=int,
        help="Target node_id for --set-owner or --create-user (optional).",
    )
    parser.add_argument(
        "--create-user",
        help="Create a web user with given login.",
    )
    parser.add_argument(
        "--password",
        help="Plaintext password for --create-user (for bootstrap only).",
    )
    parser.add_argument(
        "--display-name",
        help="Display name for --create-user (default: login).",
    )
    parser.add_argument(
        "--admin",
        action="store_true",
        help="Grant admin rights to the created user.",
    )
    parser.add_argument(
        "--list-nodes",
        action="store_true",
        help="Print all nodes (id, parent_id, name, order).",
    )
    parser.add_argument(
        "--rename-node",
        type=int,
        help="Node id to rename.",
    )
    parser.add_argument(
        "--new-name",
        help="New name for --rename-node.",
    )
    return parser.parse_args(argv)


def cmd_ensure_root(name: str) -> None:
    storage = get_admin_storage()
    nodes = storage.list_nodes()
    if nodes:
        return
    storage.create_node(name=name, parent_id=None, order=0)


def cmd_set_owner(unit_ids_raw: str, node_id: int) -> None:
    storage = get_admin_storage()
    try:
        unit_ids = [int(x) for x in unit_ids_raw.replace("\n", ",").split(",") if x.strip()]
    except ValueError as exc:
        raise SystemExit(f"Invalid unit id in --set-owner: {exc}") from exc
    for uid in unit_ids:
        storage.upsert_unit_meta(unit_id=uid, owner_node_id=node_id)


def cmd_create_user(login: str, password: str, display_name: str, node_id: int | None, is_admin: bool) -> None:
    storage = get_admin_storage()
    pwd_hash = hash_password(password)
    storage.create_user(
        login=login,
        password_hash=pwd_hash,
        display_name=display_name,
        node_id=node_id,
        is_admin=is_admin,
        can_manage_units=is_admin,
        can_manage_users=is_admin,
    )


def cmd_list_nodes() -> None:
    storage = get_admin_storage()
    nodes = storage.list_nodes()
    if not nodes:
        print("no nodes")
        return
    for n in nodes:
        print(f"id={n.id} parent_id={n.parent_id} name={n.name!r} order={n.order}")


def cmd_rename_node(node_id: int, new_name: str) -> None:
    storage = get_admin_storage()
    node = storage.rename_node(node_id, new_name)
    print(f"renamed node {node_id} -> name={node.name!r}")


def main(argv: list[str] | None = None) -> None:
    ns = parse_args(argv or sys.argv[1:])
    if ns.ensure_root:
        cmd_ensure_root(ns.ensure_root)
    if ns.set_owner:
        if not ns.node_id:
            raise SystemExit("--node-id is required when using --set-owner")
        cmd_set_owner(ns.set_owner, ns.node_id)
    if ns.create_user:
        if not ns.password:
            raise SystemExit("--password is required when using --create-user")
        display_name = ns.display_name or ns.create_user
        cmd_create_user(ns.create_user, ns.password, display_name, ns.node_id, ns.admin)
    if ns.list_nodes:
        cmd_list_nodes()
    if ns.rename_node is not None:
        if not ns.new_name:
            raise SystemExit("--new-name is required when using --rename-node")
        cmd_rename_node(ns.rename_node, ns.new_name)


if __name__ == "__main__":
    main()
