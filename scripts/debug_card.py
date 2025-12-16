#!/usr/bin/env python3
"""
Мини-диагностика карточки (тот же путь, что у бота):
- берёт unit_id (аргументом или первый из индекса),
- создаёт WialonClient на основе env/config,
- вызывает get_unit_full_for_stats,
- печатает основные поля (имя, pos/lmsg, sens, params).

Запуск:
PYTHONPATH=. python scripts/debug_card.py [unit_id]
"""

import asyncio
import os
from typing import Any, Dict

from bot.wialon_client import WialonClient
from bot_new import load_pipeline_config
from pipeline.services.unit_index import get_unit_index


def pick_unit_id() -> int:
    idx = get_unit_index()
    # Берём первый юнит из индексированных (по БД) объектов
    units = idx.search("", limit=1)
    if not units:
        raise RuntimeError("unit_index пуст — нет локальных юнитов")
    first = units[0]
    return int(first["id"])


async def main() -> None:
    import sys

    unit_id = int(sys.argv[1]) if len(sys.argv) > 1 else pick_unit_id()

    cfg = load_pipeline_config()
    token = os.getenv("PIPELINE_WIALON_TOKEN")
    if not token:
        raise RuntimeError("PIPELINE_WIALON_TOKEN не задан")

    client = WialonClient(cfg.wialon_host, token)
    item: Dict[str, Any] = await asyncio.get_running_loop().run_in_executor(
        None, client.get_unit_full_for_stats, unit_id
    )

    name = item.get("nm") or f"id {unit_id}"
    pos = item.get("pos") or {}
    lmsg = item.get("lmsg") or {}
    sens = item.get("sens") or []
    params = item.get("prms") or item.get("params") or {}

    print(f"unit_id={unit_id} name={name}")
    print(f"pos keys={list(pos.keys())} lmsg keys={list(lmsg.keys())}")
    print(f"sensors={len(sens)} params={len(params)}")
    if params:
        keys = list(params.keys())[:5]
        preview = {k: params[k] for k in keys}
        print(f"params sample={preview}")


if __name__ == "__main__":
    asyncio.run(main())
