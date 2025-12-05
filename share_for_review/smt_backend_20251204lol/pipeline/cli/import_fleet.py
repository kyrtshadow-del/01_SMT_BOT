"""Идемпотентный импорт парка устройств в Registry (Postgres).

Формат входа: CSV/TSV/`;` разделённый файл, строки вида
    Unit Name;wialon_ips;861693031184328[;priority]

Поведение:
 - нормализует protocol (lower) и uid (без пробелов);
 - создаёт записи в units/devices при отсутствии;
 - создаёт связи в unit_device_links с приоритетом (cli по умолчанию либо из 4‑го столбца);
 - повторный запуск не создаёт дубликатов.
"""

from __future__ import annotations

import argparse
import csv
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

try:  # psycopg3 — основная зависимость для Registry
    import psycopg
except Exception:  # pragma: no cover - удобное сообщение, если драйвер не установлен
    psycopg = None  # type: ignore


@dataclass
class FleetRow:
    name: str
    protocol: str
    uid: str
    priority: int


def detect_delimiter(path: Path, override: Optional[str]) -> str:
    if override:
        return override
    sample = path.read_text(encoding="utf-8", errors="ignore")[:2048]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t")
        return dialect.delimiter
    except Exception:
        return ";"


def parse_rows(path: Path, delimiter: str, default_priority: int) -> list[FleetRow]:
    rows: list[FleetRow] = []
    with path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.reader(fh, delimiter=delimiter)
        for idx, raw in enumerate(reader, start=1):
            if not raw or all(not (cell or "").strip() for cell in raw):
                continue
            # Пропускаем возможный заголовок
            lowered = [cell.lower() for cell in raw]
            if idx == 1 and any(token in lowered for token in ("protocol", "uid", "unit", "name")):
                continue

            # Нормализуем базовые поля
            try:
                name = (raw[0] or "").strip()
                protocol = (raw[1] or "").strip().lower()
                uid = (raw[2] or "").strip().replace(" ", "")
            except IndexError:
                print(f"[warn] строка {idx}: ожидалось минимум 3 столбца (name;protocol;uid)", file=sys.stderr)
                continue
            if not (name and protocol and uid):
                print(f"[warn] строка {idx}: пустые поля name/protocol/uid, пропускаю", file=sys.stderr)
                continue

            prio = default_priority
            if len(raw) >= 4:
                try:
                    prio = int(str(raw[3]).strip())
                except Exception:
                    print(f"[warn] строка {idx}: priority '{raw[3]}' не int, беру default={default_priority}", file=sys.stderr)

            rows.append(FleetRow(name=name, protocol=protocol, uid=uid, priority=prio))
    return rows


def upsert_unit(cur, name: str) -> tuple[int, bool]:
    cur.execute(
        """
        INSERT INTO units (name)
        VALUES (%s)
        ON CONFLICT DO NOTHING
        RETURNING id
        """,
        (name,),
    )
    row = cur.fetchone()
    if row:
        return int(row[0]), True
    cur.execute("SELECT id FROM units WHERE name = %s ORDER BY id LIMIT 1", (name,))
    found = cur.fetchone()
    if found:
        return int(found[0]), False
    raise RuntimeError("failed to upsert unit")


def upsert_device(cur, protocol: str, uid: str) -> tuple[int, bool]:
    cur.execute(
        """
        INSERT INTO devices (protocol, uid)
        VALUES (%s, %s)
        ON CONFLICT DO NOTHING
        RETURNING id
        """,
        (protocol, uid),
    )
    row = cur.fetchone()
    if row:
        return int(row[0]), True
    cur.execute("SELECT id FROM devices WHERE protocol = %s AND uid = %s", (protocol, uid))
    found = cur.fetchone()
    if found:
        return int(found[0]), False
    raise RuntimeError("failed to upsert device")


def upsert_link(cur, unit_id: int, device_id: int, priority: int) -> bool:
    cur.execute(
        """
        INSERT INTO unit_device_links (unit_id, device_id, priority)
        VALUES (%s, %s, %s)
        ON CONFLICT DO NOTHING
        RETURNING id
        """,
        (unit_id, device_id, priority),
    )
    row = cur.fetchone()
    if row:
        return True
    # Already exists (by UNIQUE on device_id,unit_id,valid_to) or another priority row; treat as idempotent
    return False


def process_rows(dsn: str, rows: Iterable[FleetRow]) -> tuple[int, int, int, int]:
    if psycopg is None:
        raise SystemExit("psycopg не установлен. Установите psycopg[binary] или psycopg2.")

    units_new = devices_new = links_new = 0
    rows_total = 0
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            for row in rows:
                rows_total += 1
                unit_id, was_unit_new = upsert_unit(cur, row.name)
                device_id, was_dev_new = upsert_device(cur, row.protocol, row.uid)
                if was_unit_new:
                    units_new += 1
                if was_dev_new:
                    devices_new += 1
                if upsert_link(cur, unit_id, device_id, row.priority):
                    links_new += 1
        conn.commit()
    return rows_total, units_new, devices_new, links_new


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Импорт парка в Registry (Postgres)")
    p.add_argument("path", type=Path, help="CSV/TSV/; разделённый файл с колонками: name;protocol;uid[;priority]")
    p.add_argument("--dsn", help="Postgres DSN (по умолчанию DEVICE_REGISTRY_DSN/DATABASE_URL)")
    p.add_argument("--delimiter", help="Принудительный разделитель (если не задан, автодетект)")
    p.add_argument("--priority", type=int, default=0, help="Приоритет по умолчанию для unit_device_links")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    dsn = args.dsn or _env_dsn()
    if not dsn:
        raise SystemExit("Не задан DSN. Укажите --dsn или DEVICE_REGISTRY_DSN/DATABASE_URL/PG* env")
    if not args.path.exists():
        raise SystemExit(f"Файл не найден: {args.path}")

    delimiter = detect_delimiter(args.path, args.delimiter)
    rows = parse_rows(args.path, delimiter, args.priority)
    if not rows:
        raise SystemExit("Нет валидных строк для импорта")

    total, units_new, devices_new, links_new = process_rows(dsn, rows)
    print(
        f"done: rows={total} units_new={units_new} devices_new={devices_new} links_new={links_new} dsn={dsn}"
    )


def _env_dsn() -> Optional[str]:
    import os

    return (
        os.getenv("DEVICE_REGISTRY_DSN")
        or os.getenv("DATABASE_URL")
        or _build_pg_dsn_from_env()
    )


def _build_pg_dsn_from_env() -> Optional[str]:
    import os

    host = os.getenv("PGHOST")
    user = os.getenv("PGUSER")
    password = os.getenv("PGPASSWORD")
    dbname = os.getenv("PGDATABASE")
    port = os.getenv("PGPORT", "5432")
    if host and user and dbname:
        pwd = password or ""
        return f"postgresql://{user}:{pwd}@{host}:{port}/{dbname}"
    return None


if __name__ == "__main__":
    main()
