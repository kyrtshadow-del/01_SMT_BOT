"""Manage pipeline storage (list, delete, archive days)."""

from __future__ import annotations

import argparse
import shutil
from datetime import datetime
from pathlib import Path

from pipeline.services.storage_service import get_pipeline_storage_service


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Pipeline storage maintenance")
    parser.add_argument("--list", action="store_true", help="List available days and their size")
    parser.add_argument("--delete-day", action="append", help="Delete specific day (YYYY-MM-DD)")
    parser.add_argument("--delete-before", help="Delete days older than the provided date (YYYY-MM-DD)")
    parser.add_argument("--archive-day", action="append", help="Zip a day directory into storage_root day.zip")
    return parser.parse_args()


def human_size(num: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if num < 1024:
            return f"{num:.1f}{unit}"
        num /= 1024
    return f"{num:.1f}TB"


def list_days(storage_root: Path) -> None:
    print(f"Storage root: {storage_root}")
    for day_dir in sorted(p for p in storage_root.iterdir() if p.is_dir()):
        size = sum(f.stat().st_size for f in day_dir.rglob("*"))
        print(f"{day_dir.name}: {human_size(size)}")


def delete_day(storage_root: Path, day: str) -> None:
    target = storage_root / day
    if not target.exists():
        print(f"[skip] {day}: not found")
        return
    shutil.rmtree(target)
    print(f"[deleted] {day}")


def archive_day(storage_root: Path, day: str) -> None:
    target = storage_root / day
    if not target.exists():
        print(f"[skip] {day}: not found")
        return
    archive_path = storage_root / f"{day}.zip"
    if archive_path.exists():
        print(f"[skip] {archive_path.name}: already exists")
        return
    shutil.make_archive(str(archive_path.with_suffix("")), "zip", root_dir=storage_root, base_dir=day)
    print(f"[archived] {day} -> {archive_path.name}")


def main() -> None:
    args = parse_args()
    service = get_pipeline_storage_service()
    root = service.storage_root

    if args.list:
        list_days(root)

    if args.delete_before:
        cutoff = datetime.fromisoformat(args.delete_before).date()
        for day_dir in sorted(p for p in root.iterdir() if p.is_dir()):
            try:
                day = datetime.fromisoformat(day_dir.name).date()
            except ValueError:
                continue
            if day < cutoff:
                delete_day(root, day_dir.name)

    if args.delete_day:
        for day in args.delete_day:
            delete_day(root, day)

    if args.archive_day:
        for day in args.archive_day:
            archive_day(root, day)


if __name__ == "__main__":
    main()
