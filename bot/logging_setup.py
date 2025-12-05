from __future__ import annotations

import logging
import logging.handlers
import os
from dataclasses import dataclass
from pathlib import Path


_APP_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
_SIMPLE_FORMAT = "%(asctime)s | %(levelname)s | %(message)s"
_ROTATING_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"


@dataclass(frozen=True)
class BotLoggers:
    app: logging.Logger
    cancel: logging.Logger
    nearby: logging.Logger
    dut_cal: logging.Logger
    drain_cache: logging.Logger
    message_cache_store: logging.Logger
    concurrency: logging.Logger
    message_load: logging.Logger
    message_calc: logging.Logger
    token_rotation: logging.Logger
    geo: logging.Logger
    dut: logging.Logger
    detector: logging.Logger
    drain: logging.Logger
    card: logging.Logger
    snapshot: logging.Logger


def setup_logging(log_dir: Path) -> BotLoggers:
    """Configure all bot loggers and return the instances."""

    log_dir = Path(log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)

    base_handler = logging.FileHandler(log_dir / "bot.log", encoding="utf-8")
    logging.basicConfig(level=logging.INFO, format=_APP_FORMAT, handlers=[base_handler, logging.StreamHandler()])

    app_logger = logging.getLogger("wialon_cf_bot")

    cancel_logger = _simple_file_logger("cancel_trace", log_dir / "cancel.log")
    nearby_logger = _simple_file_logger("nearby_diag", log_dir / "nearby.log")
    dut_cal_logger = _simple_file_logger("dut_cal_trace", log_dir / "dut_cal.log")
    drain_cache_logger = _simple_file_logger("drain_cache_watch", log_dir / "drain_cache.log", level=logging.INFO)
    message_cache_store_logger = _simple_file_logger(
        "message_cache_store", log_dir / "message_cache_store.log", level=logging.INFO
    )
    concurrency_logger = _simple_file_logger("concurrency_control", log_dir / "concurrency_control.log", level=logging.INFO)
    message_load_logger = _simple_file_logger("message_load", log_dir / "message_load.log", level=logging.INFO)
    message_calc_logger = _simple_file_logger("message_calc", log_dir / "message_calc.log", level=logging.INFO)
    token_rotation_logger = _simple_file_logger("token_rotation", log_dir / "token_rotation.log", level=logging.INFO)

    geo_logger = _rotating_logger(
        "geo",
        log_dir / "geozones.log",
        level_name=os.getenv("GEO_LOG_LEVEL", "INFO"),
        backup_count=7,
    )
    dut_logger = _rotating_logger(
        "dut_report",
        log_dir / "dut_report.log",
        level_name=os.getenv("DUT_LOG_LEVEL", "INFO"),
        backup_count=7,
    )

    detector_logger = logging.getLogger("detector")
    detector_logger.propagate = False
    if not detector_logger.handlers:
        for handler in dut_logger.handlers:
            detector_logger.addHandler(handler)

    drain_logger = _rotating_logger(
        "drain_debug",
        log_dir / "drain_debug.log",
        level_name=os.getenv("DRAIN_LOG_LEVEL", "DEBUG"),
        backup_count=14,
    )
    for handler in drain_logger.handlers:
        if handler not in detector_logger.handlers:
            detector_logger.addHandler(handler)
    detector_logger.setLevel(_coerce_level(os.getenv("DETECTOR_LOG_LEVEL", "DEBUG"), logging.DEBUG))

    card_logger = _rotating_logger(
        "card_perf",
        log_dir / "card_perf.log",
        level_name=os.getenv("CARD_LOG_LEVEL", "INFO"),
        backup_count=14,
    )
    snapshot_logger = _rotating_logger(
        "unit_snapshot_watch",
        log_dir / "unit_snapshot_watch.log",
        level_name=os.getenv("SNAPSHOT_LOG_LEVEL", "INFO"),
        backup_count=14,
    )

    return BotLoggers(
        app=app_logger,
        cancel=cancel_logger,
        nearby=nearby_logger,
        dut_cal=dut_cal_logger,
        drain_cache=drain_cache_logger,
        message_cache_store=message_cache_store_logger,
        concurrency=concurrency_logger,
        message_load=message_load_logger,
        message_calc=message_calc_logger,
        token_rotation=token_rotation_logger,
        geo=geo_logger,
        dut=dut_logger,
        detector=detector_logger,
        drain=drain_logger,
        card=card_logger,
        snapshot=snapshot_logger,
    )


def _simple_file_logger(name: str, path: Path, *, level: int = logging.INFO) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.propagate = False
    if not logger.handlers:
        path.parent.mkdir(parents=True, exist_ok=True)
        handler = logging.FileHandler(path, encoding="utf-8")
        handler.setFormatter(logging.Formatter(_SIMPLE_FORMAT))
        logger.addHandler(handler)
    logger.setLevel(level)
    return logger


def _rotating_logger(
    name: str,
    path: Path,
    *,
    level_name: str,
    backup_count: int,
    when: str = "midnight",
    fmt: str = _ROTATING_FORMAT,
) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.propagate = False
    if not logger.handlers:
        path.parent.mkdir(parents=True, exist_ok=True)
        handler = logging.handlers.TimedRotatingFileHandler(
            path,
            when=when,
            backupCount=backup_count,
            encoding="utf-8",
        )
        handler.setFormatter(logging.Formatter(fmt))
        logger.addHandler(handler)
    logger.setLevel(_coerce_level(level_name, logging.INFO))
    return logger


def _coerce_level(value: str | int, default: int) -> int:
    if isinstance(value, int):
        return value
    try:
        return getattr(logging, str(value).upper())
    except Exception:
        return default


__all__ = ["BotLoggers", "setup_logging"]
