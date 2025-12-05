"""Central place for pipeline configuration defaults."""

from __future__ import annotations

from dataclasses import dataclass, field
import os
from pathlib import Path


@dataclass(frozen=True)
class PipelineConfig:
    storage_root: Path
    source_kind: str = "wialon"
    wialon_token: str | None = None
    wialon_host: str = "https://hosting.wialon-online.ru"
    wialon_extra_tokens: tuple[str, ...] = field(default_factory=tuple)
    wialon_parallel_limit: int = 4
    wialon_rotate_after_units: int = 300
    wialon_token_cooldown: int = 120
    poll_active_interval: float = 2.0
    poll_warm_interval: float = 10.0
    poll_cold_interval: float = 45.0
    poll_idle_interval: float = 180.0
    poll_active_age: int = 60
    poll_warm_age: int = 600
    poll_empty_backoff: float = 15.0
    poll_jitter: float = 0.15
    poll_batch_size: int = 512
    # Galileosky ingestion (HTTP/TCP gateway)
    galileosky_host: str = "0.0.0.0"
    galileosky_port: int = 8088
    galileosky_auth_token: str | None = None
    # Wialon IPS retranslator (TCP)
    wialon_ips_host: str = "0.0.0.0"
    wialon_ips_port: int = 8088
    wialon_ips_password: str | None = None


def load_from_env() -> PipelineConfig:
    """Tiny loader; later можно расширить (YAML, CLI args)."""

    root = Path(
        os.getenv("PIPELINE_STORAGE_ROOT", "data/pipeline_storage")
    ).resolve()
    return PipelineConfig(
        storage_root=root,
        source_kind=os.getenv("PIPELINE_SOURCE_KIND", "wialon"),
        wialon_token=os.getenv("PIPELINE_WIALON_TOKEN") or os.getenv("WIALON_TOKEN"),
        wialon_host=os.getenv(
            "PIPELINE_WIALON_HOST",
            os.getenv("WIALON_LOGIN_HOST", "https://hosting.wialon-online.ru"),
        ),
        wialon_extra_tokens=_parse_tokens(
            os.getenv("PIPELINE_WIALON_EXTRA_TOKENS")
            or os.getenv("WIALON_EXTRA_TOKENS", "")
        ),
        wialon_parallel_limit=_parse_int(
            os.getenv("PIPELINE_WIALON_PARALLEL_LIMIT")
            or os.getenv("WIALON_PARALLEL_TOKEN_LIMIT"),
            default=4,
        ),
        wialon_rotate_after_units=_parse_int(
            os.getenv("PIPELINE_WIALON_ROTATE_AFTER_UNITS") or os.getenv("WIALON_TOKEN_ROTATE_AFTER_UNITS"),
            default=300,
        ),
        wialon_token_cooldown=_parse_int(
            os.getenv("PIPELINE_WIALON_TOKEN_COOLDOWN_SECONDS") or os.getenv("WIALON_TOKEN_COOLDOWN_SECONDS"),
            default=120,
        ),
        poll_active_interval=_parse_float(os.getenv("PIPELINE_POLL_ACTIVE_INTERVAL_SEC"), 2.0),
        poll_warm_interval=_parse_float(os.getenv("PIPELINE_POLL_WARM_INTERVAL_SEC"), 10.0),
        poll_cold_interval=_parse_float(os.getenv("PIPELINE_POLL_COLD_INTERVAL_SEC"), 45.0),
        poll_idle_interval=_parse_float(os.getenv("PIPELINE_POLL_IDLE_INTERVAL_SEC"), 180.0),
        poll_active_age=_parse_int(os.getenv("PIPELINE_POLL_ACTIVE_AGE_SEC"), 60),
        poll_warm_age=_parse_int(os.getenv("PIPELINE_POLL_WARM_AGE_SEC"), 600),
        poll_empty_backoff=_parse_float(os.getenv("PIPELINE_POLL_EMPTY_BACKOFF_SEC"), 15.0),
        poll_jitter=_parse_float(os.getenv("PIPELINE_POLL_JITTER"), 0.15),
        poll_batch_size=_parse_int(os.getenv("PIPELINE_POLL_BATCH_SIZE"), 512),
        galileosky_host=os.getenv("PIPELINE_GALILEOSKY_HOST", "0.0.0.0"),
        galileosky_port=_parse_int(os.getenv("PIPELINE_GALILEOSKY_PORT"), 8088),
        galileosky_auth_token=os.getenv("PIPELINE_GALILEOSKY_AUTH_TOKEN"),
        wialon_ips_host=os.getenv("PIPELINE_WIALON_IPS_HOST", "0.0.0.0"),
        wialon_ips_port=_parse_int(os.getenv("PIPELINE_WIALON_IPS_PORT"), 18081),
        wialon_ips_password=os.getenv("PIPELINE_WIALON_IPS_PASSWORD"),
    )


def _parse_tokens(raw: str) -> tuple[str, ...]:
    if not raw:
        return ()
    tokens = []
    for chunk in raw.replace(",", " ").split():
        token = chunk.strip()
        if token:
            tokens.append(token)
    return tuple(tokens)


def _parse_int(raw: str | None, default: int) -> int:
    if raw is None or not raw.strip():
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _parse_float(raw: str | None, default: float) -> float:
    if raw is None or not raw.strip():
        return default
    try:
        return float(raw)
    except ValueError:
        return default
