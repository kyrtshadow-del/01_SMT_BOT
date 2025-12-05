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
