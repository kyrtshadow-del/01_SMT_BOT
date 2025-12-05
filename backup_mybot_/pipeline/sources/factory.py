"""Resolve active data source provider."""

from __future__ import annotations

from pipeline.config.defaults import PipelineConfig

from .base import DataSourceProvider, SourceOverrides
from .wialon import WialonSourceProvider


def get_source_provider(
    config: PipelineConfig, overrides: SourceOverrides | None = None
) -> DataSourceProvider:
    """Return provider implementation for the configured source kind."""

    kind = (config.source_kind or "wialon").lower()
    if kind == "wialon":
        return WialonSourceProvider(config, overrides)
    raise SystemExit(f"Unsupported pipeline source kind: {config.source_kind!r}")
