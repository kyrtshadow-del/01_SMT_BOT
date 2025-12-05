"""Wialon-specific source provider."""

from __future__ import annotations

import logging
from typing import Sequence

from pipeline.adapters.base import HistoryAdapter, StreamAdapter
from pipeline.adapters.wialon.history import WialonHistoryAdapter
from pipeline.adapters.wialon.stream import WialonStreamAdapter
from pipeline.config.defaults import PipelineConfig
from pipeline.storage.raw_storage import RawStorage
from pipeline.storage.latest_metrics import LatestTelemetryStore

from .base import DataSourceProvider, SourceOverrides
from .wialon_pool import PooledWialonClient, WialonTokenPool

log = logging.getLogger(__name__)


class WialonSourceProvider(DataSourceProvider):
    """Factory that wires pipeline adapters to the Wialon Remote API."""

    def __init__(self, config: PipelineConfig, overrides: SourceOverrides | None = None) -> None:
        self.config = config
        self.overrides = overrides or SourceOverrides()
        self._pool: WialonTokenPool | None = None

    def make_history_adapter(self, raw_storage: RawStorage) -> HistoryAdapter:
        client = self._make_client_proxy()
        return WialonHistoryAdapter(client=client, raw_storage=raw_storage)

    def make_stream_adapter(
        self,
        raw_storage: RawStorage,
        unit_ids: Sequence[int],
        poll_interval: float,
        metrics_interval: int,
        latest_store: LatestTelemetryStore | None = None,
    ) -> StreamAdapter:
        client = self._make_client_proxy()
        return WialonStreamAdapter(
            client=client,
            raw_storage=raw_storage,
            unit_ids=unit_ids,
            poll_interval=poll_interval,
            metrics_interval=metrics_interval,
            latest_store=latest_store,
        )

    def _make_client_proxy(self) -> PooledWialonClient:
        pool = self._ensure_pool()
        return PooledWialonClient(pool)

    def _ensure_pool(self) -> WialonTokenPool:
        if self._pool is not None:
            return self._pool
        host = self.overrides.host or self.config.wialon_host
        primary_token = self.overrides.token or self.config.wialon_token
        if not primary_token:
            raise SystemExit("Missing Wialon token (set PIPELINE_WIALON_TOKEN or provide --token).")
        tokens: list[tuple[str, str]] = []
        if self.overrides.token:
            tokens.append(("override", primary_token))
        else:
            tokens.append(("primary", primary_token))
            for idx, extra in enumerate(self.config.wialon_extra_tokens, start=1):
                if extra:
                    tokens.append((f"extra-{idx}", extra))
        self._pool = WialonTokenPool(
            host=host,
            tokens=tokens,
            parallel_limit=self.config.wialon_parallel_limit,
            rotate_after_units=self.config.wialon_rotate_after_units,
            cooldown_seconds=self.config.wialon_token_cooldown,
        )
        log.info(
            "wialon_source: pool initialized tokens=%s parallel=%s rotate_after=%s cooldown=%ss",
            len(tokens),
            self.config.wialon_parallel_limit,
            self.config.wialon_rotate_after_units,
            self.config.wialon_token_cooldown,
        )
        return self._pool
