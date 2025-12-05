"""Wialon IPS retranslator source provider (TCP push)."""

from __future__ import annotations

from pipeline.adapters.base import HistoryAdapter, StreamAdapter
from pipeline.adapters.wialon_ips.stream import WialonIPSStreamAdapter
from pipeline.config.defaults import PipelineConfig
from pipeline.storage.raw_storage import RawStorage
from pipeline.storage.latest_metrics import LatestTelemetryStore
from pipeline.services.location_enricher import LocationEnricher

from .base import DataSourceProvider, SourceOverrides, PollStrategy


class WialonIPSSourceProvider(DataSourceProvider):
    """Factory for direct Wialon IPS push ingestion."""

    def __init__(self, config: PipelineConfig, overrides: SourceOverrides | None = None) -> None:
        self.config = config
        self.overrides = overrides or SourceOverrides()
        self._location_enricher: LocationEnricher | None = None

    def make_history_adapter(self, raw_storage: RawStorage) -> HistoryAdapter:
        raise SystemExit("Wialon IPS history adapter is not implemented")

    def make_stream_adapter(
        self,
        raw_storage: RawStorage,
        unit_ids: tuple[int, ...] | list[int],
        poll_interval: float,
        metrics_interval: int,
        latest_store: LatestTelemetryStore | None = None,
        poll_strategy: PollStrategy | None = None,
    ) -> StreamAdapter:
        # unit_ids is ignored for push sources, kept for interface compatibility
        return WialonIPSStreamAdapter(
            raw_storage=raw_storage,
            latest_store=latest_store,
            host=self.config.wialon_ips_host,
            port=self.config.wialon_ips_port,
            password=self.config.wialon_ips_password,
            location_enricher=self._ensure_location_enricher(),
            metrics_interval=metrics_interval,
        )

    def _ensure_location_enricher(self) -> LocationEnricher:
        if self._location_enricher is None:
            self._location_enricher = LocationEnricher()
        return self._location_enricher
