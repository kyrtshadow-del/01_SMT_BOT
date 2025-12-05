"""Galileosky-specific source provider (HTTP/TCP gateway ingestion).

This is a lightweight provider that spins up a listener accepting raw telemetry
packets from Galileosky devices (or their retranslators) and normalises them to
the project's `Event` shape.

Actual packet format must be clarified; the adapter is written defensively and
expects JSON payloads by default.
"""

from __future__ import annotations

from pipeline.adapters.base import HistoryAdapter, StreamAdapter
from pipeline.adapters.galileosky.stream import GalileoskyStreamAdapter
from pipeline.config.defaults import PipelineConfig
from pipeline.storage.raw_storage import RawStorage
from pipeline.storage.latest_metrics import LatestTelemetryStore
from pipeline.services.location_enricher import LocationEnricher

from .base import DataSourceProvider, SourceOverrides, PollStrategy


class GalileoskySourceProvider(DataSourceProvider):
    """Factory for Galileosky direct ingestion."""

    def __init__(self, config: PipelineConfig, overrides: SourceOverrides | None = None) -> None:
        self.config = config
        self.overrides = overrides or SourceOverrides()
        self._location_enricher: LocationEnricher | None = None

    def make_history_adapter(self, raw_storage: RawStorage) -> HistoryAdapter:
        # Not implemented for Galileosky yet (history is handled by streaming + storage).
        raise SystemExit("Galileosky history adapter is not implemented")

    def make_stream_adapter(
        self,
        raw_storage: RawStorage,
        unit_ids: tuple[int, ...] | list[int],
        poll_interval: float,
        metrics_interval: int,
        latest_store: LatestTelemetryStore | None = None,
        poll_strategy: PollStrategy | None = None,
    ) -> StreamAdapter:
        return GalileoskyStreamAdapter(
            raw_storage=raw_storage,
            latest_store=latest_store,
            host=self.config.galileosky_host,
            port=self.config.galileosky_port,
            auth_token=self.config.galileosky_auth_token,
            location_enricher=self._ensure_location_enricher(),
            metrics_interval=metrics_interval,
        )

    def _ensure_location_enricher(self) -> LocationEnricher:
        if self._location_enricher is None:
            self._location_enricher = LocationEnricher()
        return self._location_enricher

