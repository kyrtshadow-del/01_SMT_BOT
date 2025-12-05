"""Abstractions for pipeline data sources."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, Sequence

from pipeline.adapters.base import HistoryAdapter, StreamAdapter
from pipeline.storage.raw_storage import RawStorage
from pipeline.storage.latest_metrics import LatestTelemetryStore


@dataclass(frozen=True)
class SourceOverrides:
    """Runtime overrides that affect how a source connects to upstream systems."""

    token: str | None = None
    host: str | None = None


class DataSourceProvider(Protocol):
    """Factory that produces adapters for a particular upstream system."""

    def make_history_adapter(self, raw_storage: RawStorage) -> HistoryAdapter:
        ...

    def make_stream_adapter(
        self,
        raw_storage: RawStorage,
        unit_ids: Sequence[int],
        poll_interval: float,
        metrics_interval: int,
        latest_store: LatestTelemetryStore | None = None,
    ) -> StreamAdapter:
        ...
