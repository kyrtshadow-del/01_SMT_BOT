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
        poll_strategy: "PollStrategy" | None = None,
    ) -> StreamAdapter:
        ...


@dataclass(frozen=True)
class PollStrategy:
    """Tunable scheduling parameters for stream adapters."""

    active_age_sec: int = 60
    warm_age_sec: int = 600
    active_interval_sec: float = 2.0
    warm_interval_sec: float = 10.0
    cold_interval_sec: float = 45.0
    idle_interval_sec: float = 180.0
    empty_backoff_sec: float = 15.0
    jitter: float = 0.15
    max_batch_units: int = 512
