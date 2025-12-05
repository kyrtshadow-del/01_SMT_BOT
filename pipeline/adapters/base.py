"""Base interfaces for data source adapters."""

from __future__ import annotations

import abc
from typing import AsyncIterator, Iterable, Protocol, Sequence, Union

from pipeline.events import Event, UnitSnapshot, RawPacket


class HistoryAdapter(Protocol):
    """Loads historical data ranges from a source system."""

    async def fetch_history(
        self,
        unit_ids: Sequence[int],
        start_ts: int,
        end_ts: int,
    ) -> Iterable[Event]:
        ...

    async def list_units(self) -> Iterable[UnitSnapshot]:
        ...


class StreamSubscription(Protocol):
    """Represents a running subscription that can be cancelled."""

    async def stop(self) -> None:
        ...


class StreamAdapter(abc.ABC):
    """Abstract base class for streaming implementations."""

    @abc.abstractmethod
    async def subscribe(self) -> AsyncIterator[Union[Event, RawPacket]]:
        """Yield telemetry messages (Event or RawPacket) as they appear in the source system."""

    async def start(self) -> StreamSubscription:
        """Start the background subscription and return a handle."""

        iterator = self.subscribe()

        class _Handle(StreamSubscription):
            async def stop(self_inner) -> None:  # pragma: no cover - simple wrapper
                await iterator.aclose()

        return _Handle()


class MetricsMixin:
    """Shared helpers for adapters: anomaly counters and Prometheus textfile export."""

    def __init__(self, prom_path: str | None = None) -> None:
        from pathlib import Path

        self.prom_path = Path(prom_path) if prom_path else None
        self.metrics: dict[str, float | int] = {}

    def set_metric(self, name: str, value: float | int) -> None:
        self.metrics[name] = value
        self._flush_prom()

    def incr_metric(self, name: str, delta: float | int = 1) -> None:
        self.metrics[name] = self.metrics.get(name, 0) + delta
        self._flush_prom()

    def _flush_prom(self) -> None:
        if not self.prom_path:
            return
        lines = []
        for k, v in sorted(self.metrics.items()):
            lines.append(f"{k} {v}")
        tmp = self.prom_path.with_suffix(self.prom_path.suffix + ".tmp")
        tmp.write_text("\n".join(lines), encoding="utf-8")
        tmp.replace(self.prom_path)
