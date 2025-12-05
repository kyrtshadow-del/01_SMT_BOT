"""Base interfaces for data source adapters."""

from __future__ import annotations

import abc
from typing import AsyncIterator, Iterable, Protocol, Sequence

from pipeline.events import Event, UnitSnapshot


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
    async def subscribe(self) -> AsyncIterator[Event]:
        """Yield events as they appear in the source system."""

    async def start(self) -> StreamSubscription:
        """Start the background subscription and return a handle."""

        iterator = self.subscribe()

        class _Handle(StreamSubscription):
            async def stop(self_inner) -> None:  # pragma: no cover - simple wrapper
                await iterator.aclose()

        return _Handle()

