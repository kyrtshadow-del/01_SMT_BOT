"""Token pool that multiplexes requests across multiple Wialon tokens."""

from __future__ import annotations

import logging
import queue
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Iterator, List, Sequence, Tuple

from bot.wialon_client import WialonClient

log = logging.getLogger(__name__)


@dataclass
class _TokenState:
    label: str
    token: str
    client: WialonClient
    processed_units: int = 0
    cooldown_until: float = 0.0

    def mark_processed(self) -> None:
        self.processed_units += 1

    def reset(self) -> None:
        self.processed_units = 0
        self.cooldown_until = 0.0

    def start_cooldown(self, seconds: int) -> None:
        self.cooldown_until = time.monotonic() + max(0, seconds)
        self.processed_units = 0

    @property
    def is_available(self) -> bool:
        return time.monotonic() >= self.cooldown_until

    def cooldown_remaining(self) -> float:
        return max(0.0, self.cooldown_until - time.monotonic())


@dataclass(frozen=True)
class TokenLease:
    label: str
    token: str
    client: WialonClient


class WialonTokenPool:
    """Simple synchronous pool of WialonClient instances."""

    def __init__(
        self,
        host: str,
        tokens: Sequence[Tuple[str, str]],
        parallel_limit: int,
        rotate_after_units: int,
        cooldown_seconds: int,
    ) -> None:
        if not tokens:
            raise SystemExit("Wialon token pool requires at least one token.")
        unique_tokens: List[Tuple[str, str]] = []
        seen = set()
        for label, token in tokens:
            if not token or token in seen:
                continue
            seen.add(token)
            unique_tokens.append((label, token))
        if not unique_tokens:
            raise SystemExit("Wialon token pool could not find usable tokens.")
        self.host = host
        self.parallel_limit = max(1, int(parallel_limit or 1))
        self.rotate_after_units = max(1, int(rotate_after_units or 1))
        self.cooldown_seconds = max(0, int(cooldown_seconds or 0))
        self._lock = threading.Lock()
        self._states: List[_TokenState] = []
        for label, token in unique_tokens[: self.parallel_limit]:
            self._states.append(
                _TokenState(
                    label=label,
                    token=token,
                    client=WialonClient(self.host, token),
                )
            )
        self._available: queue.Queue[_TokenState] = queue.Queue()
        for state in self._states:
            self._available.put(state)

    def borrow(self) -> _TokenState:
        state = self._available.get()
        if not state.is_available:
            remaining = max(0.0, state.cooldown_until - time.monotonic())
            log.info(
                "wialon_pool: token=%s cooling %.1fs remaining", state.label, remaining
            )
            if remaining > 0:
                time.sleep(remaining)
            log.info("wialon_pool: token=%s back online", state.label)
        return state

    def release(self, state: _TokenState, *, broken: bool = False) -> None:
        with self._lock:
            if broken:
                # Replace client entirely.
                state.client = WialonClient(self.host, state.token)
                state.reset()
            state.mark_processed()
            if state.processed_units >= self.rotate_after_units:
                state.start_cooldown(self.cooldown_seconds)
                log.info(
                    "wialon_pool: token=%s entering cooldown for %ss",
                    state.label,
                    self.cooldown_seconds,
                )
        self._available.put(state)

    @contextmanager
    def session(self) -> Iterator[_TokenState]:
        state = self.borrow()
        try:
            yield state
        except Exception:
            self.release(state, broken=True)
            raise
        else:
            self.release(state)
    def snapshot(self) -> dict:
        with self._lock:
            tokens = [
                {
                    "label": state.label,
                    "processed_units": state.processed_units,
                    "cooldown_remaining": round(state.cooldown_remaining(), 2),
                    "available": state.is_available,
                }
                for state in self._states
            ]
            queue_size = self._available.qsize()
        return {"tokens": tokens, "queue_size": queue_size}


class PooledWialonClient:
    """Facade that borrows concrete clients from a pool per request."""

    def __init__(self, pool: WialonTokenPool) -> None:
        self._pool = pool
        self.host = pool.host
        self.max_parallel = pool.parallel_limit
        self.rotate_after_units = pool.rotate_after_units
        self.token = None  # not tied to a single token by design

    @contextmanager
    def lease(self) -> Iterator[TokenLease]:
        with self._pool.session() as state:
            yield TokenLease(state.label, state.token, state.client)

    def request(self, *args, **kwargs):
        with self.lease() as lease:
            return lease.client.request(*args, **kwargs)

    def load_messages_interval(self, *args, **kwargs):
        with self.lease() as lease:
            return lease.client.load_messages_interval(*args, **kwargs)

    def describe_pool(self) -> dict:
        return self._pool.snapshot()
