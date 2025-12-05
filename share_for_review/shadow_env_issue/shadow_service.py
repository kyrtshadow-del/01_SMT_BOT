"""Shadow device tracker for unknown IMEI/protocol pairs.

Stores lightweight metadata in Redis with hard limits to avoid bloat.
If redis-py is missing or REDIS_URL not set, service becomes a no-op.
"""

from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass, field
from typing import Dict, Iterable, Optional, Set

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None

log = logging.getLogger(__name__)


@dataclass
class ShadowPacket:
    protocol: str
    uid: str
    ip: Optional[str]
    payload: Dict[str, object]
    params_keys: Set[str] = field(default_factory=set)
    received_ts: int = field(default_factory=lambda: int(time.time()))


class ShadowService:
    def __init__(self, redis_url: Optional[str] = None, ttl_sec: int = 24 * 3600) -> None:
        self.redis_url = redis_url or os.getenv("REDIS_URL")
        self.ttl_sec = ttl_sec
        self.enabled = redis is not None and self.redis_url is not None
        self._client = redis.Redis.from_url(self.redis_url) if self.enabled else None
        self._last_update: Dict[str, float] = {}  # rate-limit per key
        self.max_payload_bytes = 2048
        self.max_params = 50
        self.min_interval_sec = 5.0
        if not self.enabled:
            log.info("shadow_service: disabled (no redis or REDIS_URL)")

    def _should_skip(self, key: str) -> bool:
        now = time.time()
        last = self._last_update.get(key, 0.0)
        if now - last < self.min_interval_sec:
            return True
        self._last_update[key] = now
        return False

    def register(self, packet: ShadowPacket) -> None:
        if not self.enabled or self._client is None:
            return
        key = f"shadow:device:{packet.protocol}:{packet.uid}"
        if self._should_skip(key):
            return
        try:
            payload_str = json.dumps(packet.payload, ensure_ascii=False)
        except Exception:
            payload_str = "{}"
        if len(payload_str) > self.max_payload_bytes:
            payload_str = payload_str[: self.max_payload_bytes] + "... (truncated)"
        params_seen = list(packet.params_keys)[: self.max_params]
        data = {
            "last_seen_ts": packet.received_ts,
            "last_ip": packet.ip or "",
            "last_sample": payload_str,
            "params_seen": json.dumps(params_seen, ensure_ascii=False),
        }
        try:
            self._client.hset(key, mapping=data)
            self._client.expire(key, self.ttl_sec)
        except Exception as exc:  # pragma: no cover - defensive
            log.warning("shadow_service: failed to store %s: %s", key, exc)


__all__ = ["ShadowService", "ShadowPacket"]

