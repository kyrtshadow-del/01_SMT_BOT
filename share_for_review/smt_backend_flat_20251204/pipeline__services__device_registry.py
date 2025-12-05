"""In-memory device registry backed by Postgres.

Goal: O(1) resolve (protocol, uid) -> unit_id/priority/config without hitting DB per packet.

The loader is intentionally simple so it can be called on startup or on a reload
signal (Redis pub/sub or timer). Psycopg is optional to keep import cost low in
non-PG deployments.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Dict, Optional, Tuple

try:  # optional dependency; runtime guard for environments without PG driver
    import psycopg
except Exception:  # pragma: no cover - fallback when driver is absent
    psycopg = None  # type: ignore

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class DeviceRegistryEntry:
    unit_id: int
    priority: int
    device_id: int
    sensors_config: Optional[dict]
    hardware: Optional[str]
    firmware: Optional[str]


class DeviceRegistry:
    """Lightweight in-process cache over devices/links/units tables."""

    def __init__(self, dsn: Optional[str] = None) -> None:
        self.dsn = dsn or _dsn_from_env()
        self._cache: Dict[Tuple[str, str], DeviceRegistryEntry] = {}
        self._loaded = False

    @property
    def enabled(self) -> bool:
        return psycopg is not None and self.dsn is not None

    def load(self) -> int:
        """Reload registry from Postgres; returns number of entries loaded."""
        if not self.enabled:
            log.warning("device_registry: disabled (no psycopg or DSN)")
            return 0
        assert psycopg is not None
        with psycopg.connect(self.dsn) as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT d.protocol, d.uid, l.priority, l.device_id, l.unit_id,
                       u.sensors_config, d.hardware, d.firmware
                  FROM unit_device_links l
                  JOIN devices d ON d.id = l.device_id
                  JOIN units u   ON u.id = l.unit_id
                 WHERE l.valid_to IS NULL
                """
            )
            cache: Dict[Tuple[str, str], DeviceRegistryEntry] = {}
            for row in cur.fetchall():
                protocol, uid, priority, device_id, unit_id, sensors_config, hardware, firmware = row
                key = (str(protocol).lower(), str(uid))
                cache[key] = DeviceRegistryEntry(
                    unit_id=int(unit_id),
                    priority=int(priority),
                    device_id=int(device_id),
                    sensors_config=sensors_config,
                    hardware=hardware,
                    firmware=firmware,
                )
            self._cache = cache
            self._loaded = True
            log.info("device_registry: loaded %s entries", len(cache))
            return len(cache)

    def resolve(self, protocol: str, uid: str) -> Optional[DeviceRegistryEntry]:
        """Sync O(1) lookup. Returns None if not found or registry not loaded."""
        if not self._loaded:
            return None
        return self._cache.get((protocol.lower(), uid))

    def size(self) -> int:
        return len(self._cache)

    def iter_entries(self):
        """Iterate over cached registry entries.

        Exposed primarily for snapshot builders / diagnostics that need the
        list of known unit_ids without re-querying Postgres.
        """

        return self._cache.values()


def _dsn_from_env() -> Optional[str]:
    dsn = os.getenv("DEVICE_REGISTRY_DSN") or os.getenv("DATABASE_URL")
    if dsn:
        return dsn
    host = os.getenv("PGHOST")
    user = os.getenv("PGUSER")
    password = os.getenv("PGPASSWORD")
    dbname = os.getenv("PGDATABASE")
    port = os.getenv("PGPORT", "5432")
    if all([host, user, dbname]):
        return f"postgresql://{user}:{password or ''}@{host}:{port}/{dbname}"
    return None


__all__ = ["DeviceRegistry", "DeviceRegistryEntry"]
