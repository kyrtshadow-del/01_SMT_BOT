"""CLI worker: detect trips and write to trips table."""

from __future__ import annotations

import logging
import os

from pipeline.services.trip_detector import TripDetector


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    dsn = os.getenv("DEVICE_REGISTRY_DSN") or os.getenv("DATABASE_URL")
    if not dsn:
        raise SystemExit("DEVICE_REGISTRY_DSN is required")
    detector = TripDetector(dsn=dsn, redis_url=os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0"))
    interval = int(os.getenv("TRIP_WORKER_INTERVAL", "30"))
    detector.run_forever(interval=interval)


if __name__ == "__main__":
    main()

