"""Simple trip/stop detector that materializes segments into trips table.

Heuristic:
 - moving if speed >= SPEED_MIN_KMH (default 3) or ignition flag present in params
 - trip ends when скорость ниже порога дольше STOP_GRACE_SEC

State per unit хранится в Redis: cursor (последний device_ts) и текущий режим.
"""

from __future__ import annotations

import math
import os
import time
import json
import logging
from dataclasses import dataclass
from typing import Iterable, List, Optional

import psycopg
import redis

log = logging.getLogger(__name__)


SPEED_MIN_KMH = float(os.getenv("TRIP_SPEED_MIN", "3.0"))
MIN_TRIP_SEC = int(os.getenv("TRIP_MIN_SEC", "60"))
STOP_GRACE_SEC = int(os.getenv("TRIP_STOP_GRACE", "60"))
BATCH_LIMIT = int(os.getenv("TRIP_BATCH", "2000"))


@dataclass
class Point:
    ts: int
    lat: Optional[float]
    lon: Optional[float]
    speed: Optional[float]


class TripDetector:
    def __init__(self, dsn: str, redis_url: str = "redis://127.0.0.1:6379/0") -> None:
        self.dsn = dsn
        self.r = redis.Redis.from_url(redis_url)

    # -------------- helpers -----------------
    def _cursor_key(self, unit_id: int) -> str:
        return f"tripdet:cursor:{unit_id}"

    def _state_key(self, unit_id: int) -> str:
        return f"tripdet:state:{unit_id}"

    @staticmethod
    def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        R = 6371000.0
        p1, p2 = math.radians(lat1), math.radians(lat2)
        dphi = math.radians(lat2 - lat1)
        dl = math.radians(lon2 - lon1)
        a = math.sin(dphi/2)**2 + math.cos(p1)*math.cos(p2)*math.sin(dl/2)**2
        c = 2*math.atan2(math.sqrt(a), math.sqrt(1-a))
        return R * c

    # -------------- main API -----------------
    def run_forever(self, interval: int = 30) -> None:
        log.info("trip_detector: started interval=%ss", interval)
        while True:
            try:
                self.process_once()
            except Exception as exc:  # pragma: no cover
                log.exception("trip_detector: failed: %s", exc)
            time.sleep(interval)

    def process_once(self) -> None:
        """Fetch new events for all units and emit trips."""
        with psycopg.connect(self.dsn) as conn:
            cur = conn.cursor()
            # Выбираем все unit_id, где есть новые события
            cur.execute("SELECT DISTINCT unit_id FROM events")
            unit_ids = [row[0] for row in cur.fetchall()]
            for uid in unit_ids:
                try:
                    self._process_unit(conn, uid)
                except Exception as exc:
                    log.warning("trip_detector: unit %s failed: %s", uid, exc)

    def _process_unit(self, conn, unit_id: int) -> None:
        last_ts = self._get_cursor(unit_id)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT device_ts, lat, lon, speed
            FROM events
            WHERE unit_id = %s AND device_ts > to_timestamp(%s)
            ORDER BY device_ts ASC
            LIMIT %s
            """,
            (unit_id, last_ts, BATCH_LIMIT),
        )
        rows = cur.fetchall()
        if not rows:
            return

        points: List[Point] = [Point(int(ts.timestamp()), lat, lon, float(speed) if speed is not None else None)
                               for ts, lat, lon, speed in rows]
        self._update_cursor(unit_id, points[-1].ts)

        # load state
        state = self._load_state(unit_id)
        buffer: List[Point] = state.get("buffer", [])
        moving = state.get("moving", False)
        last_move_ts = state.get("last_move_ts", 0)
        trips_to_save = []

        for p in points:
            speed = p.speed or 0.0
            is_moving = speed >= SPEED_MIN_KMH
            now_ts = p.ts

            buffer.append({"ts": p.ts, "lat": p.lat, "lon": p.lon, "speed": speed})

            if is_moving:
                moving = True
                last_move_ts = now_ts
            else:
                if moving and (now_ts - last_move_ts) >= STOP_GRACE_SEC:
                    # trip finished
                    trip_points = [Point(**pt) for pt in buffer]
                    trips_to_save.append(self._build_trip(unit_id, trip_points))
                    buffer = []
                    moving = False

        # save trips
        if trips_to_save:
            self._save_trips(conn, trips_to_save)
            conn.commit()

        # persist state
        self._store_state(unit_id, buffer, moving, last_move_ts)

    def _build_trip(self, unit_id: int, pts: List[Point]) -> dict:
        if not pts:
            return {}
        start = pts[0]
        end = pts[-1]
        max_speed = int(max((p.speed or 0) for p in pts))
        # distance estimate
        dist = 0.0
        prev = None
        for p in pts:
            if prev and p.lat is not None and p.lon is not None and prev.lat is not None and prev.lon is not None:
                dist += self._haversine_m(prev.lat, prev.lon, p.lat, p.lon)
            prev = p
        duration = max(1, end.ts - start.ts)
        avg_speed = int((dist / duration) * 3.6) if dist > 0 else 0
        return {
            "unit_id": unit_id,
            "start_ts": start.ts,
            "end_ts": end.ts,
            "type": "trip",
            "distance_m": int(dist),
            "max_speed": max_speed,
            "avg_speed": avg_speed,
            "start_lat": start.lat,
            "start_lon": start.lon,
            "end_lat": end.lat,
            "end_lon": end.lon,
        }

    def _save_trips(self, conn, trips: Iterable[dict]) -> None:
        cur = conn.cursor()
        rows = [(
            t["unit_id"],
            time.strftime('%Y-%m-%d %H:%M:%S+00', time.gmtime(t["start_ts"])),
            time.strftime('%Y-%m-%d %H:%M:%S+00', time.gmtime(t["end_ts"])),
            t.get("type", "trip"),
            t.get("distance_m", 0),
            t.get("max_speed", 0),
            t.get("avg_speed", 0),
            t.get("start_lat"),
            t.get("start_lon"),
            t.get("end_lat"),
            t.get("end_lon"),
        ) for t in trips if t]
        if not rows:
            return
        cur.executemany(
            """
            INSERT INTO trips (unit_id, start_ts, end_ts, type, distance_m, max_speed, avg_speed,
                               start_lat, start_lon, end_lat, end_lon)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (unit_id, start_ts) DO NOTHING
            """,
            rows,
        )

    # -------------- state persistence -----------------
    def _get_cursor(self, unit_id: int) -> int:
        try:
            val = self.r.get(self._cursor_key(unit_id))
            return int(val) if val else 0
        except Exception:
            return 0

    def _update_cursor(self, unit_id: int, ts: int) -> None:
        try:
            self.r.set(self._cursor_key(unit_id), ts)
        except Exception:
            pass

    def _load_state(self, unit_id: int) -> dict:
        try:
            raw = self.r.get(self._state_key(unit_id))
            if raw:
                return json.loads(raw)
        except Exception:
            pass
        return {"buffer": [], "moving": False, "last_move_ts": 0}

    def _store_state(self, unit_id: int, buffer: List[dict], moving: bool, last_move_ts: int) -> None:
        try:
            payload = json.dumps({"buffer": buffer, "moving": moving, "last_move_ts": last_move_ts})
            self.r.set(self._state_key(unit_id), payload)
        except Exception:
            pass


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    dsn = os.getenv("DEVICE_REGISTRY_DSN") or os.getenv("DATABASE_URL")
    if not dsn:
        raise SystemExit("DEVICE_REGISTRY_DSN is required")
    detector = TripDetector(dsn=dsn, redis_url=os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0"))
    interval = int(os.getenv("TRIP_WORKER_INTERVAL", "30"))
    detector.run_forever(interval=interval)


if __name__ == "__main__":
    main()
