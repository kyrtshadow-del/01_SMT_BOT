"""Helpers for enriching telemetry with addresses и геозонами."""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple


def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in meters."""

    r = 6371000.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return r * c


def _point_in_polygon(lat: float, lon: float, points: Sequence[Sequence[float]]) -> bool:
    """Ray casting algorithm for polygons."""

    if len(points) < 3:
        return False
    inside = False
    x = lon
    y = lat
    for i in range(len(points)):
        j = (i - 1) % len(points)
        xi, yi = points[i][1], points[i][0]
        xj, yj = points[j][1], points[j][0]
        intersect = ((yi > y) != (yj > y)) and (x < (xj - xi) * (y - yi) / (yj - yi + 1e-12) + xi)
        if intersect:
            inside = not inside
    return inside


class GeocoderCache:
    """Tiny offline cache mapping lat/lon to адресов."""

    def __init__(self, cache_path: Path | str | None = None, *, precision: int = 5) -> None:
        self.path = Path(cache_path or Path("data") / "geocoder_cache.json")
        self.precision = precision
        self._cache: Dict[str, str] = {}
        if self.path.exists():
            try:
                self._cache = json.loads(self.path.read_text(encoding="utf-8"))
            except Exception:
                self._cache = {}

    def _key(self, lat: float, lon: float) -> str:
        return f"{lat:.{self.precision}f},{lon:.{self.precision}f}"

    def lookup(self, lat: Optional[float], lon: Optional[float]) -> Optional[str]:
        if lat is None or lon is None:
            return None
        return self._cache.get(self._key(lat, lon))


@dataclass
class GeoZone:
    name: str
    shape: str
    lat: Optional[float] = None
    lon: Optional[float] = None
    radius_m: Optional[float] = None
    points: Sequence[Sequence[float]] | None = None

    def contains(self, lat: float, lon: float) -> bool:
        if self.shape == "circle" and self.lat is not None and self.lon is not None and self.radius_m:
            return _haversine_m(lat, lon, self.lat, self.lon) <= self.radius_m
        if self.shape == "polygon" and self.points:
            return _point_in_polygon(lat, lon, self.points)
        return False


class GeoZoneIndex:
    def __init__(self, geo_path: Path | str | None = None) -> None:
        self.path = Path(geo_path or Path("data") / "geozones.json")
        self._zones: List[GeoZone] = []
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            self._zones = []
            return
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except Exception:
            payload = []
        zones: List[GeoZone] = []
        for entry in payload if isinstance(payload, list) else []:
            try:
                name = str(entry.get("name") or "geozone")
                shape = str(entry.get("type") or "circle").lower()
                if shape == "circle":
                    zones.append(
                        GeoZone(
                            name=name,
                            shape=shape,
                            lat=float(entry["lat"]),
                            lon=float(entry["lon"]),
                            radius_m=float(entry.get("radius_m") or entry.get("radius") or 0),
                        )
                    )
                elif shape == "polygon" and isinstance(entry.get("points"), list):
                    zones.append(GeoZone(name=name, shape=shape, points=entry["points"]))
            except Exception:
                continue
        self._zones = zones

    def match(self, lat: Optional[float], lon: Optional[float]) -> List[str]:
        if lat is None or lon is None or not self._zones:
            return []
        hits = [zone.name for zone in self._zones if zone.contains(lat, lon)]
        return hits


class LocationEnricher:
    """Best-effort координаты -> адрес + геозоны."""

    def __init__(self, cache_path: Path | str | None = None, geozones_path: Path | str | None = None) -> None:
        self.geocoder = GeocoderCache(cache_path)
        self.geozones = GeoZoneIndex(geozones_path)

    def enrich(self, lat: Optional[float], lon: Optional[float], *, existing_address: Optional[str] = None) -> Dict[str, Any]:
        if lat is None or lon is None:
            return {"address": existing_address, "geofences": []}
        address = existing_address or self.geocoder.lookup(lat, lon)
        if not address:
            address = f"{lat:.5f}, {lon:.5f}"
        geofences = self.geozones.match(lat, lon)
        return {"address": address, "geofences": geofences}
