export interface LatLon {
  lat: number;
  lon: number;
}

/**
 * Вычисляет границы [southWest, northEast] для набора координат.
 * Используется для fitBounds при следовании за группой объектов.
 */
export function getBounds(
  coords: LatLon[]
): [[number, number], [number, number]] | null {
  if (!coords.length) {
    return null;
  }

  let minLat = 90;
  let maxLat = -90;
  let minLon = 180;
  let maxLon = -180;

  for (const { lat, lon } of coords) {
    if (lat < minLat) minLat = lat;
    if (lat > maxLat) maxLat = lat;
    if (lon < minLon) minLon = lon;
    if (lon > maxLon) maxLon = lon;
  }

  return [
    [minLon, minLat],
    [maxLon, maxLat],
  ];
}

