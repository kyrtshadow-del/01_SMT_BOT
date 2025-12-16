import { useEffect, useRef } from "react";
import type { RefObject } from "react";
import type { Marker as MaplibreMarker } from "maplibre-gl";

interface Coords {
  lat: number;
  lon: number;
}

/**
 * Плавная анимация маркера без «отката» позиции при ре-рендерах.
 * Источник правды о положении — currentPosRef, а не карта/React.
 */
export const useAnimatedMarker = (
  markerRef: RefObject<MaplibreMarker | null>,
  target: Coords,
  duration: number = 2000
): void => {
  // Текущее положение (наш источник истины)
  const currentPosRef = useRef<Coords>(target);

  // Вспомогательные refs для анимации
  const startPosRef = useRef<Coords>(target);
  const targetPosRef = useRef<Coords>(target);
  const startTimeRef = useRef<number>(0);
  const frameRef = useRef<number>();

  useEffect(() => {
    const markerInstance = markerRef.current;
    if (!markerInstance) return;

    // Проверка: изменилась ли цель
    const isSame =
      target.lat === targetPosRef.current.lat &&
      target.lon === targetPosRef.current.lon;

    if (isSame) {
      // Даже при одинаковой цели принудительно фиксируем позицию маркера,
      // чтобы перекрыть возможный откат со стороны React/MapLibre.
      markerInstance.setLngLat([target.lon, target.lat]);
      return;
    }

    // Настраиваем старт и финиш анимации
    startPosRef.current = { ...currentPosRef.current };
    targetPosRef.current = target;
    startTimeRef.current = performance.now();

    // Если прыжок слишком большой — телепортируем без анимации
    const dist = Math.sqrt(
      Math.pow(target.lat - startPosRef.current.lat, 2) +
      Math.pow(target.lon - startPosRef.current.lon, 2)
    );
    if (dist > 0.1) {
      currentPosRef.current = target;
      startPosRef.current = target;
      markerInstance.setLngLat([target.lon, target.lat]);
      return;
    }

    const animate = (time: number) => {
      const elapsed = time - startTimeRef.current;
      const progress = Math.min(elapsed / duration, 1);

      // easeOutCubic — быстрый старт, плавное торможение
      const ease = 1 - Math.pow(1 - progress, 3);

      const { lat: sLat, lon: sLon } = startPosRef.current;
      const { lat: tLat, lon: tLon } = targetPosRef.current;

      const newLat = sLat + (tLat - sLat) * ease;
      const newLon = sLon + (tLon - sLon) * ease;

      markerInstance.setLngLat([newLon, newLat]);
      currentPosRef.current = { lat: newLat, lon: newLon };

      if (progress < 1) {
        frameRef.current = requestAnimationFrame(animate);
      }
    };

    if (frameRef.current) {
      cancelAnimationFrame(frameRef.current);
    }
    frameRef.current = requestAnimationFrame(animate);

    return () => {
      if (frameRef.current) {
        cancelAnimationFrame(frameRef.current);
      }
    };
  }, [markerRef, target.lat, target.lon, duration]);
};
