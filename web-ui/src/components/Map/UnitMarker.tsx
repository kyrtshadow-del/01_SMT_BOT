import React, { memo, useRef } from "react";
import { Marker } from "react-map-gl/maplibre";
import clsx from "clsx";
import type { Unit } from "../../types";
import type { Marker as MaplibreMarker } from "maplibre-gl";
import { useAnimatedMarker } from "../../hooks/useAnimatedMarker";
import { getUnitIcon } from "../../utils/icons";

interface Props {
  unit: Unit;
  isHovered: boolean;
  isSelected: boolean;
  onClick: (evt: any) => void;
  onHoverChange?: (active: boolean) => void;
}

export const UnitMarker = memo(
  ({ unit, isHovered, isSelected, onClick, onHoverChange }: Props) => {
    const markerRef = useRef<MaplibreMarker | null>(null);

    if (unit.lat == null || unit.lon == null) return null;

    // Фикс дрожания: фиксируем стартовую позицию один раз.
    const initialPos = useRef({ lon: unit.lon!, lat: unit.lat! }).current;

    // Императивно двигаем маркер, React к координатам больше не притрагивается
    useAnimatedMarker(markerRef, { lat: unit.lat, lon: unit.lon }, 2000);

    const isOnline = unit.online;
    const shadowClass = isOnline
      ? "shadow-[0_0_15px_rgba(16,185,129,0.6)]"
      : "shadow-[0_0_10px_rgba(0,0,0,0.5)]";

    const IconComponent = getUnitIcon(unit.icon_kind);
    const scale = isHovered || isSelected ? 1.2 : 1;
    const zIndex = isHovered || isSelected ? 50 : isOnline ? 10 : 1;

    return (
      <Marker
        ref={markerRef}
        longitude={initialPos.lon}
        latitude={initialPos.lat}
        anchor="center"
        style={{ zIndex, cursor: "pointer" }}
        onClick={(e) => {
          e.originalEvent.stopPropagation();
          onClick(e);
        }}
      >
        <div
          className="group/marker relative w-10 h-10 flex items-center justify-center transition-transform duration-300 ease-out"
          style={{ transform: `scale(${scale})` }}
          onMouseEnter={() => onHoverChange?.(true)}
          onMouseLeave={() => onHoverChange?.(false)}
        >
          {/* Direction arrow */}
          <div
            className="absolute inset-0 w-full h-full"
            style={{
              transform: `rotate(${unit.course || 0}deg)`,
              transition: "transform 500ms linear",
            }}
          >
            <div className="absolute -top-1.5 left-1/2 -translate-x-1/2 drop-shadow-md">
              <svg width="10" height="8" viewBox="0 0 12 8" fill="none">
                <path d="M6 0L12 8H0L6 0Z" fill={isOnline ? "#10b981" : "#71717a"} />
              </svg>
            </div>
          </div>

          {/* Body */}
          <div
            className={clsx(
              "relative w-9 h-9 rounded-full flex items-center justify-center border-2 border-white z-10 transition-colors duration-300",
              isOnline ? "bg-emerald-500" : "bg-zinc-600",
              shadowClass
            )}
          >
            <IconComponent size={16} className="text-white" strokeWidth={2.5} />
          </div>

          {/* Tooltip */}
          <div
            className={clsx(
              "absolute -top-12 left-1/2 -translate-x-1/2",
              "px-3 py-1.5 rounded-xl",
              "bg-gray-900/90 backdrop-blur-xl border border-white/10 shadow-2xl",
              "flex items-center gap-2 whitespace-nowrap z-20 pointer-events-none",
              "transition-all duration-200 origin-bottom",
              isHovered || isSelected
                ? "opacity-100 scale-100 translate-y-0"
                : "opacity-0 scale-90 translate-y-2"
            )}
          >
            <span className="text-white font-bold text-xs">{unit.name}</span>
            <span className="w-px h-3 bg-white/20" />
            <span
              className={clsx(
                "text-xs font-mono font-medium",
                isOnline ? "text-emerald-400" : "text-gray-400"
              )}
            >
              {Math.round(unit.speed || 0)} км/ч
            </span>
          </div>
        </div>
      </Marker>
    );
  },
  (prev, next) =>
    prev.unit.id === next.unit.id &&
    prev.unit.online === next.unit.online &&
    prev.unit.course === next.unit.course &&
    prev.unit.icon_kind === next.unit.icon_kind &&
    prev.isHovered === next.isHovered &&
    prev.isSelected === next.isSelected
);
