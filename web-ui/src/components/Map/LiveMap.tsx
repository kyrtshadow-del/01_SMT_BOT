import { useCallback, useEffect, useRef } from "react";
import Map, { FullscreenControl, NavigationControl, MapRef } from "react-map-gl/maplibre";
import "maplibre-gl/dist/maplibre-gl.css";

import type { Unit } from "../../types";
import { useStore } from "../../store/useStore";
import { UnitMarker } from "./UnitMarker";
import { getBounds } from "../../utils/geo";

const MAP_STYLE = "https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json";

interface Props {
  units: Unit[];
}

export const LiveMap: React.FC<Props> = ({ units }) => {
  const mapRef = useRef<MapRef | null>(null);

  const {
    selectedUnitIds,
    selectUnit,
    deselectAll,
    hoveredUnitId,
    setHoveredUnit,
    isCameraLocked,
    setCameraLocked,
  } = useStore();

  // Follow-mode camera
  useEffect(() => {
    if (!isCameraLocked || selectedUnitIds.length === 0 || !mapRef.current) return;

    const targets = units.filter(
      (u) => selectedUnitIds.includes(u.id) && u.lat != null && u.lon != null
    );
    if (!targets.length) return;

    if (targets.length === 1) {
      const t = targets[0];
      mapRef.current.flyTo({
        center: [t.lon as number, t.lat as number],
        zoom: 15,
        speed: 1.2,
        curve: 1.1,
        padding: { left: 420, right: 50, top: 50, bottom: 50 },
      });
    } else {
      const bounds = getBounds(
        targets.map((t) => ({ lat: t.lat as number, lon: t.lon as number }))
      );
      if (!bounds) return;
      mapRef.current.fitBounds(bounds, {
        padding: { left: 450, right: 100, top: 100, bottom: 100 },
        maxZoom: 15,
        duration: 1000,
      });
    }
  }, [units, selectedUnitIds, isCameraLocked]);

  const handleMapInteraction = useCallback(() => {
    if (isCameraLocked) setCameraLocked(false);
  }, [isCameraLocked, setCameraLocked]);

  const handleUnitClick = useCallback(
    (id: number, evt: any) => {
      const original = evt?.originalEvent;
      const isMulti = original?.ctrlKey || original?.metaKey;
      selectUnit(id, isMulti);
      // selectUnit уже ставит isCameraLocked=true
    },
    [selectUnit]
  );

  return (
    <div className="w-full h-full bg-gray-900 relative">
      <Map
        ref={mapRef}
        initialViewState={{ longitude: 37.6173, latitude: 55.7558, zoom: 10 }}
        style={{ width: "100%", height: "100%" }}
        mapStyle={MAP_STYLE}
        attributionControl={false}
        onClick={() => deselectAll()}
        onDragStart={handleMapInteraction}
        onZoomStart={handleMapInteraction}
        onWheel={handleMapInteraction}
        onTouchStart={handleMapInteraction}
        onMouseDown={handleMapInteraction}
      >
        <FullscreenControl position="bottom-right" />
        <NavigationControl position="bottom-right" showCompass={false} />

        {units?.map((unit) => (
          <UnitMarker
            key={unit.id}
            unit={unit}
            isHovered={hoveredUnitId === unit.id}
            isSelected={selectedUnitIds.includes(unit.id)}
            onClick={(evt) => handleUnitClick(unit.id, evt)}
            onHoverChange={(active) => setHoveredUnit(active ? unit.id : null)}
          />
        ))}
      </Map>
    </div>
  );
};
