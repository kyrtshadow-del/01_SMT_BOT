import React, { useMemo, useRef, useState } from "react";
import { Virtuoso, type VirtuosoHandle } from "react-virtuoso";
import { Search, Radio, Check, Car, ArrowRight } from "lucide-react";
import clsx from "clsx";

import type { Unit } from "../../types";
import { useStore } from "../../store/useStore";
import { getUnitIcon } from "../../utils/icons";

interface Props {
  units: Unit[];
}

export const UnitList: React.FC<Props> = ({ units }) => {
  const {
    searchQuery, setSearch, selectUnit, setHoveredUnit, hoveredUnitId,
    shadowCount, toggleShadowPanel,
    selectedUnitIds, isSelectionMode, toggleSelectionMode, deselectAll
  } = useStore();

  const virtuosoRef = useRef<VirtuosoHandle>(null);

  const filteredUnits = useMemo(() => {
    if (!searchQuery) return units;
    const lower = searchQuery.toLowerCase();
    return units.filter(u => u.name.toLowerCase().includes(lower));
  }, [units, searchQuery]);

  const handleAvatarClick = (e: React.MouseEvent, id: number) => {
    e.stopPropagation();
    e.preventDefault();
    selectUnit(id, true);
  };

  const handleRowClick = (id: number) => {
    if (isSelectionMode) {
      selectUnit(id, true);
    } else {
      selectUnit(id, false);
    }
  };

  const onlineCount = useMemo(() => units.filter((u) => u.online).length, [units]);

  // Single-source-of-truth for inbox pulses
  const [shadowPulse, setShadowPulse] = useState(false);
  const prevShadowRef = useRef(shadowCount);
  React.useEffect(() => {
    const prev = prevShadowRef.current;
    const increased = shadowCount > prev;
    prevShadowRef.current = shadowCount;
    if (increased) {
      setShadowPulse(true);
      const t = setTimeout(() => setShadowPulse(false), 3000);
      return () => clearTimeout(t);
    }
  }, [shadowCount]);

  return (
    // УБРАЛ bg-black. Теперь фон прозрачный, работает стекло родителя.
    <div className="flex flex-col h-full bg-transparent">
      {/* HEADER */}
      <div className="px-4 py-4 border-b border-white/5 shrink-0 z-20 flex flex-col gap-3 relative bg-zinc-900/30 backdrop-blur-md">
        <div className="flex justify-between items-center h-8">
          <div className="flex items-center gap-3">
            <h2 className="text-xl font-bold text-white tracking-tight flex items-center gap-2">
              Мониторинг
              {shadowCount > 0 && (
                <button
                  onClick={() => toggleShadowPanel(true)}
                  className="relative group flex items-center justify-center w-6 h-6 rounded-full bg-blue-500 text-white shadow-[0_0_10px_rgba(59,130,246,0.5)] hover:scale-110 transition-transform cursor-pointer"
                  title="Новые сигналы"
                >
                  <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-blue-500 opacity-20"></span>
                  <span className="text-[10px] font-bold">{shadowCount > 9 ? "!" : shadowCount}</span>
                </button>
              )}
            </h2>
          </div>

          <button
            onClick={toggleSelectionMode}
            className={clsx(
              "text-xs font-semibold px-3 py-1.5 rounded-full transition-all active:scale-95",
              isSelectionMode
                ? "bg-white text-black hover:bg-zinc-200"
                : "bg-white/5 text-emerald-400 hover:bg-white/10 hover:text-emerald-300"
            )}
          >
            {isSelectionMode ? "Готово" : "Выбрать"}
          </button>
        </div>

        <div className="relative h-9 w-full group">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-zinc-500 group-focus-within:text-zinc-400 transition-colors z-10" />
          <input
            type="text"
            placeholder="Поиск объектов..."
            value={searchQuery}
            onChange={(e) => setSearch(e.target.value)}
            className="
              w-full h-full 
              bg-black/20 border border-white/5 
              focus:border-white/10 focus:bg-black/40 
              rounded-xl pl-10 pr-3 
              text-sm text-zinc-200 
              placeholder-zinc-600 
              outline-none transition-all
            "
          />
        </div>
      </div>

      {/* LIST */}
      <div className="flex-1 min-h-0">
        <Virtuoso
          ref={virtuosoRef}
          data={filteredUnits}
          className="custom-scrollbar"
          itemContent={(index, unit) => {
            const isHovered = hoveredUnitId === unit.id;
            const isSelected = selectedUnitIds.includes(unit.id);

            return (
              <div className="px-3 py-1.5">
                <div
                  onClick={() => handleRowClick(unit.id)}
                  onMouseEnter={() => setHoveredUnit(unit.id)}
                  onMouseLeave={() => setHoveredUnit(null)}
                  className={clsx(
                    "flex items-center gap-3 p-3 rounded-xl cursor-pointer transition-all duration-300 border relative overflow-hidden group",
                    isSelected
                      ? "bg-emerald-500/10 border-emerald-500/50 shadow-[0_0_20px_rgba(16,185,129,0.15)]"
                      : isHovered
                        ? "bg-white/[0.07] border-white/20 shadow-lg translate-x-1"
                        : "bg-transparent border-white/5 hover:border-white/10"
                  )}
                >
                  {/* AVATAR: squircle glass with status dot */}
                  <div
                    className={clsx(
                      "relative flex items-center justify-center w-10 h-10 rounded-full shrink-0 transition-all duration-300",
                      unit.online
                        ? "bg-emerald-500/10 text-emerald-400 shadow-[0_0_10px_rgba(16,185,129,0.2)]"
                        : "bg-zinc-800 text-zinc-600"
                    )}
                  >
                    <span
                      className={clsx(
                        "absolute top-0 right-0 w-2.5 h-2.5 border-2 border-[#121212] rounded-full",
                        unit.online ? "bg-emerald-500" : "bg-zinc-500"
                      )}
                    />
                    <Car size={18} strokeWidth={2.5} />
                  </div>

                  {/* INFO */}
                  <div className="flex-1 min-w-0 flex flex-col justify-center">
                    <div
                      className={clsx(
                        "font-bold text-sm truncate transition-colors",
                        isSelected ? "text-emerald-100" : "text-zinc-200"
                      )}
                    >
                      {unit.name}
                    </div>
                    <div className="flex items-center gap-2 text-[11px] font-medium text-zinc-500 mt-0.5">
                      <span className={clsx("truncate", unit.online && "text-zinc-400")}> 
                        {unit.status_label || unit.status}
                      </span>
                      {unit.speed !== null && unit.speed > 0 && (
                        <>
                          <span className="w-1 h-1 rounded-full bg-zinc-700" />
                          <span className="font-mono text-emerald-500">{Math.round(unit.speed)} км/ч</span>
                        </>
                      )}
                    </div>
                  </div>

                  {/* Arrow hint */}
                  <div
                    className={clsx(
                      "flex items-center justify-center w-6 h-6 rounded-full transition-all duration-300 ml-auto",
                      isSelected
                        ? "bg-emerald-500 text-white translate-x-0 opacity-100"
                        : "bg-white/5 text-zinc-500 translate-x-2 opacity-0 group-hover:translate-x-0 group-hover:opacity-100"
                    )}
                  >
                    <svg width="6" height="10" viewBox="0 0 6 10" fill="none" xmlns="http://www.w3.org/2000/svg">
                      <path d="M1 9L5 5L1 1" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
                    </svg>
                  </div>
                </div>
              </div>
            );
          }}
        />
      </div>
      
      {/* Footer */}
      <div className="px-6 py-3 border-t border-white/5 text-[10px] uppercase tracking-widest text-zinc-400 flex justify-between bg-zinc-900/50 shrink-0 backdrop-blur-xl font-bold select-none z-10">
        <span className="flex items-center gap-1.5 opacity-80 hover:opacity-100 transition-opacity cursor-help" title="Всего объектов в списке">
          Всего <span className="text-white text-xs">{units.length}</span>
        </span>
        <span className="flex items-center gap-1.5 opacity-80 hover:opacity-100 transition-opacity cursor-help" title="Объектов на связи">
          Онлайн <span className="text-emerald-400 text-xs shadow-emerald-500/50 drop-shadow-sm">{onlineCount}</span>
        </span>
      </div>
    </div>
  );
};
