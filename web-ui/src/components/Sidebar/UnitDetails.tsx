import React from "react";
import { ArrowLeft, MapPin, Gauge, Clock, Activity } from "lucide-react";
import clsx from "clsx";

import type { Unit } from "../../types";
import { useStore } from "../../store/useStore";

interface Props {
  unit: Unit;
}

export const UnitDetails: React.FC<Props> = ({ unit }) => {
  const { selectUnit } = useStore();

  const lastDate = new Date(unit.last_ts * 1000);

  return (
    <div className="flex flex-col h-full relative">
      {/* HEADER: прозрачный, только бордер */}
      <div className="shrink-0 px-4 py-4 h-[72px] flex items-center gap-3 border-b border-white/5">
        <button
          onClick={() => selectUnit(null)}
          className="p-2 -ml-2 rounded-full hover:bg-white/10 text-zinc-400 hover:text-white transition-all active:scale-95"
        >
          <ArrowLeft size={20} />
        </button>
        
        <div className="flex-1 min-w-0">
          <h2 className="font-bold text-lg leading-tight truncate pr-2 text-white">
            {unit.name}
          </h2>
          <div className="flex items-center gap-1.5 mt-0.5">
            <span className={clsx(
              "w-1.5 h-1.5 rounded-full shadow-[0_0_8px_currentColor]",
              unit.online ? "bg-emerald-500 text-emerald-500" : "bg-zinc-500 text-zinc-500"
            )} />
            <span className={clsx(
              "text-xs font-medium truncate", // длинные статусы не ломают верстку
              unit.online ? "text-emerald-400" : "text-zinc-500"
            )}>
              {unit.status_label || (unit.online ? "На связи" : "Оффлайн")}
            </span>
          </div>
        </div>
      </div>

      {/* BODY: Bento Grid */}
      <div className="flex-1 overflow-y-auto p-4 space-y-3 custom-scrollbar">
        {/* ROW 1: Speed & Time */}
        <div className="grid grid-cols-2 gap-3">
          {/* Speed Card */}
          <div className="bg-white/[0.03] p-3.5 rounded-2xl border border-white/5 flex flex-col justify-between h-24 hover:bg-white/[0.06] transition-colors">
            <div className="flex items-center gap-2 text-zinc-500 text-xs font-medium uppercase tracking-wider">
              <Gauge size={14} className="text-blue-400" /> Скорость
            </div>
            <div className="flex items-baseline gap-1">
              <span className="text-3xl font-mono font-medium text-white tracking-tight">
                {Math.round(unit.speed || 0)}
              </span>
              <span className="text-sm text-zinc-500 font-medium">км/ч</span>
            </div>
          </div>

          {/* Time Card */}
          <div className="bg-white/[0.03] p-3.5 rounded-2xl border border-white/5 flex flex-col justify-between h-24 hover:bg-white/[0.06] transition-colors">
            <div className="flex items-center gap-2 text-zinc-500 text-xs font-medium uppercase tracking-wider">
              <Clock size={14} className="text-purple-400" /> Время
            </div>
            <div className="text-2xl font-mono font-medium text-white tracking-tight mt-auto">
              {lastDate.toLocaleTimeString([], {
                hour: "2-digit",
                minute: "2-digit",
              })}
            </div>
          </div>
        </div>

        {/* ROW 2: Coordinates */}
        <div className="bg-white/[0.03] p-3.5 rounded-2xl border border-white/5 hover:bg-white/[0.06] transition-colors group cursor-copy">
          <div className="flex items-center justify-between mb-2">
            <div className="flex items-center gap-2 text-zinc-500 text-xs font-medium uppercase tracking-wider">
              <MapPin size={14} className="text-emerald-400" /> Локация
            </div>
            <span className="text-[10px] text-zinc-600 group-hover:text-zinc-400 transition-colors">
              Копировать
            </span>
          </div>
          <div className="font-mono text-sm text-zinc-300 group-hover:text-white transition-colors truncate">
            {unit.lat?.toFixed(6)}, {unit.lon?.toFixed(6)}
          </div>
        </div>

        {/* ROW 3: Placeholder */}
        <div className="h-40 rounded-2xl border border-dashed border-white/10 flex flex-col items-center justify-center text-zinc-600 gap-2 bg-white/[0.01]">
          <Activity size={24} className="opacity-50" />
          <span className="text-xs font-medium">Телеметрия загружается...</span>
        </div>
      </div>
    </div>
  );
};
