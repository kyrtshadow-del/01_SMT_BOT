import React, { forwardRef } from "react";
import { motion, AnimatePresence } from "framer-motion";
import { Radio, Plus, X, Signal, Copy, Calendar } from "lucide-react";
import useSWR from "swr";

import { fetcher, api } from "../../api/client";

interface ShadowUnit {
  uid: string;
  protocol: string;
  last_seen_ts: number;
}

interface Props {
  items?: ShadowUnit[]; // если не передан список — подтянем сами
  onClose: () => void;
}

export const ShadowList: React.FC<Props> = ({ items, onClose }) => {
  const { data, mutate } = useSWR<ShadowUnit[]>(items ? null : "/api/unknown_devices", fetcher, {
    refreshInterval: 5000,
  });

  const list = items ?? data ?? [];

  const handleCreate = async (u: ShadowUnit) => {
    try {
      await api.post(`/api/unknown_devices/${u.protocol}/${u.uid}/create`, {
        name: `${u.protocol.toUpperCase()} ${u.uid.slice(-4)}`,
      });
      mutate?.();
    } catch (e) {
      console.error(e);
    }
  };

  const handleIgnore = async (u: ShadowUnit) => {
    try {
      await api.post(`/api/unknown_devices/${u.protocol}/${u.uid}/ignore`, {});
      mutate?.();
    } catch (e) {
      console.error(e);
    }
  };

  const handleCopy = async (text: string) => {
    try {
      await navigator.clipboard.writeText(text);
    } catch (e) {
      console.error(e);
    }
  };

  return (
    <div className="flex flex-col h-full bg-transparent">
      <div className="p-4 border-b border-white/10 shrink-0 flex items-center justify-between backdrop-blur-md z-10">
        <div className="flex items-center gap-3">
          <div className="relative flex items-center justify-center w-8 h-8 rounded-full bg-blue-500/10 text-blue-400 border border-blue-500/20 shadow-[0_0_10px_rgba(59,130,246,0.2)]">
            <Radio size={16} className="animate-taptic-beat" />
          </div>
          <div>
            <h2 className="font-bold text-white text-lg leading-none">Входящие</h2>
            <div className="text-[11px] text-blue-400/80 font-medium mt-1">
              Активных сигналов: {list.length}
            </div>
          </div>
        </div>
        <button
          onClick={onClose}
          className="p-2 rounded-lg hover:bg-white/10 text-zinc-500 hover:text-white transition-colors"
        >
          <X size={18} />
        </button>
      </div>

      <div className="flex-1 overflow-y-auto custom-scrollbar p-3 space-y-2">
        <AnimatePresence mode="popLayout">
          {list.length === 0 ? (
            <motion.div
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              className="flex flex-col items-center justify-center h-40 text-zinc-600 text-sm"
            >
              <Signal size={32} className="mb-2 opacity-20" />
              Эфир чист
            </motion.div>
          ) : (
            list.map((item) => (
              <ShadowItem
                key={item.uid}
                item={item}
                onAdd={() => handleCreate(item)}
                onIgnore={() => handleIgnore(item)}
                onCopy={() => handleCopy(item.uid)}
              />
            ))
          )}
        </AnimatePresence>
      </div>
    </div>
  );
};

const ShadowItem = forwardRef<HTMLDivElement, {
  item: ShadowUnit;
  onAdd: () => void;
  onIgnore: () => void;
  onCopy: () => void;
}>(({ item, onAdd, onIgnore, onCopy }, ref) => {
  return (
    <motion.div
      ref={ref}
      layout
      initial={{ opacity: 0, x: -20, scale: 0.95 }}
      animate={{ opacity: 1, x: 0, scale: 1 }}
      exit={{ opacity: 0, scale: 0.9, transition: { duration: 0.2 } }}
      className="
        group relative bg-zinc-900/40 border border-white/5 hover:border-blue-500/30
        rounded-xl p-3 transition-colors duration-300 overflow-hidden
      "
    >
      <div className="absolute inset-0 bg-gradient-to-r from-blue-500/0 via-blue-500/0 to-blue-500/0 group-hover:via-blue-500/5 transition-all duration-500 pointer-events-none" />

      <div className="flex justify-between items-start gap-3 relative z-10">
        <div className="flex gap-3 min-w-0 flex-1">
          <div className="flex flex-col items-center gap-1 shrink-0 pt-0.5">
            <div className="w-8 h-8 rounded-lg bg-zinc-800 flex items-center justify-center text-zinc-400 group-hover:text-blue-400 group-hover:bg-blue-500/10 transition-colors">
              <Signal size={14} />
            </div>
            <span className="text-[9px] uppercase tracking-wider font-bold text-zinc-600 group-hover:text-blue-500/60 transition-colors">
              {item.protocol.split("_")[0]}
            </span>
          </div>

          <div className="flex flex-col min-w-0">
            <div className="flex items-center gap-2">
              <span className="text-sm font-mono text-zinc-200 font-medium tracking-tight truncate">
                {item.uid}
              </span>
              <button
                onClick={onCopy}
                className="opacity-0 group-hover:opacity-100 p-1 text-zinc-500 hover:text-white transition-opacity"
                title="Скопировать UID"
              >
                <Copy size={12} />
              </button>
            </div>

            <div className="text-[11px] text-zinc-500 mt-0.5 flex items-center gap-1.5">
              <span className="w-1 h-1 rounded-full bg-blue-500 animate-pulse" />
              {new Date(item.last_seen_ts * 1000).toLocaleTimeString([], {
                hour: "2-digit",
                minute: "2-digit",
              })}
            </div>
          </div>
        </div>

        <div className="flex items-center gap-1 self-center pl-2 border-l border-white/5">
          <button
            onClick={onAdd}
            className="
              flex items-center gap-1.5 px-3 py-1.5 rounded-lg
              bg-emerald-500/10 text-emerald-400
              hover:bg-emerald-500 hover:text-white
              border border-emerald-500/20 hover:border-emerald-500
              transition-all active:scale-95
            "
          >
            <Plus size={14} strokeWidth={3} />
            <span className="text-xs font-bold pr-1">В парк</span>
          </button>

          <button
            onClick={onIgnore}
            className="
              p-1.5 rounded-lg
              text-zinc-600 hover:text-red-400 hover:bg-red-500/10
              transition-colors
            "
            title="Игнорировать"
          >
            <X size={16} />
          </button>
        </div>
      </div>
    </motion.div>
  );
});

ShadowItem.displayName = "ShadowItem";
