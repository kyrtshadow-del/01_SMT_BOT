import React, { useEffect, useRef } from "react";
import { motion, AnimatePresence } from "framer-motion";
import { Radio, X, ArrowRight } from "lucide-react";

import { useStore } from "../../store/useStore";

export const ShadowToast: React.FC = () => {
  const { shadowCount, toggleShadowPanel } = useStore();
  const prevCountRef = useRef(0);

  useEffect(() => {
    prevCountRef.current = shadowCount;
  }, [shadowCount]);

  return (
    <AnimatePresence>
      {shadowCount > 0 && (
        <motion.div
          initial={{ opacity: 0, y: 30, scale: 0.95 }}
          animate={{ opacity: 1, y: 0, scale: 1 }}
          exit={{ opacity: 0, y: 20, scale: 0.95 }}
          transition={{ type: "spring", stiffness: 350, damping: 25 }}
          className="
            flex items-center gap-4 p-3 pr-3
            bg-zinc-900/80 backdrop-blur-2xl backdrop-saturate-150
            ring-1 ring-white/10 shadow-[0_8px_32px_rgba(0,0,0,0.4)]
            rounded-2xl min-w-[320px] pointer-events-auto
          "
        >
          {/* Icon block */}
          <div className="relative flex items-center justify-center w-12 h-12 shrink-0">
            <div className="center-absolute w-full h-full flex items-center justify-center">
              <div className="w-10 h-10 rounded-full bg-blue-500/20 animate-radar-wave" />
            </div>
            <div className="absolute inset-0 bg-blue-500/10 blur-md rounded-full" />
            <div className="relative z-10 animate-taptic-beat text-blue-400 drop-shadow-[0_0_8px_rgba(96,165,250,0.6)]">
              <Radio size={24} strokeWidth={2.5} />
            </div>
          </div>

          <div className="flex-1 min-w-0 flex flex-col justify-center">
            <div className="text-[15px] font-bold text-white leading-tight flex items-center gap-2">
              Inbox
              <span className="animate-taptic-beat px-2 py-0.5 rounded-full bg-blue-500 text-white text-[10px] font-extrabold shadow-lg shadow-blue-500/40">
                {shadowCount}
              </span>
            </div>
            <div className="text-[12px] text-zinc-400 mt-0.5 font-medium tracking-wide">
              Обнаружены новые сигналы
            </div>
          </div>

          <div className="flex items-center gap-2 h-full pl-4 border-l border-white/10">
            <button
              onClick={() => toggleShadowPanel(true)}
              className="
                flex items-center gap-1.5 px-3 py-2 rounded-lg
                bg-blue-500/10 text-blue-400 text-xs font-bold
                hover:bg-blue-500 hover:text-white
                transition-all duration-300
                group
              "
            >
              <span>Вход</span>
              <ArrowRight size={14} className="group-hover:translate-x-0.5 transition-transform" />
            </button>

            <button
              onClick={() => {/* Скрыть */}}
              className="w-8 h-8 flex items-center justify-center rounded-lg text-zinc-500 hover:text-white hover:bg-white/10 transition-colors"
            >
              <X size={16} />
            </button>
          </div>
        </motion.div>
      )}
    </AnimatePresence>
  );
};
