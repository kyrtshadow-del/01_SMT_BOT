import React from "react";
import { motion, AnimatePresence } from "framer-motion";
import { Navigation } from "lucide-react";

import { useStore } from "../../store/useStore";
import { ShadowToast } from "./ShadowToast";

export const MapHUD: React.FC = () => {
  const { isCameraLocked, selectedUnitIds, setCameraLocked } = useStore();
  const showTrackingButton = !isCameraLocked && selectedUnitIds.length > 0;

  return (
    <div className="fixed inset-x-0 bottom-10 z-50 pointer-events-none flex flex-col-reverse items-center gap-3">
      <AnimatePresence>
        {showTrackingButton && (
          <motion.div
            initial={{ y: 20, opacity: 0, scale: 0.9 }}
            animate={{ y: 0, opacity: 1, scale: 1 }}
            exit={{ y: 20, opacity: 0, scale: 0.9 }}
            transition={{ type: "spring", stiffness: 400, damping: 28 }}
            className="pointer-events-auto"
          >
            <button
              onClick={() => setCameraLocked(true)}
              className="
                group flex items-center gap-3 pl-4 pr-5 py-3
                bg-zinc-900/70 backdrop-blur-xl backdrop-saturate-150
                ring-1 ring-white/10 shadow-2xl shadow-black/50
                text-white rounded-2xl
                hover:bg-zinc-800/80 hover:ring-white/20
                transition-all active:scale-95
              "
            >
              <Navigation size={18} className="text-zinc-400 group-hover:text-emerald-400 transition-colors fill-current" />
              <div className="flex flex-col text-left">
                <span className="text-[10px] font-bold text-zinc-500 uppercase tracking-widest leading-none mb-1 group-hover:text-zinc-400 transition-colors">
                  Фокус
                </span>
                <span className="text-sm font-bold text-white leading-none tracking-wide">
                  Вернуться к объекту
                </span>
              </div>
            </button>
          </motion.div>
        )}
      </AnimatePresence>
      <div className="pointer-events-auto">
        <ShadowToast />
      </div>
    </div>
  );
};
