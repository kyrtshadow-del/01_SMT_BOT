import React from "react";
import { motion } from "framer-motion";
import clsx from "clsx";

interface Props {
  children: React.ReactNode;
  className?: string;
}

export const FloatingPanel: React.FC<Props> = ({ children, className }) => {
  return (
    <motion.div
      initial={{ opacity: 0, x: -20 }}
      animate={{ opacity: 1, x: 0 }}
      transition={{ duration: 0.4, ease: [0.32, 0.72, 0, 1] }}
      className={clsx(
        "absolute top-4 left-4 bottom-4 w-[380px] z-10",
        "flex flex-col",
        // APPLE MATERIAL FIX: светлее стекло, чтобы отделяться от карты
        "bg-zinc-900/70 backdrop-blur-xl backdrop-saturate-150",
        "ring-1 ring-white/10",
        "rounded-2xl shadow-2xl shadow-black/50 overflow-hidden",
        "text-white",
        className
      )}
    >
      {children}
    </motion.div>
  );
};
