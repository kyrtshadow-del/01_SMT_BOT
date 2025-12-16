import React, { useMemo } from "react";
import { AnimatePresence, motion } from "framer-motion";

import type { Unit } from "../../types";
import { useStore } from "../../store/useStore";
import { FloatingPanel } from "../UI/FloatingPanel";
import { UnitList } from "./UnitList";
import { UnitDetails } from "./UnitDetails";
import { ShadowList } from "./ShadowList";

interface Props {
  units: Unit[];
  shadowUnits?: any[]; // необязательно: можем передать извне
}

export const SidebarManager: React.FC<Props> = ({ units, shadowUnits }) => {
  const { selectedUnitIds, isShadowPanelOpen, isSelectionMode, toggleShadowPanel } = useStore();

  const selectedUnit = useMemo(() => {
    if (isSelectionMode) return null;
    if (selectedUnitIds.length === 1) {
      return units.find((u) => u.id === selectedUnitIds[0]) ?? null;
    }
    return null;
  }, [units, selectedUnitIds, isSelectionMode]);

  let content: React.ReactNode;
  let key: string;

  if (isShadowPanelOpen) {
    content = <ShadowList items={shadowUnits} onClose={() => toggleShadowPanel(false)} />;
    key = "shadow";
  } else if (selectedUnit) {
    content = <UnitDetails unit={selectedUnit} />;
    key = "details";
  } else {
    content = <UnitList units={units} />;
    key = "list";
  }

  return (
    <FloatingPanel>
      <AnimatePresence mode="wait" initial={false}>
        <motion.div
          key={key}
          initial={{ opacity: 0, x: key === "list" ? -20 : 20 }}
          animate={{ opacity: 1, x: 0 }}
          exit={{ opacity: 0, x: key === "list" ? -20 : 20 }}
          transition={{ duration: 0.25, ease: "circOut" }}
          className="h-full"
        >
          {content}
        </motion.div>
      </AnimatePresence>
    </FloatingPanel>
  );
};
