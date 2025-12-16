import { create } from "zustand";
import { persist } from "zustand/middleware";
import type { AuthState } from "../types";

interface GlobalState extends AuthState {
  sessionId: string | null;
  isAuthenticated: boolean;
  login: (sid: string) => void;
  logout: () => void;

  searchQuery: string;
  setSearch: (q: string) => void;
  hoveredUnitId: number | null;
  setHoveredUnit: (id: number | null) => void;

  selectedUnitIds: number[];
  isSelectionMode: boolean;
  toggleSelectionMode: () => void;
  selectUnit: (id: number | null, forceMulti?: boolean) => void;
  deselectAll: () => void;

  shadowCount: number;
  isShadowPanelOpen: boolean;
  setShadowCount: (n: number) => void;
  toggleShadowPanel: (isOpen: boolean) => void;

  // CAMERA STATE: единственный источник правды для follow-mode
  isCameraLocked: boolean;
  setCameraLocked: (locked: boolean) => void;
}

export const useStore = create<GlobalState>()(
  persist(
    (set) => ({
      sessionId: null,
      isAuthenticated: false,
      login: (sid: string) => set({ sessionId: sid, isAuthenticated: true }),
      logout: () =>
        set({
          sessionId: null,
          isAuthenticated: false,
          selectedUnitIds: [],
          isShadowPanelOpen: false,
          isSelectionMode: false,
          isCameraLocked: false,
        }),

      searchQuery: "",
      setSearch: (q) => set({ searchQuery: q }),
      hoveredUnitId: null,
      setHoveredUnit: (id) => set({ hoveredUnitId: id }),

      selectedUnitIds: [],
      isSelectionMode: false,

      toggleSelectionMode: () =>
        set((state) => ({
          isSelectionMode: !state.isSelectionMode,
          selectedUnitIds: [], // Apple Way: выходим — сбрасываем выбор
        })),

      deselectAll: () => set({ selectedUnitIds: [] }),

      selectUnit: (id, forceMulti) =>
        set((state) => {
          if (id === null) return { selectedUnitIds: [] };

          // Любой выбор включает follow-mode
          const baseUpdate = { isCameraLocked: true };
          const isMulti = state.isSelectionMode || !!forceMulti;

          if (isMulti) {
            let newIds;
            if (state.selectedUnitIds.includes(id)) {
              newIds = state.selectedUnitIds.filter((x) => x !== id);
            } else {
              newIds = [...state.selectedUnitIds, id];
            }
            return {
              ...baseUpdate,
              selectedUnitIds: newIds,
              isSelectionMode: state.isSelectionMode || !!forceMulti,
            };
          }

          // одиночный режим: toggle
          if (state.selectedUnitIds.length === 1 && state.selectedUnitIds[0] === id) {
            return { selectedUnitIds: [] };
          }
          return { ...baseUpdate, selectedUnitIds: [id] };
        }),

      shadowCount: 0,
      isShadowPanelOpen: false,
      setShadowCount: (n) => set({ shadowCount: n }),
      toggleShadowPanel: (isOpen) => set({ isShadowPanelOpen: isOpen }),

      isCameraLocked: false,
      setCameraLocked: (locked) => set({ isCameraLocked: locked }),
    }),
    {
      name: "smt-storage",
      partialize: (state) => ({
        sessionId: state.sessionId,
        isAuthenticated: state.isAuthenticated,
      }),
    }
  )
);
