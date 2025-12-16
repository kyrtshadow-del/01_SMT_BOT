import React, { useEffect } from "react";
import useSWR from "swr";

import { useStore } from "./store/useStore";
import { LiveMap } from "./components/Map/LiveMap";
import { LoginForm } from "./components/UI/LoginForm";
import { SidebarManager } from "./components/Sidebar";
import { MapHUD } from "./components/UI/MapHUD";
import type { Unit } from "./types";
import { fetcher } from "./api/client";

const App: React.FC = () => {
  const {
    isAuthenticated,
    setShadowCount,
    deselectAll,
    toggleShadowPanel,
  } = useStore();

  if (!isAuthenticated) {
    return <LoginForm />;
  }

  const { data: units } = useSWR<Unit[]>("/api/units", fetcher, {
    refreshInterval: 2000,
    dedupingInterval: 2000,
    keepPreviousData: true,
  });

  const { data: feedData } = useSWR(
    "/api/units/feed",
    fetcher,
    {
      refreshInterval: 5000,
      dedupingInterval: 5000,
    }
  );

  useEffect(() => {
    const count = feedData?.shadow_count;
    if (typeof count === "number") {
      setShadowCount(count);
    }
  }, [feedData, setShadowCount]);

  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.key === "Escape") {
        deselectAll();
        toggleShadowPanel(false);
      }
      if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "k") {
        e.preventDefault();
        // TODO: глобальный поиск / фокус поля поиска
      }
    };
    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [deselectAll, toggleShadowPanel]);

  const safeUnits = units ?? [];

  return (
    <div className="w-screen h-screen overflow-hidden bg-black relative">
      <div className="absolute inset-0 z-0">
        <LiveMap units={safeUnits} />
      </div>
      <SidebarManager units={safeUnits} />
      <MapHUD />
    </div>
  );
};

export default App;
