import React, { useEffect, useState } from "react";
import { Virtuoso } from "react-virtuoso";
import {
  ArrowLeft,
  Check,
  X,
  Layers,
  Loader2,
  Wifi,
  Clock,
  Smartphone,
} from "lucide-react";
import clsx from "clsx";

import { useStore } from "../../store/useStore";
import {
  getUnknownDevices,
  createUnitFromShadow,
  createUnitsBatch,
  ignoreShadowDevice,
  type UnknownDevice,
} from "../../api/shadow";

export const ShadowDeviceList: React.FC = () => {
  const { toggleShadowPanel, setShadowCount, selectUnit } = useStore();
  const [devices, setDevices] = useState<UnknownDevice[]>([]);
  const [loading, setLoading] = useState(true);

  const [isImporting, setIsImporting] = useState(false);
  const [importProgress, setImportProgress] = useState(0);

  const [connectingId, setConnectingId] = useState<string | null>(null);
  const [newName, setNewName] = useState("");

  const fetchDevices = async () => {
    try {
      setLoading(true);
      const data = await getUnknownDevices();
      setDevices(data);
      setShadowCount(data.length);
    } catch (err) {
      console.error("Failed to fetch unknown devices", err);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchDevices();
  }, []);

  const handleImportAll = async () => {
    if (!devices.length) return;
    if (!window.confirm(`Импортировать ${devices.length} устройств?`)) return;

    setIsImporting(true);
    try {
      await createUnitsBatch(devices, (doneCount) => setImportProgress(doneCount));
      setDevices([]);
      setShadowCount(0);
      toggleShadowPanel(false);
    } catch (err) {
      alert("Сбой импорта.");
    } finally {
      setIsImporting(false);
    }
  };

  const handleSingleCreate = async (device: UnknownDevice) => {
    if (!newName.trim()) return;
    try {
      const res = await createUnitFromShadow(device.protocol, device.uid, newName.trim());
      const next = devices.filter((d) => d.uid !== device.uid);
      setDevices(next);
      setShadowCount(next.length);
      setConnectingId(null);
      setNewName("");
      if (res.unit_id) {
        toggleShadowPanel(false);
        selectUnit(res.unit_id, false);
      }
    } catch (err) {
      console.error(err);
    }
  };

  const handleIgnore = async (device: UnknownDevice) => {
    try {
      await ignoreShadowDevice(device.protocol, device.uid);
      const next = devices.filter((d) => d.uid !== device.uid);
      setDevices(next);
      setShadowCount(next.length);
    } catch (err) {
      console.error(err);
    }
  };

  return (
    <div className="flex flex-col h-full bg-zinc-950/80 backdrop-blur-xl border-r border-white/10">
      {/* HEADER */}
      <div className="px-4 py-4 border-b border-white/10 shrink-0 bg-white/5 backdrop-blur-md z-10">
        <div className="flex items-center justify-between mb-4">
          <div className="flex items-center gap-3">
            <button
              onClick={() => toggleShadowPanel(false)}
              className="p-1.5 -ml-1.5 text-zinc-400 hover:text-white hover:bg-white/10 rounded-lg transition-colors"
            >
              <ArrowLeft size={20} />
            </button>
            <div>
              <h2 className="text-lg font-bold text-white tracking-tight leading-none">
                Inbox
              </h2>
              <span className="text-xs text-zinc-500 font-medium">
                Новых устройств: {devices.length}
              </span>
            </div>
          </div>
        </div>

        {/* Mass Action */}
        {devices.length > 3 && !isImporting && (
          <button
            onClick={handleImportAll}
            className="w-full bg-blue-600 hover:bg-blue-500 text-white rounded-xl py-2.5 text-sm font-semibold transition-all shadow-[0_0_12px_rgba(59,130,246,0.3)] flex items-center justify-center gap-2 active:scale-95"
          >
            <Layers size={16} />
            Импортировать все ({devices.length})
          </button>
        )}

        {isImporting && (
          <div className="space-y-2">
            <div className="flex justify-between text-xs text-blue-400 font-bold">
              <span>Импорт...</span>
              <span>{Math.round((importProgress / devices.length) * 100) || 0}%</span>
            </div>
            <div className="h-1 bg-zinc-800 rounded-full overflow-hidden">
              <div
                className="h-full bg-blue-500 transition-all duration-300"
                style={{ width: `${devices.length ? (importProgress / devices.length) * 100 : 0}%` }}
              />
            </div>
          </div>
        )}
      </div>

      {/* LIST */}
      <div className="flex-1 min-h-0 overflow-hidden">
        {loading ? (
          <div className="flex flex-col items-center justify-center h-full text-zinc-500 gap-3">
            <Loader2 className="animate-spin" size={24} />
            <span className="text-xs font-medium">Поиск устройств...</span>
          </div>
        ) : devices.length === 0 ? (
          <div className="flex flex-col items-center justify-center h-full text-zinc-600 gap-3">
            <div className="w-16 h-16 rounded-full bg-zinc-900 flex items-center justify-center border border-zinc-800">
              <Check size={24} className="text-emerald-500/50" />
            </div>
            <span className="text-sm font-medium">Входящие пусты</span>
          </div>
        ) : (
          <Virtuoso
            style={{ height: "100%" }}
            data={devices}
            className="custom-scrollbar"
            itemContent={(index, device) => {
              const compositeId = `${device.protocol}:${device.uid}`;
              const isConnecting = connectingId === compositeId;
              const lastSeen = new Date(device.last_seen_ts * 1000);

              return (
                <div className="px-3 py-2">
                  <div
                    className={clsx(
                      "rounded-2xl p-4 transition-all duration-300 border relative overflow-hidden group",
                      isConnecting
                        ? "bg-blue-500/10 border-blue-500/50"
                        : "bg-white/5 border-white/5 hover:bg-white/10 hover:border-white/10"
                    )}
                  >
                    {/* Top Row: Icon + ID + Status */}
                    <div className="flex justify-between items-start mb-3">
                      <div className="flex items-center gap-3">
                        <div className="w-10 h-10 rounded-full bg-zinc-900 flex items-center justify-center border border-white/10 text-zinc-400 shrink-0">
                          <Smartphone size={18} />
                        </div>
                        <div className="flex flex-col">
                          <span className="text-sm font-bold text-white font-mono tracking-tight">
                            {device.uid}
                          </span>
                          <span className="text-[10px] uppercase font-bold text-zinc-500 tracking-wider">
                            {device.protocol}
                          </span>
                        </div>
                      </div>

                      <div className="text-right flex flex-col items-end">
                        <div className="flex items-center gap-1.5 bg-emerald-500/10 px-2 py-0.5 rounded-full border border-emerald-500/20">
                          <span className="w-1.5 h-1.5 rounded-full bg-emerald-500 animate-pulse" />
                          <span className="text-[10px] font-bold text-emerald-400">Ready</span>
                        </div>
                        <div className="flex items-center gap-1 text-[10px] text-zinc-500 mt-1.5">
                          <Clock size={10} />
                          {lastSeen.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}
                        </div>
                      </div>
                    </div>

                    {/* Action Area (Apple Polish) */}
                    {isConnecting ? (
                      <div className="mt-3 animate-in slide-in-from-bottom-2 fade-in">
                        <input
                          autoFocus
                          className="w-full bg-black/40 border border-blue-500/50 rounded-xl px-3 py-2 text-sm text-white focus:outline-none focus:ring-1 focus:ring-blue-500 mb-2 placeholder-zinc-600 transition-all"
                          placeholder="Назовите объект..."
                          value={newName}
                          onChange={(e) => setNewName(e.target.value)}
                          onKeyDown={(e) => e.key === "Enter" && handleSingleCreate(device)}
                        />
                        <div className="flex gap-2">
                          <button
                            onClick={() => handleSingleCreate(device)}
                          className="flex-1 bg-blue-600 hover:bg-blue-500 text-white text-xs font-bold py-2 rounded-xl transition-colors shadow-[0_0_12px_rgba(59,130,246,0.3)]"
                          >
                            Сохранить
                          </button>
                          <button
                            onClick={() => setConnectingId(null)}
                            className="px-3 text-zinc-400 hover:text-white hover:bg-white/10 rounded-xl transition-colors"
                          >
                            <X size={18} />
                          </button>
                        </div>
                      </div>
                    ) : (
                      <div className="mt-3 flex gap-2">
                        {/* Primary Action: Blue, not White */}
                        <button
                          onClick={() => {
                            setConnectingId(compositeId);
                            setNewName(`${device.protocol.toUpperCase()} ${device.uid.slice(-4)}`);
                          }}
                          className="flex-1 bg-blue-600/90 hover:bg-blue-500 text-white text-xs font-bold py-2 rounded-xl transition-all shadow-[0_0_12px_rgba(59,130,246,0.3)] active:scale-[0.98]"
                        >
                          Подключить
                        </button>

                        {/* Secondary Action: Ghost Button */}
                        <button
                          onClick={() => handleIgnore(device)}
                          className="px-3 text-zinc-500 hover:text-red-400 hover:bg-red-500/10 rounded-xl transition-colors"
                          title="Скрыть"
                        >
                          <X size={18} />
                        </button>
                      </div>
                    )}
                  </div>
                </div>
              );
            }}
          />
        )}
      </div>
    </div>
  );
};
