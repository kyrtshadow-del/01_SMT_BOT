import type { LucideIcon } from "lucide-react";
import {
  Car,
  BusFront,
  Truck,
  Tractor,
  Ship,
  TrainFront,
} from "lucide-react";

export type UnitIconComponent = LucideIcon;

const ICON_MAP: Record<string, UnitIconComponent> = {
  car: Car,
  van: Truck,
  truck: Truck,
  tractor: Tractor,
  agri: Tractor,
  bus: BusFront,
  coach: BusFront,
  train: TrainFront,
  ship: Ship,
};

export function getUnitIcon(kind?: string | null): UnitIconComponent {
  const key = (kind || "").toLowerCase();
  return ICON_MAP[key] ?? Car;
}

