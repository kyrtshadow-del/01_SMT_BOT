"""Local sensor calculations (без зависимостей от Wialon API)."""

from __future__ import annotations

from statistics import mean
from typing import Dict, Iterable, List, Mapping, Tuple

from pipeline.events import Event


class SensorCalculator:
    """Конвертирует сырые параметры в физические величины."""

    def __init__(
        self,
        *,
        fuel_param: str = "fuel_raw",
        fuel_scale: float = 1.0,
        fuel_offset: float = 0.0,
        fuel_expression: str | None = None,
    ) -> None:
        self.fuel_param = fuel_param
        self.fuel_scale = fuel_scale
        self.fuel_offset = fuel_offset
        self.fuel_expression = fuel_expression

    def _eval_expression(self, params: Mapping[str, float | int]) -> float | None:
        if not self.fuel_expression:
            return None
        local_env = {k: float(v) for k, v in params.items() if isinstance(v, (int, float))}
        try:
            return float(eval(self.fuel_expression, {"__builtins__": {}}, local_env))
        except Exception:
            return None

    def fuel_series(self, events: Iterable[Event]) -> List[Tuple[int, float]]:
        pairs: List[Tuple[int, float]] = []
        for event in events:
            value = None
            if self.fuel_expression:
                value = self._eval_expression(event.params)
            else:
                raw_value = event.params.get(self.fuel_param)
                if raw_value is not None:
                    try:
                        value = float(raw_value)
                    except Exception:
                        value = None
            if value is None:
                continue
            adjusted = value * self.fuel_scale + self.fuel_offset
            pairs.append((event.device_ts, adjusted))
        return pairs

    def fuel_stats(self, events: Iterable[Event]) -> Dict[str, float]:
        series = self.fuel_series(events)
        if not series:
            return {}
        values = [value for _, value in series]
        return {
            "min": min(values),
            "max": max(values),
            "avg": mean(values),
            "samples": len(values),
        }
