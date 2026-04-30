from __future__ import annotations

from enum import Enum
from typing import Any, Callable, Dict, List, Optional

import threading
import time

from liutang.core.viscosity import (
    Viscosity,
    ViscosityPolicy as _VP,
    ViscosityController as _VC,
    FlowMetrics,
)


_GRANULARITY_ORDER = {"micro": 0, "fine": 1, "medium": 2, "coarse": 3, "macro": 4}

_GRAN_TO_VISC = {
    "micro": Viscosity.VOLATILE,
    "fine": Viscosity.FLUID,
    "medium": Viscosity.HONEYED,
    "coarse": Viscosity.SLUGGISH,
    "macro": Viscosity.FROZEN,
}

_VISC_TO_GRAN = {v: k for k, v in _GRAN_TO_VISC.items()}


class GranularityLevel(Enum):
    MICRO = "micro"
    FINE = "fine"
    MEDIUM = "medium"
    COARSE = "coarse"
    MACRO = "macro"

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, GranularityLevel):
            return NotImplemented
        return _GRANULARITY_ORDER[self.value] < _GRANULARITY_ORDER[other.value]

    def __le__(self, other: object) -> bool:
        if not isinstance(other, GranularityLevel):
            return NotImplemented
        return _GRANULARITY_ORDER[self.value] <= _GRANULARITY_ORDER[other.value]

    def __gt__(self, other: object) -> bool:
        if not isinstance(other, GranularityLevel):
            return NotImplemented
        return _GRANULARITY_ORDER[self.value] > _GRANULARITY_ORDER[other.value]

    def __ge__(self, other: object) -> bool:
        if not isinstance(other, GranularityLevel):
            return NotImplemented
        return _GRANULARITY_ORDER[self.value] >= _GRANULARITY_ORDER[other.value]

    @property
    def batch_size(self) -> int:
        return _GRANULARITY_BATCH_SIZES.get(self, 100)

    @property
    def batch_timeout(self) -> float:
        return _GRANULARITY_BATCH_TIMEOUTS.get(self, 1.0)

    @property
    def is_streaming(self) -> bool:
        return self in (GranularityLevel.MICRO, GranularityLevel.FINE)

    @property
    def is_batch_like(self) -> bool:
        return self in (GranularityLevel.COARSE, GranularityLevel.MACRO)

    def to_viscosity(self) -> Viscosity:
        return _GRAN_TO_VISC[self.value]


_GRANULARITY_BATCH_SIZES = {
    GranularityLevel.MICRO: 1,
    GranularityLevel.FINE: 10,
    GranularityLevel.MEDIUM: 100,
    GranularityLevel.COARSE: 1000,
    GranularityLevel.MACRO: 100000,
}

_GRANULARITY_BATCH_TIMEOUTS = {
    GranularityLevel.MICRO: 0.01,
    GranularityLevel.FINE: 0.1,
    GranularityLevel.MEDIUM: 0.5,
    GranularityLevel.COARSE: 2.0,
    GranularityLevel.MACRO: 10.0,
}


GranularityMetrics = FlowMetrics


class GranularityPolicy(Enum):
    THROUGHPUT = "throughput"
    LATENCY = "latency"
    BALANCED = "balanced"
    MANUAL = "manual"


_GRAN_POLICY_TO_VISC = {
    GranularityPolicy.THROUGHPUT: _VP.EFFICIENT,
    GranularityPolicy.LATENCY: _VP.RESPONSIVE,
    GranularityPolicy.BALANCED: _VP.BALANCED,
    GranularityPolicy.MANUAL: _VP.MANUAL,
}

_VISC_POLICY_TO_GRAN = {v: k for k, v in _GRAN_POLICY_TO_VISC.items()}


_VISC_STR_TO_GRAN = {v.value: k for k, v in _GRAN_TO_VISC.items()}


class GranularityController:
    def __init__(
        self,
        initial_level: GranularityLevel = GranularityLevel.MEDIUM,
        policy: GranularityPolicy = GranularityPolicy.BALANCED,
        min_level: GranularityLevel = GranularityLevel.MICRO,
        max_level: GranularityLevel = GranularityLevel.MACRO,
        adjust_interval: float = 1.0,
        thresholds: Optional[Dict[str, Any]] = None,
        on_change: Optional[Callable[[GranularityLevel, GranularityLevel], None]] = None,
    ):
        def _wrap_on_change(old_v: Viscosity, new_v: Viscosity):
            if on_change:
                on_change(
                    GranularityLevel(_VISC_TO_GRAN[old_v]),
                    GranularityLevel(_VISC_TO_GRAN[new_v]),
                )

        self._vc = _VC(
            initial=_GRAN_TO_VISC[initial_level.value],
            policy=_GRAN_POLICY_TO_VISC[policy],
            min_viscosity=_GRAN_TO_VISC[min_level.value],
            max_viscosity=_GRAN_TO_VISC[max_level.value],
            adjust_interval=adjust_interval,
            thresholds=thresholds,
            on_change=_wrap_on_change if on_change else None,
        )

    @property
    def level(self) -> GranularityLevel:
        return GranularityLevel(_VISC_TO_GRAN[self._vc.viscosity])

    @property
    def policy(self) -> GranularityPolicy:
        vp = self._vc.policy
        for gp, vp_map in _GRAN_POLICY_TO_VISC.items():
            if vp_map == vp:
                return gp
        return GranularityPolicy.BALANCED

    @property
    def metrics(self) -> FlowMetrics:
        return self._vc.metrics

    @property
    def batch_size(self) -> int:
        return self._vc.batch_size

    @property
    def batch_timeout(self) -> float:
        return self._vc.batch_timeout

    @property
    def adjustments_count(self) -> int:
        return self._vc.adjustments_count

    @property
    def history(self) -> List[Dict[str, Any]]:
        raw = self._vc.history
        result = []
        for h in raw:
            entry = dict(h)
            for key in ("from", "to"):
                if key in entry:
                    val = entry[key]
                    if isinstance(val, Viscosity):
                        entry[key] = _VISC_TO_GRAN[val]
                    elif isinstance(val, str) and val in _VISC_STR_TO_GRAN:
                        entry[key] = _VISC_STR_TO_GRAN[val]
            entry.pop("eta_from", None)
            entry.pop("eta_to", None)
            if "metrics" in entry and isinstance(entry["metrics"], dict):
                m = entry["metrics"]
                m.pop("shear_rate", None)
                m.pop("shear_stress", None)
                m.pop("measured_viscosity", None)
            result.append(entry)
        return result

    def set_level(self, level: GranularityLevel) -> None:
        self._vc.set_viscosity(_GRAN_TO_VISC[level.value])

    def update_metrics(self, **kwargs) -> None:
        self._vc.update_metrics(**kwargs)

    def adjust(self) -> GranularityLevel:
        self._vc.adjust()
        return self.level

    def coerce_to_streaming(self) -> None:
        self._vc.thaw()

    def coerce_to_batch(self) -> None:
        self._vc.freeze()

    def explain(self) -> str:
        level = self.level
        lines = [
            f"GranularityController:",
            f"  Level: {level.value}",
            f"  Policy: {self.policy.value}",
            f"  Batch size: {self.batch_size}",
            f"  Batch timeout: {self.batch_timeout}s",
            f"  Range: [{self._vc._min_level.value}, {self._vc._max_level.value}]",
            f"  Adjustments: {self.adjustments_count}",
            f"  Metrics:",
            f"    Arrival rate: {self.metrics.arrival_rate:.1f}/s",
            f"    Queue depth: {self.metrics.queue_depth}",
            f"    Latency: {self.metrics.processing_latency_ms:.1f}ms",
            f"    Throughput: {self.metrics.throughput_per_sec:.1f}/s",
            f"    Backlog: {self.metrics.backlog_size}",
            f"    Error rate: {self.metrics.error_rate:.3f}",
        ]
        return "\n".join(lines)

    @property
    def _on_change(self):
        return self._vc._on_change

    @_on_change.setter
    def _on_change(self, val):
        if val is not None:
            original = val
            def _wrapper(old_v, new_v):
                from liutang.core.viscosity import Viscosity as V
                if isinstance(old_v, V):
                    old_g = _VISC_TO_GRAN[old_v]
                elif isinstance(old_v, str):
                    old_g = _VISC_STR_TO_GRAN.get(old_v, old_v)
                else:
                    old_g = old_v
                if isinstance(new_v, V):
                    new_g = _VISC_TO_GRAN[new_v]
                elif isinstance(new_v, str):
                    new_g = _VISC_STR_TO_GRAN.get(new_v, new_v)
                else:
                    new_g = new_v
                original(GranularityLevel(old_g), GranularityLevel(new_g))
            self._vc._on_change = _wrapper
        else:
            self._vc._on_change = None