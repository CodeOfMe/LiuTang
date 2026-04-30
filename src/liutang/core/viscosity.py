from __future__ import annotations

import math
import threading
import time
from enum import Enum
from typing import Any, Callable, Dict, List, Optional


_VISCOSITY_ORDER = {
    "volatile": 0,
    "fluid": 1,
    "honeyed": 2,
    "sluggish": 3,
    "frozen": 4,
}


class Viscosity(Enum):
    VOLATILE = "volatile"
    FLUID = "fluid"
    HONEYED = "honeyed"
    SLUGGISH = "sluggish"
    FROZEN = "frozen"

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, Viscosity):
            return NotImplemented
        return _VISCOSITY_ORDER[self.value] < _VISCOSITY_ORDER[other.value]

    def __le__(self, other: object) -> bool:
        if not isinstance(other, Viscosity):
            return NotImplemented
        return _VISCOSITY_ORDER[self.value] <= _VISCOSITY_ORDER[other.value]

    def __gt__(self, other: object) -> bool:
        if not isinstance(other, Viscosity):
            return NotImplemented
        return _VISCOSITY_ORDER[self.value] > _VISCOSITY_ORDER[other.value]

    def __ge__(self, other: object) -> bool:
        if not isinstance(other, Viscosity):
            return NotImplemented
        return _VISCOSITY_ORDER[self.value] >= _VISCOSITY_ORDER[other.value]

    @property
    def batch_size(self) -> int:
        return _VISCOSITY_BATCH_SIZES.get(self, 100)

    @property
    def batch_timeout(self) -> float:
        return _VISCOSITY_BATCH_TIMEOUTS.get(self, 0.5)

    @property
    def eta(self) -> float:
        return _VISCOSITY_ETA.get(self, 0.5)

    @property
    def is_flowing(self) -> bool:
        return self in (Viscosity.VOLATILE, Viscosity.FLUID)

    @property
    def is_solid(self) -> bool:
        return self in (Viscosity.SLUGGISH, Viscosity.FROZEN)

    @property
    def description(self) -> str:
        return _VISCOSITY_DESCRIPTIONS.get(self, "")


_VISCOSITY_BATCH_SIZES = {
    Viscosity.VOLATILE: 1,
    Viscosity.FLUID: 10,
    Viscosity.HONEYED: 100,
    Viscosity.SLUGGISH: 1000,
    Viscosity.FROZEN: 100000,
}

_VISCOSITY_BATCH_TIMEOUTS = {
    Viscosity.VOLATILE: 0.01,
    Viscosity.FLUID: 0.1,
    Viscosity.HONEYED: 0.5,
    Viscosity.SLUGGISH: 2.0,
    Viscosity.FROZEN: 10.0,
}

_VISCOSITY_ETA = {
    Viscosity.VOLATILE: 0.0,
    Viscosity.FLUID: 0.25,
    Viscosity.HONEYED: 0.5,
    Viscosity.SLUGGISH: 0.75,
    Viscosity.FROZEN: 1.0,
}

_VISCOSITY_DESCRIPTIONS = {
    Viscosity.VOLATILE: "如水——逐条流淌，延迟极低",
    Viscosity.FLUID: "如溪——微批流动，低延迟",
    Viscosity.HONEYED: "如蜜——缓缓流淌，吞吐与延迟均衡",
    Viscosity.SLUGGISH: "如泥——缓慢流动，高吞吐",
    Viscosity.FROZEN: "如冰——冻结为整批处理，吞吐极高",
}


class FlowMetrics:
    def __init__(self):
        self.arrival_rate: float = 0.0
        self.queue_depth: int = 0
        self.processing_latency_ms: float = 0.0
        self.throughput_per_sec: float = 0.0
        self.backlog_size: int = 0
        self.error_rate: float = 0.0
        self._timestamp: float = time.monotonic()

    @property
    def shear_rate(self) -> float:
        return self.arrival_rate

    @property
    def shear_stress(self) -> float:
        if self.arrival_rate > 0:
            return (self.backlog_size + self.processing_latency_ms) / max(1.0, self.arrival_rate)
        return float(self.backlog_size)

    @property
    def measured_viscosity(self) -> float:
        rate = max(self.arrival_rate, 0.001)
        stress = self.backlog_size + self.processing_latency_ms * rate / 1000.0
        raw = stress / rate
        return max(0.0, min(1.0, raw / max(1.0, self._reference_stress())))

    def _reference_stress(self) -> float:
        return max(100.0, self.arrival_rate)

    def snapshot(self) -> Dict[str, Any]:
        return {
            "arrival_rate": self.arrival_rate,
            "queue_depth": self.queue_depth,
            "processing_latency_ms": self.processing_latency_ms,
            "throughput_per_sec": self.throughput_per_sec,
            "backlog_size": self.backlog_size,
            "error_rate": self.error_rate,
            "shear_rate": self.shear_rate,
            "shear_stress": self.shear_stress,
            "measured_viscosity": self.measured_viscosity,
            "timestamp": self._timestamp,
        }

    def update_timestamp(self) -> None:
        self._timestamp = time.monotonic()


class ViscosityPolicy(Enum):
    RESPONSIVE = "responsive"
    EFFICIENT = "efficient"
    BALANCED = "balanced"
    MANUAL = "manual"


class ViscosityController:
    def __init__(
        self,
        initial: Viscosity = Viscosity.HONEYED,
        policy: ViscosityPolicy = ViscosityPolicy.BALANCED,
        min_viscosity: Viscosity = Viscosity.VOLATILE,
        max_viscosity: Viscosity = Viscosity.FROZEN,
        adjust_interval: float = 1.0,
        thresholds: Optional[Dict[str, Any]] = None,
        on_change: Optional[Callable[[Viscosity, Viscosity], None]] = None,
    ):
        self._level = initial
        self._policy = policy
        self._min_level = min_viscosity
        self._max_level = max_viscosity
        self._adjust_interval = adjust_interval
        self._on_change = on_change
        self._lock = threading.Lock()
        self._last_adjust_time: float = 0.0
        self._history: List[Dict[str, Any]] = []
        self._metrics = FlowMetrics()
        self._thresholds = thresholds or self._default_thresholds()
        self._level_order = list(Viscosity)
        self._adjustments_count = 0

    def _default_thresholds(self) -> Dict[str, Any]:
        return {
            "high_arrival_rate": 1000,
            "low_arrival_rate": 10,
            "high_queue_depth": 5000,
            "low_queue_depth": 10,
            "high_latency_ms": 500,
            "low_latency_ms": 10,
            "high_backlog": 10000,
            "low_backlog": 100,
        }

    @property
    def viscosity(self) -> Viscosity:
        return self._level

    @property
    def level(self) -> Viscosity:
        return self._level

    @property
    def policy(self) -> ViscosityPolicy:
        return self._policy

    @property
    def metrics(self) -> FlowMetrics:
        return self._metrics

    @property
    def batch_size(self) -> int:
        return self._level.batch_size

    @property
    def batch_timeout(self) -> float:
        return self._level.batch_timeout

    @property
    def eta(self) -> float:
        return self._level.eta

    @property
    def adjustments_count(self) -> int:
        return self._adjustments_count

    @property
    def history(self) -> List[Dict[str, Any]]:
        return list(self._history)

    def _coerce_to_viscosity(self, val) -> Viscosity:
        if isinstance(val, Viscosity):
            return val
        if hasattr(val, 'to_viscosity'):
            return val.to_viscosity()
        if isinstance(val, str):
            for v in Viscosity:
                if v.value == val:
                    return v
        raise ValueError(f"Cannot convert {val!r} to Viscosity")

    def set_viscosity(self, viscosity) -> None:
        viscosity = self._coerce_to_viscosity(viscosity)
        with self._lock:
            self._set_viscosity_unlocked(viscosity)

    def set_level(self, level) -> None:
        self.set_viscosity(level)

    def _set_viscosity_unlocked(self, viscosity: Viscosity) -> None:
        viscosity = max(self._min_level, min(self._max_level, viscosity))
        if viscosity != self._level:
            old = self._level
            self._level = viscosity
            self._adjustments_count += 1
            entry = {
                "time": time.monotonic(),
                "from": old.value,
                "to": viscosity.value,
                "eta_from": old.eta,
                "eta_to": viscosity.eta,
                "metrics": self._metrics.snapshot(),
            }
            self._history.append(entry)
            if len(self._history) > 1000:
                self._history = self._history[-500:]
            if self._on_change:
                try:
                    self._on_change(old, viscosity)
                except Exception:
                    pass

    def update_metrics(
        self,
        arrival_rate: Optional[float] = None,
        queue_depth: Optional[int] = None,
        processing_latency_ms: Optional[float] = None,
        throughput_per_sec: Optional[float] = None,
        backlog_size: Optional[int] = None,
        error_rate: Optional[float] = None,
    ) -> None:
        with self._lock:
            if arrival_rate is not None:
                self._metrics.arrival_rate = arrival_rate
            if queue_depth is not None:
                self._metrics.queue_depth = queue_depth
            if processing_latency_ms is not None:
                self._metrics.processing_latency_ms = processing_latency_ms
            if throughput_per_sec is not None:
                self._metrics.throughput_per_sec = throughput_per_sec
            if backlog_size is not None:
                self._metrics.backlog_size = backlog_size
            if error_rate is not None:
                self._metrics.error_rate = error_rate
            self._metrics.update_timestamp()

    def adjust(self) -> Viscosity:
        if self._policy == ViscosityPolicy.MANUAL:
            return self._level
        now = time.monotonic()
        if now - self._last_adjust_time < self._adjust_interval:
            return self._level
        self._last_adjust_time = now
        with self._lock:
            if self._policy == ViscosityPolicy.EFFICIENT:
                self._adjust_efficient()
            elif self._policy == ViscosityPolicy.RESPONSIVE:
                self._adjust_responsive()
            else:
                self._adjust_balanced()
            return self._level

    def _adjust_efficient(self) -> None:
        m = self._metrics
        t = self._thresholds
        score = 0
        if m.arrival_rate > t["high_arrival_rate"]:
            score += 2
        elif m.arrival_rate > t["high_arrival_rate"] / 2:
            score += 1
        if m.queue_depth > t["high_queue_depth"]:
            score += 2
        elif m.queue_depth > t["high_queue_depth"] / 2:
            score += 1
        if m.backlog_size > t["high_backlog"]:
            score += 2
        elif m.backlog_size > t["high_backlog"] / 2:
            score += 1
        if m.error_rate > 0.1:
            score -= 1
        idx = self._level_order.index(self._level)
        if score >= 3 and idx < len(self._level_order) - 1:
            self._set_viscosity_unlocked(self._level_order[min(idx + 2, len(self._level_order) - 1)])
        elif score >= 1 and idx < len(self._level_order) - 1:
            self._set_viscosity_unlocked(self._level_order[idx + 1])
        elif score <= -1 and idx > 0:
            self._set_viscosity_unlocked(self._level_order[idx - 1])

    def _adjust_responsive(self) -> None:
        m = self._metrics
        t = self._thresholds
        score = 0
        if m.processing_latency_ms > t["high_latency_ms"]:
            score -= 2
        elif m.processing_latency_ms > t["high_latency_ms"] / 2:
            score -= 1
        if m.arrival_rate < t["low_arrival_rate"]:
            score += 1
        if m.queue_depth < t["low_queue_depth"]:
            score += 1
        if m.backlog_size < t["low_backlog"]:
            score += 1
        if m.error_rate > 0.1:
            score -= 1
        idx = self._level_order.index(self._level)
        if score >= 2 and idx > 0:
            self._set_viscosity_unlocked(self._level_order[max(idx - 2, 0)])
        elif score >= 1 and idx > 0:
            self._set_viscosity_unlocked(self._level_order[idx - 1])
        elif score <= -2 and idx < len(self._level_order) - 1:
            self._set_viscosity_unlocked(self._level_order[idx + 1])

    def _adjust_balanced(self) -> None:
        m = self._metrics
        t = self._thresholds
        score = 0
        if m.arrival_rate > t["high_arrival_rate"]:
            score += 1
        elif m.arrival_rate < t["low_arrival_rate"]:
            score -= 1
        if m.queue_depth > t["high_queue_depth"]:
            score += 1
        elif m.queue_depth < t["low_queue_depth"]:
            score -= 1
        if m.processing_latency_ms > t["high_latency_ms"]:
            score -= 1
        if m.backlog_size > t["high_backlog"]:
            score += 1
        elif m.backlog_size < t["low_backlog"]:
            score -= 1
        if m.error_rate > 0.1:
            score -= 1
        idx = self._level_order.index(self._level)
        if score >= 2 and idx < len(self._level_order) - 1:
            self._set_viscosity_unlocked(self._level_order[idx + 1])
        elif score <= -2 and idx > 0:
            self._set_viscosity_unlocked(self._level_order[idx - 1])

    def thaw(self) -> None:
        self.set_viscosity(Viscosity.VOLATILE)

    def freeze(self) -> None:
        self.set_viscosity(Viscosity.FROZEN)

    def flow_freely(self) -> None:
        self.thaw()

    def flow_as_batch(self) -> None:
        self.freeze()

    def coerce_to_streaming(self) -> None:
        self.thaw()

    def coerce_to_batch(self) -> None:
        self.freeze()

    def explain(self) -> str:
        lines = [
            f"ViscosityController:",
            f"  Viscosity: {self._level.value} (η={self.eta:.2f})",
            f"  Policy: {self._policy.value}",
            f"  Batch size: {self.batch_size}",
            f"  Batch timeout: {self.batch_timeout}s",
            f"  Description: {self._level.description}",
            f"  Range: [{self._min_level.value}, {self._max_level.value}]",
            f"  Adjustments: {self._adjustments_count}",
            f"  Flow Metrics:",
            f"    Shear rate (arrival): {self._metrics.shear_rate:.1f}/s",
            f"    Shear stress: {self._metrics.shear_stress:.1f}",
            f"    Measured η: {self._metrics.measured_viscosity:.3f}",
            f"    Queue depth: {self._metrics.queue_depth}",
            f"    Latency: {self._metrics.processing_latency_ms:.1f}ms",
            f"    Throughput: {self._metrics.throughput_per_sec:.1f}/s",
            f"    Backlog: {self._metrics.backlog_size}",
            f"    Error rate: {self._metrics.error_rate:.3f}",
        ]
        return "\n".join(lines)