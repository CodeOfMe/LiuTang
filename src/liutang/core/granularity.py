from __future__ import annotations

import threading
import time
from enum import Enum
from typing import Any, Callable, Dict, List, Optional


_GRANULARITY_ORDER = {"micro": 0, "fine": 1, "medium": 2, "coarse": 3, "macro": 4}


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


class GranularityMetrics:
    def __init__(self):
        self.arrival_rate: float = 0.0
        self.queue_depth: int = 0
        self.processing_latency_ms: float = 0.0
        self.throughput_per_sec: float = 0.0
        self.backlog_size: int = 0
        self.error_rate: float = 0.0
        self._timestamp: float = time.monotonic()

    def snapshot(self) -> Dict[str, Any]:
        return {
            "arrival_rate": self.arrival_rate,
            "queue_depth": self.queue_depth,
            "processing_latency_ms": self.processing_latency_ms,
            "throughput_per_sec": self.throughput_per_sec,
            "backlog_size": self.backlog_size,
            "error_rate": self.error_rate,
            "timestamp": self._timestamp,
        }

    def update_timestamp(self) -> None:
        self._timestamp = time.monotonic()


class GranularityPolicy(Enum):
    THROUGHPUT = "throughput"
    LATENCY = "latency"
    BALANCED = "balanced"
    MANUAL = "manual"


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
        self._level = initial_level
        self._policy = policy
        self._min_level = min_level
        self._max_level = max_level
        self._adjust_interval = adjust_interval
        self._on_change = on_change
        self._lock = threading.Lock()
        self._last_adjust_time: float = 0.0
        self._history: List[Dict[str, Any]] = []
        self._metrics = GranularityMetrics()
        self._thresholds = thresholds or self._default_thresholds()
        self._level_order = [
            GranularityLevel.MICRO,
            GranularityLevel.FINE,
            GranularityLevel.MEDIUM,
            GranularityLevel.COARSE,
            GranularityLevel.MACRO,
        ]
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
    def level(self) -> GranularityLevel:
        return self._level

    @property
    def policy(self) -> GranularityPolicy:
        return self._policy

    @property
    def metrics(self) -> GranularityMetrics:
        return self._metrics

    @property
    def batch_size(self) -> int:
        return self._level.batch_size

    @property
    def batch_timeout(self) -> float:
        return self._level.batch_timeout

    @property
    def adjustments_count(self) -> int:
        return self._adjustments_count

    @property
    def history(self) -> List[Dict[str, Any]]:
        return list(self._history)

    def set_level(self, level: GranularityLevel) -> None:
        with self._lock:
            self._set_level_unlocked(level)

    def _set_level_unlocked(self, level: GranularityLevel) -> None:
        level = max(self._min_level, min(self._max_level, level))
        if level != self._level:
            old = self._level
            self._level = level
            self._adjustments_count += 1
            entry = {
                "time": time.monotonic(),
                "from": old.value,
                "to": level.value,
                "metrics": self._metrics.snapshot(),
            }
            self._history.append(entry)
            if len(self._history) > 1000:
                self._history = self._history[-500:]
            if self._on_change:
                try:
                    self._on_change(old, level)
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

    def adjust(self) -> GranularityLevel:
        if self._policy == GranularityPolicy.MANUAL:
            return self._level
        now = time.monotonic()
        if now - self._last_adjust_time < self._adjust_interval:
            return self._level
        self._last_adjust_time = now
        with self._lock:
            if self._policy == GranularityPolicy.THROUGHPUT:
                self._adjust_throughput()
            elif self._policy == GranularityPolicy.LATENCY:
                self._adjust_latency()
            else:
                self._adjust_balanced()
            return self._level

    def _adjust_throughput(self) -> None:
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
            self._set_level_unlocked(self._level_order[min(idx + 2, len(self._level_order) - 1)])
        elif score >= 1 and idx < len(self._level_order) - 1:
            self._set_level_unlocked(self._level_order[idx + 1])
        elif score <= -1 and idx > 0:
            self._set_level_unlocked(self._level_order[idx - 1])

    def _adjust_latency(self) -> None:
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
            self._set_level_unlocked(self._level_order[max(idx - 2, 0)])
        elif score >= 1 and idx > 0:
            self._set_level_unlocked(self._level_order[idx - 1])
        elif score <= -2 and idx < len(self._level_order) - 1:
            self._set_level_unlocked(self._level_order[idx + 1])

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
            self._set_level_unlocked(self._level_order[idx + 1])
        elif score <= -2 and idx > 0:
            self._set_level_unlocked(self._level_order[idx - 1])

    def coerce_to_streaming(self) -> None:
        self.set_level(GranularityLevel.MICRO)

    def coerce_to_batch(self) -> None:
        self.set_level(GranularityLevel.MACRO)

    def explain(self) -> str:
        lines = [
            f"GranularityController:",
            f"  Level: {self._level.value}",
            f"  Policy: {self._policy.value}",
            f"  Batch size: {self.batch_size}",
            f"  Batch timeout: {self.batch_timeout}s",
            f"  Range: [{self._min_level.value}, {self._max_level.value}]",
            f"  Adjustments: {self._adjustments_count}",
            f"  Metrics:",
            f"    Arrival rate: {self._metrics.arrival_rate:.1f}/s",
            f"    Queue depth: {self._metrics.queue_depth}",
            f"    Latency: {self._metrics.processing_latency_ms:.1f}ms",
            f"    Throughput: {self._metrics.throughput_per_sec:.1f}/s",
            f"    Backlog: {self._metrics.backlog_size}",
            f"    Error rate: {self._metrics.error_rate:.3f}",
        ]
        return "\n".join(lines)