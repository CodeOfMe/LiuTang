from __future__ import annotations

from typing import Any, Optional

from liutang.core.state import WatermarkStrategy


class WatermarkTracker:
    def __init__(self, strategy: WatermarkStrategy):
        self._strategy = strategy
        self._max_timestamp: float = float("-inf")
        self._current_watermark: float = float("-inf")

    def on_event(self, timestamp: float) -> float:
        self._max_timestamp = max(self._max_timestamp, timestamp)
        if self._strategy.strategy == "monotonous":
            self._current_watermark = self._max_timestamp
        elif self._strategy.strategy == "bounded_out_of_orderness":
            self._current_watermark = self._max_timestamp - self._strategy.out_of_orderness
        return self._current_watermark

    def current_watermark(self) -> float:
        return self._current_watermark

    def is_late(self, timestamp: float) -> bool:
        if self._strategy.strategy == "monotonous":
            return timestamp < self._current_watermark
        elif self._strategy.strategy == "bounded_out_of_orderness":
            return timestamp < self._current_watermark - self._strategy.out_of_orderness
        return False