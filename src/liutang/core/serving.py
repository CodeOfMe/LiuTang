from __future__ import annotations

import threading
import time
from typing import Any, Callable, Dict, List, Optional


class ServingView:
    def __init__(self, key_fn: Optional[Callable] = None, merge_fn: Optional[Callable] = None):
        self._data: Dict[Any, Any] = {}
        self._key_fn = key_fn or (lambda x: x)
        self._merge_fn = merge_fn or (lambda batch, speed: speed if speed is not None else batch)
        self._lock = threading.Lock()
        self._last_batch_time: float = 0.0
        self._last_speed_time: float = 0.0

    def update_batch(self, results: List[Any]) -> None:
        with self._lock:
            for item in results:
                key = self._key_fn(item)
                existing = self._data.get(key)
                if existing is not None and "source" in existing:
                    self._data[key] = self._merge_fn(existing, item)
                else:
                    self._data[key] = {"value": item, "source": "batch", "ts": time.monotonic()}
            self._last_batch_time = time.monotonic()

    def update_speed(self, results: List[Any]) -> None:
        with self._lock:
            for item in results:
                key = self._key_fn(item)
                existing = self._data.get(key)
                if existing is not None and "source" in existing:
                    self._data[key] = self._merge_fn(existing, item)
                else:
                    self._data[key] = {"value": item, "source": "speed", "ts": time.monotonic()}
            self._last_speed_time = time.monotonic()

    def query(self, key: Any = None) -> Any:
        with self._lock:
            if key is not None:
                entry = self._data.get(key)
                return entry.get("value") if entry and isinstance(entry, dict) and "value" in entry else entry
            return {k: (v.get("value") if isinstance(v, dict) and "value" in v else v) for k, v in self._data.items()}

    def query_all(self) -> List[Any]:
        with self._lock:
            return [(k, v.get("value") if isinstance(v, dict) and "value" in v else v) for k, v in self._data.items()]

    @property
    def last_batch_time(self) -> float:
        return self._last_batch_time

    @property
    def last_speed_time(self) -> float:
        return self._last_speed_time

    def clear(self) -> None:
        with self._lock:
            self._data.clear()


class MergeView:
    @staticmethod
    def latest(batch_result: Any, speed_result: Any) -> Any:
        if speed_result is not None:
            return speed_result
        return batch_result

    @staticmethod
    def prefer_batch(batch_result: Any, speed_result: Any) -> Any:
        if batch_result is not None:
            return batch_result
        return speed_result

    @staticmethod
    def combine_sum(batch_result: Any, speed_result: Any) -> Any:
        b = batch_result or 0
        s = speed_result or 0
        return b + s

    @staticmethod
    def merge_dicts(batch_result: Dict, speed_result: Dict) -> Dict:
        merged = dict(batch_result) if batch_result else {}
        if speed_result:
            merged.update(speed_result)
        return merged