from __future__ import annotations

import time
import threading
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Set


@dataclass
class StateConfig:
    ttl_seconds: Optional[float] = None
    update_type: str = "on_read_and_write"


class ValueState:
    def __init__(self, name: str, ttl: Optional[float] = None):
        self._name = name
        self._value: Any = None
        self._has_value: bool = False
        self._ttl = ttl
        self._last_access: float = 0.0

    @property
    def value(self) -> Any:
        if self._ttl and self._has_value:
            if time.monotonic() - self._last_access > self._ttl:
                self._value = None
                self._has_value = False
                return None
        self._last_access = time.monotonic()
        return self._value

    @value.setter
    def value(self, val: Any) -> None:
        self._value = val
        self._has_value = True
        self._last_access = time.monotonic()

    def clear(self) -> None:
        self._value = None
        self._has_value = False

    def update(self, val: Any) -> None:
        self.value = val


class ListState:
    def __init__(self, name: str, ttl: Optional[float] = None):
        self._name = name
        self._values: List[Any] = []
        self._ttl = ttl
        self._last_access: float = time.monotonic()

    def add(self, value: Any) -> None:
        self._values.append(value)
        self._last_access = time.monotonic()

    def get(self) -> List[Any]:
        if self._ttl and self._values:
            if time.monotonic() - self._last_access > self._ttl:
                self._values = []
                return None
        self._last_access = time.monotonic()
        return list(self._values)

    def clear(self) -> None:
        self._values = []

    def __len__(self) -> int:
        return len(self._values)

    def __iter__(self):
        return iter(self._values)


class MapState:
    def __init__(self, name: str, ttl: Optional[float] = None):
        self._name = name
        self._entries: Dict[Any, Any] = {}
        self._ttl = ttl
        self._last_access: float = time.monotonic()

    def get(self, key: Any) -> Any:
        if self._ttl and self._entries:
            if time.monotonic() - self._last_access > self._ttl:
                self._entries = {}
                return None
        self._last_access = time.monotonic()
        return self._entries.get(key)

    def put(self, key: Any, value: Any) -> None:
        self._entries[key] = value
        self._last_access = time.monotonic()

    def remove(self, key: Any) -> None:
        self._entries.pop(key, None)

    def keys(self) -> Set[Any]:
        return set(self._entries.keys())

    def values(self) -> List[Any]:
        return list(self._entries.values())

    def items(self):
        return self._entries.items()

    def contains(self, key: Any) -> bool:
        return key in self._entries

    def clear(self) -> None:
        self._entries = {}


class ReducingState:
    def __init__(self, name: str, reduce_fn: Callable[[Any, Any], Any], ttl: Optional[float] = None):
        self._name = name
        self._value: Any = None
        self._has_value: bool = False
        self._reduce_fn = reduce_fn
        self._ttl = ttl
        self._last_access: float = time.monotonic()

    def add(self, value: Any) -> None:
        if self._has_value:
            self._value = self._reduce_fn(self._value, value)
        else:
            self._value = value
            self._has_value = True
        self._last_access = time.monotonic()

    def get(self) -> Any:
        if self._ttl and self._has_value:
            if time.monotonic() - self._last_access > self._ttl:
                self._value = None
                self._has_value = False
                return None
        return self._value

    def clear(self) -> None:
        self._value = None
        self._has_value = False


class AggregatingState:
    def __init__(self, name: str, add_fn: Callable[[Any, Any], Any],
                 merge_fn: Optional[Callable[[Any, Any], Any]] = None,
                 init_value: Any = None, ttl: Optional[float] = None):
        self._name = name
        self._accumulator: Any = init_value
        self._add_fn = add_fn
        self._merge_fn = merge_fn
        self._has_value: bool = init_value is not None
        self._ttl = ttl
        self._last_access: float = time.monotonic()

    def add(self, value: Any) -> None:
        if self._has_value:
            self._accumulator = self._add_fn(self._accumulator, value)
        else:
            self._accumulator = self._add_fn(value, value) if self._add_fn else value
            self._has_value = True
        self._last_access = time.monotonic()

    def get(self) -> Any:
        if self._ttl and self._has_value:
            if time.monotonic() - self._last_access > self._ttl:
                self._accumulator = None
                self._has_value = False
                return None
        return self._accumulator

    def clear(self) -> None:
        self._accumulator = None
        self._has_value = False


class RuntimeContext:
    def __init__(self) -> None:
        self._states: Dict[str, Any] = {}
        self._current_key: Any = None
        self._keyed_states: Dict[Any, Dict[str, Any]] = defaultdict(dict)
        self._event_time_timer_service: Optional[TimerService] = None

    def set_current_key(self, key: Any) -> None:
        self._current_key = key

    def get_state(self, name: str) -> ValueState:
        key_states = self._keyed_states[self._current_key]
        if name not in key_states:
            state = ValueState(name)
            key_states[name] = state
        return key_states[name]

    def get_list_state(self, name: str) -> ListState:
        key_states = self._keyed_states[self._current_key]
        if name not in key_states:
            state = ListState(name)
            key_states[name] = state
        return key_states[name]

    def get_map_state(self, name: str) -> MapState:
        key_states = self._keyed_states[self._current_key]
        if name not in key_states:
            state = MapState(name)
            key_states[name] = state
        return key_states[name]

    def get_reducing_state(self, name: str, reduce_fn: Callable) -> ReducingState:
        key_states = self._keyed_states[self._current_key]
        if name not in key_states:
            state = ReducingState(name, reduce_fn)
            key_states[name] = state
        return key_states[name]

    def get_aggregating_state(self, name: str, add_fn: Callable,
                               merge_fn: Optional[Callable] = None,
                               init_value: Any = None) -> AggregatingState:
        key_states = self._keyed_states[self._current_key]
        if name not in key_states:
            state = AggregatingState(name, add_fn, merge_fn, init_value)
            key_states[name] = state
        return key_states[name]

    def current_key(self) -> Any:
        return self._current_key

    @property
    def timer_service(self) -> Optional["TimerService"]:
        return self._event_time_timer_service

    @timer_service.setter
    def timer_service(self, service: "TimerService") -> None:
        self._event_time_timer_service = service


class TimerService:
    def __init__(self) -> None:
        self._event_time_timers: Dict[float, List[Callable]] = defaultdict(list)
        self._processing_time_timers: Dict[float, List[Callable]] = defaultdict(list)
        self._current_watermark: float = float("-inf")

    @property
    def current_watermark(self) -> float:
        return self._current_watermark

    def advance_watermark(self, timestamp: float) -> None:
        self._current_watermark = max(self._current_watermark, timestamp)

    def register_event_time_timer(self, timestamp: float, callback: Optional[Callable] = None) -> None:
        self._event_time_timers[timestamp].append(callback)

    def register_processing_time_timer(self, timestamp: float, callback: Optional[Callable] = None) -> None:
        self._processing_time_timers[timestamp].append(callback)

    def fire_event_time_timers(self, watermark: float) -> List[Callable]:
        fired = []
        for ts in sorted(self._event_time_timers.keys()):
            if ts <= watermark:
                callbacks = self._event_time_timers.pop(ts)
                fired.extend(c for c in callbacks if c is not None)
        return fired

    def fire_processing_time_timers(self, current_time: float) -> List[Callable]:
        fired = []
        for ts in sorted(self._processing_time_timers.keys()):
            if ts <= current_time:
                callbacks = self._processing_time_timers.pop(ts)
                fired.extend(c for c in callbacks if c is not None)
        return fired

    def has_pending_timers(self) -> bool:
        return bool(self._event_time_timers) or bool(self._processing_time_timers)


class KeyedProcessFunction(ABC):
    def __init__(self) -> None:
        self._runtime_context: Optional[RuntimeContext] = None

    def open(self, context: RuntimeContext) -> None:
        self._runtime_context = context

    @abstractmethod
    def process_element(self, value: Any, ctx: RuntimeContext) -> Optional[Any]:
        ...

    def on_timer(self, timestamp: float, ctx: RuntimeContext) -> Optional[Any]:
        return None

    def close(self) -> None:
        pass


class ProcessFunction(ABC):
    @abstractmethod
    def process(self, value: Any) -> Optional[Any]:
        ...


class Watermark:
    def __init__(self, timestamp: float) -> None:
        self.timestamp = timestamp

    def __repr__(self) -> str:
        return f"Watermark({self.timestamp})"


class WatermarkStrategy:
    def __init__(self, strategy: str = "monotonous", out_of_orderness: float = 0.0,
                 time_field: Optional[str] = None, idle_timeout: Optional[float] = None):
        self.strategy = strategy
        self.out_of_orderness = out_of_orderness
        self.time_field = time_field
        self.idle_timeout = idle_timeout
        self._current_watermark: float = float("-inf")
        self._max_timestamp: float = float("-inf")

    def on_event(self, record: Any, timestamp: float) -> Watermark:
        self._max_timestamp = max(self._max_timestamp, timestamp)
        if self.strategy == "monotonous":
            self._current_watermark = self._max_timestamp
        elif self.strategy == "bounded_out_of_orderness":
            self._current_watermark = self._max_timestamp - self.out_of_orderness
        return Watermark(self._current_watermark)

    def current_watermark(self) -> Watermark:
        return Watermark(self._current_watermark)

    @staticmethod
    def monotonous(time_field: Optional[str] = None) -> "WatermarkStrategy":
        return WatermarkStrategy(strategy="monotonous", time_field=time_field)

    @staticmethod
    def bounded_out_of_orderness(lateness: float, time_field: Optional[str] = None) -> "WatermarkStrategy":
        return WatermarkStrategy(strategy="bounded_out_of_orderness", out_of_orderness=lateness, time_field=time_field)

    @staticmethod
    def no_watermarks() -> "WatermarkStrategy":
        return WatermarkStrategy(strategy="monotonous")


class MemoryStateBackend:
    def __init__(self) -> None:
        self._values: Dict[str, Any] = {}
        self._lists: Dict[str, List[Any]] = {}
        self._maps: Dict[str, Dict[Any, Any]] = {}
        self._ttl: Dict[str, float] = {}
        self._lock = threading.Lock()

    def get_value(self, key: str) -> Any:
        with self._lock:
            return self._values.get(key)

    def set_value(self, key: str, value: Any) -> None:
        with self._lock:
            self._values[key] = value
            self._ttl[key] = time.monotonic()

    def get_list(self, key: str) -> List[Any]:
        with self._lock:
            return list(self._lists.get(key, []))

    def append_list(self, key: str, value: Any) -> None:
        with self._lock:
            if key not in self._lists:
                self._lists[key] = []
            self._lists[key].append(value)

    def get_map(self, key: str) -> Dict[Any, Any]:
        with self._lock:
            return dict(self._maps.get(key, {}))

    def put_map(self, key: str, map_key: Any, map_value: Any) -> None:
        with self._lock:
            if key not in self._maps:
                self._maps[key] = {}
            self._maps[key][map_key] = map_value

    def checkpoint(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "values": dict(self._values),
                "lists": {k: list(v) for k, v in self._lists.items()},
                "maps": {k: dict(v) for k, v in self._maps.items()},
            }

    def restore(self, snapshot: Dict[str, Any]) -> None:
        with self._lock:
            self._values = dict(snapshot.get("values", {}))
            self._lists = {k: list(v) for k, v in snapshot.get("lists", {}).items()}
            self._maps = {k: dict(v) for k, v in snapshot.get("maps", {}).items()}


class KeyedState:
    def __init__(self, backend: MemoryStateBackend, key: str):
        self._backend = backend
        self._key = key

    @property
    def value(self) -> Any:
        return self._backend.get_value(self._key)

    @value.setter
    def value(self, val: Any) -> None:
        self._backend.set_value(self._key, val)

    def get_list(self) -> List[Any]:
        return self._backend.get_list(self._key)

    def append(self, item: Any) -> None:
        self._backend.append_list(self._key, item)

    def get_map(self) -> Dict[Any, Any]:
        return self._backend.get_map(self._key)

    def put(self, k: Any, v: Any) -> None:
        self._backend.put_map(self._key, k, v)


class JsonFileStateBackend(MemoryStateBackend):
    def __init__(self, directory: str):
        super().__init__()
        self._directory = directory
        import os
        os.makedirs(directory, exist_ok=True)

    def _checkpoint_path(self, name: str = "state") -> str:
        import os
        return os.path.join(self._directory, f"{name}.json")

    def save(self, name: str = "state") -> None:
        import json
        snapshot = self.checkpoint()
        path = self._checkpoint_path(name)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(snapshot, f, default=str, indent=2)

    def load(self, name: str = "state") -> bool:
        import json
        import os
        path = self._checkpoint_path(name)
        if not os.path.exists(path):
            return False
        with open(path, "r", encoding="utf-8") as f:
            snapshot = json.load(f)
        self.restore(snapshot)
        return True