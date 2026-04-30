from __future__ import annotations

import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional


@dataclass
class StateConfig:
    ttl_seconds: Optional[float] = None
    update_type: str = "on_read_and_write"


class StateBackend(ABC):
    @abstractmethod
    def get_value(self, key: str) -> Any:
        ...

    @abstractmethod
    def set_value(self, key: str, value: Any) -> None:
        ...

    @abstractmethod
    def get_list(self, key: str) -> list:
        ...

    @abstractmethod
    def append_list(self, key: str, value: Any) -> None:
        ...

    @abstractmethod
    def get_map(self, key: str) -> dict:
        ...

    @abstractmethod
    def put_map(self, key: str, map_key: Any, map_value: Any) -> None:
        ...


class MemoryStateBackend(StateBackend):
    def __init__(self) -> None:
        self._values: Dict[str, Any] = {}
        self._lists: Dict[str, List[Any]] = {}
        self._maps: Dict[str, Dict[Any, Any]] = {}
        self._ttl: Dict[str, float] = {}

    def get_value(self, key: str) -> Any:
        return self._values.get(key)

    def set_value(self, key: str, value: Any) -> None:
        self._values[key] = value
        self._ttl[key] = time.monotonic()

    def get_list(self, key: str) -> list:
        return list(self._lists.get(key, []))

    def append_list(self, key: str, value: Any) -> None:
        if key not in self._lists:
            self._lists[key] = []
        self._lists[key].append(value)

    def get_map(self, key: str) -> dict:
        return dict(self._maps.get(key, {}))

    def put_map(self, key: str, map_key: Any, map_value: Any) -> None:
        if key not in self._maps:
            self._maps[key] = {}
        self._maps[key][map_key] = map_value


class KeyedState:
    def __init__(self, backend: StateBackend, key: str):
        self._backend = backend
        self._key = key

    @property
    def value(self) -> Any:
        return self._backend.get_value(self._key)

    @value.setter
    def value(self, val: Any) -> None:
        self._backend.set_value(self._key, val)

    def get_list(self) -> list:
        return self._backend.get_list(self._key)

    def append(self, item: Any) -> None:
        self._backend.append_list(self._key, item)

    def get_map(self) -> dict:
        return self._backend.get_map(self._key)

    def put(self, k: Any, v: Any) -> None:
        self._backend.put_map(self._key, k, v)