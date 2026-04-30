from __future__ import annotations

import json
import pickle
from typing import Any


def serialize(obj: Any, fmt: str = "json") -> bytes:
    if fmt == "json":
        return json.dumps(obj, default=_default_serializer).encode("utf-8")
    elif fmt == "pickle":
        return pickle.dumps(obj)
    raise ValueError(f"Unknown serialization format: {fmt}")


def deserialize(data: bytes, fmt: str = "json") -> Any:
    if fmt == "json":
        return json.loads(data.decode("utf-8"))
    elif fmt == "pickle":
        return pickle.loads(data)
    raise ValueError(f"Unknown deserialization format: {fmt}")


def to_json(obj: Any) -> str:
    return json.dumps(obj, default=_default_serializer, ensure_ascii=False)


def from_json(s: str) -> Any:
    return json.loads(s)


def _default_serializer(obj: Any) -> Any:
    if hasattr(obj, "__dict__"):
        return obj.__dict__
    if hasattr(obj, "isoformat"):
        return obj.isoformat()
    return str(obj)