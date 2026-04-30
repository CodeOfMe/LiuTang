from __future__ import annotations

from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Union

from liutang.core.schema import Schema
from liutang.core.errors import PipelineError


class RuntimeMode(Enum):
    STREAMING = "streaming"
    BATCH = "batch"


class Stream:
    def __init__(self, flow: "Flow", stream_id: str, schema: Optional[Schema] = None):
        self._flow = flow
        self._id = stream_id
        self._schema = schema
        self._operations: List[Dict[str, Any]] = []

    @property
    def flow(self) -> "Flow":
        return self._flow

    @property
    def id(self) -> str:
        return self._id

    @property
    def schema(self) -> Optional[Schema]:
        return self._schema

    def map(self, func: Callable, name: Optional[str] = None) -> "Stream":
        self._operations.append({
            "type": "map", "func": func,
            "name": name or f"map_{len(self._operations)}",
        })
        return self

    def flat_map(self, func: Callable, name: Optional[str] = None) -> "Stream":
        self._operations.append({
            "type": "flat_map", "func": func,
            "name": name or f"flat_map_{len(self._operations)}",
        })
        return self

    def filter(self, predicate: Callable, name: Optional[str] = None) -> "Stream":
        self._operations.append({
            "type": "filter", "func": predicate,
            "name": name or f"filter_{len(self._operations)}",
        })
        return self

    def key_by(self, key_selector: Union[str, Callable], name: Optional[str] = None) -> "KeyedStream":
        self._operations.append({
            "type": "key_by", "func": key_selector,
            "name": name or f"key_by_{len(self._operations)}",
        })
        return KeyedStream(self, key_selector)

    def reduce(self, func: Callable, name: Optional[str] = None) -> "Stream":
        self._operations.append({
            "type": "reduce", "func": func,
            "name": name or f"reduce_{len(self._operations)}",
        })
        return self

    def sum(self, field: Union[str, int] = 0, name: Optional[str] = None) -> "Stream":
        self._operations.append({
            "type": "sum", "field": field,
            "name": name or f"sum_{len(self._operations)}",
        })
        return self

    def count(self, name: Optional[str] = None) -> "Stream":
        self._operations.append({
            "type": "count",
            "name": name or f"count_{len(self._operations)}",
        })
        return self

    def window(self, window_type: "WindowType") -> "WindowedStream":
        return WindowedStream(self, window_type)

    def process(self, func: Callable, name: Optional[str] = None) -> "Stream":
        self._operations.append({
            "type": "process", "func": func,
            "name": name or f"process_{len(self._operations)}",
        })
        return self

    def to_table(self, schema: Optional[Schema] = None, name: Optional[str] = None) -> "TableStream":
        tbl_schema = schema or self._schema
        if tbl_schema is None:
            raise PipelineError("Schema is required to convert Stream to Table")
        return TableStream(self, tbl_schema, name or self._id)

    def sink_to(self, connector: "SinkConnector") -> None:
        self._flow._add_sink(self, connector)

    def print(self) -> None:
        self._flow._add_print_sink(self)

    def operations(self) -> List[Dict[str, Any]]:
        return list(self._operations)


class KeyedStream(Stream):
    def __init__(self, parent: Stream, key_selector: Union[str, Callable]):
        super().__init__(parent.flow, parent.id + "_keyed", parent.schema)
        self._parent_ops = list(parent.operations())
        self._key_selector = key_selector
        self._operations = list(parent.operations())

    @property
    def key_selector(self) -> Union[str, Callable]:
        return self._key_selector


class WindowedStream(Stream):
    def __init__(self, parent: Stream, window_type: "WindowType"):
        super().__init__(parent.flow, parent.id + "_windowed", parent.schema)
        self._parent = parent
        self._window_type = window_type
        self._operations = list(parent.operations())

    @property
    def window_type(self) -> "WindowType":
        return self._window_type

    def aggregate(self, func: Callable, name: Optional[str] = None) -> "Stream":
        self._operations.append({
            "type": "window_aggregate", "func": func, "window": self._window_type,
            "name": name or f"aggregate_{len(self._operations)}",
        })
        return self

    def sum(self, field: Union[str, int] = 0, name: Optional[str] = None) -> "Stream":
        self._operations.append({
            "type": "window_sum", "field": field, "window": self._window_type,
            "name": name or f"sum_{len(self._operations)}",
        })
        return self

    def count(self, name: Optional[str] = None) -> "Stream":
        self._operations.append({
            "type": "window_count", "window": self._window_type,
            "name": name or f"count_{len(self._operations)}",
        })
        return self


class TableStream:
    def __init__(self, parent: Stream, schema: Schema, name: str):
        self._parent = parent
        self._schema = schema
        self._name = name
        self._operations: List[Dict[str, Any]] = []
        self._flow = parent.flow

    @property
    def name(self) -> str:
        return self._name

    @property
    def schema(self) -> Schema:
        return self._schema

    @property
    def operations(self) -> List[Dict[str, Any]]:
        return list(self._operations)

    @property
    def parent_stream(self) -> Stream:
        return self._parent

    def select(self, *expressions: Any) -> "TableStream":
        self._operations.append({"type": "select", "expressions": expressions})
        return self

    def where(self, predicate: Any) -> "TableStream":
        self._operations.append({"type": "where", "predicate": predicate})
        return self

    def group_by(self, *keys: Any) -> "GroupedTable":
        self._operations.append({"type": "group_by", "keys": keys})
        return GroupedTable(self, keys)

    def window(self, window_type: "WindowType") -> "WindowedTable":
        return WindowedTable(self, window_type)

    def insert_into(self, connector: "SinkConnector") -> None:
        self._flow._add_table_sink(self, connector)


class GroupedTable:
    def __init__(self, parent: "TableStream", keys: tuple):
        self._parent = parent
        self._keys = keys

    def select(self, *expressions: Any) -> "TableStream":
        self._parent._operations.append({
            "type": "grouped_select", "expressions": expressions
        })
        return self._parent


class WindowedTable:
    def __init__(self, parent: "TableStream", window_type: "WindowType"):
        self._parent = parent
        self._window_type = window_type

    def group_by(self, *keys: Any) -> "GroupedTable":
        return self._parent.group_by(*keys)

    def select(self, *expressions: Any) -> "TableStream":
        self._parent._operations.append({
            "type": "windowed_select", "window": self._window_type,
            "expressions": expressions,
        })
        return self._parent


from liutang.core.window import WindowType  # noqa: E402
from liutang.core.connector import SinkConnector  # noqa: E402