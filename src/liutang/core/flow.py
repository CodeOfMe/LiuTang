from __future__ import annotations

import threading
import queue
import time
from typing import Any, Dict, List, Optional

from liutang.core.stream import Stream, TableStream, RuntimeMode
from liutang.core.schema import Schema
from liutang.core.connector import SourceConnector, SinkConnector, PrintSink, CollectSink
from liutang.core.errors import PipelineError


class Flow:
    def __init__(
        self,
        name: str = "liutang-flow",
        engine: str = "local",
        mode: RuntimeMode = RuntimeMode.STREAMING,
        parallelism: int = 1,
    ):
        self._name = name
        self._engine_name = engine
        self._mode = mode
        self._parallelism = parallelism
        self._sources: List[Dict[str, Any]] = []
        self._sinks: List[Dict[str, Any]] = []
        self._streams: List[Stream] = []
        self._config: Dict[str, Any] = {}
        self._checkpoint_dir: Optional[str] = None
        self._checkpoint_interval: float = 60.0

    @property
    def name(self) -> str:
        return self._name

    @property
    def engine(self) -> str:
        return self._engine_name

    @property
    def mode(self) -> RuntimeMode:
        return self._mode

    @property
    def parallelism(self) -> int:
        return self._parallelism

    @property
    def sources(self) -> List[Dict[str, Any]]:
        return self._sources

    @property
    def sinks(self) -> List[Dict[str, Any]]:
        return self._sinks

    @property
    def config(self) -> Dict[str, Any]:
        return self._config

    @property
    def checkpoint_dir(self) -> Optional[str]:
        return self._checkpoint_dir

    def set_parallelism(self, n: int) -> "Flow":
        self._parallelism = n
        return self

    def set_checkpoint(self, directory: str, interval: float = 60.0) -> "Flow":
        self._checkpoint_dir = directory
        self._checkpoint_interval = interval
        return self

    def configure(self, key: str, value: Any) -> "Flow":
        self._config[key] = value
        return self

    def from_source(self, connector: SourceConnector, schema: Optional[Schema] = None) -> Stream:
        stream_id = f"source_{len(self._sources)}"
        self._sources.append({"id": stream_id, "connector": connector, "schema": schema})
        stream = Stream(self, stream_id, schema)
        self._streams.append(stream)
        return stream

    def from_collection(self, data: List[Any], schema: Optional[Schema] = None) -> Stream:
        from liutang.core.connector import CollectionSource
        return self.from_source(CollectionSource(data), schema)

    def from_generator(self, gen: Any, max_items: Optional[int] = None, schema: Optional[Schema] = None) -> Stream:
        from liutang.core.connector import GeneratorSource
        return self.from_source(GeneratorSource(gen, max_items=max_items), schema)

    def from_file(self, path: str, fmt: str = "text", schema: Optional[Schema] = None) -> Stream:
        from liutang.core.connector import FileSource
        return self.from_source(FileSource(path=path, fmt=fmt, schema=schema), schema)

    def from_kafka(
        self,
        topic: str,
        bootstrap_servers: str = "localhost:9092",
        group_id: str = "liutang-group",
        schema: Optional[Schema] = None,
        start_from: str = "earliest",
    ) -> Stream:
        from liutang.core.connector import KafkaSource
        return self.from_source(
            KafkaSource(topic=topic, bootstrap_servers=bootstrap_servers,
                        group_id=group_id, start_from=start_from, schema=schema), schema)

    def _add_sink(self, stream: Stream, connector: SinkConnector) -> None:
        self._sinks.append({"stream": stream, "connector": connector})

    def _add_print_sink(self, stream: Stream) -> None:
        self._sinks.append({"stream": stream, "connector": PrintSink()})

    def _add_table_sink(self, table_stream: TableStream, connector: SinkConnector) -> None:
        self._sinks.append({
            "stream": table_stream.parent_stream, "connector": connector,
            "table_operations": table_stream.operations,
            "table_name": table_stream.name, "table_schema": table_stream.schema,
        })

    def execute(self) -> Any:
        from liutang.engine.executor import Executor
        executor = Executor(self)
        return executor.execute()

    def explain(self) -> str:
        from liutang.engine.executor import Executor
        executor = Executor(self)
        return executor.explain()