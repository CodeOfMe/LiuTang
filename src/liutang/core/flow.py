from __future__ import annotations

import threading
import queue
import time
from typing import Any, Dict, List, Optional

from liutang.core.stream import Stream, TableStream, RuntimeMode, DeliveryMode, ArchitectureMode
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
        delivery_mode: DeliveryMode = DeliveryMode.AT_LEAST_ONCE,
        max_retries: int = 3,
        architecture: ArchitectureMode = ArchitectureMode.SIMPLE,
    ):
        self._name = name
        self._engine_name = engine
        self._mode = mode
        self._parallelism = parallelism
        self._delivery_mode = delivery_mode
        self._max_retries = max_retries
        self._architecture = architecture
        self._sources: List[Dict[str, Any]] = []
        self._sinks: List[Dict[str, Any]] = []
        self._streams: List[Stream] = []
        self._config: Dict[str, Any] = {}
        self._checkpoint_dir: Optional[str] = None
        self._checkpoint_interval: float = 60.0
        self._schema_enforcement: bool = False

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
    def delivery_mode(self) -> DeliveryMode:
        return self._delivery_mode

    @property
    def max_retries(self) -> int:
        return self._max_retries

    @property
    def architecture(self) -> ArchitectureMode:
        return self._architecture

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

    def enable_schema_enforcement(self, enabled: bool = True) -> "Flow":
        self._schema_enforcement = enabled
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

    def as_lambda(self) -> "LambdaFlow":
        from liutang.core.lambda_flow import LambdaFlow
        batch_fn = None
        speed_fn = None
        for src in self._sources:
            connector = src["connector"]
            from liutang.core.connector import CollectionSource
            if isinstance(connector, CollectionSource):
                data = connector.data
                def _make_batch(d=data):
                    def fn(f):
                        s = f.from_collection(d)
                        for op_info in self._streams[0].operations() if self._streams else []:
                            s = _apply_op_to_stream(s, op_info)
                        return s
                    return fn
                batch_fn = _make_batch()
                speed_fn = _make_batch()
        return LambdaFlow(
            name=f"{self._name}-lambda",
            batch_layer_fn=batch_fn,
            speed_layer_fn=speed_fn,
            parallelism=self._parallelism,
            delivery_mode=self._delivery_mode,
        )

    def as_kappa(self, event_log_path: Optional[str] = None) -> "KappaFlow":
        from liutang.core.lambda_flow import KappaFlow
        stream_fn = None
        for src in self._sources:
            data = src["connector"].data if hasattr(src["connector"], 'data') else []
            def _make_stream(d=data):
                def fn(f):
                    s = f.from_collection(d)
                    for op_info in self._streams[0].operations() if self._streams else []:
                        s = _apply_op_to_stream(s, op_info)
                    return s
                return fn
            stream_fn = _make_stream()
        return KappaFlow(
            name=f"{self._name}-kappa",
            stream_fn=stream_fn,
            event_log_path=event_log_path,
            parallelism=self._parallelism,
            delivery_mode=self._delivery_mode,
        )

    def as_adaptive(
        self,
        policy: Optional[Any] = None,
        initial_granularity: Optional[Any] = None,
        min_granularity: Optional[Any] = None,
        max_granularity: Optional[Any] = None,
    ) -> "AdaptiveFlow":
        from liutang.core.adaptive_flow import AdaptiveFlow
        from liutang.core.granularity import GranularityPolicy, GranularityLevel
        stream_fn = None
        for src in self._sources:
            data = src["connector"].data if hasattr(src["connector"], 'data') else []
            def _make_stream(d=data):
                def fn(f):
                    s = f.from_collection(d)
                    for op_info in self._streams[0].operations() if self._streams else []:
                        s = _apply_op_to_stream(s, op_info)
                    return s
                return fn
            stream_fn = _make_stream()
        kwargs: Dict[str, Any] = {
            "name": f"{self._name}-adaptive",
            "stream_fn": stream_fn,
            "parallelism": self._parallelism,
            "delivery_mode": self._delivery_mode,
        }
        if policy is not None:
            kwargs["policy"] = policy
        if initial_granularity is not None:
            kwargs["initial_granularity"] = initial_granularity
        if min_granularity is not None:
            kwargs["min_granularity"] = min_granularity
        if max_granularity is not None:
            kwargs["max_granularity"] = max_granularity
        return AdaptiveFlow(**kwargs)


def _apply_op_to_stream(stream: Stream, op_info: Dict[str, Any]) -> Stream:
    op_type = op_info["type"]
    func = op_info.get("func")
    if op_type == "map":
        return stream.map(func)
    elif op_type == "filter":
        return stream.filter(func)
    elif op_type == "flat_map":
        return stream.flat_map(func)
    elif op_type == "key_by":
        return stream.key_by(func)
    elif op_type == "reduce":
        return stream.reduce(func)
    elif op_type == "sum":
        return stream.sum(field=op_info.get("field", 0))
    elif op_type == "count":
        return stream.count()
    elif op_type == "min":
        return stream.min(field=op_info.get("field", 0))
    elif op_type == "max":
        return stream.max(field=op_info.get("field", 0))
    elif op_type == "process":
        return stream.process(func)
    elif op_type == "assign_timestamps":
        return stream.assign_timestamps(func)
    return stream