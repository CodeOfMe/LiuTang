from __future__ import annotations

from typing import Any, Dict, List, Optional, TYPE_CHECKING

from liutang.core.stream import Stream, TableStream, RuntimeMode
from liutang.core.schema import Schema
from liutang.core.connector import SourceConnector, SinkConnector
from liutang.core.errors import EngineNotAvailableError


class Flow:
    def __init__(
        self,
        name: str = "liutang-flow",
        engine: str = "flink",
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
        self._jar_paths: List[str] = []
        self._checkpoint_dir: Optional[str] = None

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
    def jar_paths(self) -> List[str]:
        return self._jar_paths

    @property
    def checkpoint_dir(self) -> Optional[str]:
        return self._checkpoint_dir

    def set_parallelism(self, n: int) -> "Flow":
        self._parallelism = n
        return self

    def set_checkpoint(self, directory: str) -> "Flow":
        self._checkpoint_dir = directory
        return self

    def add_jar(self, path: str) -> "Flow":
        self._jar_paths.append(path)
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
                        group_id=group_id, start_from=start_from, schema=schema),
            schema,
        )

    def _add_sink(self, stream: Stream, connector: SinkConnector) -> None:
        self._sinks.append({"stream": stream, "connector": connector})

    def _add_print_sink(self, stream: Stream) -> None:
        from liutang.core.connector import PrintSink
        self._sinks.append({"stream": stream, "connector": PrintSink()})

    def _add_table_sink(self, table_stream: TableStream, connector: SinkConnector) -> None:
        self._sinks.append({
            "stream": table_stream.parent_stream,
            "connector": connector,
            "table_operations": table_stream.operations,
            "table_name": table_stream.name,
            "table_schema": table_stream.schema,
        })

    def execute(self, executor_type: Optional[str] = None) -> Any:
        engine = executor_type or self._engine_name
        executor_cls = self._resolve_executor(engine)
        executor = executor_cls(self)
        return executor.execute()

    def explain(self, executor_type: Optional[str] = None) -> str:
        engine = executor_type or self._engine_name
        executor_cls = self._resolve_executor(engine)
        executor = executor_cls(self)
        return executor.explain()

    def _resolve_executor(self, engine: str) -> type:
        registry = {
            "local": "liutang.engine.local.executor.LocalExecutor",
            "flink": "liutang.engine.flink.executor.FlinkExecutor",
            "spark": "liutang.engine.spark.executor.SparkExecutor",
        }
        if engine not in registry:
            raise EngineNotAvailableError(engine, f"Unknown engine. Choose from: {list(registry.keys())}")
        module_path, _, class_name = registry[engine].rpartition(".")
        import importlib
        try:
            mod = importlib.import_module(module_path)
            return getattr(mod, class_name)
        except ImportError as exc:
            raise EngineNotAvailableError(engine, str(exc))
        except AttributeError as exc:
            raise EngineNotAvailableError(engine, f"Executor class not found: {exc}")