"""
liutang (流淌) - A unified streaming data framework with native Python concurrency,
optional Apache Flink and Spark backends.

Design principles:
  - Zero hard dependencies: Flink/Spark are lazy-imported, never required at install
  - Native concurrency: local engine uses threading/multiprocessing/asyncio
  - API parity: same Flow/Stream API works across local, Flink, and Spark engines
  - Version isolation: engine adapters catch version mismatches with clear messages
"""

__version__ = "0.1.0"

from liutang.core.flow import Flow
from liutang.core.stream import (
    Stream,
    KeyedStream,
    WindowedStream,
    TableStream,
    GroupedTable,
    WindowedTable,
    RuntimeMode,
)
from liutang.core.schema import FieldType, Schema, Field
from liutang.core.window import WindowType, WindowKind
from liutang.core.connector import (
    SourceConnector,
    SinkConnector,
    CollectionSource,
    FileSource,
    KafkaSource,
    DatagenSource,
    SocketSource,
    PrintSink,
    FileSink,
    KafkaSink,
    CallbackSink,
    SourceKind,
    SinkKind,
)
from liutang.core.state import MemoryStateBackend, KeyedState, StateConfig
from liutang.core.errors import (
    LiuTangError,
    EngineNotAvailableError,
    EngineVersionError,
    SchemaError,
    PipelineError,
    ConnectorError,
    SerializationError,
)
from liutang.engine.registry import list_engines, is_engine_available


def quick_flow(name: str = "liutang-quick", **kwargs: object) -> Flow:
    return Flow(name=name, **kwargs)


__all__ = [
    "Flow",
    "Stream",
    "KeyedStream",
    "WindowedStream",
    "TableStream",
    "GroupedTable",
    "WindowedTable",
    "RuntimeMode",
    "FieldType",
    "Schema",
    "Field",
    "WindowType",
    "WindowKind",
    "CollectionSource",
    "FileSource",
    "KafkaSource",
    "DatagenSource",
    "SocketSource",
    "PrintSink",
    "FileSink",
    "KafkaSink",
    "CallbackSink",
    "SourceKind",
    "SinkKind",
    "MemoryStateBackend",
    "KeyedState",
    "StateConfig",
    "LiuTangError",
    "EngineNotAvailableError",
    "EngineVersionError",
    "SchemaError",
    "PipelineError",
    "ConnectorError",
    "SerializationError",
    "list_engines",
    "is_engine_available",
    "quick_flow",
]