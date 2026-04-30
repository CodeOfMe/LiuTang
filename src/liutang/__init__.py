"""
liutang (流淌) — A pure-Python streaming data framework.

No external dependencies. All stream processing features (windowing, watermark,
stateful processing, checkpointing) are implemented natively with threading/
multiprocessing for parallelism. Switch between batch and streaming mode freely.

Design principles:
  - Zero dependencies: just Python stdlib
  - Native concurrency: threading / multiprocessing / concurrent.futures
  - Full streaming: watermark, event-time windows, keyed state, timers
  - API parity: same Flow/Stream API for batch and streaming
"""

__version__ = "0.1.1"

from liutang.core.flow import Flow
from liutang.core.stream import (
    Stream,
    KeyedStream,
    WindowedStream,
    TableStream,
    GroupedTable,
    WindowedTable,
    RuntimeMode,
    DeliveryMode,
)
from liutang.core.schema import FieldType, Schema, Field
from liutang.core.window import WindowType, WindowKind
from liutang.core.connector import (
    SourceConnector,
    SinkConnector,
    CollectionSource,
    GeneratorSource,
    FileSource,
    KafkaSource,
    DatagenSource,
    SocketSource,
    PrintSink,
    FileSink,
    KafkaSink,
    CallbackSink,
    CollectSink,
    SocketSink,
    SourceKind,
    SinkKind,
)
from liutang.core.state import (
    ValueState,
    ListState,
    MapState,
    ReducingState,
    AggregatingState,
    KeyedState,
    RuntimeContext,
    TimerService,
    KeyedProcessFunction,
    ProcessFunction,
    WatermarkStrategy,
    Watermark,
    MemoryStateBackend,
    StateConfig,
)
from liutang.core.errors import (
    LiuTangError,
    PipelineError,
    SchemaError,
    ConnectorError,
    WatermarkError,
    StateError,
    DeliveryError,
)


def quick_flow(name: str = "liutang-quick", **kwargs) -> Flow:
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
    "DeliveryMode",
    "FieldType",
    "Schema",
    "Field",
    "WindowType",
    "WindowKind",
    "CollectionSource",
    "GeneratorSource",
    "FileSource",
    "KafkaSource",
    "DatagenSource",
    "SocketSource",
    "PrintSink",
    "FileSink",
    "KafkaSink",
    "CallbackSink",
    "CollectSink",
    "SocketSink",
    "SourceKind",
    "SinkKind",
    "ValueState",
    "ListState",
    "MapState",
    "ReducingState",
    "AggregatingState",
    "KeyedState",
    "RuntimeContext",
    "TimerService",
    "KeyedProcessFunction",
    "ProcessFunction",
    "WatermarkStrategy",
    "Watermark",
    "MemoryStateBackend",
    "StateConfig",
    "LiuTangError",
    "PipelineError",
    "SchemaError",
    "ConnectorError",
    "WatermarkError",
    "StateError",
    "DeliveryError",
    "quick_flow",
]