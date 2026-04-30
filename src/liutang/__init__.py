"""
liutang (流淌) — A pure-Python streaming data framework.

No external dependencies. All stream processing features (windowing, watermark,
stateful processing, checkpointing, delivery semantics, late data handling,
adaptive granularity architecture) are implemented natively with
threading/multiprocessing for parallelism. Switch between batch and streaming
mode freely, or let adaptive granularity choose for you.

Design principles:
  - Zero dependencies: just Python stdlib
  - Native concurrency: threading / multiprocessing / concurrent.futures
  - Full streaming: watermark, event-time windows, keyed state, timers
  - Delivery semantics: at-least-once, at-most-once, exactly-once
  - API parity: same Flow/Stream API for batch and streaming
  - Adaptive granularity: auto-adjust from micro-streaming to macro-batch
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
    ArchitectureMode,
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
    JsonFileStateBackend,
    StateConfig,
)
from liutang.core.eventlog import EventLog
from liutang.core.serving import ServingView, MergeView
from liutang.core.lambda_flow import LambdaFlow, KappaFlow
from liutang.core.granularity import (
    GranularityLevel,
    GranularityPolicy,
    GranularityController,
    GranularityMetrics,
)
from liutang.core.adaptive_flow import AdaptiveFlow
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
    "ArchitectureMode",
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
    "JsonFileStateBackend",
    "StateConfig",
    "EventLog",
    "ServingView",
    "MergeView",
    "LambdaFlow",
    "KappaFlow",
    "GranularityLevel",
    "GranularityPolicy",
    "GranularityController",
    "GranularityMetrics",
    "AdaptiveFlow",
    "LiuTangError",
    "PipelineError",
    "SchemaError",
    "ConnectorError",
    "WatermarkError",
    "StateError",
    "DeliveryError",
    "quick_flow",
]