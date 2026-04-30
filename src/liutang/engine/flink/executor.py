from __future__ import annotations

from typing import Any, Dict, List, Optional

from liutang.core.flow import Flow
from liutang.core.stream import RuntimeMode
from liutang.core.connector import (
    CollectionSource,
    FileSource,
    KafkaSource,
    PrintSink,
    FileSink,
    SourceKind,
    SinkKind,
)
from liutang.core.errors import EngineNotAvailableError
from liutang.core.window import WindowKind


class FlinkExecutor:
    def __init__(self, flow: Flow):
        self._flow = flow
        self._env = None
        self._t_env = None

    def _init_env(self) -> None:
        from pyflink.datastream import StreamExecutionEnvironment
        from pyflink.datastream import RuntimeExecutionMode

        self._env = StreamExecutionEnvironment.get_execution_environment()
        if self._flow.mode == RuntimeMode.BATCH:
            self._env.set_runtime_mode(RuntimeExecutionMode.BATCH)
        else:
            self._env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
        self._env.set_parallelism(self._flow.parallelism)
        for jar_path in self._flow.jar_paths:
            self._env.add_jars(f"file://{jar_path}")
        if self._flow.checkpoint_dir:
            self._env.enable_checkpointing(5000)

    def _init_table_env(self) -> None:
        from pyflink.table import StreamTableEnvironment
        self._t_env = StreamTableEnvironment.create(self._env)

    def _create_source(self, connector: Any) -> Any:
        from pyflink.datastream import StreamExecutionEnvironment
        from pyflink.common import Types

        if connector.kind() == SourceKind.COLLECTION:
            return self._env.from_collection(connector.data)
        elif connector.kind() == SourceKind.KAFKA:
            return self._create_kafka_source(connector)
        elif connector.kind() == SourceKind.FILE:
            return self._create_file_source(connector)
        else:
            raise EngineNotAvailableError("flink", f"Unsupported source type: {connector.kind()}")

    def _create_kafka_source(self, connector: KafkaSource) -> Any:
        from pyflink.datastream import FlinkKafkaConsumer
        from pyflink.common.serialization import SimpleStringSchema

        properties = {
            "bootstrap.servers": connector.bootstrap_servers,
            "group.id": connector.group_id,
        }
        consumer = FlinkKafkaConsumer(
            topics=connector.topic,
            deserialization_schema=SimpleStringSchema("UTF-8"),
            properties=properties,
        )
        if connector.start_from == "earliest":
            consumer.set_start_from_earliest()
        elif connector.start_from == "latest":
            consumer.set_start_from_latest()
        return self._env.add_source(consumer)

    def _create_file_source(self, connector: FileSource) -> Any:
        from pyflink.datastream import StreamExecutionEnvironment
        from pyflink.datastream.connectors.file_system import FileSource, StreamFormat

        if connector.fmt == "text":
            source = FileSource.for_record_stream_format(
                StreamFormat.text_line_format(), connector.path
            ).build()
            return self._env.from_source(source, "file_source")
        raise EngineNotAvailableError("flink", f"Unsupported file format: {connector.fmt}")

    def _apply_operations(self, ds: Any, operations: List[Dict[str, Any]]) -> Any:
        from liutang.engine.flink.adapter import FlinkTypeAdapter
        from liutang.core.window import WindowType

        for op in operations:
            op_type = op["type"]
            func = op.get("func")
            name = op.get("name", "")

            if op_type == "map":
                ds = ds.map(func)
            elif op_type == "flat_map":
                ds = ds.flat_map(func)
            elif op_type == "filter":
                ds = ds.filter(func)
            elif op_type == "key_by":
                ds = ds.key_by(func)
            elif op_type == "reduce":
                ds = ds.reduce(func)
            elif op_type == "sum":
                ds = ds.sum(op.get("field", 0))
            elif op_type == "count":
                ds = ds.map(lambda x: (x, 1)).key_by(lambda x: x[0]).sum(1)
            elif op_type == "process":
                ds = ds.process(func)
            elif op_type in ("window_aggregate", "window_sum", "window_count"):
                window = op.get("window")
                ds = self._apply_flink_window(ds, op, window)

            if name:
                try:
                    ds = ds.name(name)
                except Exception:
                    pass
        return ds

    def _apply_flink_window(self, ds: Any, op: Dict[str, Any], window: Any) -> Any:
        from pyflink.datastream import Time
        from pyflink.common.time import Duration

        window_type = window
        if hasattr(window_type, 'kind'):
            if window_type.kind == WindowKind.TUMBLING:
                size = window_type.size
                if isinstance(size, (int, float)):
                    ds = ds.window(TumblingProcessingTimeWindows.of(Duration.of_seconds(int(size))))
                else:
                    ds = ds.window(TumblingProcessingTimeWindows.of(size))
            elif window_type.kind == WindowKind.SLIDING:
                size = window_type.size
                slide = window_type.slide
                if isinstance(size, (int, float)) and isinstance(slide, (int, float)):
                    ds = ds.window(SlidingProcessingTimeWindows.of(
                        Duration.of_seconds(int(size)), Duration.of_seconds(int(slide))))
            elif window_type.kind == WindowKind.SESSION:
                gap = window_type.gap
                if isinstance(gap, (int, float)):
                    ds = ds.window(ProcessingTimeSessionWindows.with_gap(Duration.of_seconds(int(gap))))
            elif window_type.kind == WindowKind.OVER:
                pass

        func = op.get("func")
        if op["type"] == "window_sum":
            ds = ds.sum(op.get("field", 0))
        elif op["type"] == "window_count":
            ds = ds.sum(0).map(lambda x: x[1])
        elif op["type"] == "window_aggregate" and func:
            ds = ds.reduce(func)
        return ds

    def _create_sink(self, ds: Any, connector: Any) -> Any:
        if connector.kind() == SinkKind.PRINT:
            ds.print()
            return ds
        elif connector.kind() == SinkKind.FILE:
            return self._create_file_sink(ds, connector)
        elif connector.kind() == SinkKind.KAFKA:
            return self._create_kafka_sink(ds, connector)
        else:
            ds.print()
            return ds

    def _create_file_sink(self, ds: Any, connector: Any) -> Any:
        from pyflink.datastream.connectors.file_system import FileSink, OutputFileConfig, RollingPolicy
        import os

        base_path = os.path.abspath(connector.path)
        sink = FileSink.for_row_format(
            base_path,
           Encoder.simple_string_encoder("UTF-8")
        ).with_output_file_config(
            OutputFileConfig.builder().with_part_prefix("liutang").with_part_suffix(connector.fmt).build()
        ).build()
        self._env.add_sink(sink)
        return ds

    def _create_kafka_sink(self, ds: Any, connector: Any) -> Any:
        from pyflink.datastream import FlinkKafkaProducer
        from pyflink.common.serialization import SimpleStringSchema

        producer = FlinkKafkaProducer(
            topic=connector.topic,
            serialization_schema=SimpleStringSchema("UTF-8"),
            producer_config={"bootstrap.servers": connector.bootstrap_servers},
        )
        ds.add_sink(producer)
        return ds

    def execute(self) -> Any:
        self._init_env()
        if not self._t_env:
            self._init_table_env()

        data_streams: Dict[str, Any] = {}
        for src in self._flow.sources:
            ds = self._create_source(src["connector"])
            stream = self._find_stream(src["id"])
            if stream:
                ds = self._apply_operations(ds, stream.operations())
            data_streams[src["id"]] = ds

        for sink in self._flow.sinks:
            stream = sink["stream"]
            ds = data_streams.get(stream.id)
            if ds is not None:
                self._create_sink(ds, sink["connector"])

        return self._env.execute(self._flow.name)

    def explain(self) -> str:
        self._init_env()
        lines = [f"Flow: {self._flow.name}", f"Engine: flink", f"Mode: {self._flow.mode.value}", ""]
        for src in self._flow.sources:
            lines.append(f"Source: {src['connector'].kind().value} -> {src['id']}")
        for sink in self._flow.sinks:
            stream = sink["stream"]
            lines.append(f"Sink: {sink['connector'].kind().value} <- {stream.id}")
            for op in stream.operations():
                lines.append(f"  {op['type']}: {op.get('name', '')}")
        lines.append("")
        lines.append("(Full Flink execution plan will be available after execute())")
        return "\n".join(lines)

    def _find_stream(self, stream_id: str):
        for s in self._flow._streams:
            if s.id == stream_id:
                return s
        for sink in self._flow._sinks:
            if sink["stream"].id == stream_id:
                return sink["stream"]
        return None