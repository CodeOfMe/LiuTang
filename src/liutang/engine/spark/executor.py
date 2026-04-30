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
from liutang.core.window import WindowType, WindowKind


class SparkExecutor:
    def __init__(self, flow: Flow):
        self._flow = flow
        self._spark = None
        self._spark_session = None

    def _init_spark(self) -> None:
        from pyspark.sql import SparkSession

        builder = SparkSession.builder.appName(self._flow.name)
        if self._flow.parallelism > 1:
            builder = builder.master(f"local[{self._flow.parallelism}]")
        else:
            builder = builder.master("local[*]")
        self._spark_session = builder.getOrCreate()
        self._spark = self._spark_session.sparkContext

    def _create_dataframe(self, connector: Any) -> Any:
        if connector.kind() == SourceKind.COLLECTION:
            schema = connector.schema if hasattr(connector, 'schema') and connector.schema else None
            if schema:
                from liutang.engine.spark.adapter import SparkTypeAdapter
                spark_schema = SparkTypeAdapter.schema_to_spark(schema)
                return self._spark_session.createDataFrame(connector.data, schema=spark_schema)
            return self._spark_session.createDataFrame(connector.data)
        elif connector.kind() == SourceKind.FILE:
            return self._create_file_source(connector)
        elif connector.kind() == SourceKind.KAFKA:
            return self._create_kafka_source(connector)
        raise EngineNotAvailableError("spark", f"Unsupported source type: {connector.kind()}")

    def _create_file_source(self, connector: FileSource) -> Any:
        if connector.fmt == "csv":
            return self._spark_session.read.csv(connector.path, header=True, inferSchema=True)
        elif connector.fmt == "json":
            return self._spark_session.read.json(connector.path)
        elif connector.fmt == "text":
            return self._spark_session.read.text(connector.path)
        elif connector.fmt == "parquet":
            return self._spark_session.read.parquet(connector.path)
        raise EngineNotAvailableError("spark", f"Unsupported file format: {connector.fmt}")

    def _create_kafka_source(self, connector: KafkaSource) -> Any:
        df = self._spark_session.read.format("kafka") \
            .option("kafka.bootstrap.servers", connector.bootstrap_servers) \
            .option("subscribe", connector.topic) \
            .option("startingOffsets", connector.start_from) \
            .load()
        return df.selectExpr("CAST(value AS STRING)")

    def _apply_operations(self, df: Any, operations: List[Dict[str, Any]]) -> Any:
        for op in operations:
            op_type = op["type"]
            func = op.get("func")

            if op_type == "map":
                from pyspark.sql import functions as F
                if callable(func):
                    df = df.rdd.map(func).toDF()
                elif isinstance(func, str):
                    df = df.withColumn("mapped", F.expr(func))
            elif op_type == "flat_map":
                df = df.rdd.flatMap(func).toDF()
            elif op_type == "filter":
                if callable(func):
                    df = df.filter(func)
                elif isinstance(func, str):
                    df = df.filter(func)
            elif op_type == "key_by":
                if isinstance(func, str):
                    pass
                elif callable(func):
                    pass
            elif op_type == "reduce":
                if callable(func):
                    df = df.rdd.reduce(func)
            elif op_type == "sum":
                field = op.get("field", 0)
                col_name = field if isinstance(field, str) else df.columns[field]
                from pyspark.sql import functions as F2
                df = df.groupBy().sum(col_name)
            elif op_type == "count":
                df = df.groupBy().count()
            elif op_type in ("window_aggregate", "window_sum", "window_count"):
                window = op.get("window")
                df = self._apply_spark_window(df, op, window)

        return df

    def _apply_spark_window(self, df: Any, op: Dict[str, Any], window: WindowType) -> Any:
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window as SparkWindow

        if window.kind == WindowKind.TUMBLING:
            size = window.size
            time_col = window.time_field or df.columns[0]
            if isinstance(size, (int, float)):
                size_str = f"{int(size)} seconds"
            else:
                size_str = str(size)
            if self._flow.mode == RuntimeMode.STREAMING:
                df = df.withWatermark(time_col, size_str)
            w = SparkWindow.partitionBy().orderBy(F.col(time_col)).rowsBetween(
                SparkWindow.unboundedPreceding, SparkWindow.unboundedFollowing)
            field = op.get("field", 0)
            col_name = field if isinstance(field, str) else df.columns[field]
            if op["type"] == "window_sum":
                df = df.withColumn(f"sum_{col_name}", F.sum(col_name).over(w))
            elif op["type"] == "window_count":
                df = df.withColumn("count", F.count(F.lit(1)).over(w))
        return df

    def _create_sink(self, df: Any, connector: Any) -> None:
        if connector.kind() == SinkKind.PRINT:
            df.show()
        elif connector.kind() == SinkKind.FILE:
            if connector.fmt == "csv":
                df.write.csv(connector.path, mode="overwrite")
            elif connector.fmt == "json":
                df.write.json(connector.path, mode="overwrite")
            elif connector.fmt == "parquet":
                df.write.parquet(connector.path, mode="overwrite")
            else:
                df.write.text(connector.path, mode="overwrite")
        elif connector.kind() == SinkKind.KAFKA:
            df.write.format("kafka") \
                .option("kafka.bootstrap.servers", connector.bootstrap_servers) \
                .option("topic", connector.topic) \
                .save()

    def execute(self) -> Any:
        self._init_spark()
        data_frames: Dict[str, Any] = {}
        for src in self._flow.sources:
            df = self._create_dataframe(src["connector"])
            stream = self._find_stream(src["id"])
            if stream:
                df = self._apply_operations(df, stream.operations())
            data_frames[src["id"]] = df
        for sink in self._flow.sinks:
            stream = sink["stream"]
            df = data_frames.get(stream.id)
            if df is not None:
                self._create_sink(df, sink["connector"])
        return data_frames

    def explain(self) -> str:
        lines = [f"Flow: {self._flow.name}", f"Engine: spark", f"Mode: {self._flow.mode.value}", ""]
        for src in self._flow.sources:
            lines.append(f"Source: {src['connector'].kind().value} -> {src['id']}")
        for sink in self._flow.sinks:
            stream = sink["stream"]
            lines.append(f"Sink: {sink['connector'].kind().value} <- {stream.id}")
            for op in stream.operations():
                lines.append(f"  {op['type']}: {op.get('name', '')}")
        lines.append("")
        lines.append("(Full Spark plan will be available after execute())")
        return "\n".join(lines)

    def _find_stream(self, stream_id: str):
        for s in self._flow._streams:
            if s.id == stream_id:
                return s
        for sink in self._flow._sinks:
            if sink["stream"].id == stream_id:
                return sink["stream"]
        return None