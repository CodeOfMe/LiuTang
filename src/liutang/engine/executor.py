from __future__ import annotations

import json
import queue
import threading
import time
import socket
import csv
import io
import random
import sys
from typing import Any, Dict, List, Optional

from liutang.core.flow import Flow
from liutang.core.stream import RuntimeMode, TableStream
from liutang.core.connector import (
    SourceKind, SinkKind, CollectionSource, FileSource, KafkaSource,
    DatagenSource, SocketSource, GeneratorSource,
    PrintSink, FileSink, CallbackSink, CollectSink,
)
from liutang.core.state import WatermarkStrategy, KeyedProcessFunction, ProcessFunction
from liutang.engine.runner import StreamRunner, PipelineBuilder


class Executor:
    def __init__(self, flow: Flow):
        self._flow = flow
        self._collector: List[Any] = []

    def execute(self) -> Any:
        if self._flow.mode == RuntimeMode.BATCH:
            return self._execute_batch()
        return self._execute_streaming()

    def explain(self) -> str:
        lines = [
            f"Flow: {self._flow.name}",
            f"Engine: liutang (pure Python)",
            f"Mode: {self._flow.mode.value}",
            f"Parallelism: {self._flow.parallelism}",
            "",
        ]
        for src in self._flow.sources:
            lines.append(f"Source: {src['connector'].kind().value} -> {src['id']}")
        for sink in self._flow.sinks:
            stream = sink["stream"]
            lines.append(f"Sink: {sink['connector'].kind().value} <- {stream.id}")
            for op_dict in stream.operations():
                lines.append(f"  {op_dict['type']}: {op_dict.get('name', '')}")
        if self._flow.checkpoint_dir:
            lines.append(f"Checkpoint: {self._flow.checkpoint_dir}")
        return "\n".join(lines)

    def _execute_batch(self) -> Dict[str, List[Any]]:
        results: Dict[str, List[Any]] = {}
        for src in self._flow.sources:
            connector = src["connector"]
            data = self._read_source(connector)
            source_id = src["id"]
            processed = False
            for sink in self._flow.sinks:
                stream = sink["stream"]
                if stream.id.startswith(source_id) or stream.id == source_id:
                    operations = PipelineBuilder.from_operations(stream.operations())
                    runner = StreamRunner(operations, parallelism=self._flow.parallelism,
                                          concurrency=self._flow.config.get("concurrency", "thread"))
                    sink_data = runner.run_batch(data)
                    connector = sink["connector"]
                    if "table_operations" in sink:
                        sink_data = self._apply_table_ops(sink_data, sink["table_operations"], sink.get("table_schema"))
                    self._write_sink(connector, sink_data)
                    results[stream.id] = sink_data
                    processed = True
            if not processed:
                for s in self._flow._streams:
                    if s.id.startswith(source_id):
                        operations = PipelineBuilder.from_operations(s.operations())
                        runner = StreamRunner(operations, parallelism=self._flow.parallelism,
                                              concurrency=self._flow.config.get("concurrency", "thread"))
                        results[s.id] = runner.run_batch(data)
                        break
        return results

    def _execute_streaming(self) -> Dict[str, Any]:
        source_queues: Dict[str, queue.Queue] = {}
        stop_events: Dict[str, threading.Event] = {}
        threads: List[threading.Thread] = []
        collect_sinks: List[CollectSink] = []
        for sink_info in self._flow.sinks:
            if isinstance(sink_info["connector"], CollectSink):
                collect_sinks.append(sink_info["connector"])
        for src in self._flow.sources:
            src_queue: queue.Queue = queue.Queue(maxsize=self._flow.config.get("queue_size", 10000))
            source_queues[src["id"]] = src_queue
            stop_event = threading.Event()
            stop_events[src["id"]] = stop_event
            t = threading.Thread(target=self._run_source, args=(src, src_queue, stop_event), daemon=True)
            t.start()
            threads.append(t)
        for sink in self._flow.sinks:
            stream = sink["stream"]
            src_id = stream.id
            connector = sink["connector"]
            stream_obj = self._find_stream(stream.id)
            operations = PipelineBuilder.from_operations(stream_obj.operations()) if stream_obj else []
            runner = StreamRunner(operations, parallelism=self._flow.parallelism,
                                  concurrency=self._flow.config.get("concurrency", "thread"))
            watermark_strategy = stream_obj._watermark_strategy if stream_obj else None
            stop_event = stop_events.get(src_id, threading.Event())
            def make_callback(sink_connector: Any, collect_list: Optional[list] = None) -> Any:
                def cb(record: Any) -> None:
                    if isinstance(sink_connector, CollectSink):
                        sink_connector.results.append(record)
                    elif isinstance(sink_connector, PrintSink):
                        print(record)
                    elif isinstance(sink_connector, CallbackSink):
                        sink_connector.func(record)
                    elif isinstance(sink_connector, FileSink):
                        with open(sink_connector.path, "a", encoding="utf-8") as f:
                            f.write(json.dumps(record, default=str) + "\n")
                return cb
            t = threading.Thread(target=self._run_pipeline,
                                  args=(runner, source_queues.get(src_id, queue.Queue()),
                                        make_callback(connector), stop_event, watermark_strategy),
                                  daemon=True)
            t.start()
            threads.append(t)
        return {"threads": threads, "stop_events": stop_events, "collect_sinks": collect_sinks}

    def _run_source(self, src: Dict[str, Any], src_queue: queue.Queue, stop_event: threading.Event) -> None:
        connector = src["connector"]
        try:
            if connector.kind() == SourceKind.COLLECTION:
                for item in connector.data:
                    if stop_event.is_set():
                        break
                    src_queue.put(item)
                src_queue.put(None)
            elif connector.kind() == SourceKind.GENERATOR:
                gen = connector.generator
                count = 0
                for item in gen():
                    if stop_event.is_set():
                        break
                    src_queue.put(item)
                    count += 1
                    if connector.max_items is not None and count >= connector.max_items:
                        break
                src_queue.put(None)
            elif connector.kind() == SourceKind.FILE:
                self._stream_file(connector, src_queue, stop_event)
            elif connector.kind() == SourceKind.KAFKA:
                self._stream_kafka(connector, src_queue, stop_event)
            elif connector.kind() == SourceKind.DATAGEN:
                self._stream_datagen(connector, src_queue, stop_event)
            elif connector.kind() == SourceKind.SOCKET:
                self._stream_socket(connector, src_queue, stop_event)
        except Exception as e:
            print(f"[liutang] source error: {e}", file=sys.stderr)
            traceback.print_exc(file=sys.stderr)
            src_queue.put(None)

    def _stream_file(self, connector: FileSource, src_queue: queue.Queue, stop_event: threading.Event) -> None:
        with open(connector.path, "r", encoding="utf-8") as f:
            for line in f:
                if stop_event.is_set():
                    break
                src_queue.put(line.rstrip("\n"))
        src_queue.put(None)

    def _stream_kafka(self, connector: KafkaSource, src_queue: queue.Queue, stop_event: threading.Event) -> None:
        try:
            from kafka import KafkaConsumer
        except ImportError:
            print("[liutang] kafka-python not installed. Install with: pip install kafka-python", file=sys.stderr)
            src_queue.put(None)
            return
        consumer = KafkaConsumer(
            connector.topic,
            bootstrap_servers=connector.bootstrap_servers.split(","),
            group_id=connector.group_id,
            auto_offset_reset=connector.start_from,
        )
        try:
            for message in consumer:
                if stop_event.is_set():
                    break
                value = message.value.decode("utf-8") if isinstance(message.value, bytes) else message.value
                src_queue.put(value)
        except Exception as e:
            print(f"[liutang] kafka error: {e}", file=sys.stderr)
        finally:
            consumer.close()
            src_queue.put(None)

    def _stream_datagen(self, connector: DatagenSource, src_queue: queue.Queue, stop_event: threading.Event) -> None:
        interval = 1.0 / max(1, connector.rows_per_second)
        count = 0
        while not stop_event.is_set():
            if connector.max_records is not None and count >= connector.max_records:
                break
            record = {"id": count}
            if connector.fields:
                for fname, ftype in connector.fields.items():
                    if ftype in ("int", "integer"):
                        record[fname] = random.randint(0, 1000)
                    elif ftype in ("float", "double"):
                        record[fname] = round(random.random() * 1000, 2)
                    elif ftype == "string":
                        record[fname] = f"value_{random.randint(0, 9999)}"
                    elif ftype == "boolean":
                        record[fname] = random.choice([True, False])
                    else:
                        record[fname] = round(random.random() * 100, 2)
            else:
                record["value"] = round(random.random() * 100, 2)
            src_queue.put(record)
            count += 1
            time.sleep(interval)
        src_queue.put(None)

    def _stream_socket(self, connector: SocketSource, src_queue: queue.Queue, stop_event: threading.Event) -> None:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(1.0)
        try:
            sock.connect((connector.host, connector.port))
            buf = ""
            while not stop_event.is_set():
                try:
                    data = sock.recv(4096)
                    if not data:
                        break
                    buf += data.decode(connector.encoding, errors="replace")
                    while connector.delimiter in buf:
                        line, buf = buf.split(connector.delimiter, 1)
                        if line:
                            src_queue.put(line)
                except socket.timeout:
                    continue
        except Exception as e:
            print(f"[liutang] socket error: {e}", file=sys.stderr)
        finally:
            sock.close()
            src_queue.put(None)

    def _run_pipeline(
        self,
        runner: StreamRunner,
        src_queue: queue.Queue,
        sink_callback: Any,
        stop_event: threading.Event,
        watermark_strategy: Optional[WatermarkStrategy],
    ) -> None:
        runner.run_streaming(src_queue, sink_callback, stop_event,
                             batch_size=self._flow.config.get("batch_size", 100),
                             batch_timeout=self._flow.config.get("batch_timeout", 1.0),
                             watermark_strategy=watermark_strategy)

    def _read_source(self, connector: Any) -> List[Any]:
        if connector.kind() == SourceKind.COLLECTION:
            return list(connector.data)
        elif connector.kind() == SourceKind.FILE:
            with open(connector.path, "r", encoding="utf-8") as f:
                if connector.fmt == "text":
                    return [line.rstrip("\n") for line in f]
                elif connector.fmt == "csv":
                    content = f.read()
                    reader = csv.DictReader(io.StringIO(content))
                    return [row for row in reader]
                elif connector.fmt == "json":
                    content = f.read()
                    data = json.loads(content) if content.strip() else []
                    return data if isinstance(data, list) else [data]
            return []
        elif connector.kind() == SourceKind.GENERATOR:
            gen = connector.generator
            count = 0
            results = []
            for item in gen():
                results.append(item)
                count += 1
                if connector.max_items is not None and count >= connector.max_items:
                    break
            return results
        elif connector.kind() == SourceKind.DATAGEN:
            records = []
            for _ in range(connector.max_records or 100):
                record = {"id": len(records)}
                if connector.fields:
                    for fname, ftype in connector.fields.items():
                        if ftype in ("int", "integer"):
                            record[fname] = random.randint(0, 1000)
                        elif ftype in ("float", "double"):
                            record[fname] = round(random.random() * 1000, 2)
                        elif ftype == "string":
                            record[fname] = f"value_{random.randint(0, 9999)}"
                        else:
                            record[fname] = round(random.random() * 100, 2)
                else:
                    record["value"] = round(random.random() * 100, 2)
                records.append(record)
            return records
        return []

    def _write_sink(self, connector: Any, data: List[Any]) -> None:
        if connector.kind() == SinkKind.PRINT:
            for item in data:
                print(item)
        elif connector.kind() == SinkKind.FILE:
            with open(connector.path, "w", encoding="utf-8") as f:
                for item in data:
                    f.write(json.dumps(item, default=str) + "\n")
        elif connector.kind() == SinkKind.CALLBACK:
            for item in data:
                connector.func(item)
        elif connector.kind() == SinkKind.COLLECT:
            connector.results.extend(data)

    def _apply_table_ops(self, data: List[Any], operations: List[Dict[str, Any]],
                          schema: Any = None) -> List[Any]:
        result = list(data)
        for op in operations:
            op_type = op["type"]
            if op_type == "where":
                predicate = op["predicate"]
                result = [row for row in result if predicate(row)]
            elif op_type == "select":
                expressions = op["expressions"]
                if expressions:
                    new_result = []
                    for row in result:
                        if callable(expressions[0]):
                            new_result.append(expressions[0](row))
                        elif isinstance(row, dict):
                            new_result.append({e: row.get(e) for e in expressions if isinstance(e, str)})
                        else:
                            new_result.append(row)
                    result = new_result
            elif op_type == "group_by":
                keys = op["keys"]
                grouped: Dict[tuple, List[Any]] = {}
                for row in result:
                    key_values = tuple(row[k] if isinstance(row, dict) else getattr(row, k) for k in keys)
                    grouped.setdefault(key_values, []).append(row)
                result = list(grouped.values())
        return result

    def _find_stream(self, stream_id: str):
        for s in self._flow._streams:
            if s.id == stream_id:
                return s
        for sink in self._flow._sinks:
            if sink["stream"].id == stream_id:
                return sink["stream"]
        return None