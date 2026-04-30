from __future__ import annotations

import sys
import threading
import queue
import time
from typing import Any, Dict, List, Optional

from liutang.core.flow import Flow
from liutang.core.stream import Stream, RuntimeMode
from liutang.core.connector import (
    CollectionSource,
    FileSource,
    KafkaSource,
    PrintSink,
    FileSink,
    CallbackSink,
    SourceKind,
    SinkKind,
)
from liutang.core.errors import EngineNotAvailableError
from liutang.engine.local.runner import StreamRunner, PipelineBuilder


class LocalExecutor:
    def __init__(self, flow: Flow):
        self._flow = flow

    def execute(self) -> Any:
        if self._flow.mode == RuntimeMode.BATCH:
            return self._execute_batch()
        return self._execute_streaming()

    def explain(self) -> str:
        lines = [f"Flow: {self._flow.name}", f"Engine: local", f"Mode: {self._flow.mode.value}",
                  f"Parallelism: {self._flow.parallelism}", ""]
        for src in self._flow.sources:
            lines.append(f"Source: {src['connector'].kind().value} -> {src['id']}")
        for sink in self._flow.sinks:
            stream = sink["stream"]
            lines.append(f"Sink: {sink['connector'].kind().value} <- {stream.id}")
            for op_dict in stream.operations():
                lines.append(f"  {op_dict['type']}: {op_dict.get('name', '')}")
        return "\n".join(lines)

    def _execute_batch(self) -> List[Any]:
        results: Dict[str, List[Any]] = {}
        for src in self._flow.sources:
            connector = src["connector"]
            data = self._read_source(connector)
            stream = self._find_stream(src["id"])
            if stream is None:
                continue
            operations = PipelineBuilder.from_operations(stream.operations())
            runner = StreamRunner(operations, parallelism=self._flow.parallelism)
            results[src["id"]] = runner.run_batch(data)
        for sink in self._flow.sinks:
            stream = sink["stream"]
            connector = sink["connector"]
            data = results.get(stream.id, [])
            self._write_sink(connector, data)
        return results

    def _execute_streaming(self) -> Dict[str, threading.Thread]:
        source_queues: Dict[str, queue.Queue] = {}
        sink_events: Dict[str, threading.Event] = {}
        threads: List[threading.Thread] = []
        for src in self._flow.sources:
            src_queue: queue.Queue = queue.Queue()
            source_queues[src["id"]] = src_queue
            stop_event = threading.Event()
            sink_events[src["id"]] = stop_event
            t = threading.Thread(target=self._run_source, args=(src, src_queue, stop_event), daemon=True)
            t.start()
            threads.append(t)
        for sink in self._flow.sinks:
            stream = sink["stream"]
            src_id = self._find_source_for_stream(stream.id)
            if src_id and src_id in source_queues:
                src_queue = source_queues[src_id]
                stop_event = sink_events.get(src_id, threading.Event())
                connector = sink["connector"]
                stream_obj = self._find_stream(stream.id)
                operations = PipelineBuilder.from_operations(stream_obj.operations()) if stream_obj else []
                runner = StreamRunner(operations, parallelism=self._flow.parallelism)
                t = threading.Thread(
                    target=self._run_pipeline,
                    args=(runner, src_queue, connector, stop_event),
                    daemon=True,
                )
                t.start()
                threads.append(t)
        return {"threads": threads, "stop_events": sink_events}

    def _run_source(self, src: Dict[str, Any], src_queue: queue.Queue, stop_event: threading.Event) -> None:
        connector = src["connector"]
        try:
            if connector.kind() == SourceKind.COLLECTION:
                for item in connector.data:
                    if stop_event.is_set():
                        break
                    src_queue.put(item)
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

    def _stream_file(self, connector: FileSource, src_queue: queue.Queue, stop_event: threading.Event) -> None:
        try:
            with open(connector.path, "r", encoding="utf-8") as f:
                for line in f:
                    if stop_event.is_set():
                        break
                    src_queue.put(line.rstrip("\n"))
        except FileNotFoundError:
            raise

    def _stream_kafka(self, connector: KafkaSource, src_queue: queue.Queue, stop_event: threading.Event) -> None:
        try:
            from kafka import KafkaConsumer
        except ImportError:
            raise EngineNotAvailableError("kafka", "kafka-python is required for Kafka source")
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
                src_queue.put(message.value.decode("utf-8") if isinstance(message.value, bytes) else message.value)
        finally:
            consumer.close()

    def _stream_datagen(self, connector: DatagenSource, src_queue: queue.Queue, stop_event: threading.Event) -> None:
        import random
        interval = 1.0 / (connector.rows_per_second or 1)
        counter = 0
        while not stop_event.is_set():
            record = {"id": counter}
            if connector.fields:
                for fname, ftype in connector.fields.items():
                    if ftype in ("int", "integer"):
                        record[fname] = random.randint(0, 1000)
                    elif ftype in ("float", "double"):
                        record[fname] = random.random() * 1000
                    elif ftype == "string":
                        record[fname] = f"value_{random.randint(0, 9999)}"
                    elif ftype == "boolean":
                        record[fname] = random.choice([True, False])
                    else:
                        record[fname] = random.random()
            else:
                record["value"] = random.random() * 100
            src_queue.put(record)
            counter += 1
            time.sleep(interval)

    def _stream_socket(self, connector: Any, src_queue: queue.Queue, stop_event: threading.Event) -> None:
        import socket
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
                    buf += data.decode("utf-8", errors="replace")
                    while connector.delimiter in buf:
                        line, buf = buf.split(connector.delimiter, 1)
                        if line:
                            src_queue.put(line)
                except socket.timeout:
                    continue
        finally:
            sock.close()

    def _run_pipeline(
        self,
        runner: StreamRunner,
        src_queue: queue.Queue,
        sink_connector: Any,
        stop_event: threading.Event,
    ) -> None:
        def callback(record: Any) -> None:
            self._emit_record(sink_connector, record)

        runner.run_streaming(src_queue, callback, stop_event)

    def _emit_record(self, connector: Any, record: Any) -> None:
        if connector.kind() == SinkKind.PRINT:
            print(record)
        elif connector.kind() == SinkKind.CALLBACK:
            connector.func(record)
        elif connector.kind() == SinkKind.FILE:
            import json
            with open(connector.path, "a", encoding="utf-8") as f:
                f.write(json.dumps(record, default=str) + "\n")

    def _read_source(self, connector: Any) -> List[Any]:
        if connector.kind() == SourceKind.COLLECTION:
            return list(connector.data)
        elif connector.kind() == SourceKind.FILE:
            with open(connector.path, "r", encoding="utf-8") as f:
                if connector.fmt == "text":
                    return [line.rstrip("\n") for line in f]
                elif connector.fmt in ("csv", "json"):
                    content = f.read()
                    if connector.fmt == "json":
                        import json
                        return json.loads(content) if content.strip() else []
                    import csv
                    import io
                    reader = csv.DictReader(io.StringIO(content))
                    return [row for row in reader]
        return []

    def _write_sink(self, connector: Any, data: List[Any]) -> None:
        if connector.kind() == SinkKind.PRINT:
            for item in data:
                print(item)
        elif connector.kind() == SinkKind.FILE:
            import json
            with open(connector.path, "w", encoding="utf-8") as f:
                for item in data:
                    f.write(json.dumps(item, default=str) + "\n")
        elif connector.kind() == SinkKind.CALLBACK:
            for item in data:
                connector.func(item)

    def _find_stream(self, stream_id: str) -> Optional[Stream]:
        for s in self._flow._streams:
            if s.id == stream_id:
                return s
        for sink in self._flow._sinks:
            if sink["stream"].id == stream_id:
                return sink["stream"]
        return None

    def _find_source_for_stream(self, stream_id: str) -> Optional[str]:
        return stream_id.rsplit("_", 1)[0] if "_" in stream_id else None