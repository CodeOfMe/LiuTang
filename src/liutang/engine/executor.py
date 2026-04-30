from __future__ import annotations

import json
import os
import queue
import threading
import time
import traceback
import socket
import csv
import io
import random
import sys
from typing import Any, Dict, List, Optional

from liutang.core.flow import Flow
from liutang.core.stream import RuntimeMode, TableStream, DeliveryMode, ArchitectureMode
from liutang.core.connector import (
    SourceKind, SinkKind, CollectionSource, FileSource, KafkaSource,
    DatagenSource, SocketSource, GeneratorSource,
    PrintSink, FileSink, CallbackSink, CollectSink,
)
from liutang.core.state import WatermarkStrategy, KeyedProcessFunction, ProcessFunction
from liutang.core.errors import PipelineError
from liutang.engine.runner import StreamRunner, PipelineBuilder


class Executor:
    def __init__(self, flow: Flow):
        self._flow = flow
        self._collector: List[Any] = []
        self._checkpoint_counter = 0

    def execute(self) -> Any:
        self._validate()
        arch = self._flow.architecture
        if arch == ArchitectureMode.LAMBDA:
            return self._execute_lambda()
        elif arch == ArchitectureMode.KAPPA:
            return self._execute_kappa()
        elif arch == ArchitectureMode.ADAPTIVE:
            return self._execute_adaptive()
        if self._flow.mode == RuntimeMode.BATCH:
            return self._execute_batch()
        return self._execute_streaming()

    def _validate(self) -> None:
        if not self._flow.sources:
            raise PipelineError("Flow has no sources. Add a source before executing.")
        for src in self._flow.sources:
            connector = src["connector"]
            if connector.kind() == SourceKind.FILE:
                if hasattr(connector, 'path') and not os.path.exists(connector.path):
                    raise PipelineError(f"File source path does not exist: {connector.path}")

    def explain(self) -> str:
        lines = [
            f"Flow: {self._flow.name}",
            f"Engine: liutang (pure Python)",
            f"Mode: {self._flow.mode.value}",
            f"Architecture: {self._flow.architecture.value}",
            f"Delivery: {self._flow.delivery_mode.value}",
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

    def _execute_lambda(self) -> Dict[str, Any]:
        from liutang.core.serving import ServingView, MergeView
        from liutang.core.lambda_flow import LambdaFlow
        batch_results = {}
        batch_serving = ServingView(merge_fn=MergeView.latest)
        for src in self._flow.sources:
            connector = src["connector"]
            data = self._read_source(connector)
            source_id = src["id"]
            for sink in self._flow.sinks:
                stream = sink["stream"]
                if stream.id.startswith(source_id) or stream.id == source_id:
                    operations = PipelineBuilder.from_operations(stream.operations())
                    runner = StreamRunner(
                        operations,
                        parallelism=self._flow.parallelism,
                        concurrency=self._flow.config.get("concurrency", "thread"),
                        delivery_mode=self._flow.delivery_mode,
                        max_retries=self._flow.max_retries,
                    )
                    batch_data = runner.run_batch(data)
                    try:
                        batch_serving.update_batch(batch_data)
                    except Exception:
                        pass
                    batch_results[stream.id] = batch_data
        speed_handles = self._execute_streaming()
        return {
            "architecture": "lambda",
            "batch_results": batch_results,
            "serving": batch_serving,
            "speed_handles": speed_handles,
        }

    def _execute_kappa(self) -> Dict[str, Any]:
        from liutang.core.serving import ServingView
        serving = ServingView()
        source_data = {}
        for src in self._flow.sources:
            connector = src["connector"]
            data = self._read_source(connector)
            source_data[src["id"]] = data
            serving.update_batch(data)
        streaming_result = self._execute_streaming()
        event_log_path = self._flow.config.get("event_log_path")
        event_log = None
        if event_log_path:
            from liutang.core.eventlog import EventLog
            event_log = EventLog(event_log_path)
            for src_id, records in source_data.items():
                event_log.append_batch(records)
        return {
            "architecture": "kappa",
            "results": serving.query_all(),
            "handles": streaming_result,
            "event_log": event_log,
            "serving": serving,
        }

    def _execute_adaptive(self) -> Dict[str, Any]:
        from liutang.core.granularity import GranularityController, GranularityLevel
        controller = self._flow.config.get("granularity_controller")
        if controller is None:
            controller = GranularityController()
        initial_level = controller.level
        results: Dict[str, List[Any]] = {}
        for src in self._flow.sources:
            connector = src["connector"]
            data = self._read_source(connector)
            if self._flow._schema_enforcement and src.get("schema"):
                data = self._enforce_schema(data, src["schema"])
            source_id = src["id"]
            total_records = len(data)
            controller.update_metrics(
                arrival_rate=total_records / max(0.001, controller.batch_timeout),
                queue_depth=total_records,
                backlog_size=total_records,
            )
            for sink in self._flow.sinks:
                stream = sink["stream"]
                if stream.id.startswith(source_id) or stream.id == source_id:
                    operations = PipelineBuilder.from_operations(stream.operations())
                    runner = StreamRunner(
                        operations,
                        parallelism=self._flow.parallelism,
                        concurrency=self._flow.config.get("concurrency", "thread"),
                        delivery_mode=self._flow.delivery_mode,
                        max_retries=self._flow.max_retries,
                    )
                    chunk_size = max(1, controller.batch_size)
                    all_sink_data: List[Any] = []
                    processed = 0
                    for chunk_start in range(0, len(data), chunk_size):
                        chunk = data[chunk_start:chunk_start + chunk_size]
                        chunk_result = runner.run_batch(chunk)
                        processed += len(chunk)
                        remaining = total_records - processed
                        controller.update_metrics(
                            arrival_rate=max(1.0, remaining / max(0.001, controller.batch_timeout)),
                            queue_depth=remaining,
                            backlog_size=remaining,
                        )
                        if remaining > 0:
                            controller.adjust()
                            chunk_size = max(1, controller.batch_size)
                        all_sink_data.extend(chunk_result)
                    sink_connector = sink["connector"]
                    if "table_operations" in sink:
                        all_sink_data = self._apply_table_ops(
                            all_sink_data, sink["table_operations"], sink.get("table_schema"))
                    self._write_sink(sink_connector, all_sink_data)
                    results[stream.id] = all_sink_data
                    self._maybe_checkpoint(runner)
        return {
            "architecture": "adaptive",
            "granularity": controller.level.value,
            "batch_size": controller.batch_size,
            "batch_timeout": controller.batch_timeout,
            "initial_granularity": initial_level.value,
            "adjustments": controller.adjustments_count,
            "controller": controller,
            "results": results,
        }

    def _execute_batch(self) -> Dict[str, List[Any]]:
        results: Dict[str, List[Any]] = {}
        for src in self._flow.sources:
            connector = src["connector"]
            data = self._read_source(connector)
            if self._flow._schema_enforcement and src.get("schema"):
                data = self._enforce_schema(data, src["schema"])
            source_id = src["id"]
            processed = False
            for sink in self._flow.sinks:
                stream = sink["stream"]
                if stream.id.startswith(source_id) or stream.id == source_id:
                    operations = PipelineBuilder.from_operations(stream.operations())
                    runner = StreamRunner(
                        operations,
                        parallelism=self._flow.parallelism,
                        concurrency=self._flow.config.get("concurrency", "thread"),
                        delivery_mode=self._flow.delivery_mode,
                        max_retries=self._flow.max_retries,
                    )
                    sink_data = runner.run_batch(data)
                    connector = sink["connector"]
                    if "table_operations" in sink:
                        sink_data = self._apply_table_ops(sink_data, sink["table_operations"], sink.get("table_schema"))
                    self._write_sink(connector, sink_data)
                    results[stream.id] = sink_data
                    processed = True
                    self._maybe_checkpoint(runner)
            if not processed:
                for s in self._flow._streams:
                    if s.id.startswith(source_id):
                        operations = PipelineBuilder.from_operations(s.operations())
                        runner = StreamRunner(
                            operations,
                            parallelism=self._flow.parallelism,
                            concurrency=self._flow.config.get("concurrency", "thread"),
                            delivery_mode=self._flow.delivery_mode,
                            max_retries=self._flow.max_retries,
                        )
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
            runner = StreamRunner(
                operations,
                parallelism=self._flow.parallelism,
                concurrency=self._flow.config.get("concurrency", "thread"),
                delivery_mode=self._flow.delivery_mode,
                max_retries=self._flow.max_retries,
            )
            watermark_strategy = stream_obj._watermark_strategy if stream_obj else None
            stop_event = stop_events.get(src_id, threading.Event())
            late_collector = []
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
        except (KeyboardInterrupt, SystemExit):
            src_queue.put(None)
            raise
        except Exception as e:
            print(f"[liutang] source error: {e}", file=sys.stderr)
            traceback.print_exc(file=sys.stderr)
            src_queue.put(None)

    def _stream_file(self, connector: FileSource, src_queue: queue.Queue, stop_event: threading.Event) -> None:
        try:
            with open(connector.path, "r", encoding="utf-8") as f:
                for line in f:
                    if stop_event.is_set():
                        break
                    src_queue.put(line.rstrip("\n"))
        except FileNotFoundError as e:
            print(f"[liutang] file not found: {connector.path}", file=sys.stderr)
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
        late_callback = None
        if self._flow.config.get("late_output"):
            late_callback = self._flow.config["late_output"]
        runner.run_streaming(src_queue, sink_callback, stop_event,
                             batch_size=self._flow.config.get("batch_size", 100),
                             batch_timeout=self._flow.config.get("batch_timeout", 1.0),
                             watermark_strategy=watermark_strategy,
                             late_callback=late_callback)

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
        if connector.kind() == SinkKind.FILE:
            with open(connector.path, "w", encoding="utf-8") as f:
                for item in data:
                    f.write(json.dumps(item, default=str) + "\n")
        elif connector.kind() == SinkKind.COLLECT:
            connector.results.extend(data)
        else:
            for item in data:
                self._write_sink_item(connector, item)

    def _write_sink_item(self, connector: Any, item: Any) -> None:
        if self._flow.delivery_mode == DeliveryMode.AT_MOST_ONCE:
            try:
                if connector.kind() == SinkKind.PRINT:
                    print(item)
                elif connector.kind() == SinkKind.CALLBACK:
                    connector.func(item)
                elif connector.kind() == SinkKind.COLLECT:
                    connector.results.append(item)
            except (KeyboardInterrupt, SystemExit):
                raise
            except Exception:
                pass
        elif self._flow.delivery_mode == DeliveryMode.AT_LEAST_ONCE:
            for attempt in range(self._flow.max_retries + 1):
                try:
                    if connector.kind() == SinkKind.PRINT:
                        print(item)
                    elif connector.kind() == SinkKind.CALLBACK:
                        connector.func(item)
                    elif connector.kind() == SinkKind.COLLECT:
                        connector.results.append(item)
                    break
                except (KeyboardInterrupt, SystemExit):
                    raise
                except Exception:
                    if attempt == self._flow.max_retries:
                        raise
        else:
            if connector.kind() == SinkKind.PRINT:
                print(item)
            elif connector.kind() == SinkKind.CALLBACK:
                connector.func(item)
            elif connector.kind() == SinkKind.COLLECT:
                connector.results.append(item)

    def _apply_table_ops(self, data: List[Any], operations: List[Dict[str, Any]],
                          schema: Any = None) -> List[Any]:
        result = list(data)
        group_keys = None
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
                group_keys = keys
                grouped: Dict[tuple, List[Any]] = {}
                for row in result:
                    key_values = tuple(row[k] if isinstance(row, dict) else getattr(row, k) for k in keys)
                    grouped.setdefault(key_values, []).append(row)
                result = list(grouped.values())
            elif op_type == "order_by":
                keys = op["keys"]
                def sort_key(row):
                    if isinstance(row, dict):
                        return tuple(row.get(k, "") for k in keys)
                    return tuple(getattr(row, k, "") for k in keys)
                result = sorted(result, key=sort_key)
            elif op_type == "limit":
                n = op["n"]
                result = result[:n]
            elif op_type == "grouped_count":
                if isinstance(result, list) and result and isinstance(result[0], list):
                    result = [len(group) for group in result]
            elif op_type == "grouped_sum":
                field = op["field"]
                if isinstance(result, list) and result and isinstance(result[0], list):
                    new_result = []
                    for group in result:
                        if group and isinstance(group[0], dict):
                            s = sum(row.get(field if isinstance(field, str) else list(row.keys())[field], 0) for row in group)
                        else:
                            s = sum(self._extract_table_field(item, field) for item in group)
                        new_result.append(s)
                    result = new_result
            elif op_type == "grouped_avg":
                field = op["field"]
                if isinstance(result, list) and result and isinstance(result[0], list):
                    new_result = []
                    for group in result:
                        if group:
                            if isinstance(group[0], dict):
                                vals = [row.get(field if isinstance(field, str) else list(row.keys())[field], 0) for row in group]
                            else:
                                vals = [self._extract_table_field(item, field) for item in group]
                            new_result.append(sum(vals) / len(vals) if vals else 0)
                        else:
                            new_result.append(0)
                    result = new_result
            elif op_type == "grouped_select":
                expressions = op.get("expressions", ())
                if isinstance(result, list) and result and isinstance(result[0], list):
                    new_result = []
                    for group in result:
                        if expressions and callable(expressions[0]):
                            new_result.append(expressions[0](group))
                        else:
                            new_result.append(group)
                    result = new_result
            elif op_type == "windowed_select":
                expressions = op.get("expressions", ())
                if expressions and callable(expressions[0]):
                    result = [expressions[0](row) for row in result]
        return result

    @staticmethod
    def _extract_table_field(item: Any, field: Any) -> Any:
        if isinstance(item, (int, float)):
            return item
        if isinstance(item, dict):
            if isinstance(field, str):
                return item.get(field, 0)
            return list(item.values())[field] if field < len(item) else 0
        if isinstance(item, (list, tuple)):
            return item[field] if field < len(item) else 0
        return item

    def _maybe_checkpoint(self, runner: StreamRunner) -> None:
        if self._flow.checkpoint_dir is None:
            return
        self._checkpoint_counter += 1
        if self._checkpoint_counter >= self._flow.config.get("checkpoint_every", 1):
            self._checkpoint_counter = 0
            self._save_checkpoint(runner)

    def _save_checkpoint(self, runner: StreamRunner) -> None:
        checkpoint_dir = self._flow.checkpoint_dir
        if checkpoint_dir is None:
            return
        os.makedirs(checkpoint_dir, exist_ok=True)
        state_snapshot = runner._runtime_context._keyed_states
        checkpoint_data = {}
        for key, states in state_snapshot.items():
            key_data = {}
            for name, state in states.items():
                if isinstance(state, ValueState):
                    key_data[name] = {"type": "value", "value": state.value}
                elif isinstance(state, type(None)):
                    continue
            if key_data:
                checkpoint_data[str(key)] = key_data
        path = os.path.join(checkpoint_dir, f"checkpoint_{int(time.monotonic()*1000)}.json")
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(checkpoint_data, f, default=str, indent=2)
        except Exception as e:
            print(f"[liutang] checkpoint save failed: {e}", file=sys.stderr)

    def _enforce_schema(self, data: List[Any], schema: Any) -> List[Any]:
        if schema is None:
            return data
        enforced = []
        for record in data:
            if isinstance(record, dict):
                row = {}
                for field in schema.fields:
                    if field.name in record:
                        row[field.name] = record[field.name]
                    else:
                        row[field.name] = None
                enforced.append(row)
            else:
                enforced.append(record)
        return enforced

    def _find_stream(self, stream_id: str):
        for s in self._flow._streams:
            if s.id == stream_id:
                return s
        for sink in self._flow._sinks:
            if sink["stream"].id == stream_id:
                return sink["stream"]
        return None