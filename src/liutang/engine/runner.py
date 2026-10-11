from __future__ import annotations

import hashlib
import json
import sys
import threading
import time
import queue
import collections
import csv
import io
import random
import socket
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from typing import Any, Callable, Dict, List, Optional

from liutang.core.errors import PipelineError
from liutang.core.stream import DeliveryMode
from liutang.core.state import (
    KeyedProcessFunction, ProcessFunction, RuntimeContext, TimerService,
    WatermarkStrategy,
)
from liutang.core.window import WindowKind, WindowType
from liutang.core.connector import (
    SourceKind, SinkKind, CollectionSource, FileSource, KafkaSource,
    DatagenSource, SocketSource, GeneratorSource,
    PrintSink, FileSink, CallbackSink, CollectSink,
)


class PipelineOp:
    MAP = "map"
    FLAT_MAP = "flat_map"
    FILTER = "filter"
    KEY_BY = "key_by"
    REDUCE = "reduce"
    SUM = "sum"
    COUNT = "count"
    MIN = "min"
    MAX = "max"
    PROCESS = "process"
    KEYED_PROCESS = "keyed_process"
    KEYED_REDUCE = "keyed_reduce"
    KEYED_SUM = "keyed_sum"
    KEYED_COUNT = "keyed_count"
    WINDOW_AGGREGATE = "window_aggregate"
    WINDOW_SUM = "window_sum"
    WINDOW_COUNT = "window_count"
    WINDOW_MIN = "window_min"
    WINDOW_MAX = "window_max"
    WINDOW_APPLY = "window_apply"
    ASSIGN_TIMESTAMPS = "assign_timestamps"

    def __init__(self, op_type: str, func: Optional[Callable] = None, **kwargs: Any):
        self.op_type = op_type
        self.func = func
        self.kwargs = kwargs

    def __repr__(self) -> str:
        name = self.kwargs.get("name", "")
        return f"PipelineOp({self.op_type}, name={name})"


class PipelineBuilder:
    @staticmethod
    def from_operations(operations: List[Dict[str, Any]]) -> List[PipelineOp]:
        ops: List[PipelineOp] = []
        for op_dict in operations:
            op_type = op_dict["type"]
            func = op_dict.get("func")
            if isinstance(func, (KeyedProcessFunction, ProcessFunction)):
                pass
            ops.append(PipelineOp(op_type, func, **{k: v for k, v in op_dict.items() if k not in ("type", "func")}))
        return ops


class StreamRunner:
    MAX_PROCESSED_IDS = 100000

    def __init__(
        self,
        operations: List[PipelineOp],
        parallelism: int = 1,
        concurrency: str = "thread",
        max_workers: Optional[int] = None,
        delivery_mode: DeliveryMode = DeliveryMode.AT_LEAST_ONCE,
        max_retries: int = 3,
    ):
        self._operations = operations
        self._parallelism = parallelism
        self._concurrency = concurrency
        self._max_workers = max_workers or max(1, parallelism * 2)
        self._delivery_mode = delivery_mode
        self._max_retries = max_retries
        self._runtime_context = RuntimeContext()
        self._timer_service = TimerService()
        self._runtime_context.timer_service = self._timer_service
        self._processed_ids: collections.OrderedDict = collections.OrderedDict()
        self._late_output: List[Any] = []
        self._watermark_tracker = None
        self._batches_processed = 0
        self._records_processed = 0
        self._records_dropped = 0

    @property
    def late_output(self) -> List[Any]:
        return list(self._late_output)

    @property
    def stats(self) -> Dict[str, Any]:
        return {
            "batches_processed": self._batches_processed,
            "records_processed": self._records_processed,
            "records_dropped": self._records_dropped,
            "processed_ids_count": len(self._processed_ids),
        }

    def run_batch(self, data: List[Any]) -> List[Any]:
        result: List[Any] = list(data)
        for op in self._operations:
            result = self._apply_op_with_delivery(op, result)
        return result

    def run_streaming(
        self,
        source_queue: queue.Queue,
        sink_callback: Optional[Callable[[Any], None]] = None,
        stop_event: Optional[threading.Event] = None,
        batch_size: int = 100,
        batch_timeout: float = 1.0,
        watermark_strategy: Optional[WatermarkStrategy] = None,
        late_callback: Optional[Callable[[Any], None]] = None,
    ) -> None:
        if stop_event is None:
            stop_event = threading.Event()
        buffer: List[Any] = []
        watermark_tracker = None
        if watermark_strategy:
            from liutang.engine.watermark import WatermarkTracker
            watermark_tracker = WatermarkTracker(watermark_strategy)
            self._watermark_tracker = watermark_tracker
        adaptive_batch_size = batch_size
        while not stop_event.is_set():
            try:
                item = source_queue.get(timeout=0.1)
                if item is None:
                    break
                buffer.append(item)
                if watermark_tracker:
                    ts = self._extract_timestamp(item, watermark_strategy)
                    if ts is not None:
                        watermark_tracker.on_event(ts)
                if len(buffer) >= adaptive_batch_size:
                    self._process_batch_and_emit(buffer, sink_callback, late_callback)
                    self._batches_processed += 1
                    buffer = []
                    if watermark_tracker:
                        self._timer_service.advance_watermark(watermark_tracker.current_watermark())
                    if self._flow_config_has("adaptive_batch"):
                        queue_depth = source_queue.qsize()
                        if queue_depth > batch_size * 2:
                            adaptive_batch_size = min(batch_size * 4, adaptive_batch_size * 2)
                        elif queue_depth < batch_size // 2:
                            adaptive_batch_size = max(batch_size // 2, adaptive_batch_size // 2)
            except queue.Empty:
                if buffer:
                    self._process_batch_and_emit(buffer, sink_callback, late_callback)
                    self._batches_processed += 1
                    buffer = []
        if buffer:
            self._process_batch_and_emit(buffer, sink_callback, late_callback)

    def _flow_config_has(self, key: str) -> bool:
        return False

    def _process_batch_and_emit(self, batch: List[Any], callback: Optional[Callable[[Any], None]],
                                 late_callback: Optional[Callable[[Any], None]] = None) -> None:
        try:
            filtered_batch = []
            for item in batch:
                if self._watermark_tracker and self._is_late_record(item):
                    self._records_dropped += 1
                    self._late_output.append(item)
                    if late_callback:
                        late_callback(item)
                    continue
                filtered_batch.append(item)
            if not filtered_batch:
                return
            results = self.run_batch(filtered_batch)
            self._records_processed += len(filtered_batch)
            if callback:
                for r in results:
                    if self._delivery_mode == DeliveryMode.EXACTLY_ONCE:
                        rid = self._record_id(r)
                        if rid not in self._processed_ids:
                            self._processed_ids[rid] = True
                            self._evict_processed_ids()
                            callback(r)
                    else:
                        callback(r)
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception:
            if self._delivery_mode == DeliveryMode.AT_MOST_ONCE:
                self._records_dropped += len(batch)
            elif self._delivery_mode == DeliveryMode.AT_LEAST_ONCE:
                for attempt in range(self._max_retries):
                    try:
                        results = self.run_batch(batch)
                        if callback:
                            for r in results:
                                callback(r)
                        return
                    except (KeyboardInterrupt, SystemExit):
                        raise
                    except Exception:
                        time.sleep(0.01 * (attempt + 1))
            else:
                raise

    def _is_late_record(self, record: Any) -> bool:
        if self._watermark_tracker is None:
            return False
        ops = [op for op in self._operations if op.op_type.startswith("window_")]
        if not ops:
            return False
        window = None
        for op in ops:
            w = op.kwargs.get("window")
            if w and w.allowed_lateness > 0:
                window = w
                break
        if window is None:
            return False
        time_field = window.time_field
        if time_field is None:
            return False
        ts = self._extract_timestamp(record, None) or self._extract_field(record, time_field)
        if ts is None:
            return False
        return self._watermark_tracker.is_late(float(ts))

    def _evict_processed_ids(self) -> None:
        while len(self._processed_ids) > self.MAX_PROCESSED_IDS:
            self._processed_ids.popitem(last=False)

    @staticmethod
    def _record_id(record: Any) -> Any:
        try:
            if isinstance(record, dict):
                canonical = json.dumps(record, sort_keys=True, default=str)
            elif isinstance(record, (list, tuple)):
                canonical = json.dumps(record, default=str)
            else:
                canonical = repr(record)
            return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        except Exception:
            return repr(record)

    def _apply_op_with_delivery(self, op: PipelineOp, data: List[Any]) -> List[Any]:
        if self._delivery_mode == DeliveryMode.AT_MOST_ONCE:
            return self._apply_op_at_most_once(op, data)
        elif self._delivery_mode == DeliveryMode.EXACTLY_ONCE:
            return self._apply_op_exactly_once(op, data)
        else:
            return self._apply_op_at_least_once(op, data)

    def _apply_op_at_most_once(self, op: PipelineOp, data: List[Any]) -> List[Any]:
        try:
            return self._apply_op(op, data)
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception:
            self._records_dropped += len(data)
            return []

    def _apply_op_at_least_once(self, op: PipelineOp, data: List[Any]) -> List[Any]:
        last_error = None
        for attempt in range(self._max_retries + 1):
            try:
                return self._apply_op(op, data)
            except (KeyboardInterrupt, SystemExit):
                raise
            except Exception as e:
                last_error = e
                if attempt < self._max_retries:
                    time.sleep(0.01 * (attempt + 1))
        raise last_error

    def _apply_op_exactly_once(self, op: PipelineOp, data: List[Any]) -> List[Any]:
        deduped = []
        for r in data:
            rid = self._record_id(r)
            if rid not in self._processed_ids:
                self._processed_ids[rid] = True
                self._evict_processed_ids()
                deduped.append(r)
        try:
            return self._apply_op(op, deduped)
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception:
            for r in deduped:
                self._processed_ids.pop(self._record_id(r), None)
            raise

    def _apply_op(self, op: PipelineOp, data: List[Any]) -> List[Any]:
        keyed_ops = {PipelineOp.KEY_BY, PipelineOp.KEYED_PROCESS, PipelineOp.KEYED_REDUCE,
                     PipelineOp.KEYED_SUM, PipelineOp.KEYED_COUNT}
        windowed_ops = {PipelineOp.WINDOW_AGGREGATE, PipelineOp.WINDOW_SUM, PipelineOp.WINDOW_COUNT,
                       PipelineOp.WINDOW_MIN, PipelineOp.WINDOW_MAX, PipelineOp.WINDOW_APPLY}
        if op.op_type in keyed_ops or op.op_type in windowed_ops:
            return self._apply_op_sequential(op, data)
        if self._parallelism > 1 and op.op_type in (PipelineOp.MAP, PipelineOp.FLAT_MAP, PipelineOp.FILTER, PipelineOp.PROCESS):
            return self._apply_op_parallel(op, data)
        return self._apply_op_sequential(op, data)

    def _apply_op_sequential(self, op: PipelineOp, data: List[Any]) -> List[Any]:
        if op.op_type == PipelineOp.MAP:
            return [op.func(x) for x in data]
        elif op.op_type == PipelineOp.FLAT_MAP:
            result = []
            for x in data:
                result.extend(op.func(x))
            return result
        elif op.op_type == PipelineOp.FILTER:
            return [x for x in data if op.func(x)]
        elif op.op_type == PipelineOp.PROCESS:
            if isinstance(op.func, ProcessFunction):
                result = []
                for x in data:
                    out = op.func.process(x)
                    if out is not None:
                        result.append(out)
                return result
            result = []
            for x in data:
                out = op.func(x)
                if out is not None:
                    if isinstance(out, (list, tuple)):
                        result.extend(out)
                    else:
                        result.append(out)
            return result
        elif op.op_type == PipelineOp.KEY_BY:
            grouped: Dict[Any, List[Any]] = collections.defaultdict(list)
            for x in data:
                key = op.func(x) if callable(op.func) else self._extract_field(x, op.func)
                grouped[key].append(x)
            return [(k, v) for k, v in grouped.items()]
        elif op.op_type == PipelineOp.REDUCE:
            if not data:
                return []
            result_item = data[0]
            for x in data[1:]:
                result_item = op.func(result_item, x)
            return [result_item]
        elif op.op_type == PipelineOp.SUM:
            field_val = op.kwargs.get("field", 0)
            return [sum(self._extract_field(x, field_val) for x in data)] if data else []
        elif op.op_type == PipelineOp.COUNT:
            return [len(data)]
        elif op.op_type == PipelineOp.MIN:
            field_val = op.kwargs.get("field", 0)
            return [min(self._extract_field(x, field_val) for x in data)] if data else []
        elif op.op_type == PipelineOp.MAX:
            field_val = op.kwargs.get("field", 0)
            return [max(self._extract_field(x, field_val) for x in data)] if data else []
        elif op.op_type == PipelineOp.KEYED_PROCESS:
            return self._apply_keyed_process(op, data)
        elif op.op_type == PipelineOp.KEYED_REDUCE:
            return self._apply_keyed_reduce(op, data)
        elif op.op_type == PipelineOp.KEYED_SUM:
            return self._apply_keyed_numeric(op, data, "sum")
        elif op.op_type == PipelineOp.KEYED_COUNT:
            return self._apply_keyed_numeric(op, data, "count")
        elif op.op_type in {PipelineOp.WINDOW_AGGREGATE, PipelineOp.WINDOW_SUM, PipelineOp.WINDOW_COUNT,
                           PipelineOp.WINDOW_MIN, PipelineOp.WINDOW_MAX, PipelineOp.WINDOW_APPLY}:
            return self._apply_window_op(op, data)
        elif op.op_type == PipelineOp.ASSIGN_TIMESTAMPS:
            return data
        else:
            raise PipelineError(f"Unknown operation type: {op.op_type}")

    def _apply_keyed_process(self, op: PipelineOp, data: List[Any]) -> List[Any]:
        if not data:
            return []
        results = []
        func = op.func
        if isinstance(func, type) and issubclass(func, KeyedProcessFunction):
            func = func()
        if isinstance(func, KeyedProcessFunction):
            func.open(self._runtime_context)
            for item in data:
                if isinstance(item, tuple) and len(item) == 2:
                    key, value = item
                    self._runtime_context.set_current_key(key)
                    if isinstance(value, list):
                        for v in value:
                            out = func.process_element(v, self._runtime_context)
                            if out is not None:
                                results.append(out)
                    else:
                        out = func.process_element(value, self._runtime_context)
                        if out is not None:
                            results.append(out)
                else:
                    out = func.process_element(item, self._runtime_context)
                    if out is not None:
                        results.append(out)
            wm_timestamp = self._timer_service.current_watermark if self._timer_service else None
            if wm_timestamp is not None and wm_timestamp > float("-inf"):
                for cb in self._timer_service.fire_event_time_timers(wm_timestamp):
                    if cb:
                        results.append(cb)
            func.close()
        else:
            return [func(x) if not isinstance(x, tuple) else func(x[1]) for x in data]
        return results

    def _apply_keyed_reduce(self, op: PipelineOp, data: List[Any]) -> List[Any]:
        if not data:
            return []
        grouped: Dict[Any, Any] = {}
        for item in data:
            if isinstance(item, tuple) and len(item) == 2:
                key, value = item
            else:
                key = item
                value = item
            if key in grouped:
                grouped[key] = op.func(grouped[key], value)
            else:
                grouped[key] = value
        return [(k, v) for k, v in grouped.items()]

    def _apply_keyed_numeric(self, op: PipelineOp, data: List[Any], agg_type: str) -> List[Any]:
        if not data:
            return []
        grouped: Dict[Any, List[Any]] = collections.defaultdict(list)
        for item in data:
            if isinstance(item, tuple) and len(item) == 2:
                key, value = item
                if isinstance(value, list):
                    grouped[key].extend(value)
                else:
                    grouped[key].append(value)
            else:
                grouped[item].append(item)
        results = []
        field_val = op.kwargs.get("field", 0)
        for key, values in grouped.items():
            if agg_type == "sum":
                s = sum(self._extract_field(v, field_val) for v in values)
                results.append((key, s))
            elif agg_type == "count":
                results.append((key, len(values)))
        return results

    def _apply_op_parallel(self, op: PipelineOp, data: List[Any]) -> List[Any]:
        ExecutorCls = ThreadPoolExecutor if self._concurrency == "thread" else ProcessPoolExecutor
        chunk_size = max(1, len(data) // self._max_workers)
        chunks = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)] if chunk_size > 0 else [data]
        results: List[Any] = []
        with ExecutorCls(max_workers=self._max_workers) as pool:
            if op.op_type == PipelineOp.MAP:
                fn = op.func
                futures = [pool.submit(lambda c, f=fn: [f(x) for x in c], chunk) for chunk in chunks]
            elif op.op_type == PipelineOp.FLAT_MAP:
                fn = op.func
                futures = [pool.submit(lambda c, f=fn: [y for x in c for y in f(x)], chunk) for chunk in chunks]
            elif op.op_type == PipelineOp.FILTER:
                fn = op.func
                futures = [pool.submit(lambda c, f=fn: [x for x in c if f(x)], chunk) for chunk in chunks]
            elif op.op_type == PipelineOp.PROCESS:
                if isinstance(op.func, ProcessFunction):
                    def _process_chunk_process(pf, c):
                        r = []
                        for x in c:
                            out = pf.process(x)
                            if out is not None:
                                r.append(out)
                        return r
                    futures = [pool.submit(_process_chunk_process, op.func, chunk) for chunk in chunks]
                else:
                    def _process_chunk_func(fn, c):
                        r = []
                        for x in c:
                            out = fn(x)
                            if out is not None:
                                if isinstance(out, (list, tuple)):
                                    r.extend(out)
                                else:
                                    r.append(out)
                        return r
                    futures = [pool.submit(_process_chunk_func, op.func, chunk) for chunk in chunks]
            else:
                return self._apply_op_sequential(op, data)
            for future in as_completed(futures):
                results.extend(future.result())
        return results

    @staticmethod
    def _extract_field(record: Any, field: Any) -> Any:
        if isinstance(record, (int, float)):
            return record
        if isinstance(record, dict):
            if isinstance(field, str):
                return record.get(field, 0)
            return list(record.values())[field] if field < len(record) else 0
        if isinstance(record, (list, tuple)):
            return record[field] if field < len(record) else 0
        if hasattr(record, str(field)):
            return getattr(record, str(field), 0)
        return record

    @staticmethod
    def _extract_timestamp(record: Any, strategy: Any) -> Optional[float]:
        if strategy is None or getattr(strategy, 'time_field', None) is None:
            return None
        time_field = strategy.time_field
        if isinstance(record, dict):
            return record.get(time_field)
        if isinstance(record, (list, tuple)):
            return None
        return getattr(record, time_field, None)

    def _apply_window_op(self, op: PipelineOp, data: List[Any]) -> List[Any]:
        window = op.kwargs.get("window")
        if window is None:
            return data
        time_field = window.time_field
        if time_field and data:
            if window.kind == WindowKind.TUMBLING:
                return self._tumbling_window(op, data, window.size, time_field, window.allowed_lateness)
            elif window.kind == WindowKind.SLIDING:
                return self._sliding_window(op, data, window.size, window.slide, time_field, window.allowed_lateness)
            elif window.kind == WindowKind.SESSION:
                return self._session_window(op, data, window.gap, time_field, window.allowed_lateness)
            elif window.kind == WindowKind.OVER:
                return self._over_window(op, data, time_field)
            elif window.kind == WindowKind.GLOBAL:
                return self._apply_window_agg(op, data)
        if window.kind == WindowKind.GLOBAL or time_field is None:
            return self._apply_window_agg(op, data)
        return self._apply_window_agg(op, data)

    def _tumbling_window(self, op: PipelineOp, data: List[Any], size: Any, time_field: str,
                          allowed_lateness: float = 0.0) -> List[Any]:
        if not data:
            return []
        windows: Dict[int, List[Any]] = collections.defaultdict(list)
        size_val = size if isinstance(size, (int, float)) else 1.0
        for record in data:
            ts = self._extract_timestamp(record, None) or self._extract_field(record, time_field)
            if ts is not None:
                window_key = int(float(ts) / size_val)
                if allowed_lateness > 0 and self._watermark_tracker:
                    if self._watermark_tracker.is_late(float(ts)):
                        self._late_output.append(record)
                        continue
                windows[window_key].append(record)
            else:
                windows[0].append(record)
        results = []
        for _, window_data in sorted(windows.items()):
            results.extend(self._apply_window_agg(op, window_data))
        return results

    def _sliding_window(self, op: PipelineOp, data: List[Any], size: Any, slide: Any,
                         time_field: str, allowed_lateness: float = 0.0) -> List[Any]:
        if not data:
            return []
        size_val = size if isinstance(size, (int, float)) else 1.0
        slide_val = slide if isinstance(slide, (int, float)) else 1.0
        windows: Dict[int, List[Any]] = collections.defaultdict(list)
        for record in data:
            ts = self._extract_field(record, time_field) if time_field else None
            if ts is not None:
                if allowed_lateness > 0 and self._watermark_tracker:
                    if self._watermark_tracker.is_late(float(ts)):
                        self._late_output.append(record)
                        continue
                w_start = int(float(ts) / slide_val)
                first_window = max(0, w_start - int(size_val / slide_val) + 1)
                for i in range(first_window, w_start + 1):
                    window_start = i * slide_val
                    if float(ts) >= window_start and float(ts) < window_start + size_val:
                        windows[i].append(record)
            else:
                windows[0].append(record)
        results = []
        for _, window_data in sorted(windows.items()):
            results.extend(self._apply_window_agg(op, window_data))
        return results

    def _session_window(self, op: PipelineOp, data: List[Any], gap: Any, time_field: str,
                         allowed_lateness: float = 0.0) -> List[Any]:
        if not data:
            return []
        gap_val = gap if isinstance(gap, (int, float)) else 5.0
        sorted_data = sorted(data, key=lambda x: self._extract_field(x, time_field) if time_field else 0)
        sessions: List[List[Any]] = []
        current_session: List[Any] = [sorted_data[0]]
        for record in sorted_data[1:]:
            if time_field:
                prev_ts = self._extract_field(current_session[-1], time_field)
                curr_ts = self._extract_field(record, time_field)
            else:
                prev_ts = curr_ts = 0
            if curr_ts is not None and prev_ts is not None and float(curr_ts) - float(prev_ts) <= gap_val:
                current_session.append(record)
            else:
                sessions.append(current_session)
                current_session = [record]
        if current_session:
            sessions.append(current_session)
        results = []
        for session in sessions:
            results.extend(self._apply_window_agg(op, session))
        return results

    def _over_window(self, op: PipelineOp, data: List[Any], time_field: str) -> List[Any]:
        return self._apply_window_agg(op, data)

    def _apply_window_agg(self, op: PipelineOp, window_data: List[Any]) -> List[Any]:
        if not window_data:
            return []
        if op.op_type == PipelineOp.WINDOW_COUNT:
            return [len(window_data)]
        elif op.op_type == PipelineOp.WINDOW_SUM:
            field = op.kwargs.get("field", 0)
            return [sum(self._extract_field(x, field) for x in window_data)]
        elif op.op_type == PipelineOp.WINDOW_MIN:
            field = op.kwargs.get("field", 0)
            return [min(self._extract_field(x, field) for x in window_data)]
        elif op.op_type == PipelineOp.WINDOW_MAX:
            field = op.kwargs.get("field", 0)
            return [max(self._extract_field(x, field) for x in window_data)]
        elif op.op_type == PipelineOp.WINDOW_AGGREGATE:
            if op.func:
                return [op.func(window_data)]
            return window_data
        elif op.op_type == PipelineOp.WINDOW_APPLY:
            if op.func:
                result = op.func(window_data)
                return result if isinstance(result, list) else [result]
            return window_data
        return window_data