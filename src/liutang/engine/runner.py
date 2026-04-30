from __future__ import annotations

import json
import sys
import threading
import time
import traceback
import queue
import collections
import csv
import io
import random
import socket
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from typing import Any, Callable, Dict, List, Optional

from liutang.core.errors import PipelineError
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
    def __init__(
        self,
        operations: List[PipelineOp],
        parallelism: int = 1,
        concurrency: str = "thread",
        max_workers: Optional[int] = None,
    ):
        self._operations = operations
        self._parallelism = parallelism
        self._concurrency = concurrency
        self._max_workers = max_workers or max(1, parallelism * 2)
        self._runtime_context = RuntimeContext()
        self._timer_service = TimerService()
        self._runtime_context.timer_service = self._timer_service

    def run_batch(self, data: List[Any]) -> List[Any]:
        result: List[Any] = list(data)
        for op in self._operations:
            result = self._apply_op(op, result)
        return result

    def run_streaming(
        self,
        source_queue: queue.Queue,
        sink_callback: Optional[Callable[[Any], None]] = None,
        stop_event: Optional[threading.Event] = None,
        batch_size: int = 100,
        batch_timeout: float = 1.0,
        watermark_strategy: Optional[WatermarkStrategy] = None,
    ) -> None:
        if stop_event is None:
            stop_event = threading.Event()
        buffer: List[Any] = []
        watermark_tracker = None
        if watermark_strategy:
            from liutang.engine.watermark import WatermarkTracker
            watermark_tracker = WatermarkTracker(watermark_strategy)
        while not stop_event.is_set():
            try:
                item = source_queue.get(timeout=0.1)
                buffer.append(item)
                if watermark_tracker:
                    ts = self._extract_timestamp(item, watermark_strategy)
                    if ts is not None:
                        watermark_tracker.on_event(ts)
                if len(buffer) >= batch_size:
                    self._process_batch_and_emit(buffer, sink_callback)
                    buffer = []
            except queue.Empty:
                if buffer:
                    self._process_batch_and_emit(buffer, sink_callback)
                    buffer = []
        if buffer:
            self._process_batch_and_emit(buffer, sink_callback)

    def _process_batch_and_emit(self, batch: List[Any], callback: Optional[Callable[[Any], None]]) -> None:
        results = self.run_batch(batch)
        if callback:
            for r in results:
                callback(r)

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
            return [sum(self._extract_field(x, field_val) for x in data)] if data else [0]
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
            wm = self._runtime_context.timer_service.current_watermark() if self._runtime_context.timer_service else None
            if wm is not None:
                for cb in self._timer_service.fire_event_time_timers(wm.timestamp):
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
                futures = [pool.submit(lambda c: [op.func(x) for x in c], chunk) for chunk in chunks]
            elif op.op_type == PipelineOp.FLAT_MAP:
                futures = [pool.submit(lambda c: [y for x in c for y in op.func(x)], chunk) for chunk in chunks]
            elif op.op_type == PipelineOp.FILTER:
                futures = [pool.submit(lambda c: [x for x in c if op.func(x)], chunk) for chunk in chunks]
            elif op.op_type == PipelineOp.PROCESS:
                if isinstance(op.func, ProcessFunction):
                    def _process_chunk_process(pf, c):
                        results = []
                        for x in c:
                            r = pf.process(x)
                            if r is not None:
                                results.append(r)
                        return results
                    futures = [pool.submit(_process_chunk_process, op.func, chunk) for chunk in chunks]
                else:
                    def _process_chunk_func(fn, c):
                        results = []
                        for x in c:
                            r = fn(x)
                            if r is not None:
                                if isinstance(r, (list, tuple)):
                                    results.extend(r)
                                else:
                                    results.append(r)
                        return results
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
    def _extract_timestamp(record: Any, strategy: WatermarkStrategy) -> Optional[float]:
        if strategy.time_field is None:
            return None
        if isinstance(record, dict):
            return record.get(strategy.time_field)
        if isinstance(record, (list, tuple)):
            return None
        return getattr(record, strategy.time_field, None)

    def _apply_window_op(self, op: PipelineOp, data: List[Any]) -> List[Any]:
        window = op.kwargs.get("window")
        if window is None:
            return data
        time_field = window.time_field
        if time_field and data:
            if window.kind == WindowKind.TUMBLING:
                return self._tumbling_window(op, data, window.size, time_field)
            elif window.kind == WindowKind.SLIDING:
                return self._sliding_window(op, data, window.size, window.slide, time_field)
            elif window.kind == WindowKind.SESSION:
                return self._session_window(op, data, window.gap, time_field)
            elif window.kind == WindowKind.OVER:
                return self._over_window(op, data, time_field)
            elif window.kind == WindowKind.GLOBAL:
                return self._apply_window_agg(op, data)
        if window.kind == WindowKind.GLOBAL or time_field is None:
            return self._apply_window_agg(op, data)
        return self._apply_window_agg(op, data)

    def _tumbling_window(self, op: PipelineOp, data: List[Any], size: Any, time_field: str) -> List[Any]:
        if not data:
            return []
        windows: Dict[int, List[Any]] = collections.defaultdict(list)
        size_val = size if isinstance(size, (int, float)) else 1.0
        for record in data:
            ts = self._extract_timestamp(record, None) or self._extract_field(record, time_field)
            if ts is not None:
                window_key = int(float(ts) / size_val)
                windows[window_key].append(record)
            else:
                windows[0].append(record)
        results = []
        for _, window_data in sorted(windows.items()):
            results.extend(self._apply_window_agg(op, window_data))
        return results

    def _sliding_window(self, op: PipelineOp, data: List[Any], size: Any, slide: Any, time_field: str) -> List[Any]:
        if not data:
            return []
        size_val = size if isinstance(size, (int, float)) else 1.0
        slide_val = slide if isinstance(slide, (int, float)) else 1.0
        windows: Dict[int, List[Any]] = collections.defaultdict(list)
        for record in data:
            ts = self._extract_field(record, time_field) if time_field else None
            if ts is not None:
                end = int(float(ts) / slide_val) + 1
                start = max(0, int((float(ts) - size_val) / slide_val))
                for i in range(start, end + 1):
                    windows[i].append(record)
            else:
                windows[0].append(record)
        results = []
        for _, window_data in sorted(windows.items()):
            results.extend(self._apply_window_agg(op, window_data))
        return results

    def _session_window(self, op: PipelineOp, data: List[Any], gap: Any, time_field: str) -> List[Any]:
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