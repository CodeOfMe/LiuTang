from __future__ import annotations

import queue
import threading
import time
import traceback
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from typing import Any, Callable, Dict, List, Optional

from liutang.core.errors import PipelineError


class PipelineOp:
    MAP = "map"
    FLAT_MAP = "flat_map"
    FILTER = "filter"
    KEY_BY = "key_by"
    REDUCE = "reduce"
    SUM = "sum"
    COUNT = "count"
    PROCESS = "process"
    WINDOW_AGGREGATE = "window_aggregate"
    WINDOW_SUM = "window_sum"
    WINDOW_COUNT = "window_count"

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
        self._max_workers = max_workers or (parallelism * 2)

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
    ) -> None:
        if stop_event is None:
            stop_event = threading.Event()
        buffer: List[Any] = []
        while not stop_event.is_set():
            try:
                item = source_queue.get(timeout=0.1)
                buffer.append(item)
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
        if self._parallelism <= 1 or op.op_type in (PipelineOp.KEY_BY, PipelineOp.REDUCE,
                                                      PipelineOp.SUM, PipelineOp.COUNT,
                                                      PipelineOp.WINDOW_AGGREGATE,
                                                      PipelineOp.WINDOW_SUM, PipelineOp.WINDOW_COUNT):
            return self._apply_op_sequential(op, data)
        if op.op_type in (PipelineOp.MAP, PipelineOp.FLAT_MAP, PipelineOp.FILTER, PipelineOp.PROCESS):
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
            grouped: Dict[Any, List[Any]] = defaultdict(list)
            for x in data:
                key = op.func(x) if callable(op.func) else (x[op.func] if isinstance(x, dict) else getattr(x, op.func, x))
                grouped[key].append(x)
            return [(k, v) for k, v in grouped.items()]
        elif op.op_type == PipelineOp.REDUCE:
            if not data:
                return []
            result = data[0]
            for x in data[1:]:
                result = op.func(result, x)
            return [result]
        elif op.op_type == PipelineOp.SUM:
            field = op.kwargs.get("field", 0)
            if not data:
                return [0]
            values = [self._extract_field(x, field) for x in data]
            return [sum(values)]
        elif op.op_type == PipelineOp.COUNT:
            return [len(data)]
        elif op.op_type in (PipelineOp.WINDOW_AGGREGATE, PipelineOp.WINDOW_SUM, PipelineOp.WINDOW_COUNT):
            return self._apply_window_op(op, data)
        else:
            raise PipelineError(f"Unknown operation type: {op.op_type}")

    def _apply_op_parallel(self, op: PipelineOp, data: List[Any]) -> List[Any]:
        ExecutorCls = ThreadPoolExecutor if self._concurrency == "thread" else ProcessPoolExecutor
        results: List[Any] = []
        chunk_size = max(1, len(data) // self._max_workers)
        chunks = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)] if chunk_size > 0 else [data]
        with ExecutorCls(max_workers=self._max_workers) as pool:
            futures = []
            for chunk in chunks:
                if op.op_type == PipelineOp.MAP:
                    futures.append(pool.submit(self._map_chunk, op.func, chunk))
                elif op.op_type == PipelineOp.FLAT_MAP:
                    futures.append(pool.submit(self._flat_map_chunk, op.func, chunk))
                elif op.op_type == PipelineOp.FILTER:
                    futures.append(pool.submit(self._filter_chunk, op.func, chunk))
                elif op.op_type == PipelineOp.PROCESS:
                    futures.append(pool.submit(self._process_chunk, op.func, chunk))
            for future in as_completed(futures):
                results.extend(future.result())
        return results

    @staticmethod
    def _map_chunk(func: Callable, chunk: List[Any]) -> List[Any]:
        return [func(x) for x in chunk]

    @staticmethod
    def _flat_map_chunk(func: Callable, chunk: List[Any]) -> List[Any]:
        result = []
        for x in chunk:
            result.extend(func(x))
        return result

    @staticmethod
    def _filter_chunk(func: Callable, chunk: List[Any]) -> List[Any]:
        return [x for x in chunk if func(x)]

    @staticmethod
    def _process_chunk(func: Callable, chunk: List[Any]) -> List[Any]:
        result = []
        for x in chunk:
            out = func(x)
            if out is not None:
                if isinstance(out, (list, tuple)):
                    result.extend(out)
                else:
                    result.append(out)
        return result

    @staticmethod
    def _extract_field(record: Any, field: Any) -> Any:
        if isinstance(record, dict):
            return record.get(field, 0) if isinstance(field, str) else list(record.values())[field]
        if isinstance(record, (list, tuple)):
            return record[field]
        return getattr(record, str(field), 0)

    def _apply_window_op(self, op: PipelineOp, data: List[Any]) -> List[Any]:
        window = op.kwargs.get("window")
        if window is None:
            if op.op_type == PipelineOp.WINDOW_COUNT:
                return [len(data)]
            elif op.op_type == PipelineOp.WINDOW_SUM:
                field = op.kwargs.get("field", 0)
                return [sum(self._extract_field(x, field) for x in data)]
            elif op.op_type == PipelineOp.WINDOW_AGGREGATE and op.func:
                return [op.func(data)]
        time_field = window.time_field if window else None
        if time_field and data:
            from liutang.core.window import WindowKind
            if window.kind == WindowKind.TUMBLING:
                return self._tumbling_window(op, data, window.size, time_field)
            elif window.kind == WindowKind.SLIDING:
                return self._sliding_window(op, data, window.size, window.slide, time_field)
            elif window.kind == WindowKind.SESSION:
                return self._session_window(op, data, window.gap, time_field)
        if op.op_type == PipelineOp.WINDOW_COUNT:
            return [len(data)]
        elif op.op_type == PipelineOp.WINDOW_SUM:
            field = op.kwargs.get("field", 0)
            return [sum(self._extract_field(x, field) for x in data)]
        elif op.op_type == PipelineOp.WINDOW_AGGREGATE and op.func:
            return [op.func(data)]
        raise PipelineError(f"Cannot apply window operation: {op}")

    def _tumbling_window(self, op: PipelineOp, data: List[Any], size: Any, time_field: str) -> List[Any]:
        if not data:
            return []
        windows: Dict[Any, List[Any]] = defaultdict(list)
        for record in data:
            ts = self._get_timestamp(record, time_field)
            if ts is not None:
                window_key = int(ts / size) if isinstance(size, (int, float)) else ts
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
        windows: Dict[Any, List[Any]] = defaultdict(list)
        for record in data:
            ts = self._get_timestamp(record, time_field)
            if ts is not None:
                for i in range(max(0, int((ts - size) / slide)) + 1, int(ts / slide) + 1):
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
        sorted_data = sorted(data, key=lambda x: self._get_timestamp(x, time_field) or 0)
        sessions: List[List[Any]] = []
        current_session: List[Any] = [sorted_data[0]]
        for record in sorted_data[1:]:
            prev_ts = self._get_timestamp(current_session[-1], time_field) or 0
            curr_ts = self._get_timestamp(record, time_field) or 0
            if curr_ts - prev_ts <= gap:
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

    def _apply_window_agg(self, op: PipelineOp, window_data: List[Any]) -> List[Any]:
        if op.op_type == PipelineOp.WINDOW_COUNT:
            return [len(window_data)]
        elif op.op_type == PipelineOp.WINDOW_SUM:
            field = op.kwargs.get("field", 0)
            return [sum(self._extract_field(x, field) for x in window_data)]
        elif op.op_type == PipelineOp.WINDOW_AGGREGATE:
            return [op.func(window_data)]
        return window_data

    @staticmethod
    def _get_timestamp(record: Any, time_field: str) -> Optional[float]:
        if isinstance(record, dict):
            return record.get(time_field)
        if isinstance(record, (list, tuple)):
            return None
        return getattr(record, time_field, None)