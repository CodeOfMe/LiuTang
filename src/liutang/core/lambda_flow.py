from __future__ import annotations

import threading
import time
from typing import Any, Callable, Dict, List, Optional

from liutang.core.flow import Flow
from liutang.core.stream import Stream, RuntimeMode, DeliveryMode, ArchitectureMode
from liutang.core.schema import Schema
from liutang.core.connector import SourceConnector, CollectSink, CallbackSink
from liutang.core.eventlog import EventLog
from liutang.core.serving import ServingView, MergeView
from liutang.core.errors import PipelineError


class LambdaFlow:
    def __init__(
        self,
        name: str = "liutang-lambda",
        batch_layer_fn: Optional[Callable[[Flow], Stream]] = None,
        speed_layer_fn: Optional[Callable[[Flow], Stream]] = None,
        key_fn: Optional[Callable] = None,
        merge_fn: Optional[Callable] = None,
        event_log_path: Optional[str] = None,
        parallelism: int = 1,
        delivery_mode: DeliveryMode = DeliveryMode.AT_LEAST_ONCE,
    ):
        self._name = name
        self._batch_layer_fn = batch_layer_fn
        self._speed_layer_fn = speed_layer_fn
        self._key_fn = key_fn or (lambda x: x if not isinstance(x, (list, tuple)) else x[0])
        self._merge_fn = merge_fn or MergeView.latest
        self._parallelism = parallelism
        self._delivery_mode = delivery_mode
        self._event_log = EventLog(event_log_path) if event_log_path else None
        self._serving = ServingView(key_fn=self._key_fn, merge_fn=self._merge_fn)
        self._batch_interval: float = 30.0
        self._speed_callbacks: List[Callable] = []

    def batch_interval(self, seconds: float) -> "LambdaFlow":
        self._batch_interval = seconds
        return self

    def on_speed_result(self, callback: Callable) -> "LambdaFlow":
        self._speed_callbacks.append(callback)
        return self

    def execute(self) -> Dict[str, Any]:
        batch_results = None
        speed_results = None
        if self._batch_layer_fn:
            batch_flow = Flow(name=f"{self._name}_batch", mode=RuntimeMode.BATCH,
                              parallelism=self._parallelism, delivery_mode=self._delivery_mode)
            batch_stream = self._batch_layer_fn(batch_flow)
            batch_sink = batch_stream.collect()
            batch_flow.execute()
            batch_results = batch_sink.results
            self._serving.update_batch(batch_results)
        if self._speed_layer_fn:
            speed_flow = Flow(name=f"{self._name}_speed", mode=RuntimeMode.STREAMING,
                              parallelism=self._parallelism, delivery_mode=self._delivery_mode)
            speed_stream = self._speed_layer_fn(speed_flow)
            for cb in self._speed_callbacks:
                speed_stream.sink_to(CallbackSink(func=cb))
            speed_sink = speed_stream.collect()
            handles = speed_flow.execute()
            speed_results = speed_sink.results
            self._serving.update_speed(speed_results)
            return {
                "batch_results": batch_results or [],
                "speed_results": speed_results or [],
                "serving": self._serving,
                "handles": handles,
            }
        return {
            "batch_results": batch_results or [],
            "speed_results": speed_results or [],
            "serving": self._serving,
        }

    def execute_batch_only(self) -> Dict[str, Any]:
        if not self._batch_layer_fn:
            raise PipelineError("No batch layer function defined")
        batch_flow = Flow(name=f"{self._name}_batch", mode=RuntimeMode.BATCH,
                          parallelism=self._parallelism, delivery_mode=self._delivery_mode)
        batch_stream = self._batch_layer_fn(batch_flow)
        batch_sink = batch_stream.collect()
        batch_flow.execute()
        self._serving.update_batch(batch_sink.results)
        return {"batch_results": batch_sink.results, "serving": self._serving}

    def query(self, key: Any = None) -> Any:
        return self._serving.query(key)

    def query_all(self) -> List[Any]:
        return self._serving.query_all()


class KappaFlow:
    def __init__(
        self,
        name: str = "liutang-kappa",
        stream_fn: Optional[Callable[[Flow], Stream]] = None,
        event_log_path: Optional[str] = None,
        key_fn: Optional[Callable] = None,
        parallelism: int = 1,
        delivery_mode: DeliveryMode = DeliveryMode.EXACTLY_ONCE,
    ):
        self._name = name
        self._stream_fn = stream_fn
        self._key_fn = key_fn
        self._parallelism = parallelism
        self._delivery_mode = delivery_mode
        self._event_log = EventLog(event_log_path) if event_log_path else None
        self._serving = ServingView(key_fn=key_fn) if key_fn else None

    def execute(self) -> Dict[str, Any]:
        if not self._stream_fn:
            raise PipelineError("No stream function defined")
        flow = Flow(name=self._name, mode=RuntimeMode.STREAMING,
                    parallelism=self._parallelism, delivery_mode=self._delivery_mode)
        stream = self._stream_fn(flow)
        if self._event_log:
            original_sink = stream.collect()
            handles = flow.execute()
            return {"results": original_sink.results, "handles": handles, "event_log": self._event_log, "serving": self._serving}

        sink = stream.collect()
        handles = flow.execute()
        return {"results": sink.results, "handles": handles, "serving": self._serving}

    def replay(self, offset: int = 0, limit: Optional[int] = None) -> List[Any]:
        if self._event_log is None:
            raise PipelineError("No event log configured for replay")
        return self._event_log.read(offset, limit)

    def replay_to_stream(self, offset: int = 0, limit: Optional[int] = None,
                          transform_fn: Optional[Callable] = None) -> Dict[str, Any]:
        if self._event_log is None:
            raise PipelineError("No event log configured for replay")
        records = self._event_log.read(offset, limit)
        if transform_fn:
            records = [transform_fn(r) for r in records]
        replay_flow = Flow(name=f"{self._name}_replay", mode=RuntimeMode.BATCH,
                           parallelism=self._parallelism, delivery_mode=self._delivery_mode)
        if self._stream_fn:
            stream = self._stream_fn(replay_flow)
            stream_flat = replay_flow.from_collection(records)
            sink = stream_flat.collect()
            replay_flow.execute()
            return {"results": sink.results}
        replay_stream = replay_flow.from_collection(records)
        sink = replay_stream.collect()
        replay_flow.execute()
        return {"results": sink.results}

    def append_to_log(self, record: Any) -> int:
        if self._event_log is None:
            raise PipelineError("No event log configured")
        return self._event_log.append(record)

    @property
    def event_log(self) -> Optional[EventLog]:
        return self._event_log