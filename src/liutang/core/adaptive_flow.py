from __future__ import annotations

import threading
import time
from typing import Any, Callable, Dict, List, Optional

from liutang.core.flow import Flow
from liutang.core.stream import Stream, RuntimeMode, DeliveryMode, ArchitectureMode
from liutang.core.granularity import (
    GranularityLevel,
    GranularityPolicy,
    GranularityController,
    GranularityMetrics,
)
from liutang.core.connector import CollectionSource, CallbackSink, CollectSink
from liutang.core.eventlog import EventLog
from liutang.core.errors import PipelineError


class AdaptiveFlow:
    def __init__(
        self,
        name: str = "liutang-adaptive",
        stream_fn: Optional[Callable[[Flow], Stream]] = None,
        controller: Optional[GranularityController] = None,
        policy: GranularityPolicy = GranularityPolicy.BALANCED,
        initial_granularity: GranularityLevel = GranularityLevel.MEDIUM,
        min_granularity: GranularityLevel = GranularityLevel.MICRO,
        max_granularity: GranularityLevel = GranularityLevel.MACRO,
        event_log_path: Optional[str] = None,
        parallelism: int = 1,
        delivery_mode: DeliveryMode = DeliveryMode.AT_LEAST_ONCE,
        max_retries: int = 3,
    ):
        self._name = name
        self._stream_fn = stream_fn
        self._controller = controller or GranularityController(
            initial_level=initial_granularity,
            policy=policy,
            min_level=min_granularity,
            max_level=max_granularity,
        )
        self._event_log_path = event_log_path
        self._parallelism = parallelism
        self._delivery_mode = delivery_mode
        self._max_retries = max_retries
        self._result_sink: Optional[CollectSink] = None
        self._execution_history: List[Dict[str, Any]] = []

    @property
    def controller(self) -> GranularityController:
        return self._controller

    @property
    def granularity(self) -> GranularityLevel:
        return self._controller.level

    @property
    def event_log(self) -> Optional[EventLog]:
        return self._event_log if hasattr(self, '_event_log') else None

    def set_granularity(self, level: GranularityLevel) -> "AdaptiveFlow":
        self._controller.set_level(level)
        return self

    def set_policy(self, policy: GranularityPolicy) -> "AdaptiveFlow":
        self._controller._policy = policy
        return self

    def on_granularity_change(
        self, callback: Callable[[GranularityLevel, GranularityLevel], None]
    ) -> "AdaptiveFlow":
        self._controller._on_change = callback
        return self

    def execute(self) -> Dict[str, Any]:
        if not self._stream_fn:
            raise PipelineError("No stream function defined for AdaptiveFlow")
        flow = Flow(
            name=self._name,
            mode=RuntimeMode.STREAMING,
            parallelism=self._parallelism,
            delivery_mode=self._delivery_mode,
            max_retries=self._max_retries,
            architecture=ArchitectureMode.ADAPTIVE,
        )
        flow.configure("granularity_controller", self._controller)
        stream = self._stream_fn(flow)
        self._result_sink = stream.collect()
        handles = flow.execute()
        entry = {
            "time": time.monotonic(),
            "granularity": self._controller.level.value,
            "batch_size": self._controller.batch_size,
            "adjustments": self._controller.adjustments_count,
        }
        self._execution_history.append(entry)
        return {
            "architecture": "adaptive",
            "granularity": self._controller.level.value,
            "handles": handles,
            "controller": self._controller,
            "results": self._result_sink.results if self._result_sink else [],
        }

    def execute_batch_like(self) -> Dict[str, Any]:
        self._controller.coerce_to_batch()
        return self.execute()

    def execute_stream_like(self) -> Dict[str, Any]:
        self._controller.coerce_to_streaming()
        return self.execute()

    def execute_at_granularity(self, level: GranularityLevel) -> Dict[str, Any]:
        self._controller.set_level(level)
        return self.execute()

    def explain(self) -> str:
        lines = [
            f"AdaptiveFlow: {self._name}",
            f"  Current Granularity: {self._controller.level.value}",
            f"  Policy: {self._controller.policy.value}",
            f"  Batch Size: {self._controller.batch_size}",
            f"  Batch Timeout: {self._controller.batch_timeout}s",
            f"  Range: [{self._controller._min_level.value}, {self._controller._max_level.value}]",
            f"  Adjustments: {self._controller.adjustments_count}",
        ]
        return "\n".join(lines)