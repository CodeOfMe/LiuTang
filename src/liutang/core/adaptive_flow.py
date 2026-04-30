from __future__ import annotations

import time
from typing import Any, Callable, Dict, List, Optional

from liutang.core.flow import Flow
from liutang.core.stream import Stream, RuntimeMode, DeliveryMode, ArchitectureMode
from liutang.core.viscosity import Viscosity, ViscosityPolicy, ViscosityController, FlowMetrics
from liutang.core.connector import CollectSink
from liutang.core.errors import PipelineError
from liutang.core.granularity import (
    GranularityLevel,
    GranularityPolicy,
    GranularityController as _GC,
    _VISC_TO_GRAN,
    _GRAN_POLICY_TO_VISC,
    _VISC_POLICY_TO_GRAN,
)


class _ControllerProxy:
    def __init__(self, vc: ViscosityController):
        object.__setattr__(self, "_vc", vc)

    def __getattr__(self, name):
        attr = getattr(object.__getattribute__(self, "_vc"), name)
        if name == "policy":
            return _VISC_POLICY_TO_GRAN.get(attr, attr)
        if name == "level":
            gran = _VISC_TO_GRAN.get(attr)
            return GranularityLevel(gran) if gran else attr
        return attr

    def __setattr__(self, name, value):
        if name == "policy" and isinstance(value, GranularityPolicy):
            value = _GRAN_POLICY_TO_VISC[value]
        setattr(self._vc, name, value)


class AdaptiveFlow:
    def __init__(
        self,
        name: str = "liutang-adaptive",
        stream_fn: Optional[Callable[[Flow], Stream]] = None,
        controller: Optional[ViscosityController] = None,
        viscosity: Optional[Viscosity] = None,
        policy: Optional[ViscosityPolicy] = None,
        initial_granularity: Optional[GranularityLevel] = None,
        min_viscosity: Optional[Viscosity] = None,
        max_viscosity: Optional[Viscosity] = None,
        min_granularity: Optional[GranularityLevel] = None,
        max_granularity: Optional[GranularityLevel] = None,
        event_log_path: Optional[str] = None,
        parallelism: int = 1,
        delivery_mode: DeliveryMode = DeliveryMode.AT_LEAST_ONCE,
        max_retries: int = 3,
    ):
        self._name = name
        self._stream_fn = stream_fn
        if controller is not None:
            self._controller = controller
        elif viscosity is not None or policy is not None:
            self._controller = ViscosityController(
                initial=viscosity or Viscosity.HONEYED,
                policy=policy or ViscosityPolicy.BALANCED,
                min_viscosity=min_viscosity or Viscosity.VOLATILE,
                max_viscosity=max_viscosity or Viscosity.FROZEN,
            )
        elif initial_granularity is not None:
            from liutang.core.granularity import _GRAN_TO_VISC, _GRAN_POLICY_TO_VISC
            min_v = (min_granularity or GranularityLevel.MICRO).to_viscosity()
            max_v = (max_granularity or GranularityLevel.MACRO).to_viscosity()
            self._controller = ViscosityController(
                initial=initial_granularity.to_viscosity(),
                policy=ViscosityPolicy.BALANCED,
                min_viscosity=min_v,
                max_viscosity=max_v,
            )
        else:
            self._controller = ViscosityController()
        self._event_log_path = event_log_path
        self._parallelism = parallelism
        self._delivery_mode = delivery_mode
        self._max_retries = max_retries
        self._result_sink: Optional[CollectSink] = None
        self._execution_history: List[Dict[str, Any]] = []
        self._provided_controller = controller

    @property
    def controller(self):
        return _ControllerProxy(self._controller)

    @property
    def viscosity(self) -> Viscosity:
        return self._controller.viscosity

    @property
    def granularity(self):
        return self._controller.level

    @property
    def event_log(self):
        return self._event_log if hasattr(self, '_event_log') else None

    def set_viscosity(self, viscosity: Viscosity) -> "AdaptiveFlow":
        self._controller.set_viscosity(viscosity)
        return self

    def set_granularity(self, level) -> "AdaptiveFlow":
        if isinstance(level, GranularityLevel):
            self._controller.set_viscosity(level.to_viscosity())
        else:
            self._controller.set_viscosity(level)
        return self

    def set_policy(self, policy) -> "AdaptiveFlow":
        if isinstance(policy, GranularityPolicy):
            from liutang.core.granularity import _GRAN_POLICY_TO_VISC
            self._controller._policy = _GRAN_POLICY_TO_VISC[policy]
        else:
            self._controller._policy = policy
        return self

    def on_viscosity_change(
        self, callback: Callable[[Viscosity, Viscosity], None]
    ) -> "AdaptiveFlow":
        self._controller._on_change = callback
        return self

    def on_granularity_change(self, callback) -> "AdaptiveFlow":
        self._controller._on_change = callback
        return self

    def thaw(self) -> "AdaptiveFlow":
        self._controller.thaw()
        return self

    def freeze(self) -> "AdaptiveFlow":
        self._controller.freeze()
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
        flow.configure("viscosity_controller", self._controller)
        flow.configure("granularity_controller", self._controller)
        stream = self._stream_fn(flow)
        self._result_sink = stream.collect()
        handles = flow.execute()
        ctrl = self._controller
        is_vc = isinstance(ctrl, ViscosityController)
        viscosity_val = ctrl.viscosity.value if is_vc else ctrl.level.value
        gran_value = _VISC_TO_GRAN.get(ctrl.viscosity, ctrl.viscosity.value) if is_vc else ctrl.level.value
        eta_val = ctrl.eta if is_vc else 0.5
        entry = {
            "time": time.monotonic(),
            "viscosity": viscosity_val,
            "eta": eta_val,
            "batch_size": ctrl.batch_size,
            "adjustments": ctrl.adjustments_count,
        }
        self._execution_history.append(entry)
        return {
            "architecture": "adaptive",
            "viscosity": viscosity_val,
            "eta": eta_val,
            "granularity": gran_value,
            "handles": handles,
            "controller": ctrl,
            "results": self._result_sink.results if self._result_sink else [],
        }

    def execute_batch_like(self) -> Dict[str, Any]:
        self._controller.freeze()
        return self.execute()

    def execute_stream_like(self) -> Dict[str, Any]:
        self._controller.thaw()
        return self.execute()

    def execute_at_viscosity(self, viscosity: Viscosity) -> Dict[str, Any]:
        self._controller.set_viscosity(viscosity)
        return self.execute()

    def execute_at_granularity(self, level) -> Dict[str, Any]:
        if isinstance(level, GranularityLevel):
            self._controller.set_viscosity(level.to_viscosity())
        else:
            self._controller.set_viscosity(level)
        return self.execute()

    def explain(self) -> str:
        ctrl = self._controller
        is_vc = isinstance(ctrl, ViscosityController)
        if is_vc:
            visc = ctrl.viscosity
            gran = _VISC_TO_GRAN.get(visc, visc.value)
            policy = _VISC_POLICY_TO_GRAN.get(ctrl.policy, ctrl.policy)
            eta = ctrl.eta
        else:
            gran = ctrl.level.value
            policy = ctrl.policy
            eta = 0.5
        lines = [
            f"AdaptiveFlow: {self._name}",
            f"  Granularity: {gran} (η={eta:.2f})",
            f"  Policy: {policy.value}",
            f"  Batch Size: {ctrl.batch_size}",
            f"  Batch Timeout: {ctrl.batch_timeout}s",
            f"  Adjustments: {ctrl.adjustments_count}",
        ]
        return "\n".join(lines)