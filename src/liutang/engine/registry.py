from __future__ import annotations

from typing import Any, Dict, List, Optional


_ENGINE_REGISTRY: Dict[str, str] = {
    "local": "liutang.engine.local.executor.LocalExecutor",
    "flink": "liutang.engine.flink.executor.FlinkExecutor",
    "spark": "liutang.engine.spark.executor.SparkExecutor",
}


def get_executor(engine: str) -> Any:
    if engine not in _ENGINE_REGISTRY:
        from liutang.core.errors import EngineNotAvailableError
        raise EngineNotAvailableError(engine, f"Unknown engine. Choose from: {list(_ENGINE_REGISTRY.keys())}")
    module_path, _, class_name = _ENGINE_REGISTRY[engine].rpartition(".")
    import importlib
    try:
        mod = importlib.import_module(module_path)
        return getattr(mod, class_name)
    except ImportError as exc:
        from liutang.core.errors import EngineNotAvailableError
        raise EngineNotAvailableError(engine, str(exc))
    except AttributeError as exc:
        from liutang.core.errors import EngineNotAvailableError
        raise EngineNotAvailableError(engine, f"Executor class not found: {exc}")


def list_engines() -> List[str]:
    return list(_ENGINE_REGISTRY.keys())


def is_engine_available(engine: str) -> bool:
    if engine not in _ENGINE_REGISTRY:
        return False
    try:
        get_executor(engine)
        return True
    except Exception:
        return False