from __future__ import annotations

from enum import Enum
from typing import Any, Optional


class WindowKind(Enum):
    TUMBLING = "tumbling"
    SLIDING = "sliding"
    SESSION = "session"
    OVER = "over"
    GLOBAL = "global"


class WindowType:
    kind: WindowKind
    size: Any
    slide: Any
    gap: Any
    time_field: Optional[str]
    allowed_lateness: float

    def __init__(
        self,
        kind: WindowKind,
        size: Any = None,
        slide: Any = None,
        gap: Any = None,
        time_field: Optional[str] = None,
        allowed_lateness: float = 0.0,
    ):
        self.kind = kind
        self.size = size
        self.slide = slide
        self.gap = gap
        self.time_field = time_field
        self.allowed_lateness = allowed_lateness

    @staticmethod
    def tumbling(size: Any, time_field: Optional[str] = None, allowed_lateness: float = 0.0) -> "WindowType":
        return WindowType(WindowKind.TUMBLING, size=size, time_field=time_field, allowed_lateness=allowed_lateness)

    @staticmethod
    def sliding(size: Any, slide: Any, time_field: Optional[str] = None, allowed_lateness: float = 0.0) -> "WindowType":
        return WindowType(WindowKind.SLIDING, size=size, slide=slide, time_field=time_field, allowed_lateness=allowed_lateness)

    @staticmethod
    def session(gap: Any, time_field: Optional[str] = None) -> "WindowType":
        return WindowType(WindowKind.SESSION, gap=gap, time_field=time_field)

    @staticmethod
    def over(time_field: Optional[str] = None) -> "WindowType":
        return WindowType(WindowKind.OVER, time_field=time_field)

    @staticmethod
    def global_window() -> "WindowType":
        return WindowType(WindowKind.GLOBAL)

    def __repr__(self) -> str:
        parts = [f"{self.kind.value}"]
        if self.size is not None:
            parts.append(f"size={self.size}")
        if self.slide is not None:
            parts.append(f"slide={self.slide}")
        if self.gap is not None:
            parts.append(f"gap={self.gap}")
        if self.time_field:
            parts.append(f"on={self.time_field}")
        if self.allowed_lateness:
            parts.append(f"lateness={self.allowed_lateness}")
        return f"WindowType({' '.join(parts)})"